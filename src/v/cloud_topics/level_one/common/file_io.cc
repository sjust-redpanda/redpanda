/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_io.h"

#include "bytes/iostream.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage_clients/client.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_utils.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>

#include <algorithm>
#include <memory>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

namespace {

class staging_file_impl : public staging_file {
public:
    explicit staging_file_impl(std::filesystem::path path)
      : _path(std::move(path)) {}

    ss::future<size_t> size() override { return ss::file_size(_path.native()); }
    ss::future<ss::output_stream<char>> output_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(),
          ss::open_flags::rw | ss::open_flags::truncate
            | ss::open_flags::create);
        ss::file_output_stream_options options{};
        // The read buffer size also makes sense as the write buffer here
        // (default 128KiB).
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        // Defaults to 1, which is reasonable for write-behind as well.
        options.write_behind
          = config::shard_local_cfg().storage_read_readahead_count();
        co_return co_await ss::make_file_output_stream(
          std::move(file), std::move(options));
    }
    ss::future<> remove() override { return ss::remove_file(_path.native()); }
    ss::future<ss::input_stream<char>> input_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(), ss::open_flags::ro);
        ss::file_input_stream_options options{};
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        options.read_ahead
          = config::shard_local_cfg().storage_read_readahead_count();
        co_return ss::make_file_input_stream(
          std::move(file), std::move(options));
    }

private:
    std::filesystem::path _path;
};

// TODO: deduplicate, expose from cloud storage
struct one_time_stream_provider : public stream_provider {
    explicit one_time_stream_provider(ss::input_stream<char> s)
      : _st(std::move(s)) {}

    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(_st, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (_st.has_value()) {
            return _st->close().then([this] { _st = std::nullopt; });
        }
        return ss::now();
    }
    std::optional<ss::input_stream<char>> _st;
};

class l1_footer_index final : public object_index {
public:
    explicit l1_footer_index(footer f)
      : _footer(std::move(f)) {}

    std::optional<seek_result> seek_to_offset(
      model::topic_id_partition tidp, kafka::offset offset) const override {
        auto r = _footer.file_position_before_kafka_offset(tidp, offset);
        if (r == footer::npos) {
            return std::nullopt;
        }
        return seek_result{.file_position = r.file_position, .length = r.length};
    }

    std::optional<seek_result> seek_to_timestamp(
      model::topic_id_partition tidp, model::timestamp ts) const override {
        auto r = _footer.file_position_before_max_timestamp(tidp, ts);
        if (r == footer::npos) {
            return std::nullopt;
        }
        return seek_result{.file_position = r.file_position, .length = r.length};
    }

private:
    footer _footer;
};

class l1_native_object_handle final : public object_handle {
public:
    l1_native_object_handle(object_id oid, footer f, file_io* io)
      : _oid(oid)
      , _index(std::move(f))
      , _io(io) {}

    const object_index& index() const override { return _index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override {
        object_extent extent{
          .id = _oid,
          .position = seek.file_position,
          .size = seek.length,
        };
        auto stream_result = co_await _io->read_object(extent, as);
        if (!stream_result.has_value()) {
            co_return std::unexpected(stream_result.error());
        }
        co_return object_reader::create(std::move(stream_result).value());
    }

private:
    object_id _oid;
    l1_footer_index _index;
    file_io* _io;
};

// Implements object_index over a tiered-storage segment's offset_index.
//
// find_kaf_offset/find_timestamp are non-const on offset_index (they flush a
// write buffer internally), so _index is mutable.
class ts_segment_index final : public object_index {
public:
    ts_segment_index(
      cloud_storage::offset_index index,
      kafka::offset base_kafka_offset,
      kafka::offset max_kafka_offset,
      model::offset_delta base_delta,
      size_t segment_size)
      : _index(std::move(index))
      , _base_kafka_offset(base_kafka_offset)
      , _max_kafka_offset(max_kafka_offset)
      , _base_delta(base_delta)
      , _segment_size(segment_size) {}

    std::optional<seek_result>
    seek_to_offset(model::topic_id_partition, kafka::offset target)
      const override {
        if (target > _max_kafka_offset) {
            return std::nullopt;
        }
        // Offsets below _base_kafka_offset return {file_pos=0, full segment},
        // which causes the reader to scan from the start — correct because the
        // caller is responsible for only dispatching to a segment whose range
        // includes the target.
        auto entry = _index.find_kaf_offset(target + kafka::offset{1});
        size_t file_pos = entry ? static_cast<size_t>(entry->file_pos) : 0;
        model::offset_delta delta = entry
          ? model::offset_delta{entry->rp_offset() - entry->kaf_offset()}
          : _base_delta;
        return seek_result{
          .file_position = file_pos,
          .length = _segment_size - file_pos,
          .delta = delta};
    }

    std::optional<seek_result> seek_to_timestamp(
      model::topic_id_partition, model::timestamp ts) const override {
        auto entry = _index.find_timestamp(ts);
        size_t file_pos = entry ? static_cast<size_t>(entry->file_pos) : 0;
        model::offset_delta delta = entry
          ? model::offset_delta{entry->rp_offset() - entry->kaf_offset()}
          : _base_delta;
        return seek_result{
          .file_position = file_pos,
          .length = _segment_size - file_pos,
          .delta = delta};
    }

private:
    mutable cloud_storage::offset_index _index;
    kafka::offset _base_kafka_offset;
    kafka::offset _max_kafka_offset;
    model::offset_delta _base_delta;
    size_t _segment_size;
};

// Implements object_reader over a raw Kafka-format byte stream (a TS segment).
//
// Translates log offsets to Kafka offsets by maintaining a running delta.
// Non-data batches are skipped while incrementing the delta.
class tiered_storage_object_reader final : public object_reader {
public:
    tiered_storage_object_reader(
      ss::input_stream<char> stream, model::offset_delta delta)
      : _stream(std::move(stream))
      , _running_delta(delta) {}

    ss::future<> close() override { return _stream.close(); }

    ss::future<peek_result> peek() override {
        if (!_peeked.has_value() && !_eof) {
            auto next = co_await fetch_next_translated();
            if (next.has_value()) {
                _peeked = std::move(*next);
            } else {
                _eof = true;
            }
        }
        if (_eof) {
            co_return eof{};
        }
        co_return _peeked->header();
    }

    ss::future<result> read_next() override {
        if (_peeked.has_value()) {
            auto batch = std::move(*_peeked);
            _peeked.reset();
            co_return std::move(batch);
        }
        if (_eof) {
            co_return eof{};
        }
        auto next = co_await fetch_next_translated();
        if (!next.has_value()) {
            co_return eof{};
        }
        co_return std::move(*next);
    }

private:
    // Reads the next data batch from the raw TS stream. Skips non-data batches,
    // incrementing _running_delta for each. Returns nullopt at EOF.
    ss::future<std::optional<model::record_batch>> fetch_next_translated() {
        static const auto translator_types
          = model::offset_translator_batch_types();
        for (;;) {
            auto header_buf = co_await read_iobuf_exactly(
              _stream, model::packed_record_batch_header_size);
            if (
              header_buf.size_bytes()
              < model::packed_record_batch_header_size) {
                co_return std::nullopt;
            }
            auto header = storage::batch_header_from_disk_iobuf(
              std::move(header_buf));
            auto records_size = static_cast<size_t>(header.size_bytes)
                                - model::packed_record_batch_header_size;
            auto records_buf = co_await read_iobuf_exactly(
              _stream, records_size);
            if (records_buf.size_bytes() != records_size) {
                throw std::runtime_error(fmt::format(
                  "truncated TS segment: expected {} record bytes, got {}",
                  records_size,
                  records_buf.size_bytes()));
            }
            if (std::ranges::contains(translator_types, header.type)) {
                _running_delta += static_cast<int64_t>(header.record_count);
                continue;
            }
            header.base_offset = model::offset{
              header.base_offset() - _running_delta()};
            co_return model::record_batch(
              header,
              std::move(records_buf),
              model::record_batch::tag_ctor_ng{});
        }
    }

    ss::input_stream<char> _stream;
    model::offset_delta _running_delta;
    std::optional<model::record_batch> _peeked;
    bool _eof{false};
};

// Implements object_handle for an imported tiered-storage segment.
class tiered_storage_object_handle final : public object_handle {
public:
    tiered_storage_object_handle(
      object_extent extent,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::cache* cache,
      std::unique_ptr<ts_segment_index> index)
      : _extent(std::move(extent))
      , _remote(remote)
      , _bucket(std::move(bucket))
      , _cache(cache)
      , _index(std::move(index)) {}

    const object_index& index() const override { return *_index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override {
        vassert(_extent.imported.has_value(), "missing imported info");
        auto delta = seek.delta.value_or(_extent.imported->delta_offset);
        auto stream_result = co_await download_ts_range(
          _extent.imported->ts_path, seek.file_position, seek.length, as);
        if (!stream_result.has_value()) {
            co_return std::unexpected(stream_result.error());
        }
        co_return std::make_unique<tiered_storage_object_reader>(
          std::move(*stream_result), delta);
    }

private:
    ss::future<uint64_t> save_to_cache(
      ss::input_stream<char> stream,
      cloud_io::space_reservation_guard* reservation,
      std::filesystem::path cache_key,
      uint64_t content_length) {
        co_await _cache->put(std::move(cache_key), stream, *reservation);
        co_return content_length;
    }

    ss::future<std::expected<ss::input_stream<char>, io::errc>>
    download_ts_range(
      const ss::sstring& ts_path,
      size_t offset,
      size_t size,
      ss::abort_source* as) {
        static constexpr auto timeout = 10s;
        static constexpr auto backoff = 100ms;
        retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
        lazy_abort_source las{[as] {
            return as->abort_requested()
                     ? std::make_optional("abort requested")
                     : std::nullopt;
        }};
        std::filesystem::path cache_key = fmt::format(
          "ts_{}_position_{}_size_{}.partial", ts_path, offset, size);
        while (true) {
            auto stream_fut = co_await ss::coroutine::as_future<
              std::optional<cloud_io::cache_item_stream>>(_cache->get_stream(
              cache_key,
              config::shard_local_cfg().storage_read_buffer_size(),
              config::shard_local_cfg().storage_read_readahead_count()));
            if (stream_fut.failed()) {
                auto ex = stream_fut.get_exception();
                vlog(
                  cd_log.warn,
                  "Error reading from cache for ts {}: {}",
                  ts_path,
                  ex);
                co_return std::unexpected(io::errc::file_io_error);
            }
            auto stream = stream_fut.get();
            if (stream) {
                co_return std::move(stream->body);
            }
            auto reservation_fut = co_await ss::coroutine::as_future<
              cloud_io::space_reservation_guard>(
              _cache->reserve_space(size, 1));
            if (reservation_fut.failed()) {
                auto ex = reservation_fut.get_exception();
                vlog(
                  cd_log.warn,
                  "Error reserving cache space for ts {}: {}",
                  ts_path,
                  ex);
                co_return std::unexpected(io::errc::file_io_error);
            }
            cloud_io::try_consume_stream consumer =
              [this, r = reservation_fut.get(), &cache_key](
                uint64_t content_length,
                ss::input_stream<char> s) mutable {
                  return save_to_cache(
                    std::move(s), &r, cache_key, content_length);
              };
            auto result_fut
              = co_await ss::coroutine::as_future<cloud_io::download_result>(
                _remote->download_stream(
                  cloud_io::transfer_details{
                    .bucket = _bucket,
                    .key = cloud_storage_clients::object_key{ts_path},
                    .parent_rtc = root,
                  },
                  consumer,
                  "ts_segment_download",
                  /*acquire_hydration_units=*/true,
                  cloud_storage_clients::http_byte_range{
                    offset, offset + size - 1}));
            if (result_fut.failed()) {
                auto ex = result_fut.get_exception();
                vlog(
                  cd_log.warn, "Error downloading TS segment {}: {}", ts_path, ex);
                co_return std::unexpected(io::errc::cloud_op_error);
            }
            switch (result_fut.get()) {
            case cloud_io::download_result::success:
                continue;
            case cloud_io::download_result::notfound:
                co_return std::unexpected(io::errc::cloud_missing_object);
            case cloud_io::download_result::timedout:
                co_return std::unexpected(io::errc::cloud_op_timeout);
            case cloud_io::download_result::failed:
                co_return std::unexpected(io::errc::cloud_op_error);
            }
            std::unreachable();
        }
    }

    object_extent _extent;
    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_io::cache* _cache;
    std::unique_ptr<ts_segment_index> _index;
};

} // namespace

file_io::file_io(
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::cache* cache,
  std::optional<cloud_storage_clients::bucket_name> ts_bucket)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _ts_bucket(ts_bucket.value_or(_bucket))
  , _staging_dir(std::move(staging_dir))
  , _cache(cache) {}

ss::future<std::expected<std::unique_ptr<staging_file>, io::errc>>
file_io::create_tmp_file() {
    co_return std::make_unique<staging_file_impl>(
      _staging_dir / fmt::format("{}.tmp", uuid_t::create()));
}

ss::future<std::expected<void, io::errc>>
file_io::put_object(object_id oid, staging_file* file, ss::abort_source* as) {
    auto file_size = co_await file->size();
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[as] {
        return as->abort_requested() ? std::make_optional("abort requested")
                                     : std::nullopt;
    }};
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->upload_stream(
          cloud_io::transfer_details{
            .bucket = _bucket,
            .key = object_path_factory::level_one_path(oid),
            .parent_rtc = root,
          },
          file_size,
          [this, file]() {
              return io::read_file(file).then(
                [](ss::input_stream<char> stream)
                  -> std::unique_ptr<stream_provider> {
                    return std::make_unique<one_time_stream_provider>(
                      std::move(stream));
                });
          },
          las,
          "l1_file_upload",
          std::nullopt));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error uploading file: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::upload_result::success:
        // TODO(cloud_topics): Consider preemptively putting the object in the
        // cache
        co_return std::expected<void, io::errc>{};
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::cancelled:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::upload_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

ss::future<uint64_t> file_io::save_to_cache(
  ss::input_stream<char> stream,
  cloud_io::space_reservation_guard* reservation,
  std::filesystem::path cache_key,
  uint64_t content_length) {
    co_await _cache->put(std::move(cache_key), stream, *reservation);
    co_return content_length;
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
file_io::read_object(object_extent extent, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[as] {
        return as->abort_requested() ? std::make_optional("abort requested")
                                     : std::nullopt;
    }};
    // TODO(cloud_topics): Optimize the cache such that it understands partial
    // objects? Or we assert somehow there are no overlaps (or just live with
    // them).
    // TODO(cloud_topics): If reading just a footer, we should skip the cache.
    // Maybe we need another method for that which is iobuf based?
    std::filesystem::path cache_key = fmt::format(
      "l1_{}_position_{}_size_{}.partial",
      extent.id,
      extent.position,
      extent.size);
    while (true) {
        auto stream_fut = co_await ss::coroutine::as_future<
          std::optional<cloud_io::cache_item_stream>>(_cache->get_stream(
          cache_key,
          config::shard_local_cfg().storage_read_buffer_size(),
          config::shard_local_cfg().storage_read_readahead_count()));
        if (stream_fut.failed()) {
            auto ex = stream_fut.get_exception();
            vlog(
              cd_log.warn, "Error reading from cache for {}: {}", extent, ex);
            co_return std::unexpected(io::errc::file_io_error);
        }
        auto stream = stream_fut.get();
        if (stream) {
            co_return std::move(stream->body);
        }
        // TODO(cloud_topics): reserving space should also take an abort_source
        auto reservation_fut = co_await ss::coroutine::as_future<
          cloud_io::space_reservation_guard>(
          _cache->reserve_space(extent.size, 1));
        if (reservation_fut.failed()) {
            auto ex = reservation_fut.get_exception();
            vlog(
              cd_log.warn,
              "Error reserving cache space for download of {}: {}",
              extent,
              ex);
            co_return std::unexpected(io::errc::file_io_error);
        }
        cloud_io::try_consume_stream consumer =
          [this, r = reservation_fut.get(), &cache_key](
            uint64_t content_length, ss::input_stream<char> stream) mutable {
              return save_to_cache(
                std::move(stream), &r, cache_key, content_length);
          };
        auto result_fut
          = co_await ss::coroutine::as_future<cloud_io::download_result>(
            _remote->download_stream(
              cloud_io::transfer_details{
                .bucket = _bucket,
                .key = object_path_factory::level_one_path(extent.id),
                .parent_rtc = root,
              },
              consumer,
              "l1_file_download",
              /*acquire_hydration_units=*/true,
              cloud_storage_clients::http_byte_range{
                extent.position, extent.position + extent.size - 1}));
        if (result_fut.failed()) {
            auto ex = result_fut.get_exception();
            vlog(cd_log.warn, "Error downloading object {}: {}", extent, ex);
            co_return std::unexpected(io::errc::cloud_op_error);
        }
        switch (result_fut.get()) {
        case cloud_io::download_result::success:
            continue; // Now that it's in the cache the lookup should succeed.
        case cloud_io::download_result::notfound:
            co_return std::unexpected(io::errc::cloud_missing_object);
        case cloud_io::download_result::timedout:
            co_return std::unexpected(io::errc::cloud_op_timeout);
        case cloud_io::download_result::failed:
            co_return std::unexpected(io::errc::cloud_op_error);
        }
        std::unreachable();
    }
}

ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>>
file_io::open_object(object_extent extent, ss::abort_source* as) {
    if (extent.imported.has_value()) {
        auto index_path = cloud_storage::generate_index_path(
          cloud_storage::remote_segment_path{
            std::filesystem::path{extent.imported->ts_path}});
        cloud_storage::offset_index ts_index(
          model::offset{0},
          kafka::offset{0},
          0,
          cloud_storage::remote_segment_sampling_step_bytes,
          model::timestamp::missing());
        auto index_iobuf = co_await download_raw_iobuf(
          index_path.native(), as);
        if (index_iobuf.has_value()) {
            ts_index.from_iobuf(std::move(*index_iobuf));
        }
        auto idx = std::make_unique<ts_segment_index>(
          std::move(ts_index),
          extent.imported->base_kafka_offset,
          extent.imported->last_kafka_offset,
          extent.imported->delta_offset,
          extent.size);
        co_return std::make_unique<tiered_storage_object_handle>(
          extent, _remote, _ts_bucket, _cache, std::move(idx));
    }

    auto read_result = co_await read_object_as_iobuf(extent, as);
    if (!read_result.has_value()) {
        co_return std::unexpected(read_result.error());
    }
    auto footer_result = co_await footer::read(std::move(read_result).value());
    if (!std::holds_alternative<footer>(footer_result)) {
        vlog(
          cd_log.warn, "Failed to parse L1 footer for object {}", extent.id);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::make_unique<l1_native_object_handle>(
      extent.id, std::get<footer>(std::move(footer_result)), this);
}

ss::future<std::expected<void, io::errc>>
file_io::delete_keys(
  const cloud_storage_clients::bucket_name& bucket,
  chunked_vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& root) {
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->delete_objects(
          bucket, std::move(keys), root, [](size_t retry_count) {
              std::ignore = retry_count;
          }));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error deleting objects: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::upload_result::success:
        co_return std::expected<void, io::errc>{};
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::cancelled:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::upload_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

ss::future<std::expected<void, io::errc>>
file_io::delete_objects(
  chunked_vector<object_extent> extents, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);

    chunked_vector<cloud_storage_clients::object_key> native_keys;
    chunked_vector<cloud_storage_clients::object_key> ts_keys;
    for (const auto& extent : extents) {
        if (extent.imported.has_value()) {
            ts_keys.push_back(
              cloud_storage_clients::object_key{extent.imported->ts_path});
        } else {
            native_keys.push_back(
              object_path_factory::level_one_path(extent.id));
        }
    }

    if (!native_keys.empty()) {
        auto res = co_await delete_keys(_bucket, std::move(native_keys), root);
        if (!res.has_value()) {
            co_return res;
        }
    }
    if (!ts_keys.empty()) {
        auto ts_count = ts_keys.size();
        // Best-effort: log and continue on failure. The archiver is no longer
        // running on this partition so the segment will remain in cloud storage
        // but be unreachable from any read path.
        auto res = co_await delete_keys(_ts_bucket, std::move(ts_keys), root);
        if (!res.has_value()) {
            vlog(
              cd_log.warn,
              "Failed to delete {} imported TS segments: {}",
              ts_count,
              res.error());
        }
    }
    co_return std::expected<void, io::errc>{};
}

ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, io::errc>>
file_io::create_multipart_upload(
  object_id oid, size_t part_size, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    auto key = object_path_factory::level_one_path(oid);
    auto result_fut = co_await ss::coroutine::as_future(
      _remote->initiate_multipart_upload(_bucket, key, part_size, timeout));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error initiating multipart upload: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    auto result = result_fut.get();
    if (!result.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to initiate multipart upload for {}: {}",
          oid,
          result.error());
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::move(result.value());
}

ss::future<std::expected<iobuf, io::errc>>
file_io::download_raw_iobuf(const ss::sstring& key, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    iobuf result;
    auto res_fut = co_await ss::coroutine::as_future<cloud_io::download_result>(
      _remote->download_object({
        .transfer_details = {
          .bucket = _ts_bucket,
          .key = cloud_storage_clients::object_key{key},
          .parent_rtc = root,
        },
        .display_str = "ts_raw_download",
        .payload = result,
      }));
    if (res_fut.failed()) {
        auto ex = res_fut.get_exception();
        vlog(cd_log.warn, "Error downloading raw object {}: {}", key, ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (res_fut.get()) {
    case cloud_io::download_result::success:
        co_return std::move(result);
    case cloud_io::download_result::notfound:
        co_return std::unexpected(io::errc::cloud_missing_object);
    case cloud_io::download_result::timedout:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::download_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

} // namespace cloud_topics::l1
