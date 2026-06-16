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

#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/client.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/file_io_probe.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>

#include <memory>
#include <optional>
#include <utility>

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

// A streaming download source for L1 objects that chunks downloads per
// `chunk_size`. This may result in many GET requests per object download.
//
// The object's byte range is read one bounded chunk at a time. `get()` fully
// buffers the next chunk via `download_stream` (a separate ranged GET), which
// returns the lease to the pool as soon as the chunk's body has been read off
// the connection. The buffered chunk is then served from memory, lease-free,
// while subsequent `get()`s drain it; only once it is exhausted is the next
// chunk fetched. Peak in-memory bytes and the connection-hold duration per read
// are both bounded by `chunk_size`.
class streaming_download_source final : public ss::data_source_impl {
public:
    // chunk_size bounds both the peak in-memory bytes held per read and the
    // span of object bytes downloaded under a single held lease. _next_pos and
    // _last_pos are initialized to inclusive byte ranges.
    streaming_download_source(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key key,
      cloud_storage_clients::http_byte_range range,
      ss::abort_source& as,
      cloud_io::group_id gid,
      size_t chunk_size)
      : _remote(remote)
      , _bucket(std::move(bucket))
      , _key(std::move(key))
      , _next_pos(range.first)
      , _last_pos(range.second)
      , _as(as)
      , _gid(gid)
      , _chunk_size(chunk_size) {}

    streaming_download_source(const streaming_download_source&) = delete;
    streaming_download_source&
    operator=(const streaming_download_source&) = delete;
    streaming_download_source(streaming_download_source&&) = delete;
    streaming_download_source& operator=(streaming_download_source&&) = delete;
    ~streaming_download_source() override = default;

    ss::future<ss::temporary_buffer<char>> get() override {
        while (true) {
            _as.check();
            if (!_current.empty()) {
                auto buf = std::move(_current.front());
                _current.pop_front();
                co_return buf;
            }
            if (_next_pos > _last_pos) {
                // An empty buffer signals end-of-stream.
                co_return ss::temporary_buffer<char>();
            }
            co_await download_next_chunk();
        }
    }

    ss::future<> close() override { return ss::now(); }

private:
    // Fetches the next chunk of the object's byte range into `_current`. The
    // lease is released when `download_stream` returns, before the buffered
    // chunk is served.
    ss::future<> download_next_chunk() {
        static constexpr auto timeout = 10s;
        static constexpr auto backoff = 100ms;
        const auto chunk_first = _next_pos;
        const auto chunk_last = std::min(
          chunk_first + _chunk_size - 1, _last_pos);
        _next_pos = chunk_last + 1;
        retry_chain_node root(_as, ss::lowres_clock::now() + timeout, backoff);

        cloud_io::try_consume_stream consumer =
          [this](uint64_t /*content_length*/, ss::input_stream<char> stream) {
              return drain_chunk(std::move(stream));
          };

        auto result_fut = co_await ss::coroutine::as_future(
          _remote->download_stream(
            cloud_io::transfer_details{
              .bucket = _bucket,
              .key = _key,
              .parent_rtc = root,
            },
            consumer,
            "l1_stream_download",
            /*acquire_hydration_units=*/true,
            cloud_storage_clients::http_byte_range{chunk_first, chunk_last},
            {},
            _gid));
        if (result_fut.failed()) {
            std::rethrow_exception(result_fut.get_exception());
        }
        auto result = result_fut.get();
        if (result != cloud_io::download_result::success) {
            throw std::runtime_error(
              fmt::format("L1 streaming download failed: {}", result));
        }
    }

    // Reads a chunk's whole body into `_current`. Cleared up-front so that a
    // retried download re-buffers from scratch rather than appending to a
    // partially-read result.
    ss::future<uint64_t> drain_chunk(ss::input_stream<char> stream) {
        _current.clear();
        uint64_t total = 0;
        std::exception_ptr ex;
        try {
            while (true) {
                _as.check();
                auto buf = co_await stream.read();
                if (buf.empty()) {
                    break;
                }
                total += buf.size();
                _current.push_back(std::move(buf));
            }
        } catch (...) {
            ex = std::current_exception();
        }
        co_await stream.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
        co_return total;
    }

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _key;
    size_t _next_pos;
    size_t _last_pos;
    ss::abort_source& _as;
    cloud_io::group_id _gid;
    size_t _chunk_size;
    ss::chunked_fifo<ss::temporary_buffer<char>> _current;
};

ss::future<uint64_t> save_to_cache(
  cloud_io::cache* cache,
  ss::input_stream<char> stream,
  cloud_io::space_reservation_guard* reservation,
  std::filesystem::path cache_key,
  uint64_t content_length) {
    co_await cache->put(std::move(cache_key), stream, *reservation);
    co_return content_length;
}

// Reserve cache space, run the S3 GET for [offset, offset+size) of `key`, and
// stream the bytes into the cloud cache under `cache_key`. Succeeds, or fails
// with the mapped errc on reservation / download failure. Runs under
// single_flight so concurrent missers for the same key share one download.
ss::future<single_flight::outcome> do_download_to_cache(
  cloud_io::remote* remote,
  cloud_io::cache* cache,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& key,
  const std::filesystem::path& cache_key,
  size_t offset,
  size_t size,
  cloud_io::group_id group,
  std::string_view download_label,
  retry_chain_node& root,
  ss::abort_source& as) {
    // TODO(cloud_topics): reserving space should also take an abort_source
    auto reservation_fut
      = co_await ss::coroutine::as_future<cloud_io::space_reservation_guard>(
        cache->reserve_space(size, 1));
    if (reservation_fut.failed()) {
        auto ex = reservation_fut.get_exception();
        vlog(
          cd_log.warn, "Error reserving cache space for {}: {}", cache_key, ex);
        co_return std::unexpected(io::errc::file_io_error);
    }
    cloud_io::try_consume_stream consumer =
      [cache, rg = reservation_fut.get(), &cache_key](
        uint64_t content_length, ss::input_stream<char> s) mutable {
          return save_to_cache(
            cache, std::move(s), &rg, cache_key, content_length);
      };
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::download_result>(
        remote->download_stream(
          cloud_io::transfer_details{
            .bucket = bucket,
            .key = key,
            .parent_rtc = root,
          },
          consumer,
          download_label,
          /*acquire_hydration_units=*/true,
          cloud_storage_clients::http_byte_range{offset, offset + size - 1},
          {},
          group));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error downloading {}: {}", cache_key, ex);
        // Map abort to cloud_op_timeout so a leader-abort and a merger-abort
        // produce the same errc for the same event.
        co_return std::unexpected(
          as.abort_requested() ? io::errc::cloud_op_timeout
                               : io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::download_result::success:
        co_return single_flight::outcome{};
    case cloud_io::download_result::notfound:
        co_return std::unexpected(io::errc::cloud_missing_object);
    case cloud_io::download_result::timedout:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::download_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

// Returns a stream over [offset, offset+size) of the object at `key` in
// `bucket`. Shared by the native L1 read path and the imported TS-segment
// fetch. The cache is always consulted first; on a miss `skip_cache` chooses
// between streaming the range directly from object storage without caching it
// (bulk one-shot reads) and downloading it into the cache under `cache_key`.
// `sf` dedups concurrent caching downloads for the same `cache_key`; cache-miss
// and merge events are reported to `probe` when it is non-null.
ss::future<std::expected<ss::input_stream<char>, io::errc>>
download_cached_range(
  cloud_io::remote* remote,
  cloud_io::cache* cache,
  const cloud_storage_clients::bucket_name& bucket,
  cloud_storage_clients::object_key key,
  std::filesystem::path cache_key,
  size_t offset,
  size_t size,
  cloud_io::group_id group,
  std::string_view download_label,
  single_flight& sf,
  file_io_probe* probe,
  ss::abort_source* as,
  bool skip_cache) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    while (true) {
        auto stream_fut = co_await ss::coroutine::as_future<
          std::optional<cloud_io::cache_item_stream>>(cache->get_stream(
          cache_key,
          config::shard_local_cfg().storage_read_buffer_size(),
          config::shard_local_cfg().storage_read_readahead_count()));
        if (stream_fut.failed()) {
            auto ex = stream_fut.get_exception();
            vlog(
              cd_log.warn,
              "Error reading from cache for {}: {}",
              cache_key,
              ex);
            co_return std::unexpected(io::errc::file_io_error);
        }
        auto stream = stream_fut.get();
        if (stream) {
            co_return std::move(stream->body);
        }

        if (probe) {
            probe->register_cache_miss();
        }

        // skip_cache: stream the range directly from object storage in bounded
        // chunks without populating the cache -- bulk one-shot reads (leveling,
        // compaction) that would otherwise pollute it. The cache is still
        // consulted first (above), so a warm range is served locally.
        if (skip_cache) {
            co_return ss::input_stream<char>(ss::data_source(
              std::make_unique<streaming_download_source>(
                remote,
                bucket,
                key,
                cloud_storage_clients::http_byte_range{
                  offset, offset + size - 1},
                *as,
                group,
                config::shard_local_cfg()
                  .cloud_topics_l1_streaming_read_chunk_size())));
        }

        // single_flight dedups concurrent downloads for this cache_key.
        auto r = co_await sf.run(
          cache_key,
          *as,
          [remote,
           cache,
           &bucket,
           &key,
           &cache_key,
           offset,
           size,
           group,
           download_label,
           &root,
           as] {
              return do_download_to_cache(
                remote,
                cache,
                bucket,
                key,
                cache_key,
                offset,
                size,
                group,
                download_label,
                root,
                *as);
          },
          &cd_log);
        if (!r.has_value()) {
            co_return std::unexpected(r.error());
        }
        if (r.value()) {
            vlog(cd_log.debug, "Merged L1 read for {}", cache_key);
            if (probe) {
                probe->register_concurrent_read_merge();
            }
        }
        // The leader populated the cache (mergers waited for it); loop back to
        // serve the now-cached bytes.
    }
}

} // namespace

file_io::file_io(
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::cache* cache,
  file_io_probe* probe)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _staging_dir(std::move(staging_dir))
  , _cache(cache)
  , _probe(probe) {}

ss::future<> file_io::stop() { return _gate.close(); }

std::filesystem::path file_io::cache_key(const object_extent& extent) {
    return std::filesystem::path(
      fmt::format(
        "l1_{}_position_{}_size_{}.partial",
        extent.id,
        extent.position,
        extent.size));
}

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

ss::future<std::expected<ss::input_stream<char>, io::errc>>
file_io::read_object(
  object_extent extent,
  ss::abort_source* as,
  cloud_io::group_id gid,
  bool skip_cache) {
    if (_gate.is_closed()) {
        co_return std::unexpected(io::errc::file_io_error);
    }
    auto holder = _gate.hold();
    if (_probe) {
        _probe->register_read();
    }
    // TODO(cloud_topics): Optimize the cache such that it understands partial
    // objects? Or we assert somehow there are no overlaps (or just live with
    // them).
    // TODO(cloud_topics): If reading just a footer, we should skip the cache.
    // Maybe we need another method for that which is iobuf based?
    auto cache_key = file_io::cache_key(extent);
    co_return co_await download_cached_range(
      _remote,
      _cache,
      _bucket,
      object_path_factory::level_one_path(extent.id),
      cache_key,
      extent.position,
      extent.size,
      gid,
      "l1_file_download",
      _single_flight,
      _probe,
      as,
      skip_cache);
}

ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>>
file_io::open_object(
  object_extent extent,
  ss::abort_source* as,
  cloud_io::group_id g,
  bool skip_cache) {
    if (_probe != nullptr) {
        _probe->register_footer_read(extent.size);
    }
    auto read_result = co_await read_object_as_iobuf(extent, as, g, skip_cache);
    if (!read_result.has_value()) {
        co_return std::unexpected(read_result.error());
    }
    auto footer_result = co_await footer::read(std::move(read_result).value());
    if (!std::holds_alternative<footer>(footer_result)) {
        vlog(cd_log.warn, "Failed to parse L1 footer for object {}", extent.id);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::make_unique<l1_native_object_handle>(
      extent.id,
      std::get<footer>(std::move(footer_result)),
      this,
      g,
      skip_cache);
}

ss::future<std::expected<void, io::errc>>
file_io::delete_objects(chunked_vector<object_id> ids, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    chunked_vector<cloud_storage_clients::object_key> keys;
    for (const auto& id : ids) {
        keys.push_back(object_path_factory::level_one_path(id));
    }
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->delete_objects(
          _bucket, std::move(keys), root, [](size_t retry_count) {
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

} // namespace cloud_topics::l1
