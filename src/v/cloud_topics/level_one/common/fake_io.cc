/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/fake_io.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/ts_reader.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/object_id.h"

namespace cloud_topics::l1 {

static ss::logger fake_io_log("fake_io");

namespace {

class fake_footer_index final : public object_index {
public:
    explicit fake_footer_index(l1::footer f) : _footer(std::move(f)) {}

    std::optional<seek_result>
    seek_to_offset(model::topic_id_partition tidp, kafka::offset offset)
      const override {
        auto r = _footer.file_position_before_kafka_offset(tidp, offset);
        if (r == l1::footer::npos) {
            return std::nullopt;
        }
        return seek_result{.file_position = r.file_position, .length = r.length};
    }

    std::optional<seek_result> seek_to_timestamp(
      model::topic_id_partition tidp, model::timestamp ts) const override {
        auto r = _footer.file_position_before_max_timestamp(tidp, ts);
        if (r == l1::footer::npos) {
            return std::nullopt;
        }
        return seek_result{.file_position = r.file_position, .length = r.length};
    }

private:
    l1::footer _footer;
};

class fake_object_handle final : public object_handle {
public:
    fake_object_handle(object_extent extent, fake_io* io, l1::footer footer)
      : _extent(extent)
      , _io(io)
      , _index(std::move(footer)) {}

    const object_index& index() const override { return _index; }

    ss::future<std::expected<std::unique_ptr<l1::object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override {
        l1::object_extent read_extent{
          .id = _extent.id,
          .position = seek.file_position,
          .size = seek.length,
        };
        auto stream_result = co_await _io->read_object(read_extent, as);
        if (!stream_result.has_value()) {
            co_return std::unexpected(stream_result.error());
        }
        co_return l1::object_reader::create(std::move(*stream_result));
    }

private:
    object_extent _extent;
    fake_io* _io;
    fake_footer_index _index;
};

// Trivial index for an imported TS segment stored in fake_io. Always seeks to
// file_position=0 (conservative full-segment scan), which is correct since the
// caller is responsible for dispatching only to a segment whose range includes
// the target offset.
class fake_ts_index final : public object_index {
public:
    fake_ts_index(
      size_t segment_size,
      model::offset_delta delta,
      kafka::offset last_kafka_offset)
      : _segment_size(segment_size)
      , _delta(delta)
      , _last(last_kafka_offset) {}

    std::optional<seek_result>
    seek_to_offset(model::topic_id_partition, kafka::offset target)
      const override {
        if (target > _last) {
            return std::nullopt;
        }
        return seek_result{
          .file_position = 0, .length = _segment_size, .delta = _delta};
    }

    std::optional<seek_result>
    seek_to_timestamp(model::topic_id_partition, model::timestamp)
      const override {
        return seek_result{
          .file_position = 0, .length = _segment_size, .delta = _delta};
    }

private:
    size_t _segment_size;
    model::offset_delta _delta;
    kafka::offset _last;
};

class fake_ts_object_handle final : public object_handle {
public:
    fake_ts_object_handle(
      iobuf bytes,
      fake_ts_index index,
      absl::btree_set<model::tx_range, std::greater<>> aborted)
      : _bytes(std::move(bytes))
      , _index(std::move(index))
      , _aborted(std::move(aborted)) {}

    const object_index& index() const override { return _index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source*) override {
        auto stream = make_iobuf_input_stream(
          _bytes.share(seek.file_position, seek.length));
        auto delta = seek.delta.value_or(model::offset_delta{0});
        co_return std::make_unique<tiered_storage_object_reader>(
          std::move(stream), delta, _aborted);
    }

private:
    iobuf _bytes;
    fake_ts_index _index;
    absl::btree_set<model::tx_range, std::greater<>> _aborted;
};

} // anonymous namespace

// In-memory multipart upload state for testing.
class fake_multipart_state final
  : public cloud_storage_clients::multipart_upload_state {
public:
    fake_multipart_state(
      object_id oid, absl::btree_map<object_id, iobuf>& storage)
      : _oid(oid)
      , _storage(storage) {}

    ss::future<> initialize_multipart() override { co_return; }

    ss::future<> upload_part(size_t, iobuf data) override {
        _buffer.append(std::move(data));
        co_return;
    }

    ss::future<> complete_multipart_upload() override {
        _storage.insert_or_assign(_oid, std::move(_buffer));
        co_return;
    }

    ss::future<> abort_multipart_upload() override {
        _buffer.clear();
        co_return;
    }

    ss::future<> upload_as_single_object(iobuf data) override {
        _storage.insert_or_assign(_oid, std::move(data));
        co_return;
    }

    bool is_multipart_initialized() const override { return true; }
    ss::sstring upload_id() const override { return "fake"; }

private:
    object_id _oid;
    absl::btree_map<object_id, iobuf>& _storage;
    iobuf _buffer;
};

fake_io::fake_io() = default;

class fake_file : public staging_file {
public:
    fake_file() = default;
    fake_file(const fake_file&) = delete;
    fake_file(fake_file&&) = delete;
    fake_file& operator=(const fake_file&) = delete;
    fake_file& operator=(fake_file&&) = delete;
    ~fake_file() override {
        vassert(_removed, "staging_file must be removed before destruction");
    }

    ss::future<size_t> size() override {
        vassert(!_removed, "cannot get size of a removed file");
        co_return _data.size_bytes();
    }
    ss::future<ss::output_stream<char>> output_stream() override {
        vassert(!_removed, "cannot get output stream of a removed file");
        co_return make_iobuf_ref_output_stream(_data);
    }

    ss::future<> remove() override {
        _removed = true;
        co_return;
    }

    ss::future<ss::input_stream<char>> input_stream() override {
        vassert(!_removed, "cannot get input stream of a removed file");
        co_return make_iobuf_input_stream(_data.share(0, _data.size_bytes()));
    }

private:
    bool _removed = false;
    iobuf _data;
};

ss::future<std::expected<std::unique_ptr<staging_file>, io::errc>>
fake_io::create_tmp_file() {
    std::unique_ptr<staging_file> file = std::make_unique<fake_file>();
    co_return file;
}

ss::future<std::expected<void, io::errc>>
fake_io::put_object(object_id oid, staging_file* file, ss::abort_source*) {
    auto stream = co_await io::read_file(file);
    auto size = co_await file->size();
    auto data = co_await read_iobuf_exactly(stream, size);
    put_object(oid, std::move(data));
    co_return std::expected<void, io::errc>();
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
fake_io::read_object(object_extent extent, ss::abort_source*) {
    co_return get_object(extent.id)
      .transform(
        [&extent](
          iobuf data) -> std::expected<ss::input_stream<char>, io::errc> {
            return make_iobuf_input_stream(
              data.share(extent.position, extent.size));
        })
      .value_or(std::unexpected(io::errc::cloud_missing_object));
}

void fake_io::put_ts_segment(
  ss::sstring ts_path,
  iobuf segment_bytes,
  kafka::offset base_kafka_offset,
  kafka::offset last_kafka_offset,
  model::offset_delta delta_offset,
  absl::btree_set<model::tx_range, std::greater<>> aborted) {
    _ts_storage.insert_or_assign(
      std::move(ts_path),
      ts_segment_fixture{
        .bytes = std::move(segment_bytes),
        .base_kafka_offset = base_kafka_offset,
        .last_kafka_offset = last_kafka_offset,
        .delta_offset = delta_offset,
        .aborted = std::move(aborted),
      });
}

ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>>
fake_io::open_object(object_extent extent, ss::abort_source* as) {
    if (extent.imported.has_value()) {
        auto it = _ts_storage.find(extent.imported->ts_path);
        if (it == _ts_storage.end()) {
            co_return std::unexpected(io::errc::cloud_missing_object);
        }
        auto& fixture = it->second;
        size_t segment_size = fixture.bytes.size_bytes();
        fake_ts_index idx{
          segment_size, fixture.delta_offset, fixture.last_kafka_offset};
        co_return std::make_unique<fake_ts_object_handle>(
          fixture.bytes.share(0, segment_size),
          std::move(idx),
          fixture.aborted);
    }
    auto stream_result = co_await read_object(extent, as);
    if (!stream_result.has_value()) {
        co_return std::unexpected(stream_result.error());
    }
    auto footer_buf = co_await read_iobuf_exactly(
      *stream_result, extent.size);
    auto footer_result = co_await l1::footer::read(std::move(footer_buf));
    if (!std::holds_alternative<l1::footer>(footer_result)) {
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::make_unique<fake_object_handle>(
      extent, this, std::get<l1::footer>(std::move(footer_result)));
}

ss::future<std::expected<void, io::errc>>
fake_io::delete_objects(
  chunked_vector<object_extent> extents, ss::abort_source*) {
    for (const auto& extent : extents) {
        remove_object(extent.id);
    }
    co_return std::expected<void, io::errc>{};
}

std::optional<iobuf> fake_io::get_object(object_id id) {
    auto it = _storage.find(id);
    if (it == _storage.end()) {
        return std::nullopt;
    }
    return it->second.share(0, it->second.size_bytes());
}

void fake_io::put_object(object_id id, iobuf data) {
    _storage.insert_or_assign(id, std::move(data));
}

void fake_io::remove_object(object_id id) { _storage.erase(id); }

chunked_vector<object_id> fake_io::list_objects() const {
    chunked_vector<object_id> oids;
    for (const auto& [oid, _] : _storage) {
        oids.emplace_back(oid);
    }
    return oids;
}

ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, io::errc>>
fake_io::create_multipart_upload(
  object_id oid, size_t part_size, ss::abort_source*) {
    auto state = ss::make_shared<fake_multipart_state>(oid, _storage);
    auto upload = ss::make_shared<cloud_storage_clients::multipart_upload>(
      std::move(state), part_size, fake_io_log);
    co_return upload;
}

} // namespace cloud_topics::l1
