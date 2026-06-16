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
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/ts_object.h"

namespace cloud_topics::l1 {

static ss::logger fake_io_log("fake_io");

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
fake_io::read_object(
  object_extent extent,
  ss::abort_source*,
  [[maybe_unused]] cloud_io::group_id gid) {
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
  ts_segment_path ts_path,
  iobuf segment_bytes,
  aborted_transactions aborted,
  std::optional<iobuf> index_bytes) {
    _ts_storage.insert_or_assign(
      std::move(ts_path),
      ts_segment_fixture{
        .bytes = std::move(segment_bytes),
        .aborted = std::move(aborted),
        .index_bytes = std::move(index_bytes),
      });
}

ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>>
fake_io::open_object(
  object_extent extent, ss::abort_source* as, cloud_io::group_id g) {
    if (extent.imported.has_value()) {
        auto it = _ts_storage.find(extent.imported->ts_path);
        if (it == _ts_storage.end()) {
            co_return std::unexpected(io::errc::cloud_missing_object);
        }
        auto& fixture = it->second;
        size_t segment_size = fixture.bytes.size_bytes();
        // Seek through the real ts_segment_index, exactly as file_io does: an
        // injected .index is deserialized (from_iobuf); without one the index
        // stays empty, so seeks fall back to a full-segment scan from position
        // 0, with the delta taken from the segment's base delta --
        // file_io's missing-.index behavior.
        cloud_storage::offset_index oi(
          model::offset{0},
          kafka::offset{0},
          0,
          cloud_storage::remote_segment_sampling_step_bytes,
          model::timestamp::missing());
        if (fixture.index_bytes.has_value()) {
            oi.from_iobuf(fixture.index_bytes->copy());
        }
        auto idx = std::make_unique<ts_segment_index>(
          std::move(oi), extent.imported->delta_base, segment_size);
        // Mirror file_io's tx_state gating: when the .tx manifest is known
        // absent, the real read path skips the download, so the segment must
        // present no aborted ranges regardless of what the fixture holds. This
        // makes the gating observable in tests -- inject aborted ranges with
        // tx_state=absent and assert they take no effect.
        aborted_transactions aborted = extent.imported->tx_state
                                           == tx_manifest_state::absent
                                         ? aborted_transactions{}
                                         : fixture.aborted;
        co_return std::make_unique<ts_object_handle>(
          std::move(idx),
          extent.imported->segment_term,
          std::move(aborted),
          [bytes = fixture.bytes.copy()](
            size_t pos, size_t len, ss::abort_source*) mutable
            -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
              return ss::make_ready_future<
                std::expected<ss::input_stream<char>, io::errc>>(
                make_iobuf_input_stream(bytes.share(pos, len)));
          });
    }
    auto stream_result = co_await read_object(extent, as, g);
    if (!stream_result.has_value()) {
        co_return std::unexpected(stream_result.error());
    }
    auto footer_buf = co_await read_iobuf_exactly(*stream_result, extent.size);
    auto footer_result = co_await l1::footer::read(std::move(footer_buf));
    if (!std::holds_alternative<l1::footer>(footer_result)) {
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::make_unique<l1_native_object_handle>(
      extent.id, std::get<l1::footer>(std::move(footer_result)), this, g);
}

ss::future<std::expected<void, io::errc>>
fake_io::delete_objects(chunked_vector<object_id> oids, ss::abort_source*) {
    for (const auto& oid : oids) {
        remove_object(oid);
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
