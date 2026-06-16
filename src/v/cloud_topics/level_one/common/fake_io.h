/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "bytes/iobuf.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "model/record.h"

#include <optional>

namespace cloud_topics::l1 {

// The IO implementation that is entirely in-memory, used for testing.
class fake_io : public io {
public:
    fake_io();
    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override;

    ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) override;

    ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      object_extent, ss::abort_source*, cloud_io::group_id g) override;

    ss::future<std::expected<std::unique_ptr<object_handle>, errc>> open_object(
      object_extent, ss::abort_source*, cloud_io::group_id g) override;

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_location>, ss::abort_source*) override;

    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      object_id, size_t part_size, ss::abort_source*) override;

    // Get a full object that has been put directly from storage
    std::optional<iobuf> get_object(object_id id);

    // Put an object directly into storage, bypassing staging
    void put_object(object_id id, iobuf data);

    // Directly remove an object from storage
    void remove_object(object_id id);

    // Return a list of the object IDs that haven't been removed.
    chunked_vector<object_id> list_objects() const;

    // Whether an injected TS segment (see put_ts_segment) is still present.
    // For tests that exercise imported-object deletion.
    bool has_ts_segment(const ss::sstring& ts_path) const;

    /// Inject a raw TS-format segment for use with open_object on imported
    /// extents whose ts_path matches. open_object always seeks through the real
    /// ts_segment_index: with index_bytes (a serialized offset_index, as
    /// file_io downloads) it is deserialized; without one the index is empty,
    /// so seeks fall back to a full-segment scan from 0 (file_io's
    /// missing-.index path).
    void put_ts_segment(
      ss::sstring ts_path,
      iobuf segment_bytes,
      kafka::offset base_kafka_offset,
      kafka::offset last_kafka_offset,
      model::offset_delta delta_offset,
      absl::btree_set<model::tx_range, std::greater<>> aborted = {},
      std::optional<iobuf> index_bytes = std::nullopt);

private:
    struct ts_segment_fixture {
        iobuf bytes;
        kafka::offset base_kafka_offset;
        kafka::offset last_kafka_offset;
        model::offset_delta delta_offset;
        absl::btree_set<model::tx_range, std::greater<>> aborted;
        // Serialized offset_index (.index) for an index-backed seek; nullopt
        // means the full-segment-scan fallback.
        std::optional<iobuf> index_bytes;
    };

    absl::btree_map<object_id, iobuf> _storage;
    absl::btree_map<ss::sstring, ts_segment_fixture> _ts_storage;
};

} // namespace cloud_topics::l1
