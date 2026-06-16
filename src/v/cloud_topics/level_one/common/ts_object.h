/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_storage/remote_segment_index.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_set.h>

#include <expected>
#include <memory>
#include <optional>

namespace cloud_topics::l1 {

/// object_index over a tiered-storage segment's offset_index. Translates a
/// target Kafka offset/timestamp into a byte position plus the Kafka offset at
/// that position using the segment's downloaded .index. A seek before the first
/// index entry (or with no index at all) returns the segment start, whose Kafka
/// offset is base_kafka_offset. (find_kaf_offset/find_timestamp are non-const
/// on offset_index -- they flush a write buffer internally -- so _index is
/// mutable.)
class ts_segment_index final : public object_index {
public:
    ts_segment_index(
      cloud_storage::offset_index index,
      kafka::offset base_kafka_offset,
      kafka::offset max_kafka_offset,
      size_t segment_size);

    std::optional<seek_result> seek_to_offset(
      model::topic_id_partition, kafka::offset target) const override;

    std::optional<seek_result> seek_to_timestamp(
      model::topic_id_partition, model::timestamp) const override;

private:
    mutable cloud_storage::offset_index _index;
    kafka::offset _base_kafka_offset;
    kafka::offset _max_kafka_offset;
    size_t _segment_size;
};

/// object_handle for an imported tiered-storage segment. The byte transport is
/// injected as a fetch callback so the seek/index dispatch and the
/// tiered_storage_object_reader wiring (offset delta, term, aborted-range
/// strip) are shared across backends: file_io supplies a download-from-bucket
/// fetch; fake_io supplies an in-memory one.
class ts_object_handle final : public object_handle {
public:
    /// Returns a stream over the segment's bytes [file_position,
    /// file_position+length).
    using fetch_range_fn = ss::noncopyable_function<
      ss::future<std::expected<ss::input_stream<char>, io::errc>>(
        size_t file_position, size_t length, ss::abort_source*)>;

    ts_object_handle(
      std::unique_ptr<object_index> index,
      model::term_id term,
      absl::btree_set<model::tx_range, std::greater<>> aborted,
      fetch_range_fn fetch);

    const object_index& index() const override { return *_index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override;

private:
    std::unique_ptr<object_index> _index;
    model::term_id _term;
    absl::btree_set<model::tx_range, std::greater<>> _aborted;
    fetch_range_fn _fetch;
};

} // namespace cloud_topics::l1
