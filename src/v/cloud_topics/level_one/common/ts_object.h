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

#include "cloud_storage/offset_index.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/ts_chunk_data_source.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <expected>
#include <memory>
#include <optional>

namespace cloud_topics::l1 {

/// object_index over a tiered-storage segment's offset_index. Translates a
/// target Kafka offset/timestamp into a byte position plus the offset
/// translation delta at that position using the segment's downloaded .index.
/// At an index entry the delta is the entry's (log - Kafka) offset; a seek
/// before the first index entry (or with no index at all) returns the segment
/// start, whose delta is the segment base delta (delta_base). Returning the
/// base delta -- rather than the base Kafka offset for the reader to infer the
/// delta from the first batch -- keeps translation correct when compaction has
/// removed the segment's leading records. (find_kaf_offset/find_timestamp are
/// non-const on offset_index -- they flush a write buffer internally -- so
/// _index is mutable.)
///
/// Seeks are not upper-bounded by the segment's last offset: the caller only
/// dispatches a target the segment's extent range covers (the metastore read
/// path discards objects whose last_offset is below the target), so the bound
/// would never reject anything.
class ts_segment_index final : public object_index {
public:
    ts_segment_index(
      cloud_storage::offset_index index,
      model::offset_delta delta_base,
      size_t segment_size);

    std::optional<seek_result> seek_to_offset(
      model::topic_id_partition, kafka::offset target) const override;

    std::optional<seek_result> seek_to_timestamp(
      model::topic_id_partition, model::timestamp) const override;

private:
    mutable cloud_storage::offset_index _index;
    model::offset_delta _delta_base;
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
    /// file_position+length). Copyable (see ts_fetch_range_fn): the reader's
    /// chunk data source owns its own copy, decoupled from this handle.
    using fetch_range_fn = ts_fetch_range_fn;

    /// `chunk_size` is the download granularity: open_reader serves the segment
    /// as a lazily-fetched sequence of fixed-size chunks (only the chunks a
    /// read touches are downloaded). `chunk_size == 0` disables chunking and
    /// downloads the whole suffix from the seek point in one range (the legacy
    /// behavior / `cloud_storage_disable_chunk_reads`).
    ts_object_handle(
      std::unique_ptr<object_index> index,
      model::term_id term,
      aborted_transactions aborted,
      fetch_range_fn fetch,
      size_t chunk_size);

    const object_index& index() const override { return *_index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override;

private:
    std::unique_ptr<object_index> _index;
    model::term_id _term;
    aborted_transactions _aborted;
    fetch_range_fn _fetch;
    size_t _chunk_size;
};

} // namespace cloud_topics::l1
