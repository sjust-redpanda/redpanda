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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <expected>
#include <memory>
#include <optional>

namespace cloud_topics::l1 {

/// Result of an index seek. `file_position` is always a byte offset within
/// the object. `delta` is the offset delta at the seek point, set by
/// TS-backed indexes; it is `nullopt` for native L1 objects.
struct seek_result {
    size_t file_position{0};
    /// Bytes from file_position to the end of the readable range.
    /// Always non-zero for a valid seek; both native and TS indexes populate
    /// it.
    size_t length{0};
    std::optional<model::offset_delta> delta;
};

/// An index over a single L1 or imported TS object.
///
/// Hides the format difference between native L1 objects (l1::footer-indexed)
/// and imported tiered-storage segments (segment index-indexed).
class object_index {
public:
    virtual ~object_index() = default;

    /// Return the seek position to start reading at or before the given
    /// Kafka offset. Returns nullopt when the object has no data at or after
    /// the offset.
    virtual std::optional<seek_result>
      seek_to_offset(model::topic_id_partition, kafka::offset) const = 0;

    /// Return the seek position for data with max_timestamp >= ts.
    /// Returns nullopt when no matching data exists.
    virtual std::optional<seek_result>
      seek_to_timestamp(model::topic_id_partition, model::timestamp) const = 0;
};

/// An open reference to a single L1 or imported TS object.
///
/// Obtained from l1::io::open_object. Holds the object index (footer or TS
/// segment index) and can open readers positioned at any seek result.
class object_handle {
public:
    virtual ~object_handle() = default;

    /// Return a reference to the index for this object.
    virtual const object_index& index() const = 0;

    /// Open a reader starting at the seek result returned by index().
    /// `seek.file_position` is always a byte offset. For TS-backed objects,
    /// `seek.delta` carries the offset delta at the seek point.
    virtual ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source*) = 0;
};

} // namespace cloud_topics::l1
