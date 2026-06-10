/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/format_to.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace cloud_topics::l1 {

// An object ID is a unique identifier for a cloud topic L1 object.
using object_id = named_type<uuid_t, struct l1_object_id_tag>;

inline object_id create_object_id() { return object_id{uuid_t::create()}; }

/// The full descriptor of a tiered-storage segment imported into L1 by
/// reference, as the metastore *interface* and the IO read path see it: where
/// the segment lives plus how to interpret its data.
///
/// The metastore *storage* layer decomposes this by ownership -- the path is an
/// object property and the delta/term are extent properties (see
/// `imported_ts_object_location` / `imported_ts_segment_info` in state.h) --
/// but the API and the reader treat it as one unit. The extent's Kafka offset
/// bounds (carried separately by the API/IO types) give the segment's offset
/// range.
struct imported_ts_info
  : public serde::
      envelope<imported_ts_info, serde::version<0>, serde::compat_version<0>> {
    friend bool
    operator==(const imported_ts_info&, const imported_ts_info&) = default;
    auto serde_fields() {
        return std::tie(
          ts_path, segment_term, base_kafka_offset, last_kafka_offset);
    }

    /// Opaque path within the TS bucket (sname_format path).
    ss::sstring ts_path;
    /// Raft term the segment was written in. Stamped onto every imported batch
    /// as its partition leader epoch (a tiered-storage segment is single-term),
    /// so a Kafka fetch of the imported region reports the original leader
    /// epoch rather than -1.
    model::term_id segment_term{};
    /// First and last Kafka offset of the segment (inclusive). Redundant with
    /// the extent's base/last offset (from which they are populated on read),
    /// kept here so the IO read view is self-contained. base_kafka_offset seeds
    /// the reader's offset delta when a seek scans from the segment start (the
    /// log-to-Kafka delta there is the first batch's log offset minus this);
    /// last_kafka_offset bounds seeks in the imported segment index.
    kafka::offset base_kafka_offset{0};
    kafka::offset last_kafka_offset{0};
};

/// An L1 object or imported TS segment, as seen by the IO read path. For an
/// imported extent `imported` is the segment descriptor (path + term + Kafka
/// offset bounds); the bounds come from the metastore.
///
/// `position` and `size` carry format-specific semantics:
///   native (imported == nullopt):
///     position = footer byte offset within the object
///     size     = footer length in bytes (= object_size - footer_pos)
///   imported (imported.has_value()):
///     position = 0 (unused)
///     size     = total segment size in bytes
struct object_extent {
    object_id id;
    size_t position = 0;
    size_t size = 0;
    /// Set for an imported extent (nullopt for native L1); carries the
    /// segment's path, term, and Kafka offset bounds.
    std::optional<imported_ts_info> imported;

    fmt::iterator format_to(fmt::iterator it) const;
};

/// One object to delete via io::delete_objects. A native L1 object (ts_path
/// nullopt) is deleted from the L1 bucket by its id; an imported tiered-storage
/// segment (ts_path set) is deleted from the TS bucket at that path (the
/// segment plus its .tx and .index).
struct object_location {
    object_id id;
    std::optional<ss::sstring> ts_path;
};

} // namespace cloud_topics::l1
