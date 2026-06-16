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

#include "cloud_topics/level_one/common/object.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <absl/container/btree_set.h>

#include <optional>

namespace cloud_topics::l1 {

/// Implements object_reader over a raw Kafka-format byte stream (a TS segment).
///
/// Translates log offsets to Kafka offsets by maintaining a running delta.
/// Non-data batches are skipped while incrementing the delta.
class tiered_storage_object_reader final : public object_reader {
public:
    /// `position_kafka_offset` is the Kafka offset of the batch at the stream's
    /// start (from the seek). The log-to-Kafka delta is derived from the first
    /// batch read -- its log offset minus this -- and then maintained as
    /// non-data batches are skipped.
    ///
    /// `term` is the raft term the segment was written in; it is stamped onto
    /// every emitted batch as its leader epoch (the segment is single-term).
    /// `aborted` holds the segment's aborted-transaction ranges (raw log-offset
    /// space, from the .tx manifest); data batches that fall in them are
    /// dropped so the imported region is committed-only, like native CT L1.
    tiered_storage_object_reader(
      ss::input_stream<char> stream,
      kafka::offset position_kafka_offset,
      model::term_id term,
      absl::btree_set<model::tx_range, std::greater<>> aborted);

    ss::future<> close() override;
    ss::future<peek_result> peek() override;
    ss::future<result> read_next() override;

private:
    ss::future<std::optional<model::record_batch>> fetch_next_translated();

    ss::input_stream<char> _stream;
    // _running_delta is valid once _delta_initialized is set, derived from the
    // first batch read (its log offset minus _position_kafka_offset).
    kafka::offset _position_kafka_offset;
    model::offset_delta _running_delta;
    bool _delta_initialized{false};
    model::term_id _term;
    absl::btree_set<model::tx_range, std::greater<>> _aborted;
    std::optional<model::record_batch> _peeked;
    bool _eof{false};
};

} // namespace cloud_topics::l1
