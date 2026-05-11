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

#include "cloud_topics/level_one/common/object.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <optional>

namespace cloud_topics::l1 {

/// Implements object_reader over a raw Kafka-format byte stream (a TS segment).
///
/// Translates log offsets to Kafka offsets by maintaining a running delta.
/// Non-data batches are skipped while incrementing the delta.
class tiered_storage_object_reader final : public object_reader {
public:
    tiered_storage_object_reader(
      ss::input_stream<char> stream, model::offset_delta delta);

    ss::future<> close() override;
    ss::future<peek_result> peek() override;
    ss::future<result> read_next() override;

private:
    ss::future<std::optional<model::record_batch>> fetch_next_translated();

    ss::input_stream<char> _stream;
    model::offset_delta _running_delta;
    std::optional<model::record_batch> _peeked;
    bool _eof{false};
};

} // namespace cloud_topics::l1
