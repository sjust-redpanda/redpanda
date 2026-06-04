/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/ts_reader.h"

#include "bytes/iostream.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_utils.h"

#include <fmt/core.h>

#include <stdexcept>

namespace cloud_topics::l1 {

tiered_storage_object_reader::tiered_storage_object_reader(
  ss::input_stream<char> stream, model::offset_delta delta)
  : _stream(std::move(stream))
  , _running_delta(delta) {}

ss::future<> tiered_storage_object_reader::close() {
    return _stream.close();
}

ss::future<object_reader::peek_result> tiered_storage_object_reader::peek() {
    if (!_peeked.has_value() && !_eof) {
        auto next = co_await fetch_next_translated();
        if (next.has_value()) {
            _peeked = std::move(*next);
        } else {
            _eof = true;
        }
    }
    if (_eof) {
        co_return eof{};
    }
    co_return _peeked->header();
}

ss::future<object_reader::result> tiered_storage_object_reader::read_next() {
    if (_peeked.has_value()) {
        auto batch = std::move(*_peeked);
        _peeked.reset();
        co_return std::move(batch);
    }
    if (_eof) {
        co_return eof{};
    }
    auto next = co_await fetch_next_translated();
    if (!next.has_value()) {
        co_return eof{};
    }
    co_return std::move(*next);
}

ss::future<std::optional<model::record_batch>>
tiered_storage_object_reader::fetch_next_translated() {
    static const auto translator_types = model::offset_translator_batch_types();
    for (;;) {
        auto header_buf = co_await read_iobuf_exactly(
          _stream, model::packed_record_batch_header_size);
        if (
          header_buf.size_bytes() < model::packed_record_batch_header_size) {
            co_return std::nullopt;
        }
        auto header = storage::batch_header_from_disk_iobuf(
          std::move(header_buf));
        auto records_size = static_cast<size_t>(header.size_bytes)
                            - model::packed_record_batch_header_size;
        auto records_buf = co_await read_iobuf_exactly(_stream, records_size);
        if (records_buf.size_bytes() != records_size) {
            throw std::runtime_error(fmt::format(
              "truncated TS segment: expected {} record bytes, got {}",
              records_size,
              records_buf.size_bytes()));
        }
        // Non-data batches that the offset translator strips: advance the
        // running delta by the number of log offsets they consume (matching
        // raft::offset_translator) and skip them.
        if (std::ranges::contains(translator_types, header.type)) {
            _running_delta += static_cast<int64_t>(header.last_offset_delta + 1);
            continue;
        }
        // Emit only raft_data, like the production cloud read path. Any other
        // batch type (e.g. tx_fence) still consumes a Kafka offset -- leaving a
        // gap -- but is never surfaced to the fetch path, and does not move the
        // delta (it is not an offset-translator type).
        if (header.type != model::record_batch_type::raft_data) {
            continue;
        }
        header.base_offset = model::offset{
          header.base_offset() - _running_delta()};
        co_return model::record_batch(
          header,
          std::move(records_buf),
          model::record_batch::tag_ctor_ng{});
    }
}

} // namespace cloud_topics::l1
