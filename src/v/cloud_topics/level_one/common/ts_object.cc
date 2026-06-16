/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/ts_object.h"

#include "cloud_topics/level_one/common/ts_reader.h"

namespace cloud_topics::l1 {

ts_segment_index::ts_segment_index(
  cloud_storage::offset_index index,
  kafka::offset base_kafka_offset,
  kafka::offset max_kafka_offset,
  size_t segment_size)
  : _index(std::move(index))
  , _base_kafka_offset(base_kafka_offset)
  , _max_kafka_offset(max_kafka_offset)
  , _segment_size(segment_size) {}

std::optional<seek_result> ts_segment_index::seek_to_offset(
  model::topic_id_partition, kafka::offset target) const {
    if (target > _max_kafka_offset) {
        return std::nullopt;
    }
    // No index entry at/before the target returns the segment start
    // (file_pos=0, Kafka offset = base): the reader scans from there. Correct
    // because the caller only dispatches to a segment whose range includes the
    // target.
    auto entry = _index.find_kaf_offset(target + kafka::offset{1});
    size_t file_pos = entry ? static_cast<size_t>(entry->file_pos) : 0;
    kafka::offset kaf = entry ? entry->kaf_offset : _base_kafka_offset;
    return seek_result{
      .file_position = file_pos,
      .length = _segment_size - file_pos,
      .kafka_offset = kaf};
}

std::optional<seek_result> ts_segment_index::seek_to_timestamp(
  model::topic_id_partition, model::timestamp ts) const {
    auto entry = _index.find_timestamp(ts);
    size_t file_pos = entry ? static_cast<size_t>(entry->file_pos) : 0;
    kafka::offset kaf = entry ? entry->kaf_offset : _base_kafka_offset;
    return seek_result{
      .file_position = file_pos,
      .length = _segment_size - file_pos,
      .kafka_offset = kaf};
}

ts_object_handle::ts_object_handle(
  std::unique_ptr<object_index> index,
  model::term_id term,
  absl::btree_set<model::tx_range, std::greater<>> aborted,
  fetch_range_fn fetch)
  : _index(std::move(index))
  , _term(term)
  , _aborted(std::move(aborted))
  , _fetch(std::move(fetch)) {}

ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
ts_object_handle::open_reader(const seek_result& seek, ss::abort_source* as) {
    auto stream_result = co_await _fetch(seek.file_position, seek.length, as);
    if (!stream_result.has_value()) {
        co_return std::unexpected(stream_result.error());
    }
    co_return std::make_unique<tiered_storage_object_reader>(
      std::move(*stream_result), seek.kafka_offset.value(), _term, _aborted);
}

} // namespace cloud_topics::l1
