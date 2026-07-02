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
  model::offset_delta delta_base,
  size_t segment_size)
  : _index(std::move(index))
  , _delta_base(delta_base)
  , _segment_size(segment_size) {}

// The offset-translation delta at an index entry is its log offset minus its
// Kafka offset; with no entry at/before the target the seek returns the
// segment start (file_pos=0), whose delta is the segment base delta. Correct
// because the caller only dispatches to a segment whose range includes the
// target.
static model::offset_delta delta_at(
  const std::optional<cloud_storage::offset_index::find_result>& entry,
  model::offset_delta delta_base) {
    return entry ? model::offset_delta{entry->rp_offset - entry->kaf_offset}
                 : delta_base;
}

std::optional<seek_result> ts_segment_index::seek_to_offset(
  model::topic_id_partition, kafka::offset target) const {
    auto entry = _index.find_kaf_offset(target + kafka::offset{1});
    size_t file_pos = entry ? static_cast<size_t>(entry->file_pos) : 0;
    return seek_result{
      .file_position = file_pos,
      .length = _segment_size - file_pos,
      .delta = delta_at(entry, _delta_base)};
}

std::optional<seek_result> ts_segment_index::seek_to_timestamp(
  model::topic_id_partition, model::timestamp ts) const {
    auto entry = _index.find_timestamp(ts);
    size_t file_pos = entry ? static_cast<size_t>(entry->file_pos) : 0;
    return seek_result{
      .file_position = file_pos,
      .length = _segment_size - file_pos,
      .delta = delta_at(entry, _delta_base)};
}

ts_object_handle::ts_object_handle(
  std::unique_ptr<object_index> index,
  model::term_id term,
  aborted_transactions aborted,
  fetch_range_fn fetch,
  size_t chunk_size)
  : _index(std::move(index))
  , _term(term)
  , _aborted(std::move(aborted))
  , _fetch(std::move(fetch))
  , _chunk_size(chunk_size) {}

ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
ts_object_handle::open_reader(const seek_result& seek, ss::abort_source* as) {
    if (_chunk_size == 0) {
        // Chunking disabled: download the whole suffix [file_position, end) in
        // one range. A fetch failure here is reported up front as an error.
        auto stream_result = co_await _fetch(
          seek.file_position, seek.length, as);
        if (!stream_result.has_value()) {
            co_return std::unexpected(stream_result.error());
        }
        co_return std::make_unique<tiered_storage_object_reader>(
          std::move(*stream_result), seek.delta.value(), _term, _aborted);
    }
    // Serve the segment as lazily-fetched fixed-size chunks: only the chunks
    // the read actually consumes are downloaded. The data source owns its own
    // copy of the fetch and uses the (long-lived) abort source from the read
    // config, so it is safe for the reader to outlive this handle. A
    // chunk-fetch failure surfaces as an exception during read.
    ss::input_stream<char> stream{
      ss::data_source{std::make_unique<ts_chunk_data_source>(
        _fetch, seek.file_position, seek.length, _chunk_size, as)}};
    co_return std::make_unique<tiered_storage_object_reader>(
      std::move(stream), seek.delta.value(), _term, _aborted);
}

} // namespace cloud_topics::l1
