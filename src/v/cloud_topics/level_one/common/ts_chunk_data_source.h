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

#include "cloud_topics/level_one/common/abstract_io.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <cstddef>
#include <expected>
#include <functional>
#include <optional>

namespace cloud_topics::l1 {

/// Returns a stream over an imported TS segment's bytes [file_position,
/// file_position+length). Copyable so a reader's data source can own its own
/// copy, decoupled from the object_handle's lifetime (the reader -- and thus
/// this fetch -- outlives the handle: level_one_reader re-reads a persisted
/// stream across slices without keeping the handle alive).
using ts_fetch_range_fn
  = std::function<ss::future<std::expected<ss::input_stream<char>, io::errc>>(
    size_t file_position, size_t length, ss::abort_source*)>;

/// A data source over an imported TS segment that fetches the byte range
/// [start_pos, start_pos+total_len) one fixed-size chunk at a time, lazily, and
/// presents the result as a single contiguous stream.
///
/// Only the chunks actually consumed are fetched: a reader that stops early
/// (max_offset/max_bytes) simply stops pulling, so the un-read tail is never
/// downloaded. Chunks are fixed byte ranges and need not be batch-aligned --
/// the source concatenates one chunk's tail with the next chunk's head, so a
/// batch spanning a boundary is read seamlessly. EOF is signalled only at the
/// true end (start_pos+total_len), so a consumer never mistakes a chunk
/// boundary for the end of the object.
///
/// A chunk-fetch failure surfaces as an exception from get(); the record-batch
/// reader propagates it (its read API has no error variant), matching how the
/// reader already reports stream errors.
class ts_chunk_data_source final : public ss::data_source_impl {
public:
    ts_chunk_data_source(
      ts_fetch_range_fn fetch,
      size_t start_pos,
      size_t total_len,
      size_t chunk_size,
      ss::abort_source* as);

    ss::future<ss::temporary_buffer<char>> get() override;
    ss::future<> close() override;

private:
    ts_fetch_range_fn _fetch;
    size_t _end;
    size_t _chunk_size;
    // Start of the next chunk to fetch, aligned down to a chunk boundary.
    size_t _next_chunk_start;
    // Bytes to discard from the first chunk so the stream starts at start_pos
    // (start_pos may sit mid-chunk). Zeroed after the first chunk.
    size_t _skip_head;
    std::optional<ss::input_stream<char>> _cur;
    ss::abort_source* _as;
};

} // namespace cloud_topics::l1
