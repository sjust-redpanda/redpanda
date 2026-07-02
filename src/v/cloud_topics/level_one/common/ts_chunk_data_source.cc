/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/ts_chunk_data_source.h"

#include "base/vassert.h"

#include <seastar/core/coroutine.hh>

#include <fmt/format.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace cloud_topics::l1 {

ts_chunk_data_source::ts_chunk_data_source(
  ts_fetch_range_fn fetch,
  size_t start_pos,
  size_t total_len,
  size_t chunk_size,
  ss::abort_source* as)
  : _fetch(std::move(fetch))
  , _end(start_pos + total_len)
  , _chunk_size(chunk_size)
  , _next_chunk_start(start_pos - (start_pos % chunk_size))
  , _skip_head(start_pos % chunk_size)
  , _as(as) {
    vassert(
      chunk_size > 0, "ts_chunk_data_source requires a non-zero chunk size");
}

ss::future<ss::temporary_buffer<char>> ts_chunk_data_source::get() {
    while (true) {
        if (!_cur.has_value()) {
            if (_next_chunk_start >= _end) {
                // Past the requested range: true EOF.
                co_return ss::temporary_buffer<char>{};
            }
            auto chunk_end = std::min(_next_chunk_start + _chunk_size, _end);
            auto len = chunk_end - _next_chunk_start;
            auto stream = co_await _fetch(_next_chunk_start, len, _as);
            if (!stream.has_value()) {
                throw std::runtime_error(
                  fmt::format(
                    "failed to fetch imported TS chunk at byte {} (len {}): {}",
                    _next_chunk_start,
                    len,
                    std::to_underlying(stream.error())));
            }
            _cur = std::move(*stream);
            if (_skip_head > 0) {
                co_await _cur->skip(_skip_head);
                _skip_head = 0;
            }
            _next_chunk_start += _chunk_size;
        }
        auto buf = co_await _cur->read();
        if (buf.empty()) {
            // Current chunk exhausted; advance to the next.
            co_await _cur->close();
            _cur.reset();
            continue;
        }
        co_return buf;
    }
}

ss::future<> ts_chunk_data_source::close() {
    if (_cur.has_value()) {
        co_await _cur->close();
        _cur.reset();
    }
}

} // namespace cloud_topics::l1
