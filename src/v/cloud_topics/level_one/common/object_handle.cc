/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/object_handle.h"

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"

namespace cloud_topics::l1 {

l1_footer_index::l1_footer_index(footer f)
  : _footer(std::move(f)) {}

std::optional<seek_result> l1_footer_index::seek_to_offset(
  model::topic_id_partition tidp, kafka::offset offset) const {
    auto r = _footer.file_position_before_kafka_offset(tidp, offset);
    if (r == footer::npos) {
        return std::nullopt;
    }
    return seek_result{.file_position = r.file_position, .length = r.length};
}

std::optional<seek_result> l1_footer_index::seek_to_timestamp(
  model::topic_id_partition tidp, model::timestamp ts) const {
    auto r = _footer.file_position_before_max_timestamp(tidp, ts);
    if (r == footer::npos) {
        return std::nullopt;
    }
    return seek_result{.file_position = r.file_position, .length = r.length};
}

l1_native_object_handle::l1_native_object_handle(
  object_id oid, footer f, io* io, cloud_io::group_id group, bool skip_cache)
  : _oid(oid)
  , _index(std::move(f))
  , _io(io)
  , _group(group)
  , _skip_cache(skip_cache) {}

ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
l1_native_object_handle::open_reader(
  const seek_result& seek, ss::abort_source* as) {
    object_extent extent{
      .id = _oid,
      .position = seek.file_position,
      .size = seek.length,
    };
    auto stream_result = co_await _io->read_object(
      extent, as, _group, _skip_cache);
    if (!stream_result.has_value()) {
        co_return std::unexpected(stream_result.error());
    }
    co_return object_reader::create(std::move(stream_result).value());
}

} // namespace cloud_topics::l1
