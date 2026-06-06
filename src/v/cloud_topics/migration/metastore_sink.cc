/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/migration/metastore_sink.h"

// metastore.h transitively provides l1::imported_ts_info (via
// common/object_id.h) and l1::migration_phase (via metastore/state.h),
// whose packages are not directly visible here.
#include "cloud_topics/level_one/metastore/metastore.h"

namespace cloud_topics {

namespace {
using errc = archival::migration_metastore::errc;

errc to_errc(l1::metastore::errc e) {
    switch (e) {
    case l1::metastore::errc::transport_error:
        return errc::retry;
    case l1::metastore::errc::missing_ntp:
    case l1::metastore::errc::invalid_request:
    case l1::metastore::errc::out_of_range:
        return errc::invalid;
    }
    return errc::invalid;
}
} // namespace

ss::future<errc> migration_metastore_sink::append_imported(
  chunked_vector<imported_segment> segs) {
    chunked_vector<l1::metastore::imported_object> objs;
    objs.reserve(segs.size());
    for (auto& s : segs) {
        objs.push_back(
          l1::metastore::imported_object{
            .tidp = s.tidp,
            .term = s.term,
            .max_timestamp = s.max_timestamp,
            .size_bytes = s.size_bytes,
            .base_kafka_offset = s.base_kafka_offset,
            .imported = l1::imported_ts_info{
              .ts_path = std::move(s.ts_path),
              .delta_offset = s.delta_offset,
              .delta_offset_end = s.delta_offset_end,
              .segment_term = s.term,
              .last_kafka_offset = s.last_kafka_offset,
            }});
    }
    auto res = co_await _ms.append_imported_objects(std::move(objs));
    if (!res.has_value()) {
        co_return to_errc(res.error());
    }
    co_return errc::ok;
}

ss::future<errc> migration_metastore_sink::prune_below(
  const model::topic_id_partition& tidp, kafka::offset new_start) {
    // Prune the imported extents below new_start. The backing tiered-storage
    // objects are owned by the archiver during migration and are not deleted
    // here -- only the L1 rows are dropped (set_start_offset accounts the
    // extents as removed without touching object storage).
    auto res = co_await _ms.set_start_offset(tidp, new_start);
    if (!res.has_value()) {
        co_return to_errc(res.error());
    }
    co_return errc::ok;
}

ss::future<std::optional<archival::migration_metastore::offsets>>
migration_metastore_sink::get_offsets(const model::topic_id_partition& tidp) {
    auto res = co_await _ms.get_offsets(tidp);
    if (!res.has_value()) {
        co_return std::nullopt;
    }
    co_return offsets{
      .start_offset = res->start_offset,
      .next_offset = res->next_offset,
    };
}

ss::future<errc>
migration_metastore_sink::mark_complete(const model::topic_id_partition& tidp) {
    auto res = co_await _ms.set_migration_phase(
      tidp, l1::migration_phase::complete);
    if (!res.has_value()) {
        co_return to_errc(res.error());
    }
    co_return errc::ok;
}

} // namespace cloud_topics
