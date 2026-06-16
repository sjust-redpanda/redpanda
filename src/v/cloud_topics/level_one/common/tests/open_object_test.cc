/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_io/scheduler_types.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "storage/record_batch_utils.h"

#include <seastar/util/defer.hh>

#include <absl/container/btree_set.h>
#include <gtest/gtest.h>

using namespace cloud_topics::l1;

namespace {

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::record_batch
make_batch(kafka::offset base, kafka::offset last, model::timestamp ts = {}) {
    int count = static_cast<int>(last - base) + 1;
    std::vector<size_t> record_sizes(count, 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = kafka::offset_cast(base),
        .count = count,
        .record_sizes = record_sizes,
        .timestamp = ts,
        .all_records_have_same_timestamp = true,
      });
}

/// Builds a 2-batch L1 object, stores it in `fio`, and returns the object info
/// along with the generated object_id.
struct test_object {
    object_id oid;
    object_builder::object_info info;
    model::topic_id_partition tidp;
};

test_object make_and_store(fake_io& fio) {
    auto tid = model::topic_id(uuid_t::create());
    auto tidp = model::topic_id_partition{tid, model::partition_id{0}};
    auto oid = create_object_id();

    iobuf buf;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(buf), {.indexing_interval = 1});
    auto _ = ss::defer([&builder] { builder->close().get(); });
    builder->start_partition(tidp).get();
    builder->add_batch(make_batch(0_o, 9_o)).get();
    builder->add_batch(make_batch(10_o, 19_o)).get();
    auto info = builder->finish().get();

    fio.put_object(oid, std::move(buf));
    return {.oid = oid, .info = std::move(info), .tidp = tidp};
}

} // namespace

// Build an L1 object with two batches (all batches indexed), store it in
// fake_io, then open it and seek to the second batch. Verify that the seek
// returns a non-zero file_position (the footer index was used) and no Kafka
// offset (native object, not an imported TS segment).
TEST(OpenObjectTest, SeekReturnsNonzeroPosition) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);

    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    // Seek to offset 10 — the start of the second batch.
    auto seek = handle->index().seek_to_offset(tidp, 10_o);
    ASSERT_TRUE(seek.has_value());
    EXPECT_GT(seek->file_position, size_t{0});
    EXPECT_FALSE(seek->kafka_offset.has_value());
}

// After seeking to the second batch, open a reader and verify that only batches
// at or after offset 10 are returned, and that at least one batch is returned.
TEST(OpenObjectTest, ReadReturnsBatchesAtOrAfterTarget) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);

    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 10_o);
    ASSERT_TRUE(seek.has_value());

    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    int batch_count = 0;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        if (!std::holds_alternative<model::record_batch>(item)) {
            continue;
        }
        const auto& batch = std::get<model::record_batch>(item);
        EXPECT_GE(batch.base_offset(), kafka::offset_cast(10_o))
          << "batch starts before seek target";
        ++batch_count;
    }
    EXPECT_GT(batch_count, 0) << "no batches returned after seek";
}

// Seeking with a topic_id_partition not present in the object must return
// nullopt.
TEST(OpenObjectTest, SeekUnknownTidpReturnsNullopt) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);

    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto unknown_tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    EXPECT_FALSE(handle->index().seek_to_offset(unknown_tidp, 0_o).has_value());
    EXPECT_FALSE(handle->index()
                   .seek_to_timestamp(unknown_tidp, model::timestamp{0})
                   .has_value());
}

// Calling open_object with an object_id not present in fake_io must return
// cloud_missing_object.
TEST(OpenObjectTest, MissingObjectReturnsError) {
    fake_io fio;
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = 100,
      .imported = std::nullopt,
    };
    auto result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), io::errc::cloud_missing_object);
}

// ── Group C: fake_io imported (TS segment) path ──────────────────────────────

namespace {

// Serialise a batch into the on-disk storage format used by TS segments:
// packed header followed by raw records.
iobuf batch_to_disk_iobuf(model::record_batch batch) {
    iobuf out;
    out.append(storage::batch_header_to_disk_iobuf(batch.header()));
    out.append(std::move(batch).release_data());
    return out;
}

// Build a TS segment (on-disk format) from a single batch.
iobuf make_ts_segment(model::record_batch batch) {
    return batch_to_disk_iobuf(std::move(batch));
}

// Build a batch of a given type starting at a given LOG offset.
model::record_batch
make_log_batch(model::offset base, int count, model::record_batch_type bt) {
    std::vector<size_t> record_sizes(static_cast<size_t>(count), 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = base,
        .count = count,
        .bt = bt,
        .record_sizes = record_sizes,
      });
}

// Build a transaction control batch (raft_data + control attr) at a given
// LOG offset.
model::record_batch make_control_batch(model::offset base, int count) {
    std::vector<size_t> record_sizes(static_cast<size_t>(count), 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = base,
        .count = count,
        .bt = model::record_batch_type::raft_data,
        .is_control = true,
        .record_sizes = record_sizes,
      });
}

// Build a transactional raft_data batch at a given LOG offset, with the given
// producer identity (so it can be matched against an aborted tx range).
model::record_batch make_txn_log_batch(
  model::offset base, int count, int64_t producer_id, int16_t producer_epoch) {
    std::vector<size_t> record_sizes(static_cast<size_t>(count), 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = base,
        .count = count,
        .bt = model::record_batch_type::raft_data,
        .producer_id = producer_id,
        .producer_epoch = producer_epoch,
        .is_transactional = true,
        .record_sizes = record_sizes,
      });
}

// Concatenate batches into a single TS segment (on-disk format).
iobuf make_ts_segment_multi(std::vector<model::record_batch> batches) {
    iobuf out;
    for (auto& b : batches) {
        out.append(batch_to_disk_iobuf(std::move(b)));
    }
    return out;
}

} // namespace

// With no injected .index, open_object builds an empty ts_segment_index (as
// file_io does when the segment's .index is absent): seek_to_offset falls back
// to {file_position=0, length=segment_size, kafka_offset=base} -- a
// full-segment scan whose delta the reader derives from the base Kafka offset.
TEST(OpenObjectTsTest, SeekReturnsFullSegment) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-1-v1.log";

    auto batch = make_batch(5_o, 14_o);
    auto segment = make_ts_segment(batch.copy());
    size_t segment_size = segment.size_bytes();

    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = segment_size,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 0_o, .last_kafka_offset = 9_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 5_o);
    ASSERT_TRUE(seek.has_value());
    EXPECT_EQ(seek->file_position, size_t{0});
    EXPECT_EQ(seek->length, segment_size);
    // No index entry: the seek reports the segment's base Kafka offset, which
    // the reader uses to derive the delta from the first batch.
    ASSERT_TRUE(seek->kafka_offset.has_value());
    EXPECT_EQ(*seek->kafka_offset, 0_o);
}

// Seeking past last_kafka_offset must return nullopt.
TEST(OpenObjectTsTest, SeekBeyondLastOffsetReturnsNullopt) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-1-v1.log";

    auto segment = make_ts_segment(make_batch(0_o, 9_o));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .last_kafka_offset = 9_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());

    EXPECT_FALSE(
      (*handle_result)->index().seek_to_offset(tidp, 10_o).has_value());
}

// open_reader on an imported handle reads batches with delta-translated kafka
// offsets. Log offset 15..19 with delta=5 must yield kafka offset 10..14.
TEST(OpenObjectTsTest, ReadBatchesWithDeltaTranslation) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-2-v1.log";

    // Log offsets 15..19 (kafka 10..14: base kafka 10, so derived delta=5)
    auto batch = make_batch(15_o, 19_o);
    auto segment = make_ts_segment(batch.copy());
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 10_o, .last_kafka_offset = 14_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 10_o);
    ASSERT_TRUE(seek.has_value());

    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    auto item = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
    const auto& got = std::get<model::record_batch>(item);
    EXPECT_EQ(got.base_offset(), kafka::offset_cast(10_o));
    EXPECT_EQ(got.last_offset(), kafka::offset_cast(14_o));
}

// With an injected .index, open_object seeks through the real ts_segment_index
// (not the full-segment fallback): seek_to_offset finds the byte position of
// the batch containing the target via offset_index::find_kaf_offset, derives
// the Kafka offset of the indexed entry, and the reader resumes at that
// position. Three raft_data batches, delta 5 (log = kafka + 5):
//   batch0 log [5,9]   kafka [0,4]   file_pos 0
//   batch1 log [10,14] kafka [5,9]   file_pos p1
//   batch2 log [15,19] kafka [10,14] file_pos p2
TEST(OpenObjectTsTest, IndexBackedSeekFindsBatchPosition) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-4-v1.log";

    auto d0 = batch_to_disk_iobuf(
      make_log_batch(model::offset{5}, 5, model::record_batch_type::raft_data));
    auto d1 = batch_to_disk_iobuf(make_log_batch(
      model::offset{10}, 5, model::record_batch_type::raft_data));
    auto d2 = batch_to_disk_iobuf(make_log_batch(
      model::offset{15}, 5, model::record_batch_type::raft_data));
    const auto p1 = static_cast<int64_t>(d0.size_bytes());
    const auto p2 = p1 + static_cast<int64_t>(d1.size_bytes());
    iobuf segment;
    segment.append(std::move(d0));
    segment.append(std::move(d1));
    segment.append(std::move(d2));
    const size_t sz = segment.size_bytes();

    // Build the segment's offset_index (rp=log, kaf=kafka, one entry per batch)
    // and serialize it as file_io downloads it.
    cloud_storage::offset_index oi(
      model::offset{5},
      kafka::offset{0},
      0,
      cloud_storage::remote_segment_sampling_step_bytes,
      model::timestamp::missing());
    oi.add(model::offset{5}, kafka::offset{0}, 0, model::timestamp{1});
    oi.add(model::offset{10}, kafka::offset{5}, p1, model::timestamp{2});
    oi.add(model::offset{15}, kafka::offset{10}, p2, model::timestamp{3});

    fio.put_ts_segment(ts_path, std::move(segment), {}, oi.to_iobuf());

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 0_o, .last_kafka_offset = 14_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    // Seek to a kafka offset inside batch1 -> its (non-zero) byte position,
    // reporting the indexed entry's Kafka offset (5) at that position.
    auto seek = handle->index().seek_to_offset(tidp, 5_o);
    ASSERT_TRUE(seek.has_value());
    EXPECT_EQ(seek->file_position, static_cast<size_t>(p1));
    ASSERT_TRUE(seek->kafka_offset.has_value());
    EXPECT_EQ(*seek->kafka_offset, 5_o);

    // Batch boundaries either side: first batch at 0, third batch at p2.
    auto seek0 = handle->index().seek_to_offset(tidp, 0_o);
    ASSERT_TRUE(seek0.has_value());
    EXPECT_EQ(seek0->file_position, size_t{0});
    auto seek2 = handle->index().seek_to_offset(tidp, 10_o);
    ASSERT_TRUE(seek2.has_value());
    EXPECT_EQ(seek2->file_position, static_cast<size_t>(p2));

    // Reading from the indexed position yields batch1 first (kafka [5,9]).
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    auto item = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
    const auto& got = std::get<model::record_batch>(item);
    EXPECT_EQ(got.base_offset(), kafka::offset_cast(5_o));
    EXPECT_EQ(got.last_offset(), kafka::offset_cast(9_o));
}

// A TS segment is a raw redpanda log: besides raft_data it can hold
// offset-translator batches (e.g. raft_configuration) and other non-data
// batches (e.g. tx_fence). The reader must surface ONLY raft_data, advancing
// the running delta for offset-translator batches by their offset span and
// dropping everything else without moving the delta. Layout (log offsets):
//   raft_configuration @ [0,1]  delta 0 -> 2   (translator: advances delta)
//   tx_fence           @ [2]    dropped, delta stays 2 (leaves a kafka gap)
//   raft_data          @ [3,5]  kafka [1,3]    (3 - delta 2 .. 5 - delta 2)
TEST(OpenObjectTsTest, ReadImportedExtentEmitsOnlyRaftData) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-3-v1.log";

    std::vector<model::record_batch> batches;
    batches.push_back(make_log_batch(
      model::offset{0}, 2, model::record_batch_type::raft_configuration));
    batches.push_back(
      make_log_batch(model::offset{2}, 1, model::record_batch_type::tx_fence));
    batches.push_back(
      make_log_batch(model::offset{3}, 3, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 0_o, .last_kafka_offset = 3_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());

    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 1u) << "only the raft_data batch should surface";
    EXPECT_EQ(got[0].header().type, model::record_batch_type::raft_data);
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(1_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(3_o));
}

// Transaction control batches (commit/abort markers) are raft_data with the
// control attribute set. They must never be surfaced to Kafka clients (native
// CT L1 strips them at reconciliation). A control batch still consumes a Kafka
// offset, leaving a gap. Layout (log offsets, delta 0):
//   raft_data         @ [0,2]  -> kafka [0,2]
//   control           @ [3]    -> dropped (kafka gap at 3)
//   raft_data         @ [4,5]  -> kafka [4,5]
TEST(OpenObjectTsTest, ReadImportedExtentDropsControlBatches) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-4-v1.log";

    std::vector<model::record_batch> batches;
    batches.push_back(
      make_log_batch(model::offset{0}, 3, model::record_batch_type::raft_data));
    batches.push_back(make_control_batch(model::offset{3}, 1));
    batches.push_back(
      make_log_batch(model::offset{4}, 2, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 0_o, .last_kafka_offset = 5_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u) << "control batch must not surface";
    for (const auto& b : got) {
        EXPECT_FALSE(b.header().attrs.is_control());
    }
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(4_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(5_o));
}

// Aborted-transaction data must be stripped so the imported region is
// committed-only (like native CT L1). Committed transactional data survives.
// Layout (log offsets, delta 0), all transactional:
//   pid 99 @ [0,2]  committed -> kafka [0,2]
//   pid 42 @ [3,4]  ABORTED   -> dropped (kafka gap at 3,4)
//   pid 99 @ [5,5]  committed -> kafka [5,5]
// Aborted range: {pid 42, [3,4]}.
TEST(OpenObjectTsTest, ReadImportedExtentStripsAbortedData) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-5-v1.log";

    std::vector<model::record_batch> batches;
    batches.push_back(make_txn_log_batch(model::offset{0}, 3, 99, 0));
    batches.push_back(make_txn_log_batch(model::offset{3}, 2, 42, 0));
    batches.push_back(make_txn_log_batch(model::offset{5}, 1, 99, 0));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();

    absl::btree_set<model::tx_range, std::greater<>> aborted;
    aborted.insert(
      model::tx_range{
        model::producer_identity{42, 0}, model::offset{3}, model::offset{4}});
    fio.put_ts_segment(ts_path, std::move(segment), std::move(aborted));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 0_o, .last_kafka_offset = 5_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u) << "aborted batch must be stripped";
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(5_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(5_o));
}

// An extent whose data is entirely aborted yields no batches.
TEST(OpenObjectTsTest, ReadImportedExtentAllAborted) {
    fake_io fio;
    const ss::sstring ts_path = "00000000000000000000-6-v1.log";

    auto segment = make_ts_segment(
      make_txn_log_batch(model::offset{0}, 5, 7, 0));
    size_t sz = segment.size_bytes();

    absl::btree_set<model::tx_range, std::greater<>> aborted;
    aborted.insert(
      model::tx_range{
        model::producer_identity{7, 0}, model::offset{0}, model::offset{4}});
    fio.put_ts_segment(ts_path, std::move(segment), std::move(aborted));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .base_kafka_offset = 0_o, .last_kafka_offset = 4_o},
    };
    auto handle_result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_to_offset(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    auto item = reader->read_next().get();
    EXPECT_TRUE(std::holds_alternative<object_reader::eof>(item))
      << "all data aborted: reader must yield no batches";
}

// open_object with an imported extent whose ts_path was never injected must
// return cloud_missing_object.
TEST(OpenObjectTsTest, MissingTsSegmentReturnsError) {
    fake_io fio;
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = 100,
      .imported
      = imported_ts_info{.ts_path = "no-such-segment.log", .last_kafka_offset = 9_o},
    };
    auto result
      = fio.open_object(extent, &as, cloud_io::group_id::default_group).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), io::errc::cloud_missing_object);
}
