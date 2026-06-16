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
    EXPECT_FALSE(seek->delta.has_value());
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
