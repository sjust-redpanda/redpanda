/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {

const model::topic test_topic{"tiered-topic"};
const model::ntp test_ntp{
  model::kafka_namespace, test_topic, model::partition_id{0}};
const model::topic_namespace test_tp_ns{model::kafka_namespace, test_topic};

} // namespace

class TsImportBoundaryTest
  : public cloud_topics::cluster_fixture
  , public ::testing::Test {
public:
    void SetUp() override {
        cfg.get("enable_leader_balancer").set_value(false);
        add_node();
        wait_for_all_members(5s).get();
    }

    ss::future<> create_tiered_topic() {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::tiered;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        co_await create_topic(
          model::topic_namespace_view{model::kafka_namespace, test_topic},
          /*partitions=*/1,
          /*replication_factor=*/1,
          props);
    }

    ss::future<> set_storage_mode(model::redpanda_storage_mode mode) {
        cluster::incremental_topic_updates updates;
        updates.storage_mode.op = cluster::incremental_update_operation::set;
        updates.storage_mode.value = mode;

        auto& topics_frontend
          = instance(model::node_id{0})
              ->app.controller->get_topics_frontend()
              .local();
        cluster::topic_properties_update update(test_tp_ns);
        update.properties = updates;
        cluster::topic_properties_update_vector updates_vec;
        updates_vec.push_back(std::move(update));
        auto results = co_await topics_frontend.update_topic_properties(
          std::move(updates_vec), model::no_timeout);
        RPTEST_REQUIRE_EQ_CORO(results.size(), 1);
        RPTEST_REQUIRE_EQ_CORO(results[0].ec, cluster::errc::success);
    }

    ss::future<>
    wait_for_leader(ss::lw_shared_ptr<cluster::partition>& out) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            auto [leader_fx, leader_p] = get_leader(test_ntp);
            if (!leader_fx) {
                return false;
            }
            out = leader_p;
            return true;
        });
    }

    ss::future<>
    wait_for_migration_boundary(
      ss::lw_shared_ptr<cluster::partition>& leader_p) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            auto ctp
              = leader_p->raft()
                  ->stm_manager()
                  ->get<cloud_topics::ctp_stm>();
            if (!ctp) {
                return false;
            }
            cloud_topics::ctp_stm_api api{ctp};
            return api.get_ts_migration_boundary().has_value();
        });
    }

    scoped_config cfg;
};

// Verify that promoting a tiered partition to tiered_cloud causes the
// ctp_stm to record a ts_migration_boundary.
TEST_F(TsImportBoundaryTest, BoundarySetOnStorageModePromotion) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();
    wait_for_migration_boundary(leader_p).get();

    auto ctp
      = leader_p->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    ASSERT_TRUE(ctp);
    cloud_topics::ctp_stm_api api{ctp};
    ASSERT_TRUE(api.get_ts_migration_boundary().has_value());
}

// Verify that promoting a tiered partition to cloud also causes the ctp_stm to
// record a ts_migration_boundary.
TEST_F(TsImportBoundaryTest, BoundarySetOnStorageModePromotionToCloud) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::cloud).get();
    wait_for_migration_boundary(leader_p).get();

    auto ctp
      = leader_p->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    ASSERT_TRUE(ctp);
    cloud_topics::ctp_stm_api api{ctp};
    ASSERT_TRUE(api.get_ts_migration_boundary().has_value());
}

// Verify that setting tiered_cloud a second time does not alter the boundary
// already recorded by the first transition.  The guard in _add_ctp_stm() and
// the !cloud_topic_enabled() transition check in update_configuration both
// prevent a second start_ts_import_cmd from being replicated.
TEST_F(TsImportBoundaryTest, BoundaryStableAfterDoublePromotion) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();
    wait_for_migration_boundary(leader_p).get();

    auto ctp
      = leader_p->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    ASSERT_TRUE(ctp);
    cloud_topics::ctp_stm_api api{ctp};
    const auto first_boundary = api.get_ts_migration_boundary();
    ASSERT_TRUE(first_boundary.has_value());

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();
    ss::sleep(200ms).get();

    EXPECT_EQ(api.get_ts_migration_boundary(), first_boundary);
}
