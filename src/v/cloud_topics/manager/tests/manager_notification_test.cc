/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/manager/manager.h"
#include "cluster/partition.h"
#include "cluster/notification.h"
#include "cluster/topic_configuration.h"
#include "cluster/utils/partition_change_notifier.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>

using namespace cloud_topics;
using notif_type = cluster::partition_change_notifier::notification_type;
using partition_state = cluster::partition_change_notifier::partition_state;
using notify_current_state
  = cluster::partition_change_notifier::notify_current_state;

namespace {

// Fake notifier that captures the registered callback and lets tests fire it.
class fake_notifier : public cluster::partition_change_notifier {
public:
    cluster::notification_id_type register_partition_notifications(
      notification_cb_t cb, notify_current_state) override {
        cb_ = std::move(cb);
        return cluster::notification_id_type{0};
    }

    void unregister_partition_notifications(
      cluster::notification_id_type) override {}

    void fire(
      notif_type t,
      const model::ntp& ntp,
      std::optional<partition_state> state) {
        cb_(t, ntp, std::move(state));
    }

private:
    notification_cb_t cb_;
};

// Build a tiered_cloud topic_configuration with a valid tp_id so the
// manager's dispatch loop does not fall through to the topic_table_ lookup.
cluster::topic_configuration make_cloud_topic_cfg(const model::ntp& ntp) {
    cluster::topic_configuration cfg{
      ntp.ns,
      ntp.tp.topic,
      /*partition_count=*/1,
      /*replication_factor=*/1,
      model::topic_id::create()};
    cfg.properties.storage_mode = model::redpanda_storage_mode::tiered_cloud;
    return cfg;
}

const model::ntp test_ntp{
  model::kafka_namespace, model::topic{"test-topic"}, model::partition_id{0}};

} // namespace

// Base fixture: creates the manager and fake notifier but intentionally does
// NOT call start().  Tests register their callbacks and then call start()
// themselves, matching the documented pre-start registration requirement.
class ManagerNotificationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto owned = std::make_unique<fake_notifier>();
        notifier_ = owned.get();
        manager_ = std::make_unique<cloud_topics_manager>(
          std::move(owned),
          [](const model::ntp&) -> ss::lw_shared_ptr<cluster::partition> {
              return {};
          });
    }

    void TearDown() override {
        // stop() is a no-op when start() was never called.
        manager_->stop().get();
    }

    void fire(notif_type t, bool is_leader) {
        notifier_->fire(
          t,
          test_ntp,
          partition_state{
            model::term_id{1},
            is_leader,
            make_cloud_topic_cfg(test_ntp)});
    }

    fake_notifier* notifier_{nullptr};
    std::unique_ptr<cloud_topics_manager> manager_;
};

// partition_properties_change while already a leader must fire both
// ctp_callbacks_ (the on_leadership_change path added by the fix) and
// ctp_prop_change_callbacks_ (the on_leadership_or_properties_change path).
TEST_F(ManagerNotificationTest, PropertiesChangeLeaderFiresBothCallbacks) {
    int leader_cb_count = 0;
    int props_cb_count = 0;

    manager_->on_ctp_partition_leader(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++leader_cb_count;
      });
    manager_->on_ctp_leader_properties_change(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++props_cb_count;
      });

    manager_->start().get();

    fire(notif_type::partition_properties_change, /*is_leader=*/true);

    EXPECT_EQ(leader_cb_count, 1);
    EXPECT_EQ(props_cb_count, 1);
}

// partition_properties_change when not a leader must fire only
// ctp_prop_change_callbacks_; the on_leadership_change guard must prevent
// ctp_callbacks_ from firing.
TEST_F(ManagerNotificationTest, PropertiesChangeFollowerFiresOnlyPropsCallback) {
    int leader_cb_count = 0;
    int props_cb_count = 0;

    manager_->on_ctp_partition_leader(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++leader_cb_count;
      });
    manager_->on_ctp_leader_properties_change(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++props_cb_count;
      });

    manager_->start().get();

    fire(notif_type::partition_properties_change, /*is_leader=*/false);

    EXPECT_EQ(leader_cb_count, 0);
    EXPECT_EQ(props_cb_count, 1);
}

// leadership_change falls through to on_leadership_or_properties_change so
// both callback sets must fire exactly once.
TEST_F(ManagerNotificationTest, LeadershipChangeFiresBothCallbacks) {
    int leader_cb_count = 0;
    int props_cb_count = 0;

    manager_->on_ctp_partition_leader(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++leader_cb_count;
      });
    manager_->on_ctp_leader_properties_change(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++props_cb_count;
      });

    manager_->start().get();

    fire(notif_type::leadership_change, /*is_leader=*/true);

    EXPECT_EQ(leader_cb_count, 1);
    EXPECT_EQ(props_cb_count, 1);
}

// Notifications for non-cloud topics (tiered but not tiered_cloud) must be
// silently ignored by both callback sets.
TEST_F(ManagerNotificationTest, NonCloudTopicIgnored) {
    int leader_cb_count = 0;
    int props_cb_count = 0;

    manager_->on_ctp_partition_leader(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++leader_cb_count;
      });
    manager_->on_ctp_leader_properties_change(
      [&](const model::ntp&,
          model::topic_id_partition,
          ss::optimized_optional<
            ss::lw_shared_ptr<cluster::partition>>&) noexcept {
          ++props_cb_count;
      });

    manager_->start().get();

    cluster::topic_configuration cfg{
      test_ntp.ns,
      test_ntp.tp.topic,
      1,
      1,
      model::topic_id::create()};
    cfg.properties.storage_mode = model::redpanda_storage_mode::tiered;
    notifier_->fire(
      notif_type::partition_properties_change,
      test_ntp,
      partition_state{model::term_id{1}, /*is_leader=*/true, cfg});

    EXPECT_EQ(leader_cb_count, 0);
    EXPECT_EQ(props_cb_count, 0);
}
