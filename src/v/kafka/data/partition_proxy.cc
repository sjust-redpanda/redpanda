/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "partition_proxy.h"

#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/read_replica/stm.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/partition_manager.h"
#include "kafka/data/cloud_topic_partition.h"
#include "kafka/data/cloud_topic_read_replica.h"
#include "kafka/data/logger.h"
#include "kafka/data/replicated_partition.h"

namespace kafka {

template<typename Impl, typename... Args>
partition_proxy make_with_impl(Args&&... args) {
    return partition_proxy(std::make_unique<Impl>(std::forward<Args>(args)...));
}

partition_proxy
make_partition_proxy(const ss::lw_shared_ptr<cluster::partition>& partition) {
    // Use the replicated ctp_stm migration boundary as the authoritative
    // signal for CT routing. The local NTP config is an eventually-consistent
    // projection of controller state: by the time alter-config returns to the
    // client the config has been applied to the topic_table but the reconcile
    // fiber may not yet have propagated it to this partition's NTP config, and
    // a newly-elected replica may not have seen the delta at all.  The STM
    // boundary, being Raft-replicated, is correct on every replica regardless.
    //
    // For partitions created in CT mode from the start (no prior tiered-storage
    // data) there is no migration and no boundary; fall back to
    // cloud_topic_enabled() which is stable by the time such a partition exists.
    auto ctp = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    std::optional<kafka::offset> boundary;
    if (ctp) {
        boundary = cloud_topics::ctp_stm_api{ctp}.get_ts_migration_boundary();
    }
    bool const is_ct = boundary.has_value()
                       || partition->get_ntp_config().cloud_topic_enabled();
    vlog(
      kdlog.info,
      "[{}] routing: ctp_stm={} migration_boundary={} cloud_topic_enabled={} "
      "-> is_ct={}",
      partition->ntp(),
      ctp ? "present" : "absent",
      boundary ? fmt::to_string(*boundary) : "none",
      partition->get_ntp_config().cloud_topic_enabled(),
      is_ct);
    auto is_rr = partition->is_read_replica_mode_enabled();
    if (is_ct) {
        auto ct_state = partition->get_cloud_topics_state();
        if (!ct_state || !ct_state->local_is_initialized()) {
            throw std::runtime_error(
              "Cloud topic partition can't be created because the cloud-topics "
              "subsystem is not initialized");
        }

        // Check for read replica first (before regular cloud topic)
        if (is_rr) {
            auto& stm_manager = partition->raft()->stm_manager();
            auto stm = stm_manager->get<cloud_topics::read_replica::stm>();
            if (!stm) {
                throw std::runtime_error("Read replica partition missing STM");
            }

            return make_with_impl<cloud_topics::read_replica::partition_proxy>(
              partition, stm, &ct_state->local());
        }

        auto frontend_instance = std::make_unique<cloud_topics::frontend>(
          partition, ct_state->local().get_data_plane());
        return make_with_impl<cloud_topic_partition>(
          partition, std::move(frontend_instance));
    }
    return make_with_impl<replicated_partition>(partition);
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ktp& ktp, cluster::partition_manager& cluster_pm) {
    auto partition = cluster_pm.get(ktp);
    if (partition) {
        return make_partition_proxy(partition);
    }
    return std::nullopt;
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp& ntp, cluster::partition_manager& cluster_pm) {
    auto partition = cluster_pm.get(ntp);
    if (partition) {
        return make_partition_proxy(partition);
    }
    return std::nullopt;
}

} // namespace kafka
