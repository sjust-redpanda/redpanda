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
#include "cloud_topics/read_replica/stm.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/partition_manager.h"
#include "kafka/data/cloud_topic_partition.h"
#include "kafka/data/cloud_topic_read_replica.h"
#include "kafka/data/replicated_partition.h"

namespace kafka {

template<typename Impl, typename... Args>
partition_proxy make_with_impl(Args&&... args) {
    return partition_proxy(std::make_unique<Impl>(std::forward<Args>(args)...));
}

partition_proxy
make_partition_proxy(const ss::lw_shared_ptr<cluster::partition>& partition) {
    // Structural IO gate: a partition that still holds tiered-storage data is
    // served as tiered storage -- a plain tiered partition, or a partition mid
    // tiered->cloud migration whose data still lives in tiered storage. This
    // keys on partition-raft state (the manifest), not the cloud_topic_enabled()
    // config flag, so it is robust to the unordered controller propagation of
    // the storage-mode flip. replicated_partition serves the whole partition
    // (local log + cloud manifest). Cutover (reset_metadata) clears both the
    // live manifest and the archive, after which the partition falls through to
    // the cloud-topic path below (reading its imported extents).
    //
    // "Still holds tiered data" must include the archive: spillover offloads the
    // oldest segments out of the live STM manifest into archive (spillover)
    // sub-manifests, and retention/GC runs on a migrating partition (the
    // archiver is not dormant until cutover). So the live manifest can reach 0
    // while data remains in the archive; keying only on it would misread a
    // spilled, still-migrating partition as cut over and route it to the
    // cloud-topic path before its extents were imported. holds_archived_data()
    // consults both the live manifest and the archive.
    const auto& archival_stm = partition->archival_meta_stm();
    if (archival_stm && archival_stm->holds_archived_data()) {
        return make_with_impl<replicated_partition>(partition);
    }

    auto is_ct = partition->get_ntp_config().cloud_topic_enabled();
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
