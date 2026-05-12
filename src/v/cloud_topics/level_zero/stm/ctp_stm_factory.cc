/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_factory.h"

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"

namespace cloud_topics::l0 {

bool ctp_stm_factory::is_applicable_for(
  const storage::ntp_config& ntp_cfg) const {
    if (ntp_cfg.is_read_replica_mode_enabled()) {
        return false;
    }
    if (ntp_cfg.cloud_topic_enabled()) {
        return true;
    }
    // Install ctp_stm as a passenger on all archival-enabled (tiered) partitions
    // so that it is already present in the STM manager when a partition is
    // promoted from tiered to tiered_cloud.  This avoids any need to install
    // the STM at migration time, and eliminates the window where a promoted
    // partition could receive writes before the STM is ready.
    //
    // A passenger ctp_stm is idle: it never applies placeholder batches and
    // holds no CT data.  get_max_collectible_offset() returns
    // model::offset::max() for a passenger (neither _ts_migration_boundary nor
    // _max_applied_epoch is set), so it does not compete with
    // archival_metadata_stm for the truncation floor.  The overhead is one
    // extra STM instance per tiered partition.
    return config::shard_local_cfg().cloud_storage_enabled()
           && ntp_cfg.is_archival_enabled();
}

void ctp_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<cloud_topics::ctp_stm>(
      cloud_topics::cd_log, raft);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace cloud_topics::l0
