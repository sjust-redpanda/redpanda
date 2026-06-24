/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "proto/redpanda/core/admin/v2/cloud_topic_migration.proto.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage {
class remote;
}
namespace cloud_topics::l1 {
class replicated_metastore;
}
namespace cluster {
class topic_table;
}

namespace admin {

// Operator tooling for a tiered-storage -> cloud-topic migration. Reclaims the
// source tiered-storage objects that a migrated topic no longer references.
class cloud_topic_migration_service_impl
  : public proto::admin::cloud_topic_migration_service {
public:
    cloud_topic_migration_service_impl(
      ss::sharded<cloud_topics::l1::replicated_metastore>* metastore,
      ss::sharded<cluster::topic_table>* topic_table,
      ss::sharded<cloud_storage::remote>* remote)
      : _metastore(metastore)
      , _topic_table(topic_table)
      , _remote(remote) {}

    ss::future<proto::admin::reclaim_migrated_backing_response>
      reclaim_migrated_backing(
        serde::pb::rpc::context,
        proto::admin::reclaim_migrated_backing_request) override;

private:
    ss::sharded<cloud_topics::l1::replicated_metastore>* _metastore;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cloud_storage::remote>* _remote;
};

} // namespace admin
