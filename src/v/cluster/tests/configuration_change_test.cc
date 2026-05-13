// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/test_env.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sstring.hh>

FIXTURE_TEST(test_updating_node_rpc_ip_address, cluster_test_fixture) {
    // add three nodes
    model::node_id node_0(0);
    model::node_id node_1(1);
    model::node_id node_2(2);
    create_node_application(node_0);
    wait_for_controller_leadership(node_0).get();
    create_node_application(node_1);
    [[maybe_unused]] auto node_app_2 = create_node_application(node_2);

    // wait for cluster to be stable
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [this, node_0, node_1, node_2] {
          return get_local_cache(node_0).node_count() == 3
                 && get_local_cache(node_1).node_count() == 3
                 && get_local_cache(node_2).node_count() == 3;
      })
      .get();

    remove_node_application(node_2);
    // Change RPC port to a new dynamically allocated port
    const auto new_rpc_base = static_cast<int>(test_env::find_free_port());
    info("Restarting node {} with changed configuration", node_2);
    node_app_2 = create_node_application(
      node_2, cluster_test_default_kafka_port(), new_rpc_base);

    tests::cooperative_spin_wait_with_timeout(
      5s,
      [this, node_0, node_1, node_2, new_rpc_base] {
          auto meta = get_local_cache(node_0).get_node_metadata(node_2);
          if (!meta) {
              return false;
          }
          auto broker = get_local_cache(node_1).get_node_metadata(node_2);
          if (!broker || meta.value().broker != broker.value().broker) {
              return false;
          }

          broker = get_local_cache(node_1).get_node_metadata(node_2);
          if (!broker || meta.value().broker != broker.value().broker) {
              return false;
          }

          return meta.value().broker.rpc_address()
                 == net::unresolved_address(
                   "127.0.0.1", new_rpc_base + node_2());
      })
      .get();
}

FIXTURE_TEST(test_single_node_update, cluster_test_fixture) {
    // single node
    model::node_id node_id(0);

    [[maybe_unused]] auto node = create_node_application(node_id);
    wait_for_controller_leadership(node_id).get();

    remove_node_application(node_id);
    // Change kafka port to a new dynamically allocated port
    const auto new_kafka_base = static_cast<int>(test_env::find_free_port());
    const auto new_rpc_base2 = static_cast<int>(test_env::find_free_port());
    node = create_node_application(node_id, new_kafka_base, new_rpc_base2);

    tests::cooperative_spin_wait_with_timeout(
      5s,
      [this, node_id, new_kafka_base] {
          auto meta = get_local_cache(node_id).get_node_metadata(node_id);
          info("updated broker {}", meta);
          if (!meta) {
              return false;
          }

          return meta->broker.kafka_advertised_listeners()[0].address
                 == net::unresolved_address(
                   "127.0.0.1", new_kafka_base + node_id());
      })
      .get();
}
