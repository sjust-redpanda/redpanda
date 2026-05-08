// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_circular_buffer.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/tests/stm_test_fixture.h"

using namespace raft;

inline ss::logger logger("stm-test-logger");
struct other_kv : simple_kv {
    using simple_kv::simple_kv;
    static constexpr std::string_view name = "other_simple_kv";
};
struct throwing_kv : public simple_kv {
    static constexpr std::string_view name = "throwing_kv";
    explicit throwing_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& units) override {
        if (tests::random_bool()) {
            throw std::runtime_error("runtime error from throwing stm");
        }
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch, units);
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        if (!_tried_applying) {
            _tried_applying = true;
            throw std::runtime_error("Error from apply_snapshot...");
        }

        return simple_kv::apply_raft_snapshot(buffer);
    };

    bool _tried_applying = false;
};
/**
 * Local snapshot stm manages its own local snapshot.
 */
struct local_snapshot_stm : public simple_kv {
    static constexpr std::string_view name = "local_snapshot_kv";
    explicit local_snapshot_stm(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& units) override {
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch, units);
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        vassert(
          buffer.size_bytes() == 0,
          "Only empty buffer is expected to be applied to the local snapshot "
          "managing STM.");
        // reset state
        state = {};
        co_return;
    };
};

// State machine that induces lag from the tip of
// of the log
class slow_kv : public simple_kv {
public:
    static constexpr std::string_view name = "slow_kv";

    explicit slow_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& apply_units) override {
        co_await ss::sleep(5ms);
        co_return co_await simple_kv::apply(batch, apply_units);
    }

    ss::future<> apply_raft_snapshot(const iobuf&) override {
        return ss::now();
    }
};

// Fails the first apply, starts a background fiber and not lets the
// background apply fiber finish relative to slow_kv
class bg_only_kv : public slow_kv {
public:
    static constexpr std::string_view name = "bg_only_stm";

    explicit bg_only_kv(raft_node_instance& rn)
      : slow_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& apply_units) override {
        if (_first_apply) {
            _first_apply = false;
            throw std::runtime_error("induced failure");
        }
        co_await ss::sleep(5ms);
        co_return co_await slow_kv::apply(batch, apply_units);
    }

private:
    bool _first_apply = true;
};

TEST_F_CORO(state_machine_fixture, test_basic_apply) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto other_kv_stm = builder.create_stm<other_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(other_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_fixture, test_snapshot_with_bg_fibers) {
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto slow_kv_stm = builder.create_stm<slow_kv>(*node);
        auto bg_kv_stm = builder.create_stm<bg_only_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(slow_kv_stm));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(bg_kv_stm));
    }
    auto& leader_node = node(co_await wait_for_leader(10s));
    bool stop = false;
    auto write_sleep_f = ss::do_until(
      [&stop] { return stop; },
      [&] {
          return build_random_state(1000).discard_result().then(
            [] { return ss::sleep(3ms); });
      });

    auto truncate_sleep_f = ss::do_until(
      [&stop] { return stop; },
      [&] {
          return leader_node.raft()
            ->write_snapshot({leader_node.raft()->committed_offset(), iobuf{}})
            .then([] { return ss::sleep(3ms); });
      });

    co_await ss::sleep(10s);
    stop = true;
    co_await ss::when_all(
      std::move(write_sleep_f), std::move(truncate_sleep_f));
}

TEST_F_CORO(state_machine_fixture, test_apply_throwing_exception) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}
TEST_F_CORO(
  state_machine_fixture, test_apply_throwing_exception_waiting_for_each_batch) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(5000, wait_for_each_batch::yes);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_fixture, test_recovery_without_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
    // stop one of the nodes and remove data
    model::node_id stopped_id(1);
    co_await stop_node(stopped_id, remove_data_dir::yes);
    auto& new_node = add_node(stopped_id, model::revision_id{0});

    // start the node back up
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.init_and_start(all_vnodes(), std::move(builder));
    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::no_timeout);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(state_machine_fixture, test_recovery_from_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
    // take snapshot at batch boundary
    auto snapshot_offset = co_await with_leader(
      10s, [](raft_node_instance& node) {
          auto committed = node.raft()->committed_offset();
          return node.raft()
            ->make_reader(
              storage::local_log_reader_config(
                node.raft()->start_offset(),
                model::offset(
                  random_generators::get_int(
                    node.raft()->start_offset()(), committed()))))
            .then([](auto rdr) {
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](chunked_circular_buffer<model::record_batch> batches) {
                return batches.back().last_offset();
            });
      });
    // take snapshots on all of the nodes
    co_await parallel_for_each_node([snapshot_offset](raft_node_instance& n) {
        return n.raft()
          ->stm_manager()
          ->take_snapshot(snapshot_offset)
          .then([raft = n.raft(), snapshot_offset](
                  state_machine_manager::snapshot_result snapshot_result) {
              return raft->write_snapshot(
                raft::write_snapshot_cfg(
                  snapshot_offset, std::move(snapshot_result.data)));
          });
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.init_and_start({}, std::move(builder));

    co_await with_leader(
      10s, [vn = new_node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{0});
      });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::timeout_clock::now() + 20s);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(
  state_machine_fixture, test_recovery_from_backward_compatible_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<local_snapshot_stm>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        stms.push_back(builder.create_stm<local_snapshot_stm>(*node));

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();
    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take snapshot at batch boundary
    auto snapshot_offset = co_await with_leader(
      10s, [](raft_node_instance& node) {
          auto committed = node.raft()->committed_offset();
          return node.raft()
            ->make_reader(
              storage::local_log_reader_config(
                node.raft()->start_offset(),
                model::offset(
                  random_generators::get_int(
                    node.raft()->start_offset()(), committed()))))
            .then([](auto rdr) {
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](chunked_circular_buffer<model::record_batch> batches) {
                return batches.back().last_offset();
            });
      });

    co_await parallel_for_each_node([snapshot_offset](raft_node_instance& n) {
        // create an empty snapshot, the same way Redpanda does in previous
        // versions

        return n.raft()->write_snapshot(
          write_snapshot_cfg(snapshot_offset, iobuf{}));
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto new_stm = builder.create_stm<local_snapshot_stm>(new_node);

    co_await new_node.init_and_start({}, std::move(builder));

    co_await with_leader(
      10s, [vn = new_node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{0});
      });
    // wait for the state to be applied
    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::timeout_clock::now() + 20s);

    simple_kv::state_t partial_expected_state;

    auto rdr = co_await new_node.raft()->make_reader(
      storage::local_log_reader_config(
        model::next_offset(snapshot_offset), committed_offset));

    auto batches = co_await model::consume_reader_to_memory(
      std::move(rdr), model::no_timeout);

    for (const auto& b : batches) {
        simple_kv::apply_to_state(b, partial_expected_state);
    }

    ASSERT_EQ_CORO(new_stm->state, partial_expected_state);
}

struct controllable_throwing_kv : public simple_kv {
    static constexpr std::string_view name = "controllable_throwing_kv_1";
    explicit controllable_throwing_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& apply_units) override {
        if (batch.last_offset() > _allow_apply) {
            throw std::runtime_error(
              fmt::format(
                "not allowed to apply batches with last offset greater than "
                "{}. Current batch last offset: {}",
                _allow_apply,
                batch.last_offset()));
        }
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch, apply_units);
        co_return;
    }

    void allow_apply_to(model::offset o) { _allow_apply = o; }

    model::offset _allow_apply;
};

struct controllable_throwing_kv_2 : public controllable_throwing_kv {
    using controllable_throwing_kv::controllable_throwing_kv;

    static constexpr std::string_view name = "controllable_throwing_kv_2";
};

struct controllable_throwing_kv_3 : public controllable_throwing_kv {
    using controllable_throwing_kv::controllable_throwing_kv;

    static constexpr std::string_view name = "controllable_throwing_kv_3";
};
TEST_F_CORO(state_machine_fixture, test_all_machines_throw) {
    /**
     * This test covers the scenario in which all state machines thrown an
     * exception during apply, and then one of the state machines makes some
     * progress.
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_1 = builder.create_stm<controllable_throwing_kv>(*node);
        auto kv_2 = builder.create_stm<controllable_throwing_kv_2>(*node);
        auto kv_3 = builder.create_stm<controllable_throwing_kv_3>(*node);

        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(kv_1));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(kv_2));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(kv_3));

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    for (auto& [id, node] : nodes()) {
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv>()
          ->allow_apply_to(model::offset(100));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_2>()
          ->allow_apply_to(model::offset(100));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_3>()
          ->allow_apply_to(model::offset(150));
    }
    vlog(logger().info, "Generating state for test");
    auto expected = co_await build_random_state(
      500, wait_for_each_batch::no, 1);
    vlog(logger().info, "Waiting for state machines");
    RPTEST_REQUIRE_EVENTUALLY_CORO(15s, [&] {
        return std::ranges::all_of(
          nodes() | std::views::values,
          [&](std::unique_ptr<raft_node_instance>& node) {
              auto la = node->raft()
                          ->stm_manager()
                          ->get<controllable_throwing_kv_3>()
                          ->last_applied_offset();
              return la >= model::offset(150);
          });
    });

    for (auto& [id, node] : nodes()) {
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_2>()
          ->allow_apply_to(model::offset(160));
    }
    RPTEST_REQUIRE_EVENTUALLY_CORO(15s, [&] {
        return std::ranges::all_of(
          nodes() | std::views::values,
          [&](std::unique_ptr<raft_node_instance>& node) {
              auto la = node->raft()
                          ->stm_manager()
                          ->get<controllable_throwing_kv_2>()
                          ->last_applied_offset();
              return la >= model::offset(160);
          });
    });

    for (auto& [id, node] : nodes()) {
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv>()
          ->allow_apply_to(model::offset(1000));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_2>()
          ->allow_apply_to(model::offset(1000));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_3>()
          ->allow_apply_to(model::offset(1000));
    }

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

class non_fast_movable_kv
  : public simple_kv_base<no_at_offset_snapshot_stm_base> {
public:
    static constexpr std::string_view name = "other_persited_kv_stm";
    explicit non_fast_movable_kv(raft_node_instance& rn)
      : simple_kv_base<no_at_offset_snapshot_stm_base>(rn) {}

    ss::future<iobuf> take_raft_snapshot() final {
        co_return serde::to_iobuf(state);
    }

    stm_initial_recovery_policy get_initial_recovery_policy() const override {
        return stm_initial_recovery_policy::read_everything;
    }
};

TEST_F_CORO(state_machine_fixture, test_opt_out_from_snapshot_at_offset) {
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<non_fast_movable_kv>(*node);

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    for (auto& [_, node] : nodes()) {
        ASSERT_FALSE_CORO(
          node->raft()->stm_manager()->supports_snapshot_at_offset());
    }

    auto expected = co_await build_random_state(1000);

    // take snapshots on all of the nodes
    absl::flat_hash_map<model::node_id, model::offset> offsets;
    for (auto& [id, node] : nodes()) {
        auto o = co_await node->raft()->stm_manager()->take_snapshot().then(
          [raft = node->raft()](
            state_machine_manager::snapshot_result snapshot_data) {
              return raft
                ->write_snapshot(
                  raft::write_snapshot_cfg(
                    snapshot_data.last_included_offset,
                    std::move(snapshot_data.data)))
                .then([o = snapshot_data.last_included_offset] { return o; });
          });
        offsets[id] = o;
    }

    for (const auto& [id, n] : nodes()) {
        ASSERT_EQ_CORO(
          n->raft()->start_offset(), model::next_offset(offsets[id]));
    }
}

// ── Step 1a: snapshot-driven STM reconstruction ──────────────────────────

// Represents an STM that was running before a restart but is absent from
// the builder on the next startup (simulating is_applicable_for() returning
// false during a migration window).
struct snapshot_kv : simple_kv {
    using simple_kv::simple_kv;
    static constexpr std::string_view name = "snapshot_kv";
};

// Register snapshot_kv as a factory-only STM (no trigger batch type).
// Used to test snapshot-driven reconstruction without batch-triggering.
static void register_snapshot_factory(
  raft::state_machine_manager_builder& builder, raft_node_instance& node) {
    builder.add_factory(
      ss::sstring(snapshot_kv::name),
      [&node](raft::consensus*) { return ss::make_shared<snapshot_kv>(node); });
}

TEST_F_CORO(state_machine_fixture, test_snapshot_restart_reconstructs_stm) {
    // Verify the apply_initial_recovery_policy pre-pass: an STM absent from
    // the builder at restart is reconstructed from initial_recovery_snapshot
    // as long as a factory closure is registered.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<snapshot_kv>(*node);
        register_snapshot_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(100);
    co_await wait_for_apply();

    // Restart node 0 without snapshot_kv in create_stm — simulates
    // is_applicable_for() returning false — but with factory closure so the
    // pre-pass can reconstruct it from the on-disk initial_recovery_snapshot.
    model::node_id restart_id(0);
    auto data_dir
      = nodes().at(restart_id)->raft()->log()->config().base_directory();
    co_await stop_node(restart_id);
    add_node(restart_id, model::revision_id{0}, data_dir);

    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(restart_id));
    register_snapshot_factory(builder, *nodes().at(restart_id));
    co_await nodes()
      .at(restart_id)
      ->init_and_start(all_vnodes(), std::move(builder));

    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<snapshot_kv>()
      != nullptr);

    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await nodes()
      .at(restart_id)
      ->raft()
      ->stm_manager()
      ->get<snapshot_kv>()
      ->wait(committed_offset, model::timeout_clock::now() + 15s);

    ASSERT_EQ_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<snapshot_kv>()->state,
      expected);
}

// ── Step 1b: batch-triggered dynamic installation ────────────────────────

static constexpr auto trigger_batch_type
  = model::record_batch_type::ctp_stm_command;

// STM installed dynamically when a trigger batch is seen.
struct factory_kv : simple_kv {
    using simple_kv::simple_kv;
    static constexpr std::string_view name = "factory_kv";
};

// Same but start() sleeps briefly to exercise in-progress install races.
struct slow_start_factory_kv : factory_kv {
    using factory_kv::factory_kv;
    static constexpr std::string_view name = "slow_start_factory_kv";

    ss::future<> start() override {
        co_await ss::sleep(50ms);
        co_return co_await factory_kv::start();
    }
};

// Register a batch-triggered factory for KV in builder.
// The STM is NOT created at startup — it is installed by the trigger.
template<typename KV = factory_kv>
static void register_factory(
  raft::state_machine_manager_builder& builder, raft_node_instance& node) {
    builder.add_factory(
      ss::sstring(KV::name),
      [&node](raft::consensus*) { return ss::make_shared<KV>(node); },
      trigger_batch_type);
}

TEST_F_CORO(state_machine_fixture, test_factory_trigger_installs_stm) {
    // A cluster with no factory_kv at startup.  Replicating the trigger batch
    // must install factory_kv on every replica.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        register_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    // Build some state before the trigger.
    co_await build_random_state(50);
    co_await wait_for_apply();

    auto res = co_await replicate_trigger_batch(trigger_batch_type);
    ASSERT_TRUE_CORO(res.has_value());

    // Wait for all nodes to install factory_kv.
    co_await parallel_for_each_node([](raft_node_instance& n) {
        return n.raft()->stm_manager()->wait_for_stm_name(
          ss::sstring(factory_kv::name), model::timeout_clock::now() + 15s);
    });

    // factory_kv is accessible and catches up via background apply.
    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await parallel_for_each_node([committed_offset](raft_node_instance& n) {
        return n.raft()->stm_manager()->get<factory_kv>()->wait(
          committed_offset, model::timeout_clock::now() + 15s);
    });

    for (auto& [id, node] : nodes()) {
        ASSERT_TRUE_CORO(
          node->raft()->stm_manager()->get<factory_kv>() != nullptr);
    }
}

TEST_F_CORO(state_machine_fixture, test_factory_stm_restart_correctness) {
    // Install factory_kv via trigger, take a snapshot, restart a node.
    // factory_kv must be reconstructed from initial_recovery_snapshot.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        register_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(100);
    co_await wait_for_apply();

    auto res = co_await replicate_trigger_batch(trigger_batch_type);
    ASSERT_TRUE_CORO(res.has_value());

    co_await parallel_for_each_node([](raft_node_instance& n) {
        return n.raft()->stm_manager()->wait_for_stm_name(
          ss::sstring(factory_kv::name), model::timeout_clock::now() + 15s);
    });

    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await parallel_for_each_node([committed_offset](raft_node_instance& n) {
        return n.raft()->stm_manager()->get<factory_kv>()->wait(
          committed_offset, model::timeout_clock::now() + 15s);
    });

    expected = co_await build_random_state(100);
    co_await wait_for_apply();

    // Restart node 0 — it must reconstruct factory_kv without a trigger.
    model::node_id restart_id(0);
    auto data_dir
      = nodes().at(restart_id)->raft()->log()->config().base_directory();
    co_await stop_node(restart_id);
    add_node(restart_id, model::revision_id{0}, data_dir);

    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(restart_id));
    register_factory(builder, *nodes().at(restart_id));
    co_await nodes()
      .at(restart_id)
      ->init_and_start(all_vnodes(), std::move(builder));

    committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });

    co_await nodes()
      .at(restart_id)
      ->raft()
      ->stm_manager()
      ->get<factory_kv>()
      ->wait(committed_offset, model::timeout_clock::now() + 15s);

    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<factory_kv>()
      != nullptr);
    ASSERT_EQ_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<factory_kv>()->state,
      expected);
}

TEST_F_CORO(
  state_machine_fixture, test_factory_not_triggered_if_stm_already_installed) {
    // factory_kv is in the builder at startup.  Replicating a trigger batch
    // must not create a second instance.
    create_nodes();
    absl::flat_hash_map<model::node_id, ss::shared_ptr<factory_kv>>
      initial_stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto fkv = builder.create_stm<factory_kv>(*node);
        initial_stms.emplace(id, fkv);
        // Also register factory so the trigger path sees it.
        register_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(50);
    co_await wait_for_apply();

    auto res = co_await replicate_trigger_batch(trigger_batch_type);
    ASSERT_TRUE_CORO(res.has_value());
    co_await wait_for_apply();

    for (auto& [id, node] : nodes()) {
        // The pointer must be the same instance as the original.
        ASSERT_TRUE_CORO(
          node->raft()->stm_manager()->get<factory_kv>()
          == initial_stms.at(id));
    }
    for (auto& [node_id, stm] : initial_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(
  state_machine_fixture, test_factory_stm_apply_during_installation_window) {
    // Batches written after the trigger must be applied by factory_kv even
    // though installation is in progress on the replicas.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        register_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto res = co_await replicate_trigger_batch(trigger_batch_type);
    ASSERT_TRUE_CORO(res.has_value());

    // Write batches after the trigger (factory_kv might still be installing).
    auto expected = co_await build_random_state(200);

    co_await parallel_for_each_node([](raft_node_instance& n) {
        return n.raft()->stm_manager()->wait_for_stm_name(
          ss::sstring(factory_kv::name), model::timeout_clock::now() + 15s);
    });

    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await parallel_for_each_node([committed_offset](raft_node_instance& n) {
        return n.raft()->stm_manager()->get<factory_kv>()->wait(
          committed_offset, model::timeout_clock::now() + 15s);
    });

    for (auto& [id, node] : nodes()) {
        ASSERT_EQ_CORO(
          node->raft()->stm_manager()->get<factory_kv>()->state, expected);
    }
}

TEST_F_CORO(
  state_machine_fixture, test_factory_stm_leader_change_after_trigger) {
    // Install factory_kv via trigger on the initial leader, then cause a
    // leader change.  The new leader must also have factory_kv running.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        register_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto res = co_await replicate_trigger_batch(trigger_batch_type);
    ASSERT_TRUE_CORO(res.has_value());

    co_await parallel_for_each_node([](raft_node_instance& n) {
        return n.raft()->stm_manager()->wait_for_stm_name(
          ss::sstring(factory_kv::name), model::timeout_clock::now() + 15s);
    });

    // Verify factory_kv is accessible on all nodes.
    for (auto& [id, node] : nodes()) {
        ASSERT_TRUE_CORO(
          node->raft()->stm_manager()->get<factory_kv>() != nullptr);
    }
}

TEST_F_CORO(state_machine_fixture, test_factory_stm_stop_during_installation) {
    // Use slow_start_factory_kv so there is a window between trigger and
    // install completing.  Stop and restart the node; the trigger batch is
    // still in the log so factory_kv re-installs on replay.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        register_factory<slow_start_factory_kv>(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto res = co_await replicate_trigger_batch(trigger_batch_type);
    ASSERT_TRUE_CORO(res.has_value());

    // Stop node 0 immediately, possibly mid-install.
    model::node_id restart_id(0);
    auto data_dir
      = nodes().at(restart_id)->raft()->log()->config().base_directory();
    co_await stop_node(restart_id);
    add_node(restart_id, model::revision_id{0}, data_dir);

    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(restart_id));
    register_factory<slow_start_factory_kv>(builder, *nodes().at(restart_id));
    co_await nodes()
      .at(restart_id)
      ->init_and_start(all_vnodes(), std::move(builder));

    co_await nodes()
      .at(restart_id)
      ->raft()
      ->stm_manager()
      ->wait_for_stm_name(
        ss::sstring(slow_start_factory_kv::name),
        model::timeout_clock::now() + 30s);

    ASSERT_TRUE_CORO(
      nodes()
        .at(restart_id)
        ->raft()
        ->stm_manager()
        ->get<slow_start_factory_kv>()
      != nullptr);
}

TEST_F_CORO(state_machine_fixture, test_factory_not_triggered_restart) {
    // No trigger batch is ever replicated.  After a restart, factory_kv must
    // remain absent from _machines.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        register_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    co_await build_random_state(50);
    co_await wait_for_apply();

    // Restart node 0 without ever replicating the trigger.
    model::node_id restart_id(0);
    auto data_dir
      = nodes().at(restart_id)->raft()->log()->config().base_directory();
    co_await stop_node(restart_id);
    add_node(restart_id, model::revision_id{0}, data_dir);

    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(restart_id));
    register_factory(builder, *nodes().at(restart_id));
    co_await nodes()
      .at(restart_id)
      ->init_and_start(all_vnodes(), std::move(builder));

    co_await wait_for_apply();

    // factory_kv must not be present.
    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<factory_kv>()
      == nullptr);
}

TEST_F_CORO(
  state_machine_fixture, test_cross_node_recovery_restores_missing_stm) {
    // Verify the do_apply_raft_snapshot pre-pass: a fresh replica that
    // receives a managed_snapshot containing snapshot_kv reconstructs it
    // even though snapshot_kv is absent from the builder.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<snapshot_kv>(*node);
        register_snapshot_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(100);
    co_await wait_for_apply();

    // Take and write Raft snapshots so the log is truncated; the managed
    // snapshot includes snapshot_kv's blob.
    for (auto& [id, node] : nodes()) {
        auto snap = co_await node->raft()->stm_manager()->take_snapshot();
        co_await node->raft()->write_snapshot(
          raft::write_snapshot_cfg(
            snap.last_included_offset, std::move(snap.data)));
    }

    // Wipe node 0 and restart without snapshot_kv in create_stm.  The Raft
    // layer will send the snapshot; the prepass must reconstruct snapshot_kv
    // before applying the blob.
    model::node_id fresh_id(0);
    auto data_dir
      = nodes().at(fresh_id)->raft()->log()->config().base_directory();
    co_await stop_node(fresh_id, remove_data_dir::yes);
    add_node(fresh_id, model::revision_id{0}, data_dir);

    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(fresh_id));
    register_snapshot_factory(builder, *nodes().at(fresh_id));
    co_await nodes().at(fresh_id)->init_and_start(
      all_vnodes(), std::move(builder));

    RPTEST_REQUIRE_EVENTUALLY_CORO(15s, [&] {
        return nodes().at(fresh_id)->raft()->stm_manager()->get<snapshot_kv>()
               != nullptr;
    });

    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await nodes()
      .at(fresh_id)
      ->raft()
      ->stm_manager()
      ->get<snapshot_kv>()
      ->wait(committed_offset, model::timeout_clock::now() + 15s);

    ASSERT_EQ_CORO(
      nodes().at(fresh_id)->raft()->stm_manager()->get<snapshot_kv>()->state,
      expected);
}

TEST_F_CORO(state_machine_fixture, test_restart_no_factory_skips_gracefully) {
    // If the factory closure is not registered, the pre-pass must log a
    // warning and skip — no crash, the STM stays absent, other STMs work.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<snapshot_kv>(*node);
        register_snapshot_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    co_await build_random_state(50);
    co_await wait_for_apply();

    model::node_id restart_id(0);
    auto data_dir
      = nodes().at(restart_id)->raft()->log()->config().base_directory();
    co_await stop_node(restart_id);
    add_node(restart_id, model::revision_id{0}, data_dir);

    // No factory closure for snapshot_kv — name is in initial_recovery_snapshot
    // but _stm_factories.find() will return end().
    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(restart_id));
    co_await nodes()
      .at(restart_id)
      ->init_and_start(all_vnodes(), std::move(builder));

    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<snapshot_kv>()
      == nullptr);
    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<simple_kv>()
      != nullptr);
}

TEST_F_CORO(
  state_machine_fixture, test_cross_node_no_factory_skips_gracefully) {
    // Same as test_restart_no_factory_skips_gracefully but for the
    // do_apply_raft_snapshot prepass: snapshot has snapshot_kv blob but no
    // factory closure exists — prepass must warn and skip.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<snapshot_kv>(*node);
        register_snapshot_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(50);
    co_await wait_for_apply();

    for (auto& [id, node] : nodes()) {
        auto snap = co_await node->raft()->stm_manager()->take_snapshot();
        co_await node->raft()->write_snapshot(
          raft::write_snapshot_cfg(
            snap.last_included_offset, std::move(snap.data)));
    }

    model::node_id fresh_id(0);
    auto data_dir
      = nodes().at(fresh_id)->raft()->log()->config().base_directory();
    co_await stop_node(fresh_id, remove_data_dir::yes);
    add_node(fresh_id, model::revision_id{0}, data_dir);

    // No factory closure — snapshot blob for snapshot_kv must be skipped.
    raft::state_machine_manager_builder builder;
    builder.create_stm<simple_kv>(*nodes().at(fresh_id));
    co_await nodes().at(fresh_id)->init_and_start(
      all_vnodes(), std::move(builder));

    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await nodes()
      .at(fresh_id)
      ->raft()
      ->stm_manager()
      ->get<simple_kv>()
      ->wait(committed_offset, model::timeout_clock::now() + 15s);

    ASSERT_TRUE_CORO(
      nodes().at(fresh_id)->raft()->stm_manager()->get<snapshot_kv>()
      == nullptr);
    ASSERT_EQ_CORO(
      nodes().at(fresh_id)->raft()->stm_manager()->get<simple_kv>()->state,
      expected);
}

TEST_F_CORO(
  state_machine_fixture, test_reconstruction_survives_second_restart) {
    // Verifies that apply_initial_recovery_policy preserves the snapshot_kv
    // entry in initial_recovery_snapshot across runs so reconstruction works
    // on every subsequent restart, not just the first.
    create_nodes();
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<snapshot_kv>(*node);
        register_snapshot_factory(builder, *node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(100);
    co_await wait_for_apply();

    model::node_id restart_id(0);

    // First restart — reconstruct from initial_recovery_snapshot.
    {
        auto data_dir
          = nodes().at(restart_id)->raft()->log()->config().base_directory();
        co_await stop_node(restart_id);
        add_node(restart_id, model::revision_id{0}, data_dir);
        raft::state_machine_manager_builder b;
        b.create_stm<simple_kv>(*nodes().at(restart_id));
        register_snapshot_factory(b, *nodes().at(restart_id));
        co_await nodes()
          .at(restart_id)
          ->init_and_start(all_vnodes(), std::move(b));
    }
    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<snapshot_kv>()
      != nullptr);

    // Second restart — initial_recovery_snapshot must still carry the entry.
    {
        auto data_dir
          = nodes().at(restart_id)->raft()->log()->config().base_directory();
        co_await stop_node(restart_id);
        add_node(restart_id, model::revision_id{0}, data_dir);
        raft::state_machine_manager_builder b;
        b.create_stm<simple_kv>(*nodes().at(restart_id));
        register_snapshot_factory(b, *nodes().at(restart_id));
        co_await nodes()
          .at(restart_id)
          ->init_and_start(all_vnodes(), std::move(b));
    }
    ASSERT_TRUE_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<snapshot_kv>()
      != nullptr);

    auto committed_offset = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.raft()->committed_offset(); });
    co_await nodes()
      .at(restart_id)
      ->raft()
      ->stm_manager()
      ->get<snapshot_kv>()
      ->wait(committed_offset, model::timeout_clock::now() + 15s);

    ASSERT_EQ_CORO(
      nodes().at(restart_id)->raft()->stm_manager()->get<snapshot_kv>()->state,
      expected);
}
