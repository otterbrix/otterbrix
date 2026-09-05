// A dropped table must take its index agents with it. An agent owns an open store (btree_t over
// its directory, or a segment/txn-log/keydir triplet); only its destructor closes that, and only
// manager_index_t owns it. Two teardown paths must take the owning pointer, not just routing
// state: unregister_collection (commit/abort-time; operator_commit_transaction awaits it before
// telling manager_disk_t to free the table's files) and on_horizon_advanced (once the snapshot
// floor passes the drop's commit id). Erasing only the routing maps leaks one open agent per
// index, forever -- there is no later reaper.
//
// The teardown may not free the agent while a message it sent is still unanswered (the
// use-after-free test_index_agent_lifetime.cpp pins), so the reap uses drop_index's shape: take
// ownership into the handler's frame before the terminal drop is sent, await the reply, then let
// the frame destroy it.
//
// Witnessed by live_index_agents(), a DEV_MODE count bumped in each agent's ctor/dtor -- it's
// the only way to tell "table dropped" from "table dropped AND its agent freed". Both cases
// drive the manager and agent by hand (direct handler calls, cooperative_actor::resume(1)) so
// the interleaving is chosen, not raced for.

// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>

#include <services/index/index_agent_contract.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
#include <filesystem>
#include <set>

#include "index_fixture_path.hpp"

using components::session::session_id_t;
using services::index::live_index_agents;
using services::index::manager_index_t;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17300;
    constexpr components::catalog::oid_t kIndexOid = 17301;

    // Resume the coroutine that `fut` is suspended in, the way the manager's own loop
    // thread does it: claim the deepest awaited continuation atomically and run it.
    // Returns false when there is nothing suspended.
    template<typename T>
    bool resume_awaited(const actor_zeta::unique_future<T>& fut) {
        auto handle = fut.coroutine_handle();
        if (!handle || handle.done()) {
            return false;
        }
        auto* cont_ptr = handle.promise().awaited_continuation_;
        if (!cont_ptr) {
            return false;
        }
        auto cont = cont_ptr->exchange(nullptr, std::memory_order_acq_rel);
        if (!cont) {
            return false;
        }
        cont.resume();
        return true;
    }

    components::index::keys_base_storage_t one_key(std::pmr::memory_resource* resource) {
        components::index::keys_base_storage_t keys(resource);
        keys.emplace_back(components::expressions::key_t{resource, "count"});
        return keys;
    }

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{services::index::tests::index_fixture_path(name)};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

} // namespace

TEST_CASE("services::index::on_horizon_advanced frees the agents of a reclaimed table") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_reaping_horizon");

    // Never started: the agent is driven by hand below, so nothing runs behind the test's
    // back. enqueue() on an unstarted scheduler only parks the job.
    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

    // The counter is process-wide, so the test measures a DIFFERENCE rather than an
    // absolute: another case in this binary may hold agents of its own.
    const auto agents_before = live_index_agents();

    auto manager = actor_zeta::spawn<manager_index_t>(&resource,
                                                      scheduler.get(),
                                                      log,
                                                      path,
                                                      /*bitcask_flush_threshold=*/1000,
                                                      /*bitcask_segment_record_limit=*/100,
                                                      /*btree_flush_threshold=*/1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             one_key(&resource),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());
    REQUIRE(live_index_agents() == agents_before + 1);

    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent_raw = agents.front();

    // DROP TABLE marked the oid; the snapshot floor then passes its commit id.
    manager->mark_table_dropped_sync(kTableOid, /*dropped_at_commit_id=*/10);
    auto horizon_future = manager->on_horizon_advanced(/*new_horizon=*/11);

    // The sweep sent the terminal drop and is waiting for it; freeing the agent before the
    // reply lands is the use-after-free test_index_agent_lifetime.cpp pins. Returning
    // immediately here would mean firing the drop and forgetting the agent -- the original leak.
    INFO("the horizon sweep must wait for the terminal drop it sent");
    REQUIRE_FALSE(horizon_future.is_ready());

    agent_raw->resume(1);
    REQUIRE(resume_awaited(horizon_future));
    REQUIRE(horizon_future.is_ready());
    // agent_raw is dead from here on. Nothing below may touch it.

    INFO("the reclaimed table's agent must be destroyed, not merely unrouted");
    REQUIRE(live_index_agents() == agents_before);

    manager.reset();
    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::unregister_collection frees the agents of the table it tears down") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_reaping_unregister");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

    const auto agents_before = live_index_agents();

    auto manager = actor_zeta::spawn<manager_index_t>(&resource,
                                                      scheduler.get(),
                                                      log,
                                                      path,
                                                      /*bitcask_flush_threshold=*/1000,
                                                      /*bitcask_segment_record_limit=*/100,
                                                      /*btree_flush_threshold=*/1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             one_key(&resource),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());
    REQUIRE(live_index_agents() == agents_before + 1);

    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent_raw = agents.front();

    const auto session = session_id_t::generate_uid();
    auto unregister_future = manager->unregister_collection(session, kTableOid);

    // The caller (operator_commit_transaction / operator_abort_transaction) awaits this before
    // telling the disk manager to free the table's files, so the store must be closed by the
    // time it returns -- returning immediately would mean erasing two maps and walking away.
    INFO("the teardown must wait for the terminal drop it sent");
    REQUIRE_FALSE(unregister_future.is_ready());

    agent_raw->resume(1);
    REQUIRE(resume_awaited(unregister_future));
    REQUIRE(unregister_future.is_ready());

    INFO("the torn-down table's agent must be destroyed before its files are freed");
    REQUIRE(live_index_agents() == agents_before);

    manager.reset();
    std::filesystem::remove_all(path);
}
