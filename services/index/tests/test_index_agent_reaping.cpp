// A DROPPED TABLE MUST TAKE ITS INDEX AGENTS WITH IT.
//
// An index agent OWNS an open store: the ordered family a core::b_plus_tree::btree_t over
// its directory, the hashed one a segment handle, a txn-log handle and a keydir file. The
// only thing that closes any of that is the agent's destructor, and the only owner is
// manager_index_t.
//
// Both teardown paths used to erase the ROUTING state -- engines_[oid] and the per-oid
// address vector -- and leave the owning pointer standing:
//
//   * unregister_collection is the commit-time (and abort-time) physical teardown:
//     operator_commit_transaction awaits it for every dropped oid and only then tells
//     manager_disk_t to free the table's files. It erased two maps and returned.
//   * on_horizon_advanced sent a terminal drop to each agent of the reclaimed oid and
//     said in a comment that the owning pointers were "reaped later (next force_flush
//     pass or base_spaces shutdown)". There was no such reaper. force_flush only flushes,
//     and shutdown is the end of the process.
//
// So every dropped indexed table leaked one agent per index, each holding files open --
// files the disk manager then unlinked underneath it -- for the life of the process.
//
// What the fix may NOT do is free the agent while a message it was sent is still
// unanswered; that is the hole test_index_agent_lifetime.cpp pins, and it is why the
// reap here has the same shape drop_index uses: take the ownership into the handler's
// frame BEFORE the terminal drop is sent (so nothing can address the agent behind it),
// await the reply, then let the frame destroy it.
//
// The witness is services::index::live_index_agents() -- a DEV_MODE count bumped in each
// agent's constructor and destructor. It is what separates "the table was dropped" from
// "the table was dropped AND its agent was freed"; no assertion about maps, addresses or
// files can tell those two apart.
//
// Both cases drive the manager and the agent by hand (the manager's handlers are called
// directly, the agent is pumped with cooperative_actor::resume(1)) so the interleaving is
// chosen rather than raced for.

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

    // The sweep sent the terminal drop and is waiting for it. That wait is half the fix:
    // freeing an agent whose reply is still outstanding is the use-after-free
    // test_index_agent_lifetime.cpp pins. A sweep that returns immediately here is one
    // that fired the drop and forgot the agent -- which is exactly what leaked it.
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

    // The caller (operator_commit_transaction / operator_abort_transaction) awaits this
    // BEFORE it tells the disk manager to free the table's files, so the store has to be
    // closed by the time it returns -- which means this handler has to wait for the drop.
    // Returning immediately means it erased two maps and walked away from the agent.
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
