// C5c — THE PHYSICAL ERASE OF AN INDEX ENTRY WAITS FOR THE SNAPSHOT FLOOR.
//
// An index is allowed to name rows a reader may not see: the table filters them on the
// point fetch (C4b). It is NOT allowed to withhold an id, because nothing downstream can
// put back a row the index never named. So a committed DELETE may publish its erase only
// once EVERY live snapshot already hides the row — that is, once the commit-id horizon has
// reached the delete's commit_id.
//
// The in-memory index used to get this for free: it stamped delete_id = commit_id and left
// the entry in place until cleanup_versions(lowest_active). There is no stamp on a disk
// index and no in-memory index left to carry one, so the wait is rebuilt as a QUEUE in the
// manager — the actor that owns both halves of the decision, the commit_id and the horizon.
//
// integration/cpp/test/test_index_delete_horizon.cpp pins the user-visible half of this
// (two overlapping transactions over SQL). What THAT test cannot see is the other end: that
// the entry is eventually erased for real rather than kept forever, and that the erase is
// driven by the horizon and by nothing else. Both are asked here, of the manager, with the
// agent pumped by hand so the interleaving is chosen rather than raced for.

// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <components/context/execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>

#include <services/index/index_agent_contract.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <set>
#include <utility>
#include <vector>

#include "index_fixture_path.hpp"

using components::expressions::compare_type;
using components::session::session_id_t;
using components::table::TRANSACTION_ID_START;
using components::types::logical_value_t;
using services::index::index_agent_contract;
using services::index::manager_index_t;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17400;
    constexpr components::catalog::oid_t kIndexOid = 17401;

    // Resume the coroutine `fut` is suspended in, the way the manager's own loop thread
    // does. Copied from test_index_agent_reaping.cpp deliberately: a shared helper header
    // for two test files would be the start of a test framework nobody asked for.
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

    // Drive a manager handler to completion, pumping the one agent it can talk to. A
    // handler that sends nothing is already finished on the first check; one that fans out
    // needs the agent resumed and its own continuation claimed.
    template<typename T, typename Agent>
    void settle(actor_zeta::unique_future<T>& fut, Agent* agent) {
        for (int attempt = 0; attempt < 8 && !fut.is_ready(); ++attempt) {
            agent->resume(1);
            resume_awaited(fut);
        }
        REQUIRE(fut.is_ready());
    }

    // One message to the agent, pumped to completion, its reply taken.
    template<auto Handler, typename Agent, typename... Args>
    auto ask(Agent* agent, Args&&... args) {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send<Handler>(agent->address(), std::forward<Args>(args)...);
        agent->resume(1);
        REQUIRE(future.is_ready());
        return std::move(future).take_ready();
    }

    components::index::keys_base_storage_t one_key(std::pmr::memory_resource* resource) {
        components::index::keys_base_storage_t keys(resource);
        keys.emplace_back(components::expressions::key_t{resource, "id"});
        return keys;
    }

    std::vector<std::pair<logical_value_t, size_t>> one_entry(std::pmr::memory_resource* resource,
                                                              int64_t key,
                                                              size_t row_id) {
        std::vector<std::pair<logical_value_t, size_t>> values;
        values.emplace_back(logical_value_t(resource, key), row_id);
        return values;
    }

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{services::index::tests::index_fixture_path(name)};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

    components::execution_context_t ctx_for(session_id_t session, uint64_t txn_id) {
        return components::execution_context_t{session, components::table::transaction_data{txn_id, 0}, {}};
    }

} // namespace

TEST_CASE("services::index::a committed delete reaches the store only once the horizon passes it") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_delete_horizon");

    // Never started: everything below is driven by hand.
    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

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
    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent = agents.front();

    const auto session = session_id_t::generate_uid();
    const uint64_t writer_txn = TRANSACTION_ID_START + 1;
    const uint64_t deleter_txn = TRANSACTION_ID_START + 2;
    const uint64_t onlooker_txn = TRANSACTION_ID_START + 3;
    const uint64_t delete_commit_id = 100;
    // The meter is process-wide (see index_deferred_deletes), so every check below is a
    // DIFFERENCE against what this binary was already holding.
    const auto deferred_before = services::index::index_deferred_deletes();

    // A committed row in the store, put there through the agent's own doors.
    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_inserts>(agent, session, writer_txn, one_entry(&resource, 42, 7))
            .contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, writer_txn).contains_error());

    const auto probe = [&](uint64_t txn_id) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, int64_t{42}),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        auto rows = std::move(answer.value());
        return std::vector<int64_t>(rows.begin(), rows.end());
    };
    REQUIRE(probe(onlooker_txn) == std::vector<int64_t>{7});

    // The deleting transaction stages its delete and then COMMITS through the manager,
    // which is where the erase used to be dispatched.
    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_deletes>(agent, session, deleter_txn, one_entry(&resource, 42, 7))
            .contains_error());

    std::pmr::vector<components::catalog::oid_t> oids(&resource);
    oids.emplace_back(kTableOid);
    auto commit_future = manager->commit_deletes(ctx_for(session, deleter_txn), std::move(oids), delete_commit_id);
    settle(commit_future, agent);
    REQUIRE_FALSE(std::move(commit_future).take_ready().contains_error());

    INFO("the manager must be HOLDING the erase, not have dispatched it");
    CHECK(services::index::index_deferred_deletes() == deferred_before + 1);

    INFO("a reader whose snapshot predates the commit must still be given the id");
    // RED before C5c: the commit fanned the erase out to the agent immediately, so the
    // entry was gone from the tree and no reader could be handed it any more.
    CHECK(probe(onlooker_txn) == std::vector<int64_t>{7});

    INFO("a horizon that has NOT reached the commit id changes nothing");
    {
        auto early = manager->on_horizon_advanced(delete_commit_id - 1);
        settle(early, agent);
        CHECK(services::index::index_deferred_deletes() == deferred_before + 1);
        CHECK(probe(onlooker_txn) == std::vector<int64_t>{7});
    }

    INFO("and the horizon reaching it is what publishes the erase");
    {
        auto sweep = manager->on_horizon_advanced(delete_commit_id);
        settle(sweep, agent);
        CHECK(services::index::index_deferred_deletes() == deferred_before);
        CHECK(probe(onlooker_txn).empty());
    }

    manager.reset();
    std::filesystem::remove_all(path);
}

// A held-back erase belongs to an index that still exists. DROP INDEX takes the agent away
// and destroys it; a queue entry that outlived it would be a send to a torn-down routing
// entry on the next horizon advance — the class of dangling the registry consolidation
// exists to make impossible.
TEST_CASE("services::index::tearing an index down drops the erases it was still owed") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_delete_horizon_drop");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

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
    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent = agents.front();

    const auto session = session_id_t::generate_uid();
    const uint64_t deleter_txn = TRANSACTION_ID_START + 5;

    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_inserts>(agent, session, deleter_txn, one_entry(&resource, 11, 3))
            .contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, deleter_txn).contains_error());
    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_deletes>(agent, session, deleter_txn, one_entry(&resource, 11, 3))
            .contains_error());

    const auto deferred_before = services::index::index_deferred_deletes();
    std::pmr::vector<components::catalog::oid_t> oids(&resource);
    oids.emplace_back(kTableOid);
    auto commit_future = manager->commit_deletes(ctx_for(session, deleter_txn), std::move(oids), /*commit_id=*/200);
    settle(commit_future, agent);
    REQUIRE_FALSE(std::move(commit_future).take_ready().contains_error());
    REQUIRE(services::index::index_deferred_deletes() == deferred_before + 1);

    auto drop_future = manager->drop_index(session, kTableOid, kIndexOid);
    settle(drop_future, agent);
    // agent is dead from here on. Nothing below may touch it.

    INFO("the queue must not keep an erase for an index that no longer exists");
    CHECK(services::index::index_deferred_deletes() == deferred_before);

    // The sweep that follows must find nothing to do and must not reach for a torn-down
    // record. It completes without any agent to pump, which is itself the assertion.
    auto sweep = manager->on_horizon_advanced(/*new_horizon=*/500);
    REQUIRE(sweep.is_ready());

    manager.reset();
    std::filesystem::remove_all(path);
}
