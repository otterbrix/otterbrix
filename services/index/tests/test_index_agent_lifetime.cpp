// An agent may not be destroyed while a request already queued on it is unanswered: close_impl cancels
// every message still in the mailbox (actor-zeta impl/mailbox/default_mailbox.ipp), so the caller gets
// operation_canceled instead of the agent's own refusal -- a cancellation says "nobody answered", which
// a caller cannot tell apart from a lost message.
//
// The read goes STRAIGHT to the agent's address on purpose. Routing it through the manager cannot build
// this race: detach_index erases the registry entry synchronously, before drop_index's first co_await
// (actor-zeta coroutines start eagerly), so search_with_preferred_type refuses before it sends anything.

// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <actor-zeta/detail/state_flags.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>
#include <core/date/date_types.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <services/index/btree_index_agent.hpp>
#include <services/index/index_agent_contract.hpp>
#include <services/index/manager_index.hpp>

#include "index_fixture_path.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

using components::session::session_id_t;
using components::types::logical_value_t;
using services::index::manager_index_t;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17100;
    constexpr components::catalog::oid_t kIndexOid = 17101;

    // A ready future has already run its final_suspend, and unique_future's promise_type destroys its
    // own frame there (unlike behavior_t, which stays suspended until its dtor). coroutine_handle() is
    // then a live-looking pointer into freed memory, so readiness must be read off the shared state
    // FIRST -- actor-zeta does the same before touching a handle (future_awaiters.hpp, propagate_awaited_state).
    template<typename T>
    bool resume_awaited(const actor_zeta::unique_future<T>& fut) {
        if (fut.is_ready()) {
            return false;
        }
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

    // Same gate as resume_awaited: a synchronous refusal leaves the frame already destroyed.
    template<typename T>
    bool awaited_request_failed(const actor_zeta::unique_future<T>& fut) {
        if (fut.is_ready()) {
            return false;
        }
        auto handle = fut.coroutine_handle();
        if (!handle || handle.done()) {
            return false;
        }
        auto* flags = handle.promise().awaited_flags_;
        if (!flags) {
            return false;
        }
        return (flags->load(std::memory_order_acquire) & actor_zeta::detail::state_flags::error_set) != 0;
    }

    components::index::keys_base_storage_t one_key(std::pmr::memory_resource* resource) {
        components::index::keys_base_storage_t keys(resource);
        keys.emplace_back(components::expressions::key_t{resource, "count"});
        return keys;
    }

}

TEST_CASE("services::index::drop_index keeps the agent alive under an outstanding read") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");

    const std::filesystem::path path{index_fixture_path("agent_lifetime")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                        std::to_string(static_cast<unsigned>(kIndexOid)));

    // Never started: driven entirely by hand, so nothing runs behind the test's back.
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
    auto* agent_raw = agents.front();

    const auto session = session_id_t::generate_uid();

    // drop first: the manager detaches the index and posts drop(), then suspends on the reply.
    auto drop_future = manager->drop_index(session, kTableOid, kIndexOid);
    REQUIRE_FALSE(drop_future.is_ready());

    // Straight to the agent, so it lands in the agent's OWN mailbox behind the drop.
    auto [read_needs_sched, read_future] =
        actor_zeta::otterbrix::send<&services::index::index_agent_contract::read_rows>(
            agent_raw->address(),
            session,
            components::expressions::compare_type::eq,
            logical_value_t(&resource, int64_t{42}),
            /*txn_id=*/uint64_t{0});
    REQUIRE_FALSE(read_future.is_ready());

    // Mailbox is FIFO and drop() was posted first, so this resume is the drop; the read stays queued.
    agent_raw->resume(1);

    // drop_index gives up the registry entry here; the agent itself must survive, because the read is
    // still sitting in its mailbox.
    REQUIRE(resume_awaited(drop_future));
    REQUIRE(drop_future.is_ready());

    // Readiness IS the discriminator, and asking it first is what keeps the broken form a readable
    // failure instead of a use-after-free: a destroyed agent has close_impl cancel the queued read, so
    // the future is already ready here, while a surviving one has not answered yet and needs a resume.
    if (!read_future.is_ready()) {
        agent_raw->resume(1);
    }
    REQUIRE(read_future.is_ready());
    auto answer = std::move(read_future).take_ready();
    REQUIRE(answer.has_error());
    INFO("error was: " << answer.error().what.c_str());
    CHECK(answer.error().type == core::error_code_t::index_not_exists);

    std::filesystem::remove_all(path);
}
