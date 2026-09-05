// An agent may not be destroyed while a request the manager itself issued is still
// unanswered.
//
// WHY THIS IS NOT A STYLE POINT. A cross-actor reply travels in a shared_state whose
// promise lives IN THE MESSAGE. Destroying an actor closes its mailbox, and closing a
// mailbox deletes every message still queued in it; ~message() then runs the slot's
// cleanup, which sets operation_canceled on the shared_state and releases the promise
// (actor-zeta mailbox/message.hpp init_future_slot, impl/mailbox/default_mailbox.ipp
// close_impl). state_flags::result_set is value_set|error_set, so THAT COUNTS AS A
// RESULT: the waiter's co_await is resumed, and the awaiter's await_resume() does
//
//     assert(!state->has_error());  // <- compiled out under NDEBUG
//     return state->take_value();   // <- moves out of an UNINITIALISED union
//
// (actor-zeta detail/future_awaiters.hpp owning_awaiter, detail/result_storage.hpp
// take()). In a release build the reader of a std::pmr::vector<int64_t> reply gets a
// vector with a garbage pointer: foreign "rows", then free() on a wild address. There is
// no way to intercept that on our side -- actor-zeta's promise types offer await_transform
// for unique_future and nothing else, so an actor coroutine cannot co_await a checked
// wrapper, and it cannot inspect the state before await_resume has already taken from it.
// The only place the hole can be closed is the LIFETIME: the agent must still be there.
//
// The window that made it reachable: manager_index_t::drop_index awaited the agent's
// drop() and only afterwards erased the owning pointer from the manager. A search
// suspended on read_rows() -- or one that STARTED after the drop was sent, because the
// index was still registered in the engine at that moment -- was left waiting on an agent
// that got destroyed underneath it.
//
// The test does not race for that window, it lays it out by hand: the agent is pumped one
// message at a time (cooperative_actor::resume(1)), so "drop handled, read not yet" is a
// state the test chooses, and the manager's own handlers are called directly so no loop
// thread decides the interleaving.

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

    // Resume the coroutine that `fut` is suspended in, the way an actor loop does it:
    // claim the deepest awaited continuation atomically and run it. Returns false when
    // there is nothing suspended (the coroutine already ran to completion).
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

    // Did the request this coroutine is suspended on come back as a FAILED future rather
    // than as an answer? That is the shape a destroyed agent leaves behind
    // (operation_canceled from the mailbox close), and it is the shape the waiter cannot
    // survive: its await_resume takes a value that was never set.
    template<typename T>
    bool awaited_request_failed(const actor_zeta::unique_future<T>& fut) {
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

} // namespace

TEST_CASE("services::index::drop_index keeps the agent alive under an outstanding read") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");

    const std::filesystem::path path{index_fixture_path("agent_lifetime")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                        std::to_string(static_cast<unsigned>(kIndexOid)));

    // Never started: the agent is driven by hand below, so nothing runs behind the test's
    // back. enqueue() on an unstarted scheduler only parks the job (work_sharing
    // central_enqueue), which is exactly what is wanted here.
    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

    auto manager = actor_zeta::spawn<manager_index_t>(&resource,
                                                      scheduler.get(),
                                                      log,
                                                      path,
                                                      /*bitcask_flush_threshold=*/1000,
                                                      /*bitcask_segment_record_limit=*/100,
                                                      /*btree_flush_threshold=*/1000);

    manager->bootstrap_engine_sync(kTableOid);
    // The manager raises the agent itself now (one class per storage family, one factory
    // that picks between them), so the test asks it for the agent instead of spawning one
    // and handing it over. index_type::single is the ORDERED family, hence the b+tree
    // accessor below.
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

    // (1) DROP INDEX starts. It sends drop() to the agent and suspends on the reply.
    auto drop_future = manager->drop_index(session, kTableOid, kIndexOid);
    REQUIRE_FALSE(drop_future.is_ready());

    // (2) A SELECT arrives while the drop is still in flight. Whatever it decides to do,
    //     it must not end up parked on a request nobody will answer.
    auto search_future = manager->search_with_preferred_type(session,
                                                             kTableOid,
                                                             one_key(&resource),
                                                             logical_value_t(&resource, int64_t{42}),
                                                             components::expressions::compare_type::eq,
                                                             components::logical_plan::index_type::no_valid,
                                                             /*start_time=*/0,
                                                             /*txn_id=*/0,
                                                             core::date::timezone_offset_t{});

    // (3) Let the agent handle EXACTLY ONE message. Its mailbox is FIFO, and drop() was
    //     posted first, so this is the drop -- and anything the search posted afterwards
    //     is still sitting there unanswered.
    agent_raw->resume(1);

    // (4) The drop reply is in; hand the manager's suspended drop_index its continuation.
    //     This is where the owning pointer used to be erased, taking the agent with it.
    REQUIRE(resume_awaited(drop_future));
    REQUIRE(drop_future.is_ready());
    // agent_raw is dead from here on: whoever owns the agent, drop_index finishes by
    // destroying it. Nothing below may touch it.

    // (5) THE POINT. The search must not be left holding a cancelled request. Either it
    //     was answered before the agent went away, or it was refused outright -- never
    //     "resumed with a result that is really an error", which is what a destroyed agent
    //     hands back and what await_resume reads as an uninitialised value under NDEBUG.
    REQUIRE_FALSE(awaited_request_failed(search_future));

    // ... and the refusal is LOUD: an error, not a quietly empty match set. An empty
    // vector out of manager_index_t::search means "no row matches" and nothing else.
    REQUIRE(search_future.is_ready());
    auto answer = std::move(search_future).take_ready();
    REQUIRE(answer.has_error());

    std::filesystem::remove_all(path);
}
