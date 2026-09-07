// An agent may not be destroyed while a request the manager itself issued is still unanswered:
// destroying it closes the mailbox and cancels the reply's promise (which lives in the message),
// but the waiter's co_await still resumes and reads that cancelled state as a value under NDEBUG
// (actor-zeta mailbox/message.hpp init_future_slot / impl/mailbox/default_mailbox.ipp close_impl).

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
    // The manager spawns the agent itself (one factory picks bitcask vs b+tree by index type);
    // index_type::single is the ORDERED family, hence the b+tree accessor.
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

    auto drop_future = manager->drop_index(session, kTableOid, kIndexOid);
    REQUIRE_FALSE(drop_future.is_ready());

    auto search_future = manager->search_with_preferred_type(session,
                                                             kTableOid,
                                                             one_key(&resource),
                                                             logical_value_t(&resource, int64_t{42}),
                                                             components::expressions::compare_type::eq,
                                                             components::logical_plan::index_type::no_valid,
                                                             /*start_time=*/0,
                                                             /*txn_id=*/0,
                                                             core::date::timezone_offset_t{});

    // Mailbox is FIFO and drop() was posted first, so this resume is the drop; the search stays queued.
    agent_raw->resume(1);

    // drop_index erases the owning pointer here, destroying the agent.
    REQUIRE(resume_awaited(drop_future));
    REQUIRE(drop_future.is_ready());
    // agent_raw is dead from here on -- nothing below may touch it.

    REQUIRE_FALSE(awaited_request_failed(search_future));

    // An empty vector from search means only "no match", never a refusal.
    REQUIRE(search_future.is_ready());
    auto answer = std::move(search_future).take_ready();
    REQUIRE(answer.has_error());

    std::filesystem::remove_all(path);
}
