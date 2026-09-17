#pragma once

// Shared by bitcask_index_agent_t and btree_index_agent_t; duplication beats the coupling a base would add.

#include <core/result_wrapper.hpp>

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/memory.hpp>
#include <actor-zeta/mailbox/forwards.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>

#include <concepts>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <type_traits>
#include <utility>
#include <vector>

#ifdef DEV_MODE
#include <atomic>
#endif

namespace services::index {

    // An index stores only non-NULL keys; a free function so both agent families share one check.
    [[nodiscard]] inline bool index_key_is_null(const components::types::logical_value_t& key) noexcept {
        return key.is_null();
    }

    struct index_agent_contract {
        using session_id_t = components::session::session_id_t;
        using value_t = components::types::logical_value_t;

        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Terminal: releases the store and unlinks its directory, so every later handler refuses.
        unique_future<void> drop(session_id_t session);

        // Wipes store and pending buckets together, or a rebuilt index reports rows its scan never made.
        unique_future<core::error_t> clear(session_id_t session);

        // Stages inserts/deletes in this transaction's bucket; nothing reaches the store before commit.
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);

        // commit_id is contract-wide: message ids are positional, so the ordered family takes but ignores it.
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // Discards this transaction's bucket; nothing durable was written, so it touches no store.
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);

        // One full reply, duplicates included, no cursor: merges committed rows with staged inserts minus deletes.
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);

        // Ordered behind this agent's pending writes; must report, or a checkpoint could truncate the WAL.
        unique_future<core::error_t> force_flush(session_id_t session);

        // The order IS the message-id space: a method's position in this list. Reordering renumbers
        // everything after it; change this list and both agent classes' `implements<>` lists together.
        using dispatch_traits = actor_zeta::dispatch_traits<&index_agent_contract::drop,
                                                            &index_agent_contract::clear,
                                                            &index_agent_contract::stage_inserts,
                                                            &index_agent_contract::stage_deletes,
                                                            &index_agent_contract::commit_inserts,
                                                            &index_agent_contract::commit_deletes,
                                                            &index_agent_contract::revert_inserts,
                                                            &index_agent_contract::revert_deletes,
                                                            &index_agent_contract::read_rows,
                                                            &index_agent_contract::force_flush>;

        index_agent_contract() = delete;
    };

    // Checked at each class's definition (no base class exists); create() is excluded since its
    // factories' extra parameters are specialization, not drift.
    template<typename agent_t>
    concept index_agent_impl =
        !std::is_abstract_v<agent_t> &&
        std::same_as<typename agent_t::dispatch_traits::contract_type, index_agent_contract> &&
        std::same_as<typename agent_t::agent_ptr_t, std::unique_ptr<agent_t, actor_zeta::pmr::deleter_t>> &&
        std::same_as<std::remove_cv_t<decltype(agent_t::index_type_v)>, components::logical_plan::index_type> &&
        std::same_as<std::remove_cv_t<decltype(agent_t::supports_ordered_probe_v)>, bool> &&
        requires(agent_t & agent,
                 index_agent_contract::session_id_t session,
                 uint64_t txn_id,
                 uint64_t commit_id,
                 std::vector<std::pair<index_agent_contract::value_t, size_t>> values,
                 index_agent_contract::value_t key,
                 actor_zeta::mailbox::message* msg) {
        { agent.drop(session) }
        ->std::same_as<actor_zeta::unique_future<void>>;
        { agent.clear(session) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.stage_inserts(session, txn_id, values) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.stage_deletes(session, txn_id, values) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.commit_inserts(session, txn_id, commit_id) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.commit_deletes(session, txn_id, commit_id) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.revert_inserts(session, txn_id) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.revert_deletes(session, txn_id) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.read_rows(session, components::expressions::compare_type::eq, key, txn_id) }
        ->std::same_as<actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>>;
        { agent.force_flush(session) }
        ->std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.table_oid() }
        ->std::same_as<components::catalog::oid_t>;
        { agent.address() }
        ->std::same_as<actor_zeta::actor::address_t>;
        { agent.behavior(msg) }
        ->std::same_as<actor_zeta::behavior_t>;
        { agent.make_type() }
        ->std::same_as<const char*>;
    };

#ifdef DEV_MODE
    // Live-agent count for tests: distinguishes a dropped table from one whose agent was also freed.
    inline std::atomic<uint64_t> g_live_index_agents{0};
    [[nodiscard]] inline uint64_t live_index_agents() noexcept {
        return g_live_index_agents.load(std::memory_order_relaxed);
    }
#endif

} // namespace services::index
