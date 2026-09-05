#pragma once

// The mailbox surface shared by the two disk-index agent classes (bitcask_index_agent_t /
// btree_index_agent_t), each holding its store BY VALUE and BY CONCRETE TYPE -- one owner, one
// static type, no erased base needed, no shared base or template. What they share is stated
// here instead, checked two ways:
//
//   * index_agent_contract -- the actor-zeta interface. A message id is a method's position in
//     dispatch_traits, so `implements<>` must agree on that order before manager_index_t can
//     hold a bare actor_zeta::address_t and send to either family.
//   * index_agent_impl -- the concept asserted after each class, catching what `override` can't
//     (missing handler, drifted signature, wrong owning-pointer type, missing static backend
//     answer) at the class's own definition rather than the send site in manager_index.cpp.
//
// An agent is the whole index: it owns the COMMITTED half (its store) and the UNCOMMITTED half
// (this transaction's staged inserts/deletes, since nothing writes through before commit).
// read_rows merges both halves here rather than the caller stitching them post-hoc.
//
// The duplication between the two implementations is accepted: cheaper than the coupling a
// shared base would reintroduce.

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

    // An index stores exactly the non-NULL keys of live rows. A free function, not a member of
    // either class, so both agents call the SAME check rather than risk two copies drifting --
    // the failure mode isn't a missing row, it's a b+tree ordered by something that isn't a
    // strict weak ordering (a NULL key carries logical_type::NA, and casting NA to the column's
    // type fails). SQL backs this: a NULL satisfies no value comparison, and IS NULL is answered
    // from the validity mask, never from an index.
    [[nodiscard]] inline bool index_key_is_null(const components::types::logical_value_t& key) noexcept {
        return key.is_null();
    }

    struct index_agent_contract {
        using session_id_t = components::session::session_id_t;
        using value_t = components::types::logical_value_t;

        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // TERMINAL. Releases the backing store and unlinks its directory. The agent stays
        // addressable afterwards (its owner destroys it, not this call) but has nothing
        // left to serve, so every other handler below refuses once this has run.
        unique_future<void> drop(session_id_t session);

        // Wipes stored data AND every pending bucket, keeping the agent alive and writable (not
        // the terminal drop -- the repopulate path clears then re-stages with txn_id == 0). Both
        // halves go together: a clear that wiped only the durable half would leave a rebuilt
        // index reporting rows the scan it was rebuilt from never produced.
        unique_future<core::error_t> clear(session_id_t session);

        // Records a statement's inserts/deletes in this transaction's bucket; nothing reaches
        // the store (no write-through before commit). txn_id == 0 is the rebuild feeds' bucket.
        // A batch of (key, row_id) pairs, not a data_chunk_t -- the manager already resolved the
        // key column, so forwarding the whole chunk would clone every other column per agent.
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);

        // Publishes bucket `txn_id` and bucket 0 (committed-but-not-durable) into the store and
        // drops both, returning the IO failure as a value so the commit handler can fail the
        // statement, not the process.
        //
        // commit_id is a CONTRACT parameter, not a bitcask-only one, because the message id is
        // this method's position in dispatch_traits -- a parameter on one side only would send
        // every later handler's messages to the wrong body. Only the hashed family spends it
        // (stamped into the txn-log frame, matched by the recover gate against the WAL's
        // committed set); the ordered family takes and ignores it.
        //
        // Commit id, not txn id, decides the replay: txn ids restart at TRANSACTION_ID_START
        // every process (an earlier incarnation's marker could vouch for a later one's frame of
        // the same id), while commit ids are re-derived from the durable frontier at every
        // reopen and never repeat. 0 = "no commit id"; only the txn_id == 0 rebuild feed passes it.
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // DISCARD this transaction's bucket. Nothing durable was written for it, so the
        // abort is a bucket erase and touches no store.
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);

        // Every row id whose key satisfies `compare` against `key`, duplicates included, in one
        // reply -- no cursor. Merges the committed store rows with `txn_id`'s (and bucket 0's)
        // staged inserts minus deletes, which is why the predicate travels with the key: a
        // staged row keyed 3 belongs in the answer to `x < 5` but not `x = 5`.
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);

        // Checkpoint fan-out from manager_index_t::flush_all_indexes, ordered behind any pending
        // write in this agent's FIFO. Must report: a void return would let the checkpoint
        // truncate the WAL behind an index whose entries were still only in memory.
        unique_future<core::error_t> force_flush(session_id_t session);

        // The order IS the message id space (action_id_impl -> find_method_index: a method's
        // index in this list). Reordering renumbers everything after it, and a method not
        // found resolves to 0 -- a misrouted send lands on drop(). Change this list and both
        // agent classes' `implements<>` lists together, never one alone.
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

    // Checked at each class's own definition (there's no base class to catch this otherwise):
    //
    //   * a handler never written, or one with a drifted signature -- `implements<>` compares
    //     positionally, so two swapped-but-same-shaped methods would pass it, but not these
    //     requirements;
    //   * an owning-pointer alias typed by anything but the class itself: actor_zeta::pmr::deleter_t
    //     deallocates sizeof(static type), so a wrong owner type returns the wrong byte count
    //     to the pool;
    //   * a missing static answer to "which backend am I, ordered or not" -- manager_index_t
    //     needs both BEFORE any send, to refuse a range predicate without an abort in the agent.
    //
    // Deliberately NOT checked: create(). The two factories take different parameters (segment-
    // record limit and WAL committed-txn set exist only for the family with a txn log), and
    // that's the specialization, not a drift.
    template<typename agent_t>
    concept index_agent_impl =
        !std::is_abstract_v<agent_t> &&
        // The message-id space, bound to the contract by the library.
        std::same_as<typename agent_t::dispatch_traits::contract_type, index_agent_contract> &&
        // The owning handle, typed by the class it destroys.
        std::same_as<typename agent_t::agent_ptr_t, std::unique_ptr<agent_t, actor_zeta::pmr::deleter_t>> &&
        // The backend this family IS, and whether it has an ordering. Compile-time, not
        // asked of an instance: there is nothing about a live agent that could change
        // either answer.
        std::same_as<std::remove_cv_t<decltype(agent_t::index_type_v)>, components::logical_plan::index_type> &&
        std::same_as<std::remove_cv_t<decltype(agent_t::supports_ordered_probe_v)>, bool> &&
        requires(agent_t& agent,
                 index_agent_contract::session_id_t session,
                 uint64_t txn_id,
                 uint64_t commit_id,
                 std::vector<std::pair<index_agent_contract::value_t, size_t>> values,
                 index_agent_contract::value_t key,
                 actor_zeta::mailbox::message* msg) {
        { agent.drop(session) } -> std::same_as<actor_zeta::unique_future<void>>;
        { agent.clear(session) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.stage_inserts(session, txn_id, values) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.stage_deletes(session, txn_id, values) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.commit_inserts(session, txn_id, commit_id) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.commit_deletes(session, txn_id, commit_id) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.revert_inserts(session, txn_id) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        { agent.revert_deletes(session, txn_id) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        {
            agent.read_rows(session, components::expressions::compare_type::eq, key, txn_id)
            } -> std::same_as<actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>>;
        { agent.force_flush(session) } -> std::same_as<actor_zeta::unique_future<core::error_t>>;
        // The table this agent's index belongs to. manager_index_t reaps by table oid and
        // asks the agent rather than keeping a second map that could disagree with it.
        { agent.table_oid() } -> std::same_as<components::catalog::oid_t>;
        // The mailbox identity and the dispatch entry point.
        { agent.address() } -> std::same_as<actor_zeta::actor::address_t>;
        { agent.behavior(msg) } -> std::same_as<actor_zeta::behavior_t>;
        { agent.make_type() } -> std::same_as<const char*>;
    };

#ifdef DEV_MODE
    // Test-observable count of LIVE disk index agents (both families): separates "table
    // dropped" from "table dropped AND its agent freed" -- an unreaped agent keeps files open
    // with nothing left able to address it. Bumped in each agent's ctor/dtor, never read live.
    inline std::atomic<uint64_t> g_live_index_agents{0};
    [[nodiscard]] inline uint64_t live_index_agents() noexcept {
        return g_live_index_agents.load(std::memory_order_relaxed);
    }
#endif

} // namespace services::index
