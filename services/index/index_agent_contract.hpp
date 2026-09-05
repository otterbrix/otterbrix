#pragma once

// THE MAILBOX SURFACE OF A DISK INDEX AGENT, AND THE CONTRACT THAT HOLDS IT.
//
// There are TWO agent classes, one per storage family: bitcask_index_agent_t holds a
// bitcask_index_disk_t and btree_index_agent_t holds a btree_index_disk_t, each BY VALUE and BY ITS
// CONCRETE TYPE -- one owner, one static type, no erased base, so the indirection bought nothing.
// Neither derives from the other; there is no common base, no CRTP and no template with two
// instantiations, because a template is one body serving two types and that unification is what
// this split exists to undo. What the two bodies share is stated here instead, and checked twice:
//
//   * index_agent_contract -- the actor-zeta INTERFACE. A message id is a method's POSITION in a
//     dispatch_traits list, so an address_t send lands on the right handler only if both classes
//     agree on that order; `implements<>` makes them agree, and the library refuses a binding whose
//     count or signatures drift. It is also what lets manager_index_t hold a bare
//     actor_zeta::address_t and send to EITHER family -- the erasure is the framework's, and no
//     base class of ours is needed for it.
//   * index_agent_impl -- the CONCEPT, asserted on the line after each class, catching what
//     `override` cannot because there is no base for it to attach to: a handler never written, a
//     drifted signature, an owning-pointer alias typed by something other than the class itself, a
//     missing static answer to "what backend am I". The diagnostic lands where the class is
//     WRITTEN rather than at the send site in manager_index.cpp.
//
// AN AGENT IS THE WHOLE INDEX. It owns BOTH halves of every answer: the COMMITTED half in the store
// it opened, and the UNCOMMITTED half -- this transaction's own inserts and deletes, in per-txn
// buckets, since nothing writes through before commit. Splitting the two across actors means
// stitching one answer together after the read comes back; one owner is why read_rows below takes
// the asking transaction's id and merges where both halves are.
//
// Duplication between the two implementations is ACCEPTED: it is cheaper than the coupling a shared
// base would reintroduce, and the contract plus the concept are what keep the two copies honest.

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

    // AN INDEX STORES EXACTLY THE NON-NULL KEYS OF THE LIVE ROWS.
    //
    // ONE function, called by BOTH agent classes at every door a key can enter or be probed
    // through. A free function and not a member of either class, because it is an invariant OF AN
    // INDEX rather than a policy of one family: two private copies would be two chances to forget
    // one, and the consequence of forgetting it is not a missing row -- it is a b+tree ordered by
    // something that is not a strict weak ordering.
    //
    // The invariant is what SQL requires: a NULL satisfies no value comparison, and the only
    // predicates the planner routes to an index are eq/lt/lte/gt/gte (IS NULL is answered from the
    // validity mask, never from an index). It is also what keeps the stores sound: a NULL key
    // carries logical_type::NA rather than the column's type, and both backends cast every key to
    // the column type before storing it -- a cast that fails for NA. Admitting one corrupts the
    // index (a truncated batch, or, when the NULL arrives first, a tree of mixed NA/typed keys
    // under a comparator that is not a strict weak ordering).
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

        // Wipe the stored index data AND every pending bucket while keeping the agent alive and
        // writable (bitcask: segments + keydir + txn-log + applied-offset sidecar; btree: the tree
        // file). NOT the terminal drop -- the runtime repopulate path clears and then re-stages
        // with txn_id == 0. The buckets go WITH the store: they are the not-yet-durable half of the
        // same index, and a clear that wiped only the durable half would leave a rebuilt index
        // reporting rows the scan it was rebuilt from never produced.
        unique_future<core::error_t> clear(session_id_t session);

        // RECORD a whole statement's inserts / deletes in this transaction's bucket. Nothing
        // reaches the store here -- there is no write-through before commit. txn_id == 0 is the
        // committed-for-everyone bucket the rebuild feeds use. The unit is a BATCH OF (key, row_id)
        // PAIRS rather than a data_chunk_t: the manager already resolved which chunk column carries
        // this index's key, so forwarding the chunk would clone every column of it, per agent, to
        // read one.
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);

        // PUBLISH this transaction's bucket into the store and drop it. Both buckets are
        // taken: `txn_id` (this transaction's own) and 0 (committed for everyone but not
        // yet durable), which is the pair the commit path has always folded together.
        // Returns the IO failure as a VALUE so manager_index_t's commit handlers can fail
        // the statement instead of the process.
        //
        // commit_id IS CARRIED FOR THE DURABLE JOURNAL, and the hashed family is the only one that
        // spends it: it goes into the txn-log frame, and the recover gate matches it against the
        // WAL's committed COMMIT-ID set. It has to be a CONTRACT parameter rather than a bitcask-only
        // one -- the message id is this method's POSITION in dispatch_traits, so a parameter added on
        // one side only would send every later handler's messages to the wrong body. The ordered
        // family takes it and does not use it, which is the honest shape: an agent that keeps no
        // journal has nothing to stamp.
        //
        // WHY THE COMMIT ID AND NOT THE TXN ID DECIDES THE REPLAY: txn ids restart at
        // TRANSACTION_ID_START in every process, so a marker of an earlier incarnation vouches for a
        // later one's frame of the same id; commit ids are re-derived from the durable frontier at
        // every reopen (transaction_manager_t::restore_commit_clock) and are never handed out twice.
        // 0 is the "no commit id" value (the clock starts at 1) and the txn_id == 0 rebuild feed,
        // which journals nothing, is the one caller that passes it.
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // DISCARD this transaction's bucket. Nothing durable was written for it, so the
        // abort is a bucket erase and touches no store.
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);

        // THE read: every row id whose key satisfies `compare` against `key`, duplicates included,
        // in ONE reply -- there is no cursor. BOTH HALVES, merged here rather than by the caller:
        // the committed rows out of the store, plus `txn_id`'s own staged inserts (and bucket 0's),
        // minus its own staged deletes. That is why the predicate travels with the key -- a staged
        // row keyed 3 belongs in the answer to `x < 5` and not in the answer to `x = 5`. An empty
        // vector means "no row satisfies the predicate"; anything else that went wrong comes back
        // as the error half of the wrapper.
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);

        // Checkpoint fan-out from manager_index_t::flush_all_indexes. Ordered behind any pending
        // write in this agent's FIFO, so it never races one. IT REPORTS: a void return would have
        // both families log the store's io_error and reply as if the flush had happened, and the
        // checkpoint that asked for it would go on to truncate the WAL behind an index whose
        // entries were still only in memory.
        unique_future<core::error_t> force_flush(session_id_t session);

        // THE ORDER IS THE MESSAGE ID SPACE. actor_zeta::msg_id is the method's INDEX in
        // this list (action_id_impl -> find_method_index), so removing or reordering an
        // entry renumbers every entry after it -- and find_method_index answers 0 for a
        // method it cannot find, which is a send that lands on drop(). Change this list
        // and both agent classes' `implements<>` lists in ONE edit, never one of the three
        // alone.
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

    // WHAT EVERY DISK INDEX AGENT MUST BE, checked AT ITS OWN DEFINITION. Four things nothing else
    // catches, because these classes have no base class:
    //
    //   * a handler never written -- no abstract-class diagnostic and no `override` to attach,
    //     so the first word would arrive at a send site in another translation unit;
    //   * a handler with a drifted signature -- `implements<>` compares against the contract
    //     POSITIONALLY, so two swapped-but-same-shaped methods pass it; the requirements below
    //     do not;
    //   * an owning-pointer alias typed by anything but the class itself:
    //     actor_zeta::pmr::deleter_t deallocates sizeof(static type), so an owner typed by a
    //     base -- or by the other family's agent -- returns the wrong number of bytes to the
    //     pool, which is why manager_index_t owns two typed vectors and not one;
    //   * a missing STATIC answer to "which backend am I, and can I be asked an ordered probe".
    //     manager_index_t copies both into its per-index record so a range predicate is refused
    //     BEFORE any send; without the requirement the refusal becomes an abort in the agent.
    //
    // What is deliberately NOT here: create(). The two factories take different parameters (a
    // segment-record limit and the WAL committed-txn set exist only for the family that keeps a txn
    // log) and that difference is the specialization, not a drift.
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
    // Test-observable count of LIVE disk index agents, both families together. It is what
    // separates "the table was dropped" from "the table was dropped AND its agent was
    // freed": a reaped agent closes its store, an unreaped one keeps the files open with
    // nothing left that can address it. Bumped in each agent's constructor and destructor
    // (the two .cpp files), never read on a decision path.
    inline std::atomic<uint64_t> g_live_index_agents{0};
    [[nodiscard]] inline uint64_t live_index_agents() noexcept {
        return g_live_index_agents.load(std::memory_order_relaxed);
    }
#endif

} // namespace services::index
