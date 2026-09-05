#pragma once

// THE HASHED FAMILY'S AGENT, AND THE WHOLE HASHED INDEX. It holds a
// bitcask_index_disk_t BY VALUE AND BY ITS CONCRETE TYPE -- the store is a member, not a
// pointee -- so every question the erased agent used to ask its backend at runtime --
// does this backend own a txn log? does it have a bulk window to open? can it answer an
// ordered probe? -- is answered here by the type instead. See btree_index_agent.hpp for
// the ordered twin; the two are deliberately separate bodies, and
// index_agent_contract.hpp says why.
//
// IT ALSO HOLDS THE UNCOMMITTED HALF. What used to be disk_hash_single_field_index_t --
// a "facade" registered in a per-table index registry above the mailbox, whose seven
// read/write doors all aborted because it had neither data nor a search -- was never an
// index. It was a BUFFER of this transaction's not-yet-durable writes, sitting in a
// different actor from the store those writes belong to, so the two halves of one answer
// had to be stitched together after the read came back. The buffer is here now, beside
// the store, and read_rows below returns both halves already merged.

#include "bitcask_index_disk.hpp"
#include "index_agent_contract.hpp"

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace services::index {

    // Owns its bitcask store exclusively; callers reach it only via mailbox sends to its
    // address (no shared mutable state across the actor boundary, rule 10).
    //
    // No DROP TABLE GC handler here: on-disk index files sit alongside table files and
    // are unlinked by manager_disk_t's on_horizon_advanced sweep.
    class bitcask_index_agent_t final : public actor_zeta::basic_actor<bitcask_index_agent_t> {
        using path_t = std::filesystem::path;
        using session_id_t = index_agent_contract::session_id_t;
        using value_t = index_agent_contract::value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // The agent's own type, spelled here because the alias below it needs the class to
        // be named first. Typed by THIS class: actor_zeta::pmr::deleter_t deallocates
        // sizeof(static type).
        using agent_ptr_t = std::unique_ptr<bitcask_index_agent_t, actor_zeta::pmr::deleter_t>;

        // WHICH BACKEND THIS FAMILY IS, as a compile-time constant of the class.
        //
        // It replaces index_t::type(), a virtual accessor on a per-index object, and it
        // must not be lost with it: manager_index_t copies this into the record it keeps
        // per index and publishes it to the planner through all_indexed_descriptions ->
        // context_storage_t. That is the ONLY thing that tells an ordered index from a
        // hashed one OVER THE SAME COLUMN, which is a legal pair, and losing it makes
        // `USING hash` a word the planner cannot honour.
        static constexpr components::logical_plan::index_type index_type_v =
            components::logical_plan::index_type::hashed;

        // A HASH BUCKET HAS NO ORDERING, so nothing but equality can be asked of this
        // family -- also a compile-time constant, and also copied into the manager's
        // record. It replaces index_t::supports_ordered_probe(), and it has to survive in
        // a form the manager can read BEFORE it sends anything: the manager refuses a
        // range predicate on this family with a core::error_t, and without a static
        // answer up there the misrouted read would reach read_rows() below and be
        // refused only after a round trip -- or, before the error channel existed, abort
        // the process inside the store.
        static constexpr bool supports_ordered_probe_v = false;

        // THE DOOR. An agent, or the reason its store could not be opened -- as a VALUE,
        // not as a flag on an agent that exists anyway. The agent opens its own backing
        // (rule 10): the open runs in this function, in this translation unit, and nothing
        // is created at a spawn site and handed across. There is no state-then-ask
        // convention left to forget -- an agent only ever exists over a store that opened,
        // and a caller cannot reach one without going through result_wrapper_t first.
        //
        // On an error the half-built agent is destroyed here, so the caller has nothing to
        // unwind and leaves the index UNREGISTERED -- an index that will not open costs a
        // full scan, while aborting costs the whole engine its start (integration test
        // test_index_bootstrap_failure).
        //
        // TWO STEPS INSIDE, ONE STEP OUTSIDE: the agent is spawned (which builds its store
        // in place, doing no I/O) and then open_store() is run on it, before its address
        // has been handed to anyone. Splitting them is what the by-value store costs and
        // all it costs -- a store that holds a shared_mutex cannot be built elsewhere and
        // moved in, so the open cannot happen before the agent exists.
        //
        // committed_txn_ids: the WAL-replay set of committed transaction ids, forwarded to
        // the txn-log recover gate (M1.1). Fresh, post-bootstrap agents pass an EMPTY set
        // -- a fresh dir has no txn-log to gate. This parameter exists ONLY here: the
        // ordered family owns no txn log and its factory does not take one.
        //
        // index_oid = pg_index.indexrelid; the agent's on-disk directory is
        // ${path_db}/${table_oid}/${index_oid}/ -- oid-keyed, never name-keyed.
        [[nodiscard]] static core::result_wrapper_t<agent_ptr_t> create(std::pmr::memory_resource* resource,
                                                                        const path_t& path_db,
                                                                        components::catalog::oid_t table_oid,
                                                                        components::catalog::oid_t index_oid,
                                                                        uint64_t flush_threshold,
                                                                        uint64_t segment_record_limit,
                                                                        log_t& log,
                                                                        std::pmr::set<std::uint64_t> committed_txn_ids);

        // BUILDS THE STORE, it does not receive one. The store is a member BY VALUE and
        // cannot be moved into place (bitcask_index_disk_t holds a shared_mutex, which is
        // immovable, and its deleted copy ctor suppresses the implicit move), so what
        // crosses this signature is the store's PARAMETERS and the store is constructed in
        // the member initializer list.
        //
        // Construction is still infallible, because the ctor it runs does no I/O: the open
        // is open_store() below, and create() above is the only thing that calls it. That
        // is what keeps rule 2 -- a constructor cannot refuse, so the step that can fail is
        // not a constructor.
        //
        // Public because actor_zeta::spawn placement-news the actor.
        bitcask_index_agent_t(std::pmr::memory_resource* resource,
                              const path_t& path_db,
                              components::catalog::oid_t table_oid,
                              components::catalog::oid_t index_oid,
                              uint64_t flush_threshold,
                              uint64_t segment_record_limit,
                              log_t& log,
                              std::pmr::set<std::uint64_t> committed_txn_ids);
        ~bitcask_index_agent_t();

        [[nodiscard]] components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        unique_future<void> drop(session_id_t session);
        unique_future<core::error_t> clear(session_id_t session);
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);
        unique_future<void> force_flush(session_id_t session);

        // Bound to the contract, in the contract's order -- that is what makes
        // msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::drop> and
        // msg_id<btree_index_agent_t, &btree_index_agent_t::drop> the same number, and it
        // is the only reason manager_index_t may send to a bare address.
        using dispatch_traits = actor_zeta::implements<index_agent_contract,
                                                       &bitcask_index_agent_t::drop,
                                                       &bitcask_index_agent_t::clear,
                                                       &bitcask_index_agent_t::stage_inserts,
                                                       &bitcask_index_agent_t::stage_deletes,
                                                       &bitcask_index_agent_t::commit_inserts,
                                                       &bitcask_index_agent_t::commit_deletes,
                                                       &bitcask_index_agent_t::revert_inserts,
                                                       &bitcask_index_agent_t::revert_deletes,
                                                       &bitcask_index_agent_t::read_rows,
                                                       &bitcask_index_agent_t::force_flush>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // Opens the store this agent just built and says why it could not, as a VALUE.
        // Called by create() IMMEDIATELY after the agent is spawned and by nothing else:
        // the pair (spawn, open_store) is one step seen from outside, and the agent is
        // destroyed unpublished when the second half of it fails. That is the whole reason
        // an agent can be assumed to have an open store everywhere below.
        [[nodiscard]] core::error_t open_store();

        log_t log_;
        components::catalog::oid_t table_oid_;
        // BY VALUE, and OPEN: create() is the only door, and it destroys the agent rather
        // than publishing one whose store did not open. Every handler can read it without
        // asking first. There is no unique_ptr here because there is nothing a pointer
        // could buy -- this agent is the sole owner, the type is fixed at compile time,
        // and the erased base that once forced the indirection is gone. CONCRETE type,
        // and there is no erased base left to hold it by: the long-key loader
        // (load_hash_key_at_unlocked, handed to every disk_hash_table_t call) and the
        // durable txn log are bitcask's alone, and holding the type is what keeps them
        // reachable without a runtime question.
        bitcask_index_disk_t store_;
        bool is_dropped_{false};

        // THE UNCOMMITTED HALF, in per-transaction buckets.
        //
        // A pending entry keeps its key ENCODED, in the record format the bitcask store
        // uses (codec::append_logical_value over the NORMALIZED key, byte-for-byte what
        // codec::encode_disk_hash_key produces for the same value). Two reasons, both
        // load-bearing:
        //   * the comparison that decides whether a pending key satisfies the probe must
        //     be the SAME comparison the committed half was answered by, and that one
        //     hashes and memcmps these exact bytes
        //     (bitcask_index_disk_t::key_bytes_for_hash);
        //   * the encoding is where the HASHED family's normalization happens (narrow
        //     integers widened to BIGINT / UBIGINT), so a SMALLINT probe and the
        //     BIGINT-stored key it should match land on the same bytes.
        //
        // Bucket 0 is "committed for everyone but not yet durable" -- the rebuild feeds
        // stage into it and commit_inserts publishes it alongside whatever transaction is
        // committing.
        using pending_row_t = std::pair<std::pmr::string, int64_t>;
        using pending_rows_t = std::pmr::vector<pending_row_t>;
        using pending_txn_map_t = std::pmr::unordered_map<uint64_t, pending_rows_t>;
        pending_txn_map_t pending_inserts_;
        pending_txn_map_t pending_deletes_;

        // key -> the bitcask store's key bytes, NORMALIZED first. The row id is NOT
        // appended: a bucket carries it beside the key, and the merge needs the key alone.
        [[nodiscard]] std::pmr::string encode_key(const value_t& key) const;

        // Publish one bucket pair ({txn_id} and 0) into the store and erase them.
        // Shared by the insert and delete legs, which differ only in which map they take
        // from and which store call they make -- that difference is the `apply` callable.
        template<typename ApplyFn>
        [[nodiscard]] core::error_t publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply);
    };

    // The contract, checked where the class is written (see index_agent_contract.hpp): a
    // handler this agent forgot, mistyped or bound out of order fails HERE and not at the
    // send site in manager_index.cpp.
    static_assert(index_agent_impl<bitcask_index_agent_t>,
                  "bitcask_index_agent_t does not satisfy the index agent contract");

    using bitcask_index_agent_ptr = bitcask_index_agent_t::agent_ptr_t;

} // namespace services::index
