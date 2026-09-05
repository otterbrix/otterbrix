#pragma once

// THE ORDERED FAMILY'S AGENT, AND THE WHOLE ORDERED INDEX. It holds a btree_index_disk_t BY VALUE
// AND BY ITS CONCRETE TYPE -- the store is a member, not a pointee -- so every question an erased
// agent would have to ask its backend at runtime (does this backend own a txn log? does it have a
// bulk window to open? can it answer an ordered probe?) is answered here by the type instead. See
// bitcask_index_agent.hpp for the hashed twin; the two are deliberately separate bodies, and
// index_agent_contract.hpp says why.
//
// IT ALSO HOLDS THE UNCOMMITTED HALF: the BUFFER of this transaction's not-yet-durable writes.
// Keeping that buffer in a different actor from the tree those writes belong to means the two
// halves of one answer have to be stitched together after the read comes back. It sits beside the
// tree instead, and read_rows below returns both halves already merged.

#include "btree_index_disk.hpp"
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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace services::index {

    // Owns its b+tree exclusively; callers reach it only via mailbox sends to its address
    // (no shared mutable state across the actor boundary, rule 10).
    //
    // No DROP TABLE GC handler here: on-disk index files sit alongside table files and
    // are unlinked by manager_disk_t's on_horizon_advanced sweep.
    class btree_index_agent_t final : public actor_zeta::basic_actor<btree_index_agent_t> {
        using path_t = std::filesystem::path;
        using session_id_t = index_agent_contract::session_id_t;
        using value_t = index_agent_contract::value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // The agent's own type, spelled here because the alias below it needs the class to
        // be named first. Typed by THIS class: actor_zeta::pmr::deleter_t deallocates
        // sizeof(static type).
        using agent_ptr_t = std::unique_ptr<btree_index_agent_t, actor_zeta::pmr::deleter_t>;

        // WHICH BACKEND THIS FAMILY IS, as a compile-time constant of the class. manager_index_t
        // copies it into the record it keeps per index and publishes it to the planner through
        // all_indexed_descriptions -> context_storage_t; that is the ONLY thing that tells an
        // ordered index from a hashed one OVER THE SAME COLUMN, which is a legal pair.
        //
        // `single` rather than the catalog's own word: SQL has exactly one explicit spelling,
        // `USING hash`, and everything else (composite / multikey / wildcard / no USING clause at
        // all) is built by this family and published as `single`.
        static constexpr components::logical_plan::index_type index_type_v =
            components::logical_plan::index_type::single;

        // A B+TREE IS ORDERED, so every predicate is answerable -- also a compile-time
        // constant, and also copied into the manager's record. This is the index the
        // planner requires before it will route a range predicate at all (can_use_index
        // refuses lt/lte/gt/gte unless a NON-hashed index covers the key), and the
        // manager reads it to decide whether a range may be dispatched.
        static constexpr bool supports_ordered_probe_v = true;

        // THE DOOR, and the same shape as the hashed family's: an agent, or the reason its tree
        // could not be opened, as a VALUE. The agent opens its own backing (rule 10): the open runs
        // in this function, in this translation unit, and nothing is created at a spawn site and
        // handed across.
        //
        // What is MISSING from this parameter list is the specialization: there is no
        // segment-record limit (the ordered store has no segments) and no WAL committed-txn set (it
        // owns no txn log, so there is no recover gate to arm). Those belong to the hashed family
        // alone.
        //
        // index_oid = pg_index.indexrelid; the agent's on-disk directory is
        // ${path_db}/${table_oid}/${index_oid}/ -- oid-keyed, never name-keyed.
        [[nodiscard]] static core::result_wrapper_t<agent_ptr_t> create(std::pmr::memory_resource* resource,
                                                                        const path_t& path_db,
                                                                        components::catalog::oid_t table_oid,
                                                                        components::catalog::oid_t index_oid,
                                                                        uint64_t flush_threshold,
                                                                        log_t& log);

        // BUILDS THE TREE, it does not receive one. The store is a member BY VALUE and cannot be
        // moved into place (btree_index_disk_t's deleted copy ctor suppresses the implicit move),
        // so what crosses this signature is the store's PARAMETERS and the store is opened in the
        // member initializer list.
        //
        // NO DEFERRED OPEN HERE, and the asymmetry with the hashed family is the point. That family
        // splits construction from open() because its open can FAIL and rule 2 forbids a
        // constructor from refusing. This one has nothing to return: btree_t::load() is void, and a
        // load failure is recorded on the tree's own channel and refused by the first operation
        // that consults it (btree_index_disk.cpp). An open() that could only ever answer no_error()
        // would be inventing a failure to make the shapes match.
        //
        // Public because actor_zeta::spawn placement-news the actor.
        btree_index_agent_t(std::pmr::memory_resource* resource,
                            const path_t& path_db,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t index_oid,
                            uint64_t flush_threshold,
                            log_t& log);
        ~btree_index_agent_t();

        [[nodiscard]] components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        unique_future<void> drop(session_id_t session);
        unique_future<core::error_t> clear(session_id_t session);
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        // commit_id is taken and NOT spent here, and that is the honest shape rather than an
        // oversight: it exists so the hashed family can stamp its durable txn-log frame with an
        // identity no restart hands out twice, and this family keeps no journal at all. It cannot be
        // a bitcask-only parameter either -- the contract is POSITIONAL by msg_id, so both classes
        // carry it or neither does (index_agent_contract.hpp). It is traced, so a publish can still
        // be tied to the transaction that caused it.
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);
        unique_future<core::error_t> force_flush(session_id_t session);

        // Bound to the contract, in the contract's order -- that is what makes
        // msg_id<btree_index_agent_t, &btree_index_agent_t::drop> and
        // msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::drop> the same number, and
        // it is the only reason manager_index_t may send to a bare address.
        using dispatch_traits = actor_zeta::implements<index_agent_contract,
                                                       &btree_index_agent_t::drop,
                                                       &btree_index_agent_t::clear,
                                                       &btree_index_agent_t::stage_inserts,
                                                       &btree_index_agent_t::stage_deletes,
                                                       &btree_index_agent_t::commit_inserts,
                                                       &btree_index_agent_t::commit_deletes,
                                                       &btree_index_agent_t::revert_inserts,
                                                       &btree_index_agent_t::revert_deletes,
                                                       &btree_index_agent_t::read_rows,
                                                       &btree_index_agent_t::force_flush>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        log_t log_;
        components::catalog::oid_t table_oid_;
        // BY VALUE, and open by the time the constructor returns. There is no unique_ptr
        // here because there is nothing a pointer could buy -- this agent is the sole
        // owner, the type is fixed at compile time, and the erased base that once forced
        // the indirection is gone. CONCRETE type, and there is no erased base left to hold
        // it by: the ordered scan_range contract -- the whole reason this family exists --
        // is what holding the type keeps reachable without a runtime question.
        btree_index_disk_t store_;
        bool is_dropped_{false};

        // THE UNCOMMITTED HALF, in per-transaction buckets.
        //
        // A pending entry keeps its key ENCODED, in the b+tree's own record format
        // (codec::append_logical_value). Two reasons, both load-bearing: the comparison that
        // decides whether a staged key satisfies the probe must be the SAME comparison the tree
        // used for the committed half, and codec::read_logical_value_as_view decodes exactly these
        // bytes into the physical_value the tree orders by (comparing logical_value_t instead would
        // need a cast into one type domain, and logical_value_t::operator< asserts both sides carry
        // the same type); and the encoding is what the store's own key getter reads, so a staged
        // key and a committed key are the same kind of value by construction.
        //
        // Bucket 0 is "committed for everyone but not yet durable" -- the rebuild feeds stage into
        // it and commit_inserts publishes it alongside whatever transaction is committing.
        using pending_row_t = std::pair<std::pmr::string, int64_t>;
        using pending_rows_t = std::pmr::vector<pending_row_t>;
        using pending_txn_map_t = std::pmr::unordered_map<uint64_t, pending_rows_t>;
        pending_txn_map_t pending_inserts_;
        pending_txn_map_t pending_deletes_;

        // key -> the b+tree record's key bytes, in the column's own type. NO
        // normalization: the tree stores and compares the column's type, which is the
        // hashed family's step and not this one's. The row id is NOT appended: a bucket
        // carries it beside the key, and the merge needs the key alone.
        [[nodiscard]] std::pmr::string encode_key(const value_t& key) const;

        // Publish one bucket pair ({txn_id} and 0) into the tree and erase them. Shared by
        // the insert and delete legs, which differ only in which map they take from and
        // which store call they make -- that difference is the `apply` callable.
        template<typename ApplyFn>
        [[nodiscard]] core::error_t publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply);
    };

    // The contract, checked where the class is written (see index_agent_contract.hpp): a
    // handler this agent forgot, mistyped or bound out of order fails HERE and not at the
    // send site in manager_index.cpp.
    static_assert(index_agent_impl<btree_index_agent_t>,
                  "btree_index_agent_t does not satisfy the index agent contract");

    using btree_index_agent_ptr = btree_index_agent_t::agent_ptr_t;

} // namespace services::index
