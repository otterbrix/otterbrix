#pragma once

// Holds bitcask_index_disk_t by value, not type-erased, so backend capabilities resolve by type
// -- see index_agent_contract.hpp for why the hashed and ordered families aren't a common base.

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

    // No separate DROP TABLE handler: manager_index_t's unregister_collection sends the same
    // drop() to every agent when the owning table is dropped (services/index/manager_index.cpp).
    class bitcask_index_agent_t final : public actor_zeta::basic_actor<bitcask_index_agent_t> {
        using path_t = std::filesystem::path;
        using session_id_t = index_agent_contract::session_id_t;
        using value_t = index_agent_contract::value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Must name the concrete class: actor_zeta::pmr::deleter_t deallocates sizeof(static type).
        using agent_ptr_t = std::unique_ptr<bitcask_index_agent_t, actor_zeta::pmr::deleter_t>;

        // The only thing distinguishing an ordered index from a hashed one on the same column.
        static constexpr components::logical_plan::index_type index_type_v =
            components::logical_plan::index_type::hashed;

        // Read at compile time to refuse a range predicate before send, not after a round trip to read_rows().
        static constexpr bool supports_ordered_probe_v = false;

        // On failure the half-built agent is destroyed and the index stays unregistered (full scan
        // cost) rather than aborting the engine's start (see test_index_bootstrap_failure). index_oid
        // is pg_index.indexrelid; on-disk path is ${path_db}/${table_oid}/${index_oid}/.
        [[nodiscard]] static core::result_wrapper_t<agent_ptr_t>
        create(std::pmr::memory_resource* resource,
               const path_t& path_db,
               components::catalog::oid_t table_oid,
               components::catalog::oid_t index_oid,
               uint64_t flush_threshold,
               uint64_t segment_record_limit,
               log_t& log,
               std::pmr::set<std::uint64_t> commit_ids);

        // The ctor does no I/O so it cannot fail; open_store() below is the fallible half, called
        // only from create(). Public because actor_zeta::spawn placement-news the actor.
        bitcask_index_agent_t(std::pmr::memory_resource* resource,
                              const path_t& path_db,
                              components::catalog::oid_t table_oid,
                              components::catalog::oid_t index_oid,
                              uint64_t flush_threshold,
                              uint64_t segment_record_limit,
                              log_t& log,
                              std::pmr::set<std::uint64_t> commit_ids);
        ~bitcask_index_agent_t();

        [[nodiscard]] components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        unique_future<void> drop(session_id_t session);
        unique_future<core::error_t> clear(session_id_t session);
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);
        unique_future<core::error_t> force_flush(session_id_t session);

        // Order must match btree_index_agent_t's dispatch_traits exactly: msg_id positions are what
        // let manager_index_t send to a bare address across both families.
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
        // Called only by create(), right after spawn; failure destroys the agent unpublished.
        [[nodiscard]] core::error_t open_store();

        // Runs post-write compaction the store's rotations left owed, on this agent's own thread.
        // Returns the write's error if any, else the merge's.
        [[nodiscard]] core::error_t pay_merge_debt(core::error_t write_error);

        log_t log_;
        components::catalog::oid_t table_oid_;
        // Concrete, not a pointer: load_hash_key_at and the durable txn log are reachable only through it.
        bitcask_index_disk_t store_;
        bool is_dropped_{false};

        // Keys are encoded exactly as bitcask_index_disk_t::key_bytes_for_hash hashes/memcmps (narrow
        // ints widened to BIGINT/UBIGINT, so a SMALLINT probe matches a BIGINT-stored key). Bucket 0
        // is committed-but-not-yet-durable, published by commit_inserts alongside the committing transaction.
        using pending_row_t = std::pair<std::pmr::string, int64_t>;
        using pending_rows_t = std::pmr::vector<pending_row_t>;
        using pending_txn_map_t = std::pmr::unordered_map<uint64_t, pending_rows_t>;
        pending_txn_map_t pending_inserts_;
        pending_txn_map_t pending_deletes_;

        // Normalized key bytes only, no row id -- the bucket carries that separately.
        [[nodiscard]] std::pmr::string encode_key(const value_t& key) const;

        // Publishes/erases the ({txn_id}, 0) pair; `apply` is the only difference between insert and delete.
        template<typename ApplyFn>
        [[nodiscard]] core::error_t publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply);
    };

    // Catches a forgotten/mistyped/misordered handler here, not at the send site in manager_index.cpp.
    static_assert(index_agent_impl<bitcask_index_agent_t>,
                  "bitcask_index_agent_t does not satisfy the index agent contract");

    using bitcask_index_agent_ptr = bitcask_index_agent_t::agent_ptr_t;

} // namespace services::index
