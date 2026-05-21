#pragma once

#include "index_contract.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include "index_agent_disk.hpp"
#include <components/catalog/catalog_codes.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/file/local_file_system.hpp>
#include <mutex>

namespace services::index {

    // INDEXES_METADATA_FILENAME retired. Index metadata lives in
    // pg_catalog.pg_index now; this constant is kept as a comment so anyone reading
    // legacy data dirs can still recognize the filename.

    class manager_index_t final : public actor_zeta::actor::actor_mixin<manager_index_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t>;
        using run_fn_t = std::function<void()>;

        manager_index_t(
            std::pmr::memory_resource* resource,
            actor_zeta::scheduler_raw scheduler,
            log_t& log,
            std::filesystem::path path_db = {},
            run_fn_t run_fn = [] { std::this_thread::yield(); });
        ~manager_index_t() = default;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(address_pack pack);

        // Collection lifecycle
        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

        // DML: txn-aware bulk index operations.
        unique_future<void> insert_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        uint64_t start_row_id,
                                        uint64_t count);
        unique_future<void> delete_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        std::pmr::vector<int64_t> row_ids);
        unique_future<void> update_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> old_data,
                                        std::unique_ptr<components::vector::data_chunk_t> new_data,
                                        std::pmr::vector<int64_t> row_ids,
                                        int64_t new_start_row_id);

        // MVCC commit/revert/cleanup
        unique_future<void>
        commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<void>
        commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);
        unique_future<void> rebuild_indexes(session_id_t session, components::catalog::oid_t table_oid);

        // DDL: index management
        unique_future<uint32_t> create_index(session_id_t session,
                                             components::catalog::oid_t table_oid,
                                             index_name_t index_name,
                                             components::index::keys_base_storage_t keys,
                                             components::logical_plan::index_type type);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        // Query (txn-aware)
        unique_future<std::pmr::vector<int64_t>> search(session_id_t session,
                                                        components::catalog::oid_t table_oid,
                                                        components::index::keys_base_storage_t keys,
                                                        components::types::logical_value_t value,
                                                        components::expressions::compare_type compare,
                                                        uint64_t start_time,
                                                        uint64_t txn_id);

        unique_future<bool>
        has_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        unique_future<void> flush_all_indexes(session_id_t session);

        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
        get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid);

        using dispatch_traits = actor_zeta::implements<index_contract,
                                                       &manager_index_t::register_collection,
                                                       &manager_index_t::unregister_collection,
                                                       &manager_index_t::insert_rows,
                                                       &manager_index_t::delete_rows,
                                                       &manager_index_t::update_rows,
                                                       &manager_index_t::commit_insert,
                                                       &manager_index_t::commit_delete,
                                                       &manager_index_t::revert_insert,
                                                       &manager_index_t::cleanup_all_versions,
                                                       &manager_index_t::rebuild_indexes,
                                                       &manager_index_t::create_index,
                                                       &manager_index_t::drop_index,
                                                       &manager_index_t::search,
                                                       &manager_index_t::has_index,
                                                       &manager_index_t::flush_all_indexes,
                                                       &manager_index_t::get_indexed_keys>;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        run_fn_t run_fn_;
        log_t log_;
        std::filesystem::path path_db_;
        std::mutex mutex_;

        // Per-collection in-memory index engines (keyed by table oid)
        std::pmr::unordered_map<components::catalog::oid_t, components::index::index_engine_ptr> engines_;

        // Per-index disk persistence (child actors)
        std::vector<index_agent_disk_ptr> disk_agents_;

        // indexes_METADATA file + write/read/remove_indexes_from_metafile retired
        // — all index metadata lives in pg_catalog.pg_index now.
        core::filesystem::local_file_system_t fs_;

        // Address of manager_disk_t (for scan_segment when populating indexes)
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();

        // Find disk agent by address and schedule it if needed
        void schedule_agent(const actor_zeta::address_t& addr, bool needs_sched);

        // Pending futures
        std::pmr::vector<unique_future<void>> pending_void_;
        void poll_pending();

        actor_zeta::behavior_t current_behavior_;
    };

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index