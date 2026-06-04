#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/index/forward.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <core/result_wrapper.hpp>

namespace services::index {

    using session_id_t = components::session::session_id_t;
    using index_name_t = std::string;
    using transaction_data = components::table::transaction_data;
    using execution_context_t = components::execution_context_t;

    struct index_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Collection lifecycle (oid-keyed)
        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

        // DML: txn-aware bulk index operations
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
        // commit_insert / commit_delete return a core::error_t directly
        // (project-wide convention — error_t::no_error() means success,
        // contains_error() flags failure). Lets callers branch on a future
        // index-side abort path without another signature migration.
        unique_future<core::error_t>
        commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<core::error_t>
        commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);
        unique_future<void> rebuild_indexes(session_id_t session, components::catalog::oid_t table_oid);

        // DDL: index management
        unique_future<uint32_t> create_index(session_id_t session,
                                             components::catalog::oid_t table_oid,
                                             index_name_t index_name,
                                             components::index::keys_base_storage_t keys,
                                             components::logical_plan::index_type type,
                                             core::date::timezone_offset_t session_tz);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        // Query (txn-aware)
        unique_future<std::pmr::vector<int64_t>> search(session_id_t session,
                                                        components::catalog::oid_t table_oid,
                                                        components::index::keys_base_storage_t keys,
                                                        components::types::logical_value_t value,
                                                        components::expressions::compare_type compare,
                                                        uint64_t start_time,
                                                        uint64_t txn_id,
                                                        core::date::timezone_offset_t session_tz);
        // Query (txn-aware)
        unique_future<std::pmr::vector<int64_t>> search_with_preferred_type(session_id_t session,
                                                        components::catalog::oid_t table_oid,
                                                        components::index::keys_base_storage_t keys,
                                                        components::types::logical_value_t value,
                                                        components::expressions::compare_type compare,
                                                        components::logical_plan::index_type preferred_index_type,
                                                        uint64_t start_time,
                                                        uint64_t txn_id,
                                                        core::date::timezone_offset_t session_tz);

        unique_future<bool>
        has_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        unique_future<void> flush_all_indexes(session_id_t session);

        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
        get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<std::pmr::vector<components::index::index_description_t>>
        get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid);

        // Event-driven GC subscriber. Walks dropped_table_agents_ and
        // erases routing entries whose dropped_at_commit_id < new_horizon.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Runtime DROP TABLE path — operator_dynamic_cascade_delete sends this
        // from inside the executor actor so the manager_index side records the
        // (oid, dropped_at_commit_id) pair into dropped_table_agents_ for the
        // next horizon-advance GC sweep. Pair with
        // manager_dispatcher_t::on_drop_resource_marked(INDEX_KIND).
        unique_future<void> mark_table_dropped(session_id_t session,
                                               components::catalog::oid_t table_oid,
                                               uint64_t dropped_at_commit_id);

        // CREATE INDEX catchup. Called per matching WAL record by
        // operator_create_index_backfill to apply a
        // PHYSICAL_{INSERT,DELETE,UPDATE} effect to the build's in-memory
        // index_engine_t. The chunk + row metadata are shipped over the
        // mailbox so the handler can drive engine_->insert_row /
        // mark_delete_row directly (mirrors the insert_rows / delete_rows
        // DML path). physical_data is the row chunk (NEW rows for
        // INSERT/UPDATE, empty/null for DELETE — see TBD note in the
        // handler). physical_row_start is the WAL header's row-id base used
        // when row_ids is not supplied. txn_id is the CREATE INDEX
        // transaction so replayed entries land in the PENDING bucket and
        // get committed by the post-pipeline commit_insert. session_tz is
        // forwarded into the index key encoder.
        unique_future<void> apply_wal_record_for_index(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t index_oid,
                                                       uint64_t wal_record_id,
                                                       uint8_t record_type,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::unique_ptr<components::vector::data_chunk_t> physical_data,
                                                       uint64_t physical_row_start,
                                                       uint64_t txn_id,
                                                       core::date::timezone_offset_t session_tz);

        using dispatch_traits = actor_zeta::dispatch_traits<&index_contract::register_collection,
                                                            &index_contract::unregister_collection,
                                                            &index_contract::insert_rows,
                                                            &index_contract::delete_rows,
                                                            &index_contract::update_rows,
                                                            &index_contract::commit_insert,
                                                            &index_contract::commit_delete,
                                                            &index_contract::revert_insert,
                                                            &index_contract::cleanup_all_versions,
                                                            &index_contract::rebuild_indexes,
                                                            &index_contract::create_index,
                                                            &index_contract::drop_index,
                                                            &index_contract::search,
                                                            &index_contract::search_with_preferred_type,
                                                            &index_contract::has_index,
                                                            &index_contract::flush_all_indexes,
                                                            &index_contract::get_indexed_keys,
                                                            &index_contract::get_indexed_descriptions,
                                                            &index_contract::on_horizon_advanced,
                                                            &index_contract::mark_table_dropped,
                                                            &index_contract::apply_wal_record_for_index>;

        index_contract() = delete;
    };

} // namespace services::index
