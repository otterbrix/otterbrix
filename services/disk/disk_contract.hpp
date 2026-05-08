#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>
#include <optional>
#include <utility>
#include <vector>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/ddl_result.hpp>
#include <services/disk/invalidation_ring_buffer.hpp>
#include <services/disk/resolve_result.hpp>
#include <services/wal/base.hpp>

namespace services::disk {

    using session_id_t = components::session::session_id_t;
    using execution_context_t = components::execution_context_t;

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        actor_zeta::unique_future<void> flush(session_id_t session, services::wal::id_t wal_id);

        actor_zeta::unique_future<services::wal::id_t> checkpoint_all(session_id_t session,
                                                                      services::wal::id_t current_wal_id);
        actor_zeta::unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        actor_zeta::unique_future<void> maybe_cleanup(execution_context_t ctx, uint64_t lowest_active_start_time);

        // DDL pipeline — kept methods only
        actor_zeta::unique_future<ddl_result_t>
        ddl_adopt_computing_schema(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<ddl_result_t>
        ddl_computed_append(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::string field_name,
                            components::catalog::oid_t type_oid);
        actor_zeta::unique_future<ddl_result_t>
        ddl_computed_drop(execution_context_t ctx,
                          components::catalog::oid_t table_oid,
                          std::string field_name);
        actor_zeta::unique_future<ddl_result_t>
        ddl_add_column(execution_context_t ctx, components::catalog::oid_t table_oid,
                       components::table::column_definition_t column);

        actor_zeta::unique_future<resolve_namespace_result_t>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<resolve_table_result_t>
        resolve_table(execution_context_t ctx, components::catalog::oid_t namespace_oid, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<resolve_type_result_t>
        resolve_type(execution_context_t ctx, components::catalog::oid_t namespace_oid, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<resolve_function_result_t>
        resolve_function(execution_context_t ctx, components::catalog::oid_t namespace_oid, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<std::pmr::vector<resolve_function_result_t>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<std::pmr::vector<std::string>>
        list_namespaces(execution_context_t ctx);
        actor_zeta::unique_future<std::pmr::vector<std::pair<components::catalog::oid_t, std::string>>>
        list_tables_in_namespace(execution_context_t ctx, components::catalog::oid_t namespace_oid);

        actor_zeta::unique_future<invalidation_ring_buffer_t::snapshot_t>
        recent_invalidations_since(session_id_t session, std::uint64_t since_version);

        actor_zeta::unique_future<void>
        commit_pg_catalog_appends(execution_context_t ctx, std::uint64_t commit_id);

        actor_zeta::unique_future<void>
        revert_pg_catalog_appends(execution_context_t ctx);

        actor_zeta::unique_future<std::vector<components::catalog::oid_t>>
        allocate_oids_batch(std::size_t count);

        actor_zeta::unique_future<void>
        append_pg_catalog_row(execution_context_t ctx,
                              collection_full_name_t name,
                              components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        actor_zeta::unique_future<void>
        delete_pg_catalog_rows(execution_context_t ctx,
                               collection_full_name_t name,
                               std::int64_t oid_col_idx,
                               components::catalog::oid_t target_oid);

        // Pure storage scan: row_ids of committed+txn-visible rows in `name` where
        // every key_col_names[i] == key_values[i].  No FK/semantic knowledge.
        actor_zeta::unique_future<std::pmr::vector<std::int64_t>>
        scan_by_key(execution_context_t ctx,
                    collection_full_name_t name,
                    std::vector<std::string> key_col_names,
                    std::vector<components::types::logical_value_t> key_values);

        // Index-based lookup: find first txn-visible row in the table indexed by
        // `index_oid` where indexed columns == key_values (in indkey order).
        // Returns nullopt when collection unknown, no match, or indisvalid=false.
        actor_zeta::unique_future<std::optional<std::int64_t>>
        point_lookup_by_index(execution_context_t ctx,
                              components::catalog::oid_t index_oid,
                              std::vector<components::types::logical_value_t> key_values);

        // Pure row-data scan: returns the full column values for every txn-visible row
        // in `name` where key_col_names[i] == key_values[i].  Same filter semantics as
        // scan_by_key but returns complete row data instead of row_ids.
        // Outer vector = rows, inner vector = column values in schema order.
        actor_zeta::unique_future<std::vector<std::vector<components::types::logical_value_t>>>
        read_rows_by_key(execution_context_t ctx,
                         collection_full_name_t name,
                         std::vector<std::string> key_col_names,
                         std::vector<components::types::logical_value_t> key_values);

        // OID-keyed scan: row_ids of txn-visible rows in the table with the given OID
        // where key_col_names[i] == key_values[i].  Resolves table_oid → full name
        // internally; returns empty on unknown OID.  Used by operator_fk_check_t.
        actor_zeta::unique_future<std::pmr::vector<std::int64_t>>
        scan_by_table_oid(execution_context_t ctx,
                          components::catalog::oid_t table_oid,
                          std::vector<std::string> key_col_names,
                          std::vector<components::types::logical_value_t> key_values);

        // Storage management
        actor_zeta::unique_future<void> create_storage(session_id_t session, collection_full_name_t name);
        actor_zeta::unique_future<void>
        create_storage_with_columns(session_id_t session,
                                    collection_full_name_t name,
                                    std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<void>
        create_storage_disk(session_id_t session,
                            collection_full_name_t name,
                            std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<void> drop_storage(session_id_t session, collection_full_name_t name);

        // Storage queries
        actor_zeta::unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, collection_full_name_t name);
        actor_zeta::unique_future<uint64_t> storage_total_rows(session_id_t session, collection_full_name_t name);
        actor_zeta::unique_future<uint64_t> storage_calculate_size(session_id_t session, collection_full_name_t name);
        // Storage data operations
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     collection_full_name_t name,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      collection_full_name_t name,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session, collection_full_name_t name, int64_t start, uint64_t count);

        actor_zeta::unique_future<std::pair<uint64_t, uint64_t>>
        storage_append(execution_context_t ctx, std::unique_ptr<components::vector::data_chunk_t> data);

        actor_zeta::unique_future<std::pair<int64_t, uint64_t>>
        storage_update(execution_context_t ctx,
                       components::vector::vector_t row_ids,
                       std::unique_ptr<components::vector::data_chunk_t> data);
        actor_zeta::unique_future<uint64_t>
        storage_delete_rows(execution_context_t ctx, components::vector::vector_t row_ids, uint64_t count);

        // MVCC commit/revert
        actor_zeta::unique_future<void>
        storage_commit_append(execution_context_t ctx, uint64_t commit_id, int64_t row_start, uint64_t count);
        actor_zeta::unique_future<void>
        storage_revert_append(execution_context_t ctx, int64_t row_start, uint64_t count);
        actor_zeta::unique_future<void> storage_commit_delete(execution_context_t ctx, uint64_t commit_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&disk_contract::flush,
                                                            &disk_contract::checkpoint_all,
                                                            &disk_contract::vacuum_all,
                                                            &disk_contract::maybe_cleanup,
                                                            // Storage management
                                                            &disk_contract::create_storage,
                                                            &disk_contract::create_storage_with_columns,
                                                            &disk_contract::create_storage_disk,
                                                            &disk_contract::drop_storage,
                                                            // Storage queries
                                                            &disk_contract::storage_types,
                                                            &disk_contract::storage_total_rows,
                                                            &disk_contract::storage_calculate_size,
                                                            // Storage data operations
                                                            &disk_contract::storage_scan,
                                                            &disk_contract::storage_fetch,
                                                            &disk_contract::storage_scan_segment,
                                                            &disk_contract::storage_append,
                                                            &disk_contract::storage_update,
                                                            &disk_contract::storage_delete_rows,
                                                            // MVCC commit/revert
                                                            &disk_contract::storage_commit_append,
                                                            &disk_contract::storage_revert_append,
                                                            &disk_contract::storage_commit_delete,
                                                            // DDL pipeline
                                                            &disk_contract::ddl_adopt_computing_schema,
                                                            &disk_contract::ddl_computed_append,
                                                            &disk_contract::ddl_computed_drop,
                                                            &disk_contract::ddl_add_column,
                                                            // resolve + invalidation pull
                                                            &disk_contract::resolve_namespace,
                                                            &disk_contract::resolve_table,
                                                            &disk_contract::resolve_type,
                                                            &disk_contract::resolve_function,
                                                            &disk_contract::resolve_function_by_name,
                                                            &disk_contract::list_namespaces,
                                                            &disk_contract::list_tables_in_namespace,
                                                            &disk_contract::recent_invalidations_since,
                                                            &disk_contract::commit_pg_catalog_appends,
                                                            &disk_contract::revert_pg_catalog_appends,
                                                            &disk_contract::allocate_oids_batch,
                                                            &disk_contract::append_pg_catalog_row,
                                                            &disk_contract::delete_pg_catalog_rows,
                                                            &disk_contract::scan_by_key,
                                                            &disk_contract::point_lookup_by_index,
                                                            &disk_contract::read_rows_by_key,
                                                            &disk_contract::scan_by_table_oid>;

        disk_contract() = delete;
    };

} // namespace services::disk
