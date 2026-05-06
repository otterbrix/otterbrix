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

        // DDL pipeline + resolve + populate
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_database(execution_context_t ctx, std::string name);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_database(execution_context_t ctx, components::catalog::oid_t database_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_namespace(execution_context_t ctx, std::string name);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_namespace(execution_context_t ctx, components::catalog::oid_t namespace_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_table(execution_context_t ctx,
                         components::catalog::oid_t namespace_oid,
                         std::string name,
                         std::vector<components::table::column_definition_t> columns,
                         char relkind);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_table(execution_context_t ctx, components::catalog::oid_t table_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_adopt_computing_schema(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_computing_table(execution_context_t ctx,
                                    components::catalog::oid_t namespace_oid,
                                    std::string name);
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
        ddl_create_sequence(execution_context_t ctx, components::catalog::oid_t namespace_oid, std::string name,
                            std::int64_t start, std::int64_t increment,
                            std::int64_t min_value, std::int64_t max_value, bool cycle);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_sequence(execution_context_t ctx, components::catalog::oid_t sequence_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_view(execution_context_t ctx, components::catalog::oid_t namespace_oid, std::string name,
                        std::string body);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_view(execution_context_t ctx, components::catalog::oid_t view_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_macro(execution_context_t ctx, components::catalog::oid_t namespace_oid, std::string name,
                         std::string body);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_macro(execution_context_t ctx, components::catalog::oid_t macro_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_index(execution_context_t ctx,
                         components::catalog::oid_t namespace_oid,
                         components::catalog::oid_t table_oid,
                         std::string index_name,
                         std::vector<std::string> column_names);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_index(execution_context_t ctx, components::catalog::oid_t index_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_type(execution_context_t ctx,
                        components::catalog::oid_t namespace_oid,
                        std::string type_name,
                        std::string type_spec);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_type(execution_context_t ctx, components::catalog::oid_t type_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_function(execution_context_t ctx, components::catalog::oid_t namespace_oid,
                            std::string function_name, std::int32_t pronargs, std::int64_t prouid,
                            std::string proargmatchers, std::string prorettype);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_function(execution_context_t ctx, components::catalog::oid_t function_oid, drop_behavior_t behavior);
        actor_zeta::unique_future<ddl_result_t>
        ddl_index_set_valid(execution_context_t ctx, components::catalog::oid_t index_oid, bool valid);
        actor_zeta::unique_future<ddl_result_t>
        ddl_add_column(execution_context_t ctx, components::catalog::oid_t table_oid,
                       components::table::column_definition_t column);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_column(execution_context_t ctx, components::catalog::oid_t table_oid,
                        std::string column_name,
                        drop_behavior_t behavior = drop_behavior_t::restrict_);
        actor_zeta::unique_future<ddl_result_t>
        ddl_rename_column(execution_context_t ctx, components::catalog::oid_t table_oid,
                          std::string old_name, std::string new_name);
        actor_zeta::unique_future<ddl_result_t>
        ddl_create_constraint(execution_context_t ctx, components::catalog::oid_t table_oid,
                              std::string constraint_name, char contype,
                              components::catalog::oid_t ref_table_oid,
                              std::vector<components::catalog::oid_t> fk_column_attoids,
                              std::vector<components::catalog::oid_t> ref_column_attoids,
                              char fk_matchtype, char fk_del_action, char fk_upd_action,
                              std::string check_expr);
        actor_zeta::unique_future<ddl_result_t>
        ddl_drop_constraint(execution_context_t ctx, components::catalog::oid_t constraint_oid,
                            drop_behavior_t behavior);

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
                                                            &disk_contract::ddl_create_database,
                                                            &disk_contract::ddl_drop_database,
                                                            &disk_contract::ddl_create_namespace,
                                                            &disk_contract::ddl_drop_namespace,
                                                            &disk_contract::ddl_create_table,
                                                            &disk_contract::ddl_drop_table,
                                                            &disk_contract::ddl_adopt_computing_schema,
                                                            &disk_contract::ddl_create_computing_table,
                                                            &disk_contract::ddl_computed_append,
                                                            &disk_contract::ddl_computed_drop,
                                                            &disk_contract::ddl_create_sequence,
                                                            &disk_contract::ddl_drop_sequence,
                                                            &disk_contract::ddl_create_view,
                                                            &disk_contract::ddl_drop_view,
                                                            &disk_contract::ddl_create_macro,
                                                            &disk_contract::ddl_drop_macro,
                                                            &disk_contract::ddl_create_index,
                                                            &disk_contract::ddl_drop_index,
                                                            &disk_contract::ddl_create_type,
                                                            &disk_contract::ddl_drop_type,
                                                            &disk_contract::ddl_create_function,
                                                            &disk_contract::ddl_drop_function,
                                                            &disk_contract::ddl_index_set_valid,
                                                            &disk_contract::ddl_add_column,
                                                            &disk_contract::ddl_drop_column,
                                                            &disk_contract::ddl_rename_column,
                                                            &disk_contract::ddl_create_constraint,
                                                            &disk_contract::ddl_drop_constraint,
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
                                                            &disk_contract::scan_by_key,
                                                            &disk_contract::point_lookup_by_index>;

        disk_contract() = delete;
    };

} // namespace services::disk
