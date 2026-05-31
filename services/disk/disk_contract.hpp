#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/catalog/results/resolve_result.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
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

        // ddl_add_column / ddl_adopt_computing_schema replaced by pipeline operators.

        actor_zeta::unique_future<resolve_namespace_result_t>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<resolve_table_result_t> resolve_table(execution_context_t ctx,
                                                                        components::catalog::oid_t namespace_oid,
                                                                        std::string name,
                                                                        std::uint64_t since_version);
        actor_zeta::unique_future<resolve_type_result_t> resolve_type(execution_context_t ctx,
                                                                      components::catalog::oid_t namespace_oid,
                                                                      std::string name,
                                                                      std::uint64_t since_version);
        actor_zeta::unique_future<resolve_function_result_t> resolve_function(execution_context_t ctx,
                                                                              components::catalog::oid_t namespace_oid,
                                                                              std::string name,
                                                                              std::uint64_t since_version);
        actor_zeta::unique_future<std::pmr::vector<resolve_function_result_t>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<std::pmr::vector<std::string>> list_namespaces(execution_context_t ctx);

        actor_zeta::unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        actor_zeta::unique_future<components::pg_catalog_append_range_t>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        actor_zeta::unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::int64_t oid_col_idx,
                                                               components::catalog::oid_t target_oid);

        // patch the pg_attribute row identified by
        // `attoid` with `commit_id` written into the added_at or dropped_at column
        // (selected by `kind`). Drained by operator_commit_transaction_t once the
        // commit_id is allocated; pairs with a physical_update WAL record.
        actor_zeta::unique_future<void>
        update_pg_attribute_commit_id_field(execution_context_t ctx,
                                            components::catalog::oid_t attoid,
                                            components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                            std::uint64_t commit_id);

        // Pure storage scan: row_ids of committed+txn-visible rows in the table with
        // the given OID where every key_col_names[i] == key_values[i].
        actor_zeta::unique_future<std::pmr::vector<std::int64_t>>
        scan_by_key(execution_context_t ctx,
                    components::catalog::oid_t table_oid,
                    std::pmr::vector<std::string> key_col_names,
                    std::pmr::vector<components::types::logical_value_t> key_values);

        // Pure row-data scan: returns the full column values for every txn-visible row
        // in the table with the given OID where key_col_names[i] == key_values[i].
        // Outer vector = rows, inner vector = column values in schema order.
        actor_zeta::unique_future<std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>>
        read_rows_by_key(execution_context_t ctx,
                         components::catalog::oid_t table_oid,
                         std::pmr::vector<std::string> key_col_names,
                         std::pmr::vector<components::types::logical_value_t> key_values);

        // Physical column compaction for an IN_MEMORY relkind='g'
        // table_storage_t.
        actor_zeta::unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                                           components::catalog::oid_t table_oid,
                                                                           std::set<std::string> live_attnames);

        // Storage management
        actor_zeta::unique_future<void> create_storage(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t database_oid);
        actor_zeta::unique_future<void>
        create_storage_with_columns(session_id_t session,
                                    components::catalog::oid_t table_oid,
                                    components::catalog::oid_t database_oid,
                                    std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<void>
        create_storage_disk(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t database_oid,
                            std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<void> drop_storage(session_id_t session, components::catalog::oid_t table_oid);

        // Storage queries
        actor_zeta::unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        actor_zeta::unique_future<uint64_t> storage_total_rows(session_id_t session,
                                                               components::catalog::oid_t table_oid);
        // Storage data operations
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     components::catalog::oid_t table_oid,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);
        // Batched + projected variant: returns a vector of chunks (PR #483 multi-chunk)
        // and applies index-based column projection at the disk layer (PR #477).
        // Empty `projected_cols` means "read all columns" (pass-through).
        actor_zeta::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        storage_scan_batched(session_id_t session,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::table::table_filter_t> filter,
                             int64_t limit,
                             std::vector<size_t> projected_cols,
                             components::table::transaction_data txn);
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session, components::catalog::oid_t table_oid, int64_t start, uint64_t count);

        actor_zeta::unique_future<std::pair<uint64_t, uint64_t>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::vector::data_chunk_t> data);

        actor_zeta::unique_future<std::pair<int64_t, uint64_t>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       components::vector::vector_t row_ids,
                       std::unique_ptr<components::vector::data_chunk_t> data);
        actor_zeta::unique_future<uint64_t> storage_delete_rows(execution_context_t ctx,
                                                                components::catalog::oid_t table_oid,
                                                                components::vector::vector_t row_ids,
                                                                uint64_t count);

        // MVCC commit/revert
        actor_zeta::unique_future<void> storage_publish_commit(execution_context_t ctx,
                                                              components::catalog::oid_t table_oid,
                                                              uint64_t commit_id,
                                                              int64_t row_start,
                                                              uint64_t count);
        actor_zeta::unique_future<void> storage_revert_append(execution_context_t ctx,
                                                              components::catalog::oid_t table_oid,
                                                              int64_t row_start,
                                                              uint64_t count);
        actor_zeta::unique_future<void>
        storage_publish_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);

        // Batched MVCC swap. Each range carries its own table_oid.
        actor_zeta::unique_future<void>
        storage_publish_commits(execution_context_t ctx,
                               uint64_t commit_id,
                               std::vector<components::pg_catalog_append_range_t> ranges);
        actor_zeta::unique_future<void> storage_publish_deletes(execution_context_t ctx,
                                                               uint64_t commit_id,
                                                               std::set<components::catalog::oid_t> tables);
        actor_zeta::unique_future<void>
        storage_revert_appends(execution_context_t ctx, std::vector<components::pg_catalog_append_range_t> ranges);

        // event-driven GC subscriber. Stub today; real body lands
        // with Block D DML (walk dropped_storages_ + GC entries whose
        // pinned start_time < new_horizon).
        actor_zeta::unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Runtime DROP TABLE path — operator_dynamic_cascade_delete sends this
        // from inside the executor actor so the manager_disk side records a
        // pending GC entry (path + sidecars derived from the live storages_
        // map) before the file is removed by drop_storage. Pair with
        // manager_dispatcher_t::on_drop_resource_marked(DISK_KIND).
        actor_zeta::unique_future<void> mark_storage_dropped(session_id_t session,
                                                             components::catalog::oid_t table_oid,
                                                             uint64_t dropped_at_commit_id);

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
                                                            // Storage data operations
                                                            &disk_contract::storage_scan,
                                                            &disk_contract::storage_scan_batched,
                                                            &disk_contract::storage_fetch,
                                                            &disk_contract::storage_scan_segment,
                                                            &disk_contract::storage_append,
                                                            &disk_contract::storage_update,
                                                            &disk_contract::storage_delete_rows,
                                                            // MVCC commit/revert
                                                            &disk_contract::storage_publish_commit,
                                                            &disk_contract::storage_revert_append,
                                                            &disk_contract::storage_publish_delete,
                                                            &disk_contract::storage_publish_commits,
                                                            &disk_contract::storage_publish_deletes,
                                                            &disk_contract::storage_revert_appends,
                                                            // resolve + invalidation pull
                                                            &disk_contract::resolve_namespace,
                                                            &disk_contract::resolve_table,
                                                            &disk_contract::resolve_type,
                                                            &disk_contract::resolve_function,
                                                            &disk_contract::resolve_function_by_name,
                                                            &disk_contract::list_namespaces,
                                                            &disk_contract::allocate_oids_batch,
                                                            &disk_contract::append_pg_catalog_row,
                                                            &disk_contract::delete_pg_catalog_rows,
                                                            &disk_contract::update_pg_attribute_commit_id_field,
                                                            &disk_contract::scan_by_key,
                                                            &disk_contract::read_rows_by_key,
                                                            &disk_contract::compact_relkind_g_storage,
                                                            &disk_contract::on_horizon_advanced,
                                                            &disk_contract::mark_storage_dropped>;

        disk_contract() = delete;
    };

} // namespace services::disk
