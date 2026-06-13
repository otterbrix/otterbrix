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

    // One pg_catalog row-delete request for delete_pg_catalog_rows_many: deletes
    // every row of `table_oid` where column[oid_col_idx] == target_oid.
    struct pg_catalog_delete_spec_t {
        components::catalog::oid_t table_oid;
        std::int64_t oid_col_idx;
        components::catalog::oid_t target_oid;
    };

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        actor_zeta::unique_future<void> flush(session_id_t session, services::wal::id_t wal_id);

        // compact_watermark (here and below): the dispatcher's visible-to-all
        // horizon (txn_compact_watermark_msg / txn_publish_msg return); any
        // version stamp above it makes the MVCC-gated compact a no-op.
        actor_zeta::unique_future<services::wal::id_t>
        checkpoint_all(session_id_t session, services::wal::id_t current_wal_id, uint64_t compact_watermark);
        actor_zeta::unique_future<void>
        vacuum_all(session_id_t session, uint64_t lowest_active_start_time, uint64_t compact_watermark);
        // Batched GC-threshold check + compact: routes each table_oid to its owning
        // agent's maybe_cleanup_inner with the shared compact_watermark.
        // operator_commit_transaction sends one call covering all just-touched tables.
        actor_zeta::unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                                           std::pmr::vector<components::catalog::oid_t> table_oids,
                                                           uint64_t compact_watermark);

        // ddl_add_column / ddl_adopt_computing_schema replaced by pipeline operators.

        actor_zeta::unique_future<resolve_namespace_result_t>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);
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

        // Batched WAL-safe delete: loops the singular delete_pg_catalog_rows logic
        // per spec, emitting the same WAL records as N singular calls.
        actor_zeta::unique_future<void>
        delete_pg_catalog_rows_many(execution_context_t ctx, std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Patches each backfill's pg_attribute row with the shared `commit_id` written
        // into the added_at or dropped_at column (selected by the marker's kind).
        // Drained by operator_commit_transaction_t once the commit_id is allocated;
        // each backfill pairs with its own physical_update WAL record.
        actor_zeta::unique_future<void>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        // Batched keyed scan for one table: result[i] = match row_ids for key-tuple i.
        // Keys are columnar: `keys` is a data_chunk whose column j holds key_col_names[j]
        // and whose row i is the i-th key-tuple, so no row-major logical_value_t crosses
        // the boundary. All keys share the same table_oid (and therefore the same owning
        // agent), so the per-key loop runs intra-agent via a single scan_by_keys_inner.
        actor_zeta::unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Columnar row-data scan for ONE key-tuple: returns the txn-visible rows where
        // key_col_names[j] == keys.value(j, 0) as batched data_chunk_t (each chunk <=
        // DEFAULT_VECTOR_CAPACITY rows). `keys` is a 1-row columnar carrier (column j ==
        // key_col_names[j]), so no row-major logical_value_t crosses the boundary. Callers
        // read cells via chunk.value(col_idx, row_idx).
        actor_zeta::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::string> key_col_names,
                           components::vector::data_chunk_t keys);

        // Batched multi-key columnar row-data scan for one table: result[i] = matched chunks
        // for key-tuple i (each chunk <= DEFAULT_VECTOR_CAPACITY rows). `keys` is an N-row
        // columnar carrier (column j == key_col_names[j], row i == i-th key-tuple), so no
        // row-major logical_value_t crosses the boundary. All keys share `table_oid` (one owning
        // agent), so the per-key loop runs intra-agent via a single read_chunks_by_keys_inner
        // message. The outer vector always has one (possibly empty) entry per key in input
        // order, so result.size() == keys.size(). Callers read cells via chunk.value(col, row).
        actor_zeta::unique_future<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::string> key_col_names,
                            components::vector::data_chunk_t keys);

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
        // Batched DROP: partition oids per agent, fan out one inner per agent.
        actor_zeta::unique_future<void> drop_storage_many(session_id_t session,
                                                          std::pmr::vector<components::catalog::oid_t> table_oids);

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

        // MVCC delete-revert (abort path). The mirror of storage_publish_deletes:
        // instead of stamping this txn's pending delete marks with a commit_id, the
        // owning agent un-stamps them back to NOT_DELETED_ID via
        // data_table_t::revert_all_deletes(ctx.txn.transaction_id), restoring row
        // visibility for an aborted DELETE. Routed per owning agent by oid.
        actor_zeta::unique_future<void>
        storage_revert_deletes(execution_context_t ctx, std::vector<components::catalog::oid_t> tables);

        // Event-driven GC subscriber. Walks per-agent dropped_storages_
        // slices and physically removes entries whose
        // dropped_at_commit_id < new_horizon.
        actor_zeta::unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Runtime DROP TABLE path — operator_dynamic_cascade_delete sends this
        // from inside the executor actor so the manager_disk side records a
        // pending GC entry (path + sidecars derived from the live storages_
        // map) before the file is removed by drop_storage_many. Pair with
        // manager_dispatcher_t::on_drop_resource_marked(DISK_KIND).
        // Batched: one call marks every storage dropped in a cascade with the
        // SAME dropped_at_commit_id (the cascade operator computes a single
        // txn_id upper bound for the whole DROP). Partitioned per owning agent
        // (pool_idx_for_oid) and fanned out in parallel, mirroring drop_storage_many.
        actor_zeta::unique_future<void>
        mark_storage_dropped_many(session_id_t session,
                                  std::pmr::vector<components::catalog::oid_t> table_oids,
                                  uint64_t dropped_at_commit_id);

        // DROP-GC value-space remap. mark_storage_dropped_many records
        // dropped_at_commit_id in TXN-ID space (>= 2^62) because the cascade-delete
        // operator only knows the in-flight txn_id at the time. Once the transaction
        // commits and a real commit_id is allocated, operator_commit_transaction
        // sends this so the manager fans out to every agent and rewrites the GC
        // entry's dropped_at_commit_id from the TXN-ID placeholder to the real
        // commit_id, putting it in the same value space the on_horizon_advanced
        // sweep compares against.
        actor_zeta::unique_future<void> storage_dropped_committed(session_id_t session,
                                                                  uint64_t txn_id,
                                                                  uint64_t commit_id);

        // DROP-rollback un-mark. The mirror of storage_dropped_committed for the
        // abort path: a DROP TABLE inside a transaction records its GC entry with
        // dropped_at_commit_id in TXN-ID space via mark_storage_dropped_many, but if the
        // transaction ABORTS the table must survive. operator_abort_transaction sends
        // this so the manager fans out to every agent, and each agent ERASES (not
        // remaps) its own dropped_storages_ entries whose dropped_at_commit_id == txn_id,
        // un-marking the DROP so on_horizon_advanced never reclaims the still-live .otbx.
        actor_zeta::unique_future<void> storage_drop_aborted(session_id_t session, uint64_t txn_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&disk_contract::flush,
                                                            &disk_contract::checkpoint_all,
                                                            &disk_contract::vacuum_all,
                                                            &disk_contract::maybe_cleanup_many,
                                                            // Storage management
                                                            &disk_contract::create_storage,
                                                            &disk_contract::create_storage_with_columns,
                                                            &disk_contract::create_storage_disk,
                                                            &disk_contract::drop_storage_many,
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
                                                            &disk_contract::storage_publish_commits,
                                                            &disk_contract::storage_publish_deletes,
                                                            &disk_contract::storage_revert_appends,
                                                            &disk_contract::storage_revert_deletes,
                                                            // resolve + invalidation pull
                                                            &disk_contract::resolve_namespace,
                                                            &disk_contract::resolve_function_by_name,
                                                            &disk_contract::list_namespaces,
                                                            &disk_contract::allocate_oids_batch,
                                                            &disk_contract::append_pg_catalog_row,
                                                            &disk_contract::delete_pg_catalog_rows,
                                                            &disk_contract::delete_pg_catalog_rows_many,
                                                            &disk_contract::update_pg_attribute_commit_id_fields,
                                                            &disk_contract::scan_by_keys,
                                                            &disk_contract::read_chunks_by_key,
                                                            &disk_contract::read_chunks_by_keys,
                                                            &disk_contract::compact_relkind_g_storage,
                                                            &disk_contract::on_horizon_advanced,
                                                            &disk_contract::mark_storage_dropped_many,
                                                            &disk_contract::storage_dropped_committed,
                                                            &disk_contract::storage_drop_aborted>;

        disk_contract() = delete;
    };

} // namespace services::disk
