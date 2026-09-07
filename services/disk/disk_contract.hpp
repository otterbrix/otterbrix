#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>
#include <limits>
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
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp> // aggregate-pushdown reduce spec
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

    // One row-delete request for delete_pg_catalog_rows_many: every row where column[oid_col_idx] == target_oid.
    struct pg_catalog_delete_spec_t {
        components::catalog::oid_t table_oid;
        std::int64_t oid_col_idx;
        components::catalog::oid_t target_oid;
    };

    // Reply of storage_fetch_next_batch: OPEN returns the minted cursor_id; a drained cursor replies
    // an EMPTY chunk. `batch` stays unique_ptr so the struct is default-constructible (send's
    // null-target machinery requires it).
    struct fetch_batch_t {
        std::unique_ptr<components::vector::data_chunk_t> batch;
        uint64_t cursor_id{0};

        fetch_batch_t() = default;
        fetch_batch_t(std::unique_ptr<components::vector::data_chunk_t>&& b, uint64_t id)
            : batch(std::move(b))
            , cursor_id(id) {}
    };

    // storage_fetch's "not from an index answer" sentinel; the real epoch starts at 0 per process.
    inline constexpr uint64_t k_fetch_epoch_unchecked = std::numeric_limits<uint64_t>::max();

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // compact_watermark (below): the dispatcher's visible-to-all horizon; a stamp above it no-ops the compact.
        actor_zeta::unique_future<services::wal::id_t>
        checkpoint_all(session_id_t session, services::wal::id_t current_wal_id, uint64_t compact_watermark);
        // No compact_watermark: nothing here compacts, so no physical row id moves.
        actor_zeta::unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        actor_zeta::unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                                           std::pmr::vector<components::catalog::oid_t> table_oids,
                                                           uint64_t compact_watermark);

        actor_zeta::unique_future<core::result_wrapper_t<resolve_namespace_result_t>>
        resolve_namespace(execution_context_t ctx, std::string name);
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
        resolve_function_by_name(execution_context_t ctx, std::string name);
        actor_zeta::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        find_cast_oid(execution_context_t ctx,
                      components::catalog::oid_t source_oid,
                      components::catalog::oid_t target_oid);
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>>
        list_namespaces(execution_context_t ctx);

        actor_zeta::unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        actor_zeta::unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::int64_t oid_col_idx,
                                                               components::catalog::oid_t target_oid);

        // A zero count means "no row ctx.txn can see carried that oid" -- mixing a transaction-less read breaks that.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
        delete_pg_catalog_rows_many(execution_context_t ctx, std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Sits below the durable commit marker, so a refusal here can only report, never take it back.
        actor_zeta::unique_future<core::error_t>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        // `keys` stays columnar to avoid a row-major crossing.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Batched at <= DEFAULT_VECTOR_CAPACITY rows per chunk; `keys` stays columnar.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::uint64_t> key_col_indices,
                           components::vector::data_chunk_t keys,
                           std::pmr::vector<std::uint64_t> projected_cols);

        // Multi-key version of read_chunks_by_key: result.size() == keys.size(), input order preserved.
        actor_zeta::unique_future<
            core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::uint64_t> key_col_indices,
                            components::vector::data_chunk_t keys,
                            std::pmr::vector<std::uint64_t> projected_cols);

        // A not-owned oid REFUSES rather than answering an empty fold indistinguishable from a real COUNT=0.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce(session_id_t session,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::table::table_filter_t> filter,
                       std::vector<size_t> projected_cols,
                       components::table::transaction_data txn,
                       components::operators::pushed_aggregate_spec_t spec);

        // See manager_disk_t::compact_relkind_g_storage for the mechanism.
        actor_zeta::unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                                           components::catalog::oid_t table_oid,
                                                                           std::set<std::string> live_attnames);

        // Unlike compact_relkind_g_storage (SUBTRACTIVE), this is ADDITIVE: a gap can never drop a surviving column.
        actor_zeta::unique_future<core::result_wrapper_t<bool>>
        drop_storage_column(session_id_t session, components::catalog::oid_t table_oid, std::string attname);

        // rearm_dropped_column_blocks_sync matches by ATTOID, so a missed rename reads as a stale name, not a drop.
        actor_zeta::unique_future<core::result_wrapper_t<bool>> rename_storage_column(
            session_id_t session,
            components::catalog::oid_t table_oid,
            std::string old_attname,
            std::string new_attname);

        actor_zeta::unique_future<void>
        create_storage_disk(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t database_oid,
                            std::vector<components::table::column_definition_t> columns,
                            bool is_computed);
        actor_zeta::unique_future<void> drop_storage_many(session_id_t session,
                                                          std::pmr::vector<components::catalog::oid_t> table_oids);

        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_total_rows(session_id_t session, components::catalog::oid_t table_oid);
        // An OPEN over an unowned oid REFUSES rather than the drained sentinel (misread as "empty table").
        actor_zeta::unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        // A source that stops early otherwise leaks the active_scans_ entry and gates compact() forever. Idempotent.
        actor_zeta::unique_future<void>
        storage_close_cursor(session_id_t session, components::catalog::oid_t table_oid, uint64_t cursor_id);
        // Must be opened BEFORE the ids are minted, or a compact renumbers them (test_index_scan_compact_race).
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_open_scan_hold(session_id_t session, components::catalog::oid_t table_oid);
        // SNAPSHOT drops invisible rows (paired by row_ids, not position); RAW skips the check
        // (CREATE INDEX backfill needs deleted rows). `limit` caps POST-visibility (measured: without
        // this, test_index_scan_limit_cap.cpp's LIMIT 7 answered 0 rows). A stale
        // `expected_compact_epoch` REFUSES rather than reading a renumbered row.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        // projected_cols: EMPTY means every column; columns outside the set come back as buffer-less stubs.
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols,
                      components::table::transaction_data txn,
                      components::table::fetch_visibility_t visibility,
                      int64_t limit,
                      uint64_t expected_compact_epoch);

        // Reply wraps (start_row, count); an empty batch is a (0,0) success, an unowned oid is not.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::data_chunk_t> data);

        // Reply wraps (updated, appended); an empty request answers (0,0) and stays a success.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data);
        // A count smaller than requested is legitimate (a duplicate id already stamped); a bare 0 travels as an error.
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_delete_rows(execution_context_t ctx,
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

        // Abort path: un-stamps this txn's pending delete marks back to NOT_DELETED_ID.
        actor_zeta::unique_future<void> storage_revert_deletes(execution_context_t ctx,
                                                               std::vector<components::catalog::oid_t> tables);

        actor_zeta::unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Sent before drop_storage_many removes the file, so the GC entry is recorded from live storages_ first.
        actor_zeta::unique_future<void>
        mark_storage_dropped_many(session_id_t session,
                                  std::pmr::vector<components::catalog::oid_t> table_oids,
                                  uint64_t dropped_at_commit_id);

        // dropped_at_commit_id starts in TXN-ID space (>= 2^62); rewritten to the real commit_id once allocated.
        actor_zeta::unique_future<void>
        storage_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // Abort mirror of storage_dropped_committed: ERASES (not remaps) so the table survives.
        actor_zeta::unique_future<void> storage_drop_aborted(session_id_t session, uint64_t txn_id);

        // Must be read STRICTLY BEFORE the scan that feeds the index, else a loud refusal, never a wrong row.
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_compact_epoch(session_id_t session, components::catalog::oid_t table_oid);

        using dispatch_traits = actor_zeta::dispatch_traits<&disk_contract::checkpoint_all,
                                                            &disk_contract::vacuum_all,
                                                            &disk_contract::maybe_cleanup_many,
                                                            &disk_contract::create_storage_disk,
                                                            &disk_contract::drop_storage_many,
                                                            &disk_contract::storage_types,
                                                            &disk_contract::storage_total_rows,
                                                            &disk_contract::storage_fetch_next_batch,
                                                            &disk_contract::storage_close_cursor,
                                                            &disk_contract::storage_reduce,
                                                            &disk_contract::storage_fetch,
                                                            &disk_contract::storage_append,
                                                            &disk_contract::storage_update,
                                                            &disk_contract::storage_delete_rows,
                                                            &disk_contract::storage_publish_commits,
                                                            &disk_contract::storage_publish_deletes,
                                                            &disk_contract::storage_revert_appends,
                                                            &disk_contract::storage_revert_deletes,
                                                            &disk_contract::resolve_namespace,
                                                            &disk_contract::resolve_function_by_name,
                                                            &disk_contract::find_cast_oid,
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
                                                            &disk_contract::drop_storage_column,
                                                            &disk_contract::rename_storage_column,
                                                            &disk_contract::on_horizon_advanced,
                                                            &disk_contract::mark_storage_dropped_many,
                                                            &disk_contract::storage_dropped_committed,
                                                            &disk_contract::storage_drop_aborted,
                                                            // Appended LAST: msg ids are positional
                                                            // (find_method_index), insertion above
                                                            // would renumber every later method.
                                                            &disk_contract::storage_open_scan_hold,
                                                            &disk_contract::storage_compact_epoch>;

        disk_contract() = delete;
    };

} // namespace services::disk
