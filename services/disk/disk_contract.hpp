#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>
#include <limits>
#include <optional>
#include <set>
#include <type_traits>
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
#include <components/storage/storage.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/pushed_filter.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/transaction.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/storage_contract.hpp>
#include <services/wal/base.hpp>

namespace services::disk {

    // One row-delete request for delete_pg_catalog_rows_many: every row where column[oid_col_idx] == target_oid.
    struct pg_catalog_delete_spec_t {
        components::catalog::oid_t table_oid;
        std::int64_t oid_col_idx;
        components::catalog::oid_t target_oid;
    };

    // storage_fetch's "not from an index answer" sentinel; the real epoch starts at 0 per process.
    inline constexpr uint64_t k_fetch_epoch_unchecked = std::numeric_limits<uint64_t>::max();

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // storage_contract's prefix, number for number (static_assert below).
        // An OPEN over an unowned oid REFUSES rather than the drained sentinel (misread as "empty table").
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        // A source that stops early otherwise leaks the active_scans_ entry and gates compact() forever. Idempotent.
        unique_future<void>
        storage_close_cursor(session_id_t session, components::catalog::oid_t table_oid, uint64_t cursor_id);
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(session_id_t session,
                                                                           components::catalog::oid_t table_oid);
        // An empty batch is a (0,0) success, an unowned oid is not; `attoids` must be the table's live columns.
        unique_future<core::result_wrapper_t<write_receipt_t>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::catalog::oid_t> attoids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning);
        unique_future<core::result_wrapper_t<update_receipt_t>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning);
        unique_future<core::result_wrapper_t<write_receipt_t>> storage_delete_rows(execution_context_t ctx,
                                                                                   components::catalog::oid_t table_oid,
                                                                                   components::vector::vector_t row_ids,
                                                                                   uint64_t count,
                                                                                   bool returning);
        unique_future<core::result_wrapper_t<ddl_reply_t>>
        create_storage_disk(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t database_oid,
                            qualified_name_t name,
                            std::vector<components::table::column_definition_t> columns,
                            bool is_computed);
        // Sent before drop_storage_many removes the file, so the GC entry is recorded from live storages_ first.
        unique_future<core::result_wrapper_t<ddl_reply_t>>
        mark_storage_dropped_many(execution_context_t ctx,
                                  std::pmr::vector<components::catalog::oid_t> table_oids,
                                  std::pmr::vector<qualified_name_t> names);
        unique_future<core::result_wrapper_t<ddl_reply_t>>
        add_storage_column(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           components::table::column_definition_t column);
        unique_future<core::result_wrapper_t<ddl_reply_t>> stamp_column_dropped(execution_context_t ctx,
                                                                                components::catalog::oid_t table_oid,
                                                                                components::catalog::oid_t attoid);
        unique_future<core::error_t> storage_prepare(execution_context_t ctx,
                                                     std::pmr::set<components::catalog::oid_t> tables);
        unique_future<void> storage_release_prepared(execution_context_t ctx,
                                                     std::pmr::set<components::catalog::oid_t> tables);
        // Un-stamps ctx.txn's delete marks and column stamps on `tables`; its appended rows, DROP marks and created
        // storages go by undo_appends_and_storages.
        unique_future<core::error_t> abort_transaction(execution_context_t ctx,
                                                       uint64_t from_sub_id,
                                                       std::pmr::set<components::catalog::oid_t> tables);

        // The disk's own messages.
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_open_filtered_scan(session_id_t session,
                                   components::catalog::oid_t table_oid,
                                   std::unique_ptr<components::table::pushed_filter_t> filter,
                                   int64_t limit,
                                   std::vector<size_t> projected_cols,
                                   components::table::transaction_data txn);

        // compact_watermark (below): the dispatcher's visible-to-all horizon; a stamp above it no-ops the compact.
        unique_future<services::wal::id_t>
        checkpoint_all(session_id_t session, services::wal::id_t current_wal_id, uint64_t compact_watermark);
        // No compact_watermark: nothing here compacts, so no physical row id moves.
        unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                               std::pmr::vector<components::catalog::oid_t> table_oids,
                                               uint64_t compact_watermark);

        unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
        resolve_function_by_name(execution_context_t ctx, std::string name);
        unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        find_cast_oid(execution_context_t ctx,
                      components::catalog::oid_t source_oid,
                      components::catalog::oid_t target_oid);

        unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                   components::catalog::oid_t table_oid,
                                                   std::int64_t oid_col_idx,
                                                   components::catalog::oid_t target_oid);

        // A zero count means "no row ctx.txn can see carried that oid" -- mixing a transaction-less read breaks that.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
        delete_pg_catalog_rows_many(execution_context_t ctx, std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Sits ABOVE the durable commit marker, so a refusal here can still abort the commit.
        unique_future<components::pg_attribute_backfill_result_t>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        // `keys` stays columnar to avoid a row-major crossing.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Batched at <= DEFAULT_VECTOR_CAPACITY rows per chunk; `keys` stays columnar.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::uint64_t> key_col_indices,
                           components::vector::data_chunk_t keys,
                           std::pmr::vector<std::uint64_t> projected_cols);

        // Multi-key version of read_chunks_by_key: result.size() == keys.size(), input order preserved.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::uint64_t> key_col_indices,
                            components::vector::data_chunk_t keys,
                            std::pmr::vector<std::uint64_t> projected_cols);

        // A not-owned oid REFUSES rather than answering an empty fold indistinguishable from a real COUNT=0.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce(session_id_t session,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::table::pushed_filter_t> filter,
                       std::vector<size_t> projected_cols,
                       components::table::transaction_data txn,
                       components::operators::pushed_aggregate_spec_t spec);

        // See manager_disk_t::compact_relkind_g_storage for the mechanism.
        unique_future<core::result_wrapper_t<std::uint64_t>>
        compact_relkind_g_storage(execution_context_t ctx,
                                  components::catalog::oid_t table_oid,
                                  std::set<std::string> live_attnames);

        unique_future<void> publish_column_stamps(execution_context_t ctx,
                                                  uint64_t commit_id,
                                                  std::pmr::set<components::catalog::oid_t> tables);

        // reconcile_storage_with_catalog_sync matches by ATTOID, so a missed rename reads as a stale name, not a drop.
        unique_future<core::result_wrapper_t<bool>> rename_storage_column(session_id_t session,
                                                                          components::catalog::oid_t table_oid,
                                                                          std::string old_attname,
                                                                          std::string new_attname);

        unique_future<void> drop_storage_many(session_id_t session,
                                              std::pmr::vector<components::catalog::oid_t> table_oids);

        // Must be opened BEFORE the ids are minted, or a compact renumbers them (test_index_scan_compact_race).
        unique_future<core::result_wrapper_t<uint64_t>> storage_open_scan_hold(session_id_t session,
                                                                               components::catalog::oid_t table_oid);

        // SNAPSHOT drops invisible rows (paired by row_ids, not position); RAW skips the check
        // (CREATE INDEX backfill needs deleted rows). `limit` caps POST-visibility (measured: without
        // this, test_index_scan_limit_cap.cpp's LIMIT 7 answered 0 rows). A stale
        // `expected_compact_epoch` REFUSES rather than reading a renumbered row.
        // projected_cols: EMPTY means every column; columns outside the set come back as buffer-less stubs.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols,
                      components::table::transaction_data txn,
                      components::table::fetch_visibility_t visibility,
                      int64_t limit,
                      uint64_t expected_compact_epoch);

        // Batched MVCC swap. Each range carries its own table_oid.
        unique_future<void> storage_publish_commits(execution_context_t ctx,
                                                    uint64_t commit_id,
                                                    std::vector<components::pg_catalog_append_range_t> ranges);
        unique_future<void> storage_publish_deletes(execution_context_t ctx,
                                                    uint64_t commit_id,
                                                    std::set<components::catalog::oid_t> tables);

        // The rest of a transaction's undo on the disk: its appended rows (physically, so their never-committed
        // stamps defer no checkpoint), its DROP marks (ERASED, not remapped, so the tables survive) and the storages
        // it created. A refused append rollback is the error; every other step runs regardless.
        unique_future<core::error_t> undo_appends_and_storages(session_id_t session,
                                                               components::table::txn_abort_drain_t drain);

        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // dropped_at_commit_id starts in TXN-ID space (>= 2^62); rewritten to the real commit_id once allocated.
        unique_future<void> storage_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // Must be read STRICTLY BEFORE the scan that feeds the index, else a loud refusal, never a wrong row.
        unique_future<core::result_wrapper_t<uint64_t>> storage_compact_epoch(session_id_t session,
                                                                              components::catalog::oid_t table_oid);

        // ALTER's PRIMARY KEY over the column `attoid`: NOT NULL for ctx.txn now and for everyone from its COMMIT;
        // refused while a row holds NULL in it ("column ... contains null values").
        unique_future<core::error_t> storage_set_not_null(execution_context_t ctx,
                                                          components::catalog::oid_t table_oid,
                                                          components::catalog::oid_t attoid);
        // The table's physical columns: the disk's own (a host's table has its columns in the catalog).
        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);

        // The order IS the message-id space: storage_contract's prefix first, then the disk's own.
        using dispatch_traits = actor_zeta::dispatch_traits<&disk_contract::storage_fetch_next_batch,
                                                            &disk_contract::storage_close_cursor,
                                                            &disk_contract::storage_total_rows,
                                                            &disk_contract::storage_append,
                                                            &disk_contract::storage_update,
                                                            &disk_contract::storage_delete_rows,
                                                            &disk_contract::create_storage_disk,
                                                            &disk_contract::mark_storage_dropped_many,
                                                            &disk_contract::add_storage_column,
                                                            &disk_contract::stamp_column_dropped,
                                                            &disk_contract::storage_prepare,
                                                            &disk_contract::storage_release_prepared,
                                                            &disk_contract::abort_transaction,
                                                            &disk_contract::storage_open_filtered_scan,
                                                            &disk_contract::checkpoint_all,
                                                            &disk_contract::vacuum_all,
                                                            &disk_contract::maybe_cleanup_many,
                                                            &disk_contract::resolve_function_by_name,
                                                            &disk_contract::find_cast_oid,
                                                            &disk_contract::allocate_oids_batch,
                                                            &disk_contract::append_pg_catalog_row,
                                                            &disk_contract::delete_pg_catalog_rows,
                                                            &disk_contract::delete_pg_catalog_rows_many,
                                                            &disk_contract::update_pg_attribute_commit_id_fields,
                                                            &disk_contract::scan_by_keys,
                                                            &disk_contract::read_chunks_by_key,
                                                            &disk_contract::read_chunks_by_keys,
                                                            &disk_contract::storage_reduce,
                                                            &disk_contract::compact_relkind_g_storage,
                                                            &disk_contract::publish_column_stamps,
                                                            &disk_contract::rename_storage_column,
                                                            &disk_contract::drop_storage_many,
                                                            &disk_contract::storage_open_scan_hold,
                                                            &disk_contract::storage_fetch,
                                                            &disk_contract::storage_publish_commits,
                                                            &disk_contract::storage_publish_deletes,
                                                            &disk_contract::undo_appends_and_storages,
                                                            &disk_contract::on_horizon_advanced,
                                                            &disk_contract::storage_dropped_committed,
                                                            &disk_contract::storage_compact_epoch,
                                                            &disk_contract::storage_set_not_null,
                                                            &disk_contract::storage_types>;

        disk_contract() = delete;
    };

    namespace detail {
        template<auto Disk, auto Storage>
        constexpr bool same_message() {
            using disk_trait = actor_zeta::type_traits::callable_trait<decltype(Disk)>;
            using storage_trait = actor_zeta::type_traits::callable_trait<decltype(Storage)>;
            constexpr auto disk_id =
                actor_zeta::action_id_impl<disk_contract, Disk, disk_contract::dispatch_traits::methods>::value;
            constexpr auto storage_id = actor_zeta::
                action_id_impl<storage_contract, Storage, storage_contract::dispatch_traits::methods>::value;
            return disk_id == storage_id &&
                   std::is_same_v<typename disk_trait::result_type, typename storage_trait::result_type> &&
                   std::is_same_v<typename disk_trait::args_types, typename storage_trait::args_types>;
        }
    } // namespace detail

    static_assert(
        detail::same_message<&disk_contract::storage_fetch_next_batch, &storage_contract::storage_fetch_next_batch>());
    static_assert(
        detail::same_message<&disk_contract::storage_close_cursor, &storage_contract::storage_close_cursor>());
    static_assert(detail::same_message<&disk_contract::storage_total_rows, &storage_contract::storage_total_rows>());
    static_assert(detail::same_message<&disk_contract::storage_append, &storage_contract::storage_append>());
    static_assert(detail::same_message<&disk_contract::storage_update, &storage_contract::storage_update>());
    static_assert(detail::same_message<&disk_contract::storage_delete_rows, &storage_contract::storage_delete_rows>());
    static_assert(detail::same_message<&disk_contract::create_storage_disk, &storage_contract::create_storage_disk>());
    static_assert(detail::same_message<&disk_contract::mark_storage_dropped_many,
                                       &storage_contract::mark_storage_dropped_many>());
    static_assert(detail::same_message<&disk_contract::add_storage_column, &storage_contract::add_storage_column>());
    static_assert(
        detail::same_message<&disk_contract::stamp_column_dropped, &storage_contract::stamp_column_dropped>());
    static_assert(detail::same_message<&disk_contract::storage_prepare, &storage_contract::storage_prepare>());
    static_assert(
        detail::same_message<&disk_contract::storage_release_prepared, &storage_contract::storage_release_prepared>());
    static_assert(detail::same_message<&disk_contract::abort_transaction, &storage_contract::abort_transaction>());
    static_assert(actor_zeta::action_id_impl<disk_contract,
                                             &disk_contract::storage_open_filtered_scan,
                                             disk_contract::dispatch_traits::methods>::value ==
                      storage_contract::prefix_size,
                  "the disk's own messages start right after storage_contract's prefix");

} // namespace services::disk
