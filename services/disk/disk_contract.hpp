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

    // One pg_catalog row-delete request for delete_pg_catalog_rows_many: deletes
    // every row of `table_oid` where column[oid_col_idx] == target_oid.
    struct pg_catalog_delete_spec_t {
        components::catalog::oid_t table_oid;
        std::int64_t oid_col_idx;
        components::catalog::oid_t target_oid;
    };

    // One reply payload of storage_fetch_next_batch: the next scan batch plus the
    // agent-minted cursor_id that keys the LIVE scan state in agent_disk::active_scans_.
    // On OPEN (request cursor_id==0) the reply carries the minted id so the source
    // operator can advance the same cursor on subsequent fetches. A drained cursor
    // replies with an EMPTY chunk (cardinality 0) and the (now-erased) cursor_id.
    // `batch` is a unique_ptr (never null on any non-error reply) so the struct is
    // default-constructible — actor_zeta::otterbrix::send's null-target / ready-future
    // machinery requires a default-constructible reply payload (data_chunk_t has no
    // default ctor), the same reason storage_fetch ships unique_ptr<data_chunk_t>.
    struct fetch_batch_t {
        std::unique_ptr<components::vector::data_chunk_t> batch;
        uint64_t cursor_id{0};

        fetch_batch_t() = default;
        fetch_batch_t(std::unique_ptr<components::vector::data_chunk_t>&& b, uint64_t id)
            : batch(std::move(b))
            , cursor_id(id) {}
    };

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // compact_watermark (here and below): the dispatcher's visible-to-all
        // horizon (txn_compact_watermark_msg / txn_publish_msg return); any
        // version stamp above it makes the MVCC-gated compact a no-op.
        actor_zeta::unique_future<services::wal::id_t>
        checkpoint_all(session_id_t session, services::wal::id_t current_wal_id, uint64_t compact_watermark);
        // Returns how many storages had PHYSICAL ROW IDS moved (index entries store one, so
        // this is what a caller rebuilds indexes on). No compact_watermark: nothing here compacts.
        actor_zeta::unique_future<uint64_t> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        // Batched GC-threshold check + compact: routes each table_oid to its owning
        // agent's maybe_cleanup_inner with the shared compact_watermark.
        // operator_commit_transaction sends one call covering all just-touched tables.
        actor_zeta::unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                                           std::pmr::vector<components::catalog::oid_t> table_oids,
                                                           uint64_t compact_watermark);

        // ddl_add_column / ddl_adopt_computing_schema replaced by pipeline operators.

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

        // Appends the row and reports the occupied range, or refuses via the wrapper: a
        // zero-count range means nothing was asked to be written, never that the write failed.
        actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        actor_zeta::unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::int64_t oid_col_idx,
                                                               components::catalog::oid_t target_oid);

        // Batched WAL-safe delete: one count per spec, in spec order (result.size() ==
        // specs.size()), or a refusal via the wrapper — needed because six callers (DROP
        // FUNCTION/CAST, ALTER DROP COLUMN, DROP cascade, VACUUM, DROP INDEX) must tell a
        // completed scrub from one that never happened.
        // A zero count is honest, not an error: it means "no row ctx.txn CAN SEE carried that
        // oid". The scan runs under ctx.txn — the same snapshot read_chunks_by_key/scan_by_keys
        // use — so a caller that just read the row it's deleting can trust 0 as a refusal;
        // mixing a transaction-less read with this delete (or vice versa) breaks that.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
        delete_pg_catalog_rows_many(execution_context_t ctx, std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Patches each backfill's pg_attribute row with the shared `commit_id` written
        // into the added_at or dropped_at column (selected by the marker's kind).
        // Drained by operator_commit_transaction_t once the commit_id is allocated;
        // each backfill pairs with its own physical_update WAL record.
        // Answers with the first refusal after attempting every marker; it sits below the
        // durable commit marker, so it can only report, never take the commit back.
        actor_zeta::unique_future<core::error_t>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        // Batched keyed scan for one table: result[i] = match row_ids for key-tuple i.
        // Keys are columnar: `keys` is a data_chunk whose column j holds key_col_names[j]
        // and whose row i is the i-th key-tuple, so no row-major logical_value_t crosses
        // the boundary. All keys share the same table_oid (and therefore the same owning
        // agent), so the per-key loop runs intra-agent via a single scan_by_keys_inner.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Columnar row-data scan for ONE key-tuple: returns the txn-visible rows whose column
        // key_col_indices[j] equals keys.value(j, 0) as batched data_chunk_t (each chunk <=
        // DEFAULT_VECTOR_CAPACITY rows). `keys` is a 1-row columnar carrier (column j carries
        // key_col_indices[j]), so no row-major logical_value_t crosses the boundary. Callers
        // read cells via chunk.value(col_idx, row_idx).
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::uint64_t> key_col_indices,
                           components::vector::data_chunk_t keys,
                           std::pmr::vector<std::uint64_t> projected_cols);

        // Batched multi-key columnar row-data scan for one table: result[i] = matched chunks
        // for key-tuple i (each chunk <= DEFAULT_VECTOR_CAPACITY rows). `keys` is an N-row
        // columnar carrier (column j carries key_col_indices[j], row i == i-th key-tuple), so no
        // row-major logical_value_t crosses the boundary. All keys share `table_oid` (one owning
        // agent), so the per-key loop runs intra-agent via a single read_chunks_by_keys_inner
        // message. The outer vector always has one (possibly empty) entry per key in input
        // order, so result.size() == keys.size(). Callers read cells via chunk.value(col, row).
        actor_zeta::unique_future<
            core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::uint64_t> key_col_indices,
                            components::vector::data_chunk_t keys,
                            std::pmr::vector<std::uint64_t> projected_cols);

        // Aggregate-pushdown REDUCE: a dedicated protocol leg, not a scan mode — the owning agent
        // runs the whole GROUP BY over its slice and replies all final rows in one shot (bounded
        // by #groups, no cursor).
        // A not-owned oid is a REFUSAL, not an empty fold: a scalar aggregate's empty-input
        // finalize emits a COUNT=0/NULL row indistinguishable from a real empty table's answer.
        // Single-owner only — sharded slices need operator_group_merge, not this leg.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce(session_id_t session,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::table::table_filter_t> filter,
                       std::vector<size_t> projected_cols,
                       components::table::transaction_data txn,
                       components::operators::pushed_aggregate_spec_t spec);

        // Physical column compaction for a relkind='g' table_storage_t — see the long note on
        // manager_disk_t::compact_relkind_g_storage.
        actor_zeta::unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                                           components::catalog::oid_t table_oid,
                                                                           std::set<std::string> live_attnames);

        // ALTER TABLE DROP COLUMN's physical release of ONE named column. A sibling of
        // compact_relkind_g_storage, not a mode on it: that leg is SUBTRACTIVE (drops the
        // complement of a live set, right for VACUUM); this leg is ADDITIVE and touches only the
        // column it's handed, so a gap in a live-set derivation can never drop a surviving column.
        //   true  — column existed and was removed.
        //   false — column never materialized (ALTER ADD COLUMN only writes pg_attribute).
        //   error — oid names no storage on its owning agent (broken invariant during an ALTER).
        actor_zeta::unique_future<core::result_wrapper_t<bool>>
        drop_storage_column(session_id_t session, components::catalog::oid_t table_oid, std::string attname);

        // ALTER TABLE RENAME COLUMN's physical leg — not a convenience: the storage name is a
        // cache of pg_attribute.attoid, and callers that still address columns by name (append's
        // column expansion, drop_storage_column) would read a stale one until the next restart.
        // Not what keeps data alive though: rearm_dropped_column_blocks_sync matches by ATTOID,
        // so a rename this leg missed reads as a stale name, not a drop.
        //   true  — column existed and now carries new_attname.
        //   false — old_attname never materialized (nothing to rename).
        //   error — no storage for the oid, or new_attname already exists on it.
        actor_zeta::unique_future<core::result_wrapper_t<bool>> rename_storage_column(
            session_id_t session,
            components::catalog::oid_t table_oid,
            std::string old_attname,
            std::string new_attname);

        // Storage management
        // `is_computed` = the pg_class.relkind='g' fact, derived by the caller
        // (see manager_disk_t::create_storage_disk).
        actor_zeta::unique_future<void>
        create_storage_disk(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t database_oid,
                            std::vector<components::table::column_definition_t> columns,
                            bool is_computed);
        // Batched DROP: partition oids per agent, fan out one inner per agent.
        actor_zeta::unique_future<void> drop_storage_many(session_id_t session,
                                                          std::pmr::vector<components::catalog::oid_t> table_oids);

        // Storage queries. Both wrap their answer for the same reason storage_delete_rows does:
        // the natural reply (empty type list / 0 rows) is also what a routing refusal would
        // produce, so a missing storage must travel as an error instead.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_total_rows(session_id_t session, components::catalog::oid_t table_oid);
        // Storage data operations. The read contract has exactly two legs, deliberately not
        // reducible to one: a row-id SET isn't expressible as a scan filter (a mask can't
        // repeat/reorder rows), and parking ids on a cursor would bloat the position-only
        // active_scan_t and gate point reads on compact() for no reason.
        // Streaming fetch-next scan source; LIVE per-cursor state on the owning agent, not a
        // materialized batch vector. cursor_id==0 OPENs (mints an id); non-zero ADVANCES (filter
        // ignored). An EMPTY chunk (cardinality 0) is the drained sentinel, and the cursor is
        // erased. An OPEN over an unowned oid is a REFUSAL, not a drained sentinel — the sentinel
        // would read as "this table is empty". ADVANCING an unknown cursor stays drained: the
        // drain path already erased it.
        actor_zeta::unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        // Closes a fetch-next cursor without draining it — a source that stops early (error,
        // LIMIT, dropped sub-plan) otherwise leaks the active_scans_ entry forever, gating
        // compact() on that oid. Idempotent: an unknown/already-drained cursor is a no-op.
        actor_zeta::unique_future<void>
        storage_close_cursor(session_id_t session, components::catalog::oid_t table_oid, uint64_t cursor_id);
        // Wraps buffer-pool OOM/data_corruption as a value, AND the routing refusal: an unowned
        // oid must not come back looking like a point fetch whose rows are all invisible to
        // `txn`. count==0 stays a success with no chunks, for any oid.
        // `visibility` has no default (a fetch that doesn't name the mode does not compile).
        //   SNAPSHOT: rows invisible to `txn` are dropped; the reply is shorter than the request
        //             and paired by each chunk's own row_ids, not by position.
        //   RAW:      no visibility check — CREATE INDEX backfill needs it to read deleted rows.
        // An empty `txn` is NOT raw: it means "see all committed rows".
        // `limit` caps rows POST-visibility — the only place a LIMIT over an index scan can be
        // applied correctly, since the index returns a superset of ids and this leg drops the
        // invisible ones. Measured: without this, a LIMIT 7 over
        // integration/cpp/test/test_index_scan_limit_cap.cpp answered with 0 rows. -1 = uncapped;
        // no default.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        // projected_cols holds storage chunk indices; EMPTY means every column, matching
        // storage_fetch_next_batch above. Columns outside the set keep their ordinal slot and come
        // back as buffer-less stubs, so the reply is indexed the same way either way.
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols,
                      components::table::transaction_data txn,
                      components::table::fetch_visibility_t visibility,
                      int64_t limit);

        // Reply wraps (start_row, count) so a write_conflict/out_of_memory reaches
        // operator_insert as a value, on the same channel as the routing refusal. An empty
        // batch stays a (0,0) success; an unowned oid does not.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::data_chunk_t> data);

        // Reply wraps (updated, appended) on the same channel/reasoning as storage_append
        // above. An empty request answers (0, 0) and stays a success.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data);
        // Marks `count` rows deleted under ctx.txn and reports how many marks it set — the
        // count is not the error channel, so the reply is wrapped. A count smaller than
        // requested is legitimate (chunk_vector_info::delete_rows skips an already-stamped row,
        // e.g. a duplicate id), so a bare 0 can't be told apart from "never reached a storage" —
        // which used to let ON DELETE CASCADE report success while deleting nothing. Refusals
        // travel as an error instead.
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

        // MVCC delete-revert (abort path). The mirror of storage_publish_deletes:
        // instead of stamping this txn's pending delete marks with a commit_id, the
        // owning agent un-stamps them back to NOT_DELETED_ID via
        // data_table_t::revert_all_deletes(ctx.txn.transaction_id), restoring row
        // visibility for an aborted DELETE. Routed per owning agent by oid.
        actor_zeta::unique_future<void> storage_revert_deletes(execution_context_t ctx,
                                                               std::vector<components::catalog::oid_t> tables);

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
        actor_zeta::unique_future<void>
        storage_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // DROP-rollback un-mark. The mirror of storage_dropped_committed for the
        // abort path: a DROP TABLE inside a transaction records its GC entry with
        // dropped_at_commit_id in TXN-ID space via mark_storage_dropped_many, but if the
        // transaction ABORTS the table must survive. operator_abort_transaction sends
        // this so the manager fans out to every agent, and each agent ERASES (not
        // remaps) its own dropped_storages_ entries whose dropped_at_commit_id == txn_id,
        // un-marking the DROP so on_horizon_advanced never reclaims the still-live .otbx.
        actor_zeta::unique_future<void> storage_drop_aborted(session_id_t session, uint64_t txn_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&disk_contract::checkpoint_all,
                                                            &disk_contract::vacuum_all,
                                                            &disk_contract::maybe_cleanup_many,
                                                            // Storage management
                                                            &disk_contract::create_storage_disk,
                                                            &disk_contract::drop_storage_many,
                                                            // Storage queries
                                                            &disk_contract::storage_types,
                                                            &disk_contract::storage_total_rows,
                                                            // Storage data operations
                                                            &disk_contract::storage_fetch_next_batch,
                                                            &disk_contract::storage_close_cursor,
                                                            &disk_contract::storage_reduce,
                                                            &disk_contract::storage_fetch,
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
                                                            &disk_contract::storage_drop_aborted>;

        disk_contract() = delete;
    };

} // namespace services::disk
