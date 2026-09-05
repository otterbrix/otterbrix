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
    using transaction_data = components::table::transaction_data;
    using execution_context_t = components::execution_context_t;

    struct index_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Collection lifecycle (oid-keyed)
        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

        // DML: txn-aware bulk index operations. insert_rows/update_rows take the whole
        // chunk batch; rows are indexed in vector order with contiguous row-ids based at
        // start_row_id / new_start_row_id.
        unique_future<core::error_t> insert_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                                 uint64_t start_row_id,
                                                 uint64_t count);
        unique_future<core::error_t> delete_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                                 std::pmr::vector<int64_t> row_ids);
        unique_future<core::error_t> update_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> old_data,
                                                 std::pmr::vector<components::vector::data_chunk_t> new_data,
                                                 std::pmr::vector<int64_t> row_ids,
                                                 int64_t new_start_row_id);

        // MVCC commit/revert/cleanup. commit_inserts / commit_deletes return
        // core::error_t by the project-wide convention (no_error() = success,
        // contains_error() = failure) so callers can branch on an index-side abort.
        // Both take a batch of table oids.
        //
        // THE TWO ARE NOT MIRRORS, and the asymmetry is a correctness rule rather than an
        // implementation detail:
        //
        //   commit_inserts publishes NOW: it folds every oid's pending disk work into one
        //     send-all-then-await-all fan-out; the first contains_error() across the batch
        //     wins (the remaining awaits still drain, so no future is dropped). An index
        //     that names a row a reader may not see costs a fetch the table then discards.
        //
        //   commit_deletes publishes LATER, and touches no store here. Erasing an entry at
        //     commit time takes the row away from every reader whose snapshot PREDATES the
        //     commit and still owns it — and an id the index never returns is never fetched
        //     and never filtered, so a short index answer is a silently wrong result. The
        //     batch is queued against its commit_id and published by on_horizon_advanced
        //     once the snapshot floor has passed it. Nothing is left here to fail, so this
        //     returns no_error(); a failure at publish time is logged there and leaves the
        //     index a superset, which is the safe direction. See
        //     manager_index.hpp (deferred_deletes_) for the queue and its price.
        unique_future<core::error_t> commit_inserts(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> revert_delete(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);
        // Runtime index rebuild driver (the vacuum/checkpoint repopulate path). Returns the oids whose engine holds >= 1
        // index, EXCLUDING oids mid-GC (present in dropped_table_agents_). The
        // vacuum operator enumerates these and repopulate_table's each from the
        // just-compacted storage.
        unique_future<std::pmr::vector<components::catalog::oid_t>> all_indexed_oids(session_id_t session);

        // Repopulate one table's indexes from a post-compact storage scan.
        // (a) clears each disk-backed index's on-disk backing via the agent
        //     clear() fan-out (covers btree duplicate-growth and disk_hash
        //     wrong-row), (b) clears the in-memory engine, (c) re-inserts every
        //     row keyed by its PHYSICAL id from chunk.row_ids (the
        //     visibility-filtered scan compacts positions, not ids — a
        //     tombstone surviving a refused compact leaves gaps) under txn_id=0
        //     (committed-for-everyone, no commit needed). Empty chunks are
        //     valid: clear still runs, nothing re-inserted. Returns an error —
        //     before any clearing — when a non-empty chunk carries no physical
        //     row_ids (a producer defect; positional numbering is never used).
        unique_future<core::error_t> repopulate_table(session_id_t session,
                                                      components::catalog::oid_t table_oid,
                                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                      uint64_t row_count,
                                                      core::date::timezone_offset_t session_tz);

        // DDL: index management. index_oid = pg_index.indexrelid — the index's
        // ONLY identity below the planner boundary (rule 16); the name is
        // resolved to it exactly once, at plan time.
        // Answers with the reason the index could not be brought up, or no_error(). It
        // used to answer with a uint32 "index id" -- the index's POSITION in a per-table
        // list of index objects that no longer exists -- and no caller ever read the
        // number.
        unique_future<core::error_t> create_index(session_id_t session,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t index_oid,
                                             components::index::keys_base_storage_t keys,
                                             components::logical_plan::index_type type,
                                             core::date::timezone_offset_t session_tz);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // Query (txn-aware).
        //
        // The result is WRAPPED, and the wrapper is the point: a bare vector was empty
        // in three cases a caller could not tell apart -- no engine for the oid, no
        // index matching the keys, and a search that legitimately matched no row. The
        // first two are planner-invariant violations (an index_scan is only ever built
        // for a predicate the planner saw an index for), so they now come back as
        // errors, and an empty vector means exactly one thing: no row carries the key.
        // A failed read inside the index's disk agent is the fourth value and travels
        // the same channel.
        //
        // Still NOT covered: index_scan's own empty-address early return, which yields
        // an empty window before this message is ever sent.
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        search(session_id_t session,
               components::catalog::oid_t table_oid,
               components::index::keys_base_storage_t keys,
               components::types::logical_value_t value,
               components::expressions::compare_type compare,
               uint64_t start_time,
               uint64_t txn_id,
               core::date::timezone_offset_t session_tz);
        // Query (txn-aware)
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        search_with_preferred_type(session_id_t session,
                                   components::catalog::oid_t table_oid,
                                   components::index::keys_base_storage_t keys,
                                   components::types::logical_value_t value,
                                   components::expressions::compare_type compare,
                                   components::logical_plan::index_type preferred_index_type,
                                   uint64_t start_time,
                                   uint64_t txn_id,
                                   core::date::timezone_offset_t session_tz);

        // Fans force_flush out to every owned agent and reports the FIRST refusal. An index
        // whose entries never reached the device must not be presented to the checkpoint above
        // it as flushed: that checkpoint truncates the WAL.
        unique_future<core::error_t> flush_all_indexes(session_id_t session);

        // Compact gate: returns the subset of the input oids that are safe to
        // compact — those with NO index engine, plus those whose engine holds
        // ZERO indexes (an engine is created empty for every table, so engine
        // presence alone does not mean indexed). Input order preserved.
        // operator_commit_transaction queries this before fanning out
        // maybe_cleanup — compact() rebuilds the row_group and shifts row
        // positions, which would silently invalidate the positional row refs
        // every in-memory index holds (index-rebuild-on-compact is a separate
        // task; until then indexed tables must not compact mid-session).
        unique_future<std::pmr::vector<components::catalog::oid_t>>
        tables_without_indexes(session_id_t session, std::pmr::vector<components::catalog::oid_t> table_oids);

        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
        get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<std::pmr::vector<components::index::index_description_t>>
        get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid);

        // Event-driven GC subscriber, draining TWO queues in this order:
        //   1. dropped_table_agents_ — routing entries (and their agents) whose
        //      dropped_at_commit_id < new_horizon;
        //   2. deferred_deletes_ — the committed index erases commit_deletes held back,
        //      published once commit_id <= new_horizon (a snapshot sitting AT commit_id
        //      already hides the row, so that is the first horizon at which the erase is
        //      invisible to every live reader). Second, because step 1 destroys agents.
        // The on_subscriber_empty(INDEX_KIND) ack is sent only when BOTH are empty — it
        // switches off the broadcast that is the sole trigger for either.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Runtime DROP TABLE path: operator_dynamic_cascade_delete records the
        // (oid, dropped_at_commit_id) pair into dropped_table_agents_ for the
        // next on_horizon_advanced GC sweep. Pairs with
        // manager_dispatcher_t::on_drop_resource_marked(INDEX_KIND).
        unique_future<void>
        mark_table_dropped(session_id_t session, components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);

        // DROP-GC value-space remap. mark_table_dropped recorded dropped_at_commit_id
        // in TXN-ID space (>= 2^62) because the cascade-delete operator only knew the
        // in-flight txn_id. on_horizon_advanced compares against a commit-id horizon,
        // so the TXN-ID placeholder would never be reclaimed. Once the transaction
        // commits and a real commit_id is allocated, operator_commit_transaction sends
        // this so the manager rewrites every dropped_table_agents_ entry whose value
        // equals txn_id to commit_id, moving it into commit-id space.
        unique_future<void> table_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // DROP-rollback un-mark — the abort mirror of table_dropped_committed.
        // mark_table_dropped recorded dropped_table_agents_[oid] in TXN-ID space
        // (>= 2^62). If the transaction ABORTS instead of committing, the table must
        // remain indexed, so operator_abort_transaction sends this; the manager ERASES
        // every dropped_table_agents_ entry whose value == txn_id, un-marking the DROP
        // so on_horizon_advanced never reaps the engine.
        unique_future<void> table_drop_aborted(session_id_t session, uint64_t txn_id);

        // CREATE INDEX catchup: operator_create_index_backfill calls this per
        // matching WAL record to apply a PHYSICAL_{INSERT,DELETE,UPDATE} effect to the
        // indexes of the table being built, by staging it into each index agent's bucket
        // for the CREATE INDEX transaction — the same stage_inserts / stage_deletes path
        // the DML insert_rows / delete_rows handlers take.
        //   physical_data:      NEW rows for INSERT/UPDATE, empty/null for DELETE.
        //   physical_row_start: WAL row-id base, used when row_ids is empty.
        //   txn_id:             the CREATE INDEX txn, so entries land in the
        //                       PENDING bucket and are committed by the
        //                       post-pipeline commit_insert.
        unique_future<void> apply_wal_record_for_index(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t index_oid,
                                                       uint64_t wal_record_id,
                                                       uint8_t record_type,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::pmr::vector<components::vector::data_chunk_t> physical_data,
                                                       uint64_t physical_row_start,
                                                       uint64_t txn_id,
                                                       core::date::timezone_offset_t session_tz);

        using dispatch_traits = actor_zeta::dispatch_traits<&index_contract::register_collection,
                                                            &index_contract::unregister_collection,
                                                            &index_contract::insert_rows,
                                                            &index_contract::delete_rows,
                                                            &index_contract::update_rows,
                                                            &index_contract::commit_inserts,
                                                            &index_contract::commit_deletes,
                                                            &index_contract::revert_insert,
                                                            &index_contract::revert_delete,
                                                            &index_contract::cleanup_all_versions,
                                                            &index_contract::all_indexed_oids,
                                                            &index_contract::repopulate_table,
                                                            &index_contract::create_index,
                                                            &index_contract::drop_index,
                                                            &index_contract::search,
                                                            &index_contract::search_with_preferred_type,
                                                            &index_contract::flush_all_indexes,
                                                            &index_contract::tables_without_indexes,
                                                            &index_contract::get_indexed_keys,
                                                            &index_contract::get_indexed_descriptions,
                                                            &index_contract::on_horizon_advanced,
                                                            &index_contract::mark_table_dropped,
                                                            &index_contract::table_dropped_committed,
                                                            &index_contract::table_drop_aborted,
                                                            &index_contract::apply_wal_record_for_index>;

        index_contract() = delete;
    };

} // namespace services::index
