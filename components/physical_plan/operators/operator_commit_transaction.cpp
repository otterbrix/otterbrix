#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <algorithm>
#include <iterator>
#include <set>
#include <vector>

namespace components::operators {

    // Handles COMMIT only; ROLLBACK and statement-failure abort go through
    // operator_abort_transaction_t. When a DML statement inside an explicit txn
    // aborts, rows already written by prior statements stay on disk but carry
    // insert_id >= TRANSACTION_ID_START, so the visibility filter rejects them
    // and VACUUM later reclaims them — no explicit cleanup needed here.

    operator_commit_transaction_t::operator_commit_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::commit_transaction) {}

    actor_zeta::unique_future<void> operator_commit_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // No storage flush here — the old manager_disk_t::flush call at this point was a stub (traced,
        // flushed nothing). The WAL commit marker in STEP 2 below is the real durability point for both
        // DDL and DML.

        // Single dispatcher round-trip: drains txn_data + allocates commit_id via transaction_manager_t::commit(),
        // which purges the active map, so everything must return by value. INVARIANT: must NOT call publish()
        // here — that ProcArray barrier is txn_publish_msg, deferred until after storage_publish_* / WAL.
        components::table::transaction_data txn_data{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends;
        std::set<components::catalog::oid_t> swap_deletes;
        // backfill markers (added by operator_alter_column_{add,drop,rename}
        // and accumulated onto transaction_t by the executor's explicit-txn
        // branch). Patched after commit_id_ is allocated below. Plain std to
        // match the cross-mailbox drain field (txn_commit_drain_t.swap_backfills);
        // moved straight into the batched update_pg_attribute_commit_id_fields.
        std::vector<components::pg_attribute_commit_id_backfill_t> swap_backfills;
        // Explicit-txn base-table DML ranges parked by the executor commit phase.
        // Batched into storage_publish_* alongside the pg_catalog ranges, all
        // BEFORE the ProcArray publish() barrier so readers see an atomic flip.
        std::vector<components::pg_catalog_append_range_t> base_appends;
        std::set<components::catalog::oid_t> base_delete_tables;
        // Storage oids actually dropped by this txn's DDL (recorded by
        // operator_dynamic_cascade_delete into the pipeline ctx, lifted into the
        // accumulate payload and parked on transaction_t). Drives the DROP-GC
        // value-space remap below, keyed off ACTUAL drops rather than the lower
        // mode flag.
        std::vector<components::catalog::oid_t> dropped_storage_oids;
        // Null-sender guard: with no dispatcher to talk to there is no txn to
        // drain — leave commit_id_ = 0 and skip.
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::txn_commit_drain_msg,
                                            ctx->session);
            services::dispatcher::txn_commit_drain_t drain = co_await std::move(drf);
            txn_data = drain.txn;
            swap_appends = std::move(drain.swap_appends);
            swap_deletes = std::move(drain.swap_deletes);
            swap_backfills = std::move(drain.swap_backfills);
            base_appends = std::move(drain.base_appends);
            base_delete_tables = std::move(drain.base_delete_tables);
            dropped_storage_oids = std::move(drain.dropped_storage_oids);
            commit_id_ = drain.commit_id;
        }

        // Commit back-channel: surface the just-allocated commit_id to the
        // executor tail (e.g. inline CREATE INDEX commit) via the pipeline ctx.
        ctx->committed_id = commit_id_;

        // ORDERING INVARIANT (STEPS 1-6 below): no step that can fail may run after the first step that
        // stamps commit_id. Only STEP 1 (commit_inserts) and STEP 2 (WAL marker) can still fail and abort
        // via txn_discard_msg; every step after them only stamps/publishes an already-durable commit, so a
        // failure there can no longer be discarded without resurrecting it. Leaving commit_id_ unerased on
        // an early co_return leaks it in in_flight_commits_ forever, flooring visible_to_all_locked() and
        // stalling compact()/GC sweeps for the process lifetime — hence no early exit past STEP 2.

        // Capture unique base-table oids ONCE, before storage_publish_* moves base_appends/base_delete_tables
        // out; index commits, storage publish and compact fan-out below each take their own per-send copy.
        std::pmr::set<components::catalog::oid_t> append_oid_set{resource_};
        for (const auto& r : base_appends) {
            append_oid_set.insert(r.table_oid);
        }
        std::pmr::vector<components::catalog::oid_t> base_append_oids{append_oid_set.begin(),
                                                                      append_oid_set.end(),
                                                                      resource_};
        std::pmr::vector<components::catalog::oid_t> base_delete_table_oids{base_delete_tables.begin(),
                                                                            base_delete_tables.end(),
                                                                            resource_};

        // STEP 1 — index insert-commits, first of the two steps that can fail. On error, discard commit_id
        // and co_return: the WAL marker isn't written yet, so nothing durable or reader-visible has happened.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0 && !base_append_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> append_oids{base_append_oids.begin(),
                                                                     base_append_oids.end(),
                                                                     resource_};
            auto [_ic, icf] = actor_zeta::otterbrix::send(
                ctx->index_address,
                &services::index::manager_index_t::commit_inserts,
                components::execution_context_t{ctx->session, txn_data, ctx->execution_context.timezone_offset},
                std::move(append_oids),
                commit_id_);
            core::error_t result = co_await std::move(icf);
            if (result.contains_error()) {
                // Clean abort: commit_id is stamped nowhere yet, so discarding it can't publish anything.
                if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
                    auto [_dx, dxf] =
                        actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                    &services::dispatcher::manager_dispatcher_t::txn_discard_msg,
                                                    commit_id_);
                    co_await std::move(dxf);
                }
                set_error(std::move(result));
                co_return;
            }
        }

        // STEP 2 — WAL commit marker: the single durable commit point and the last step that can fail.
        // Both modes emit it HERE (after the drain), not before it with cid=0 as DDL-commit used to: an
        // earlier marker would carry no real commit_id and could survive a STEP-1 refusal, resurrecting a
        // discarded commit on replay (replay keys off the marker's transaction_id).
        if (ctx->wal_address != actor_zeta::address_t::empty_address() && commit_id_ > 0 &&
            (is_ddl_commit_ ? txn_id_ != 0 : txn_data.transaction_id != 0)) {
            const std::uint64_t marker_txn_id = is_ddl_commit_ ? txn_id_ : txn_data.transaction_id;
            const components::catalog::oid_t marker_db_oid =
                is_ddl_commit_ ? database_oid_ : components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                        &services::wal::manager_wal_replicate_t::commit_txn,
                                                        ctx->session,
                                                        marker_txn_id,
                                                        services::wal::wal_sync_mode::FULL,
                                                        marker_db_oid,
                                                        commit_id_);
            // Reply is checked: a refused fsync here is still a clean abort, since commit_id is stamped nowhere yet.
            if (auto commit_result = co_await std::move(wf); commit_result.has_error()) {
                if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
                    auto [_dx, dxf] =
                        actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                    &services::dispatcher::manager_dispatcher_t::txn_discard_msg,
                                                    commit_id_);
                    co_await std::move(dxf);
                }
                set_error(commit_result.error());
                co_return;
            }
        }

        // STEP 3 — DROP-GC remap: a DROP tombstone is keyed by transaction_id; the horizon-advance GC compares
        // against commit_id, so it must be remapped now that commit_id is known. Keyed off the actual
        // dropped_storage_oids from the drain, not off is_ddl_commit_, so any lowering path remaps correctly.
        if (!dropped_storage_oids.empty() && txn_data.transaction_id != 0 && commit_id_ > 0) {
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                auto [_sd, sdf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_dropped_committed,
                                                ctx->session,
                                                txn_data.transaction_id,
                                                commit_id_);
                co_await std::move(sdf);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_td, tdf] =
                    actor_zeta::otterbrix::send(ctx->index_address,
                                                &services::index::manager_index_t::table_dropped_committed,
                                                ctx->session,
                                                txn_data.transaction_id,
                                                commit_id_);
                co_await std::move(tdf);
            }
        }

        // STEP 4 — patch placeholder commit_id columns on the ALTER's pg_attribute rows (swap_backfills).
        // Safe here: the rows still carry insert_id == transaction_id, invisible to every concurrent snapshot.
        // dropped_at markers also carry the physical column release; split those out here (before the move
        // below empties swap_backfills) and perform them further down, after the publish barrier.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> column_releases{resource_};
        for (const auto& b : swap_backfills) {
            if (b.kind == components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at &&
                !b.release_attname.empty() && b.release_table_oid != components::catalog::INVALID_OID) {
                column_releases.push_back(b);
            }
        }
        // The RENAME's storage half, split out the same way and also kept OUT of the batch below: renaming
        // preserves added_at_commit_id, so update_pg_attribute_commit_id_field_inner would stamp dropped_at
        // over a LIVE row if these markers reached it.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> column_renames{resource_};
        for (const auto& b : swap_backfills) {
            if (b.kind == components::pg_attribute_commit_id_backfill_t::kind_t::storage_rename &&
                !b.release_attname.empty() && !b.rename_to_attname.empty() &&
                b.release_table_oid != components::catalog::INVALID_OID) {
                column_renames.push_back(b);
            }
        }
        // Only the kinds that name a commit_id column reach the patcher.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfill_markers{resource_};
        backfill_markers.reserve(swap_backfills.size());
        for (auto& b : swap_backfills) {
            if (b.kind != components::pg_attribute_commit_id_backfill_t::kind_t::storage_rename) {
                backfill_markers.push_back(std::move(b));
            }
        }
        swap_backfills.clear();
        if (!backfill_markers.empty() && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t backfill_ctx{ctx->session, txn_data, {}};
            // Log the marker count before the move empties the vector.
            const auto backfill_count = backfill_markers.size();
            auto [_b, bf] =
                actor_zeta::otterbrix::send(ctx->disk_address,
                                            &services::disk::manager_disk_t::update_pg_attribute_commit_id_fields,
                                            backfill_ctx,
                                            std::move(backfill_markers),
                                            commit_id_);
            // Reply is checked but must NOT set_error/co_return: commit_id is already durable past STEP 2, so
            // refusing here would strand it in in_flight_commits_ forever. A refused stamp just leaves
            // added_at/dropped_at_commit_id == 0, which reads as "added before every snapshot" — a bounded
            // visibility bug, not a correctness one.
            if (auto backfill_result = co_await std::move(bf); backfill_result.contains_error()) {
                error(log_,
                      "operator_commit_transaction: the pg_attribute backfill of {} marker(s) for txn {} "
                      "commit_id {} reported refused stamp(s); each refused column keeps commit_id 0 and is "
                      "visible to older snapshots: {}",
                      backfill_count,
                      txn_data.transaction_id,
                      commit_id_,
                      backfill_result.what.c_str());
            } else {
                trace(log_,
                      "operator_commit_transaction: OPTION X drained {} pg_attribute backfill markers "
                      "for txn {} commit_id {} (patched in-place)",
                      backfill_count,
                      txn_data.transaction_id,
                      commit_id_);
            }
        }

        // STEP 5 — index delete-commits. Deliberately not hoisted with STEP 1: this queues a deferred_delete_t
        // stamped with commit_id (manager_index_t::deferred_deletes_, swept later), so it must sit below the
        // durable marker. commit_deletes has zero cross-actor awaits and always returns no_error(), hence no
        // early-return path here.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0 && !base_delete_table_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> delete_oids{base_delete_table_oids.begin(),
                                                                     base_delete_table_oids.end(),
                                                                     resource_};
            auto [_dc, dcf] = actor_zeta::otterbrix::send(
                ctx->index_address,
                &services::index::manager_index_t::commit_deletes,
                components::execution_context_t{ctx->session, txn_data, ctx->execution_context.timezone_offset},
                std::move(delete_oids),
                commit_id_);
            if (core::error_t result = co_await std::move(dcf); result.contains_error()) {
                error(log_,
                      "operator_commit_transaction: commit_deletes reported an error for txn {} commit_id {} — "
                      "the contract says it cannot ({}); the commit marker is already durable, so the publish "
                      "below proceeds and the deferred index erase is the thing that was lost",
                      txn_data.transaction_id,
                      commit_id_,
                      result.what);
            }
        }

        // STEP 6 — flip MVCC state: merge pg_catalog + base-table ranges into one publish_commits and one
        // publish_deletes (the manager partitions per oid internally, so concatenation is order-independent).
        // Both are void — this is the only step allowed to stamp commit_id on live rows, since it runs after
        // the durable WAL marker.
        if (txn_data.transaction_id != 0 && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t swap_ctx{ctx->session, txn_data, {}};
            // Concatenate pg_catalog appends + base-table appends into one publish.
            // Move both sources into a single ranges vector (both are
            // std::vector<pg_catalog_append_range_t>, so a flat append preserves
            // each range verbatim — the manager re-partitions per oid).
            std::vector<components::pg_catalog_append_range_t> all_appends = std::move(swap_appends);
            all_appends.insert(all_appends.end(),
                               std::make_move_iterator(base_appends.begin()),
                               std::make_move_iterator(base_appends.end()));
            if (!all_appends.empty()) {
                auto [_a, af] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_publish_commits,
                                                            swap_ctx,
                                                            commit_id_,
                                                            std::move(all_appends));
                co_await std::move(af);
            }
            // Concatenate pg_catalog deletes + base-table deletes into one publish
            // (both are std::set<oid_t>; the union is the full set of dropped/
            // deleted-from tables, deduped by the set, partitioned per oid).
            std::set<components::catalog::oid_t> all_deletes = std::move(swap_deletes);
            all_deletes.insert(base_delete_tables.begin(), base_delete_tables.end());
            if (!all_deletes.empty()) {
                auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_publish_deletes,
                                                            swap_ctx,
                                                            commit_id_,
                                                            std::move(all_deletes));
                co_await std::move(df);
            }
        }

        // ProcArray publish barrier — must be the LAST step: everything above is already durable, so a crash
        // before this cannot lose a reader-visible commit. Returns the compact watermark used below.
        uint64_t compact_watermark = 0;
        if (commit_id_ > 0 && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_p, pf] = actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                        &services::dispatcher::manager_dispatcher_t::txn_publish_msg,
                                                        commit_id_);
            compact_watermark = co_await std::move(pf);
        }

        // Commit-time physical DROP: operator_dynamic_cascade_delete only tombstoned dropped storages/indexes
        // at plan time so the DROP stayed revertible until COMMIT. Tear down for real only now, past the
        // publish barrier: unregister ALL indexes first and await them all (the two managers are separate
        // mailboxes, so FIFO alone doesn't order them), THEN batch-drop storage, so no index ever references
        // storage the disk actor is about to free.
        if (commit_id_ > 0 && !dropped_storage_oids.empty()) {
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<actor_zeta::unique_future<void>> unregister_futures{resource_};
                unregister_futures.reserve(dropped_storage_oids.size());
                for (auto oid : dropped_storage_oids) {
                    auto [_u, uf] =
                        actor_zeta::otterbrix::send(ctx->index_address,
                                                    &services::index::manager_index_t::unregister_collection,
                                                    ctx->session,
                                                    oid);
                    unregister_futures.push_back(std::move(uf));
                }
                // Await EVERY unregister before any disk drop (index-before-disk).
                for (auto& uf : unregister_futures) {
                    co_await std::move(uf);
                }
            }
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<components::catalog::oid_t> drop_oids{dropped_storage_oids.begin(),
                                                                       dropped_storage_oids.end(),
                                                                       resource_};
                auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::drop_storage_many,
                                                            ctx->session,
                                                            std::move(drop_oids));
                co_await std::move(df);
            }
        }

        // Commit-time physical COLUMN drop (mirrors the table DROP above): operator_alter_column_drop_t only
        // tombstoned the pg_attribute row. The storage rebuild is irreversible, so it runs only once the
        // tombstone is itself durable (past the WAL marker + publish barrier) — no window has the physical
        // drop durable without the tombstone. Reply is checked: a committed tombstone over a storage refusal
        // is not a success, since nothing re-derives this drop later.
        if (commit_id_ > 0 && !column_releases.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (const auto& release : column_releases) {
                auto [_rc, rcf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::drop_storage_column,
                                                              ctx->session,
                                                              release.release_table_oid,
                                                              release.release_attname);
                auto released = co_await std::move(rcf);
                if (released.has_error()) {
                    set_error(released.error());
                    co_return;
                }
                trace(log_,
                      "operator_commit_transaction: released column '{}' of oid {} — {} (commit_id {})",
                      release.release_attname,
                      static_cast<unsigned>(release.release_table_oid),
                      released.value() ? "storage rebuilt without it" : "storage never carried it",
                      commit_id_);
            }
        }

        // Commit-time physical COLUMN rename: pg_attribute carries the new name once committed, but storage
        // indexes columns BY NAME (append/drop_storage_column need it), so the storage copy must be renamed
        // too or the next INSERT expands its chunk against a stale name. NOT what keeps the column alive across
        // a restart — bootstrap reconciles storage against pg_attribute by ATTOID
        // (rearm_dropped_column_blocks_sync), not by name, so a lost rename here is repaired at next boot, not
        // destructive. Reply is checked: a committed rename over a storage refusal is not a success.
        if (commit_id_ > 0 && !column_renames.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (const auto& rename : column_renames) {
                auto [_rn, rnf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::rename_storage_column,
                                                              ctx->session,
                                                              rename.release_table_oid,
                                                              rename.release_attname,
                                                              rename.rename_to_attname);
                auto renamed = co_await std::move(rnf);
                if (renamed.has_error()) {
                    set_error(renamed.error());
                    co_return;
                }
                trace(log_,
                      "operator_commit_transaction: renamed column '{}' -> '{}' of oid {} — {} (commit_id {})",
                      rename.release_attname,
                      rename.rename_to_attname,
                      static_cast<unsigned>(rename.release_table_oid),
                      renamed.value() ? "storage schema updated" : "storage never carried it",
                      commit_id_);
            }
        }

        // MVCC-compact fan-out: nudge the disk manager to reclaim dead row versions now the commit is
        // published (compact_watermark gates it, see data_table_t::compact()). Gated on
        // !base_delete_table_oids.empty(): dead = total - committed-live, and an append-only commit produces
        // zero dead rows, so with no deletes the whole fan-out is provably a no-op.
        if (ctx->disk_address != actor_zeta::address_t::empty_address() && commit_id_ > 0 &&
            !base_delete_table_oids.empty()) {
            // Compact set = appends ∪ deletes. Both masters are sorted+unique
            // pmr-vectors; merge them, dropping the duplicates that appear in both.
            std::pmr::vector<components::catalog::oid_t> compact_oids{resource_};
            compact_oids.reserve(base_append_oids.size() + base_delete_table_oids.size());
            std::set_union(base_append_oids.begin(),
                           base_append_oids.end(),
                           base_delete_table_oids.begin(),
                           base_delete_table_oids.end(),
                           std::back_inserter(compact_oids));
            // Index gate: compact() shifts row positions, but in-memory index engines hold POSITIONAL row
            // refs, so compacting an indexed table would break index_scan. Filter to tables with no index engine.
            std::pmr::vector<components::catalog::oid_t> safe_oids{resource_};
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_ti, tif] = actor_zeta::otterbrix::send(ctx->index_address,
                                                              &services::index::manager_index_t::tables_without_indexes,
                                                              ctx->session,
                                                              std::move(compact_oids));
                safe_oids = co_await std::move(tif);
            } else {
                safe_oids = std::move(compact_oids);
            }
            // Single batched message: the disk manager fans the per-table compact
            // out internally.
            if (!safe_oids.empty()) {
                auto [_mc, mcf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::maybe_cleanup_many,
                                                components::execution_context_t{ctx->session,
                                                                     txn_data,
                                                                     ctx->execution_context.timezone_offset,
                                                                     components::catalog::INVALID_OID},
                                                std::move(safe_oids),
                                                compact_watermark);
                co_await std::move(mcf);
            }
        }

        // No row output — like operator_checkpoint_t, success surfaces via the
        // operator's executed state; the commit_id rides back to the executor
        // tail through ctx->committed_id (written right after the drain).
        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
