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

    void operator_commit_transaction_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // A dispatcher round-trip (txn_commit_drain_msg) plus async disk/WAL
        // sends. Defer the entire body to await_async_and_resume so every send
        // participates in the pipeline's await chain.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_commit_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // In DDL-commit mode, prepend the durability barrier + WAL commit record.
        if (is_ddl_commit_) {
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::flush,
                                                 ctx->session,
                                                 services::wal::id_t{0});
                co_await std::move(ff);
            }
            if (ctx->wal_address != actor_zeta::address_t::empty_address() && txn_id_ != 0) {
                // commit_id isn't allocated yet (this prefix runs before commit()),
                // so pass 0. In DDL-commit mode this cid=0 marker is the ONLY WAL
                // commit record: replay gating keys off transaction_id, which the
                // marker carries, so no real-cid DDL record is needed afterwards.
                // The commit_id on the marker only feeds the replay-horizon
                // max-scan, which a 0 here simply does not advance.
                // This cid=0 record is replay-safe BECAUSE replay decides
                // visibility by transaction_id, not by the recorded cid. The
                // invariant is therefore a constraint on any FUTURE replay change:
                // if replay ever starts gating on the marker's cid, this 0 would
                // silently hide the DDL — such a change must first stop emitting
                // cid=0 here.
                auto [_c, cf] = actor_zeta::send(ctx->wal_address,
                                                 &services::wal::manager_wal_replicate_t::commit_txn,
                                                 ctx->session,
                                                 txn_id_,
                                                 services::wal::wal_sync_mode::FULL,
                                                 database_oid_,
                                                 uint64_t{0});
                co_await std::move(cf);
            }
        }

        // Snapshot txn_data, drain all swap-info and allocate the commit_id in a
        // single dispatcher round-trip. The dispatcher (sole owner of
        // transaction_manager_t) does find_transaction → drain_* → remap →
        // commit(), all on its own loop thread, and returns everything by value
        // because after commit() purges the active map the txn_t is unreadable.
        // The drained struct fields arrive in exactly the shapes the publish
        // block below consumes: base appends are pre-remapped to
        // pg_catalog_append_range_t, base deletes pre-collapsed to a table-oid set.
        // INVARIANT: the handler must NOT call publish() — that is the ProcArray
        // barrier, deferred to txn_publish_msg after storage_publish_* / WAL.
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
            auto [_dr, drf] = actor_zeta::send(ctx->current_message_sender,
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

        // DROP-GC value-space remap. DDL that drops a storage/index registers a
        // tombstone keyed by transaction_id at DROP time; the horizon-advance GC
        // compares against commit_id, so the tombstone must be remapped from
        // txn-id space into commit-id space once the real commit_id is known.
        // Triggered off the ACTUAL drops carried in the drain
        // (dropped_storage_oids, recorded by operator_dynamic_cascade_delete) —
        // decoupled from is_ddl_commit_, i.e. from which mode lowered the
        // statement: a txn that ran no DROP has an empty vector and pays nothing,
        // and a DROP that arrived through any lowering path remaps correctly.
        // Placed right after the drain so commit_id_ is final.
        if (!dropped_storage_oids.empty() && txn_data.transaction_id != 0 && commit_id_ > 0) {
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                auto [_sd, sdf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_dropped_committed,
                                                   ctx->session,
                                                   txn_data.transaction_id,
                                                   commit_id_);
                co_await std::move(sdf);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_td, tdf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::table_dropped_committed,
                                                   ctx->session,
                                                   txn_data.transaction_id,
                                                   commit_id_);
                co_await std::move(tdf);
            }
        }

        // Patch the placeholder commit_id columns on the ALTER's pg_attribute
        // rows (swap_backfills names the (attoid, kind) pairs). Safe to do here
        // BEFORE storage_publish_commits: the rows still carry insert_id ==
        // transaction_id and are invisible to every concurrent snapshot, so this
        // is a metadata-only update nobody else can observe. WAL safety:
        // update_pg_attribute_commit_id_fields emits a physical_update per marker
        // paired with the matching physical_insert, so replay materializes them
        // together.
        if (!swap_backfills.empty() && commit_id_ > 0 && ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t backfill_ctx{ctx->session, txn_data, {}};
            // Log the marker count before the move empties the vector.
            const auto backfill_count = swap_backfills.size();
            // The disk handler takes a pmr vector; materialize one (operator
            // resource_) from the plain-std drain local at the send site.
            std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfill_markers{
                std::make_move_iterator(swap_backfills.begin()),
                std::make_move_iterator(swap_backfills.end()),
                resource_};
            auto [_b, bf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::update_pg_attribute_commit_id_fields,
                                             backfill_ctx,
                                             std::move(backfill_markers),
                                             commit_id_);
            co_await std::move(bf);
            trace(log_,
                  "operator_commit_transaction: OPTION X drained {} pg_attribute backfill markers "
                  "for txn {} commit_id {} (patched in-place)",
                  backfill_count,
                  txn_data.transaction_id,
                  commit_id_);
        }

        // Materialize the UNIQUE base-table oids touched by appends / deletes
        // ONCE, here, before any consumer. base_appends / base_delete_tables are
        // moved out by the storage_publish_* block further down, so these unique
        // sets must be captured first. They serve THREE consumers — the per-table
        // index commits, the storage publishes, and the MVCC-compact fan-out —
        // each of which builds its own per-send pmr-vector copy (the sends move
        // their argument, these masters stay intact). Single dedup pass via a
        // std::pmr::set; resource from the operator (resource_).
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

        // Per-table index commits run BEFORE the storage publishes. Reason:
        // an index-commit IO failure must be able to abort the whole commit with
        // NOTHING reader-visible. commit_inserts / commit_deletes flip every
        // touched table's index entries from PENDING to the real commit_id; on
        // error we set_error + co_return BEFORE any storage_publish_* / WAL marker
        // runs — so the rows stay txn-pending (insert_id == transaction_id),
        // invisible to every snapshot; the WAL commit marker is never written, so
        // replay drops the physicals; only the already-allocated commit_id stays
        // in the dispatcher's in_flight_commits_ unpublished. That is a KNOWN
        // leak: under pure MVCC there is no undo, so an aborted-after-allocation
        // commit_id can never be reclaimed; it is bounded by process lifetime and
        // accepted as the no-undo tradeoff. base_append_oids / base_delete_table_oids
        // are the masters; copy into per-send pmr-vectors so the masters survive
        // for the publish + compact consumers below.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0) {
            if (!base_append_oids.empty()) {
                std::pmr::vector<components::catalog::oid_t> append_oids{base_append_oids.begin(),
                                                                         base_append_oids.end(),
                                                                         resource_};
                auto [_ic, icf] =
                    actor_zeta::send(ctx->index_address,
                                     &services::index::manager_index_t::commit_inserts,
                                     components::execution_context_t{ctx->session, txn_data, ctx->session_tz},
                                     std::move(append_oids),
                                     commit_id_);
                core::error_t result = co_await std::move(icf);
                if (result.contains_error()) {
                    // Clean abort: nothing published yet (this block runs before
                    // storage_publish_* and the WAL marker). Rows stay txn-pending
                    // and invisible; replay drops them; the commit_id leaks in
                    // in_flight_commits_ (no-undo-under-MVCC tradeoff, see above).
                    set_error(std::move(result));
                    co_return;
                }
            }
            if (!base_delete_table_oids.empty()) {
                std::pmr::vector<components::catalog::oid_t> delete_oids{base_delete_table_oids.begin(),
                                                                         base_delete_table_oids.end(),
                                                                         resource_};
                auto [_dc, dcf] =
                    actor_zeta::send(ctx->index_address,
                                     &services::index::manager_index_t::commit_deletes,
                                     components::execution_context_t{ctx->session, txn_data, ctx->session_tz},
                                     std::move(delete_oids),
                                     commit_id_);
                core::error_t result = co_await std::move(dcf);
                if (result.contains_error()) {
                    // Clean abort BEFORE any publish — see commit_inserts note above.
                    set_error(std::move(result));
                    co_return;
                }
            }
        }

        // Flip MVCC state on the pg_catalog rows AND the base-table DML ranges
        // drained above: ONE publish_commits + ONE publish_deletes cover every
        // table touched between BEGIN and COMMIT. The swap (pg_catalog) and base
        // (user-table DML) sets are merged into a single send each: the manager's
        // storage_publish_commits / storage_publish_deletes partition their whole
        // argument by pool_idx_for_oid internally and the per-agent inner handlers
        // are idempotent for not-owned oids, so concatenation is value-correct and
        // order-independent within one call (4 awaited sends → 2).
        // This in-memory MVCC flip happens BEFORE the WAL commit marker, yet
        // is crash-safe — durability of the flip comes from the WAL physical
        // records (already written by the DML operators) plus checkpoint, NOT from
        // this in-memory publish. A crash before the WAL commit marker discards
        // the flip and the txn TOGETHER: replay sees no commit marker, so it never
        // re-publishes these ranges and the rows stay txn-pending (dropped). The
        // marker is the single durable commit point; this flip is purely the
        // in-process reader-visibility step.
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
                auto [_a, af] = actor_zeta::send(ctx->disk_address,
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
                auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_publish_deletes,
                                                 swap_ctx,
                                                 commit_id_,
                                                 std::move(all_deletes));
                co_await std::move(df);
            }
        }

        // Durability: emit the WAL commit_txn marker BEFORE the ProcArray
        // publish barrier. The marker must be durable first so a crash between
        // it and the barrier cannot lose a reader-visible commit. Skip when the
        // DDL-commit branch above already emitted one.
        if (!is_ddl_commit_ && ctx->wal_address != actor_zeta::address_t::empty_address() &&
            txn_data.transaction_id != 0 && commit_id_ > 0) {
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::commit_txn,
                                             ctx->session,
                                             txn_data.transaction_id,
                                             services::wal::wal_sync_mode::FULL,
                                             db_oid,
                                             commit_id_);
            co_await std::move(wf);
        }

        // ProcArray publish barrier: advances published_horizon_ so subsequent
        // snapshots see this txn. MUST be the LAST step of the commit: every
        // storage_publish_*, the index commits and the WAL marker are already
        // done, so a crash before this barrier cannot lose a reader-visible
        // commit (the WAL marker is already durable and replay re-publishes).
        // Routed to the dispatcher (sole txn_manager owner) via txn_publish_msg —
        // the drain handler deliberately left this barrier un-advanced. Returns
        // the compact watermark (visible-to-all commit-id horizon) used below.
        uint64_t compact_watermark = 0;
        if (commit_id_ > 0 && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_p, pf] = actor_zeta::send(ctx->current_message_sender,
                                             &services::dispatcher::manager_dispatcher_t::txn_publish_msg,
                                             commit_id_);
            compact_watermark = co_await std::move(pf);
        }

        // Commit-time physical DROP. operator_dynamic_cascade_delete only
        // MARKED the dropped storages/indexes (tombstones) at plan time and left
        // them physically intact so the DROP stayed revertible until COMMIT and
        // other sessions kept reading the table. Now that the txn is published —
        // the ProcArray barrier above has flipped every reader's snapshot past
        // this commit — physically tear them down. Per drained dropped oid:
        // ALL unregister_collection (manager_index) THEN ONE drop_storage_many
        // (manager_disk), in THAT order so no index consumer references a collection
        // whose backing storage the disk actor is about to free. The two managers are
        // distinct mailboxes, so FIFO gives no cross-mailbox ordering — we batch every
        // unregister and AWAIT THEM ALL before issuing the batched disk drop. This is
        // strictly stronger than the previous per-oid interleave: every index
        // unregister completes BEFORE any disk drop, preserving the index-before-disk
        // invariant globally. unregister_collection runs on the index MANAGER's own
        // maps (not a per-oid router), so the N sends pipeline onto one mailbox;
        // drop_storage_many partitions the oids per disk agent and fans out in
        // parallel, collapsing N per-oid disk round-trips into one. The DROP-GC remap
        // (storage_dropped_committed / table_dropped_committed, above) already stamped
        // the tombstones with commit_id so on_horizon_advanced reclaims any residue;
        // this block does the eager removal of the now-committed drop.
        // Gated on commit_id_ > 0 (mirrors the publish barrier / DROP-GC remap):
        // a DROP makes has_accumulated() true, so a txn with drops always gets a
        // real commit_id — but if commit_id_ is 0 (the empty-COMMIT abort, or a
        // missing txn) nothing committed, so nothing may be physically removed.
        if (commit_id_ > 0 && !dropped_storage_oids.empty()) {
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<actor_zeta::unique_future<void>> unregister_futures{resource_};
                unregister_futures.reserve(dropped_storage_oids.size());
                for (auto oid : dropped_storage_oids) {
                    auto [_u, uf] = actor_zeta::send(ctx->index_address,
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
                auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::drop_storage_many,
                                                 ctx->session,
                                                 std::move(drop_oids));
                co_await std::move(df);
            }
        }

        // MVCC-compact fan-out. For every UNIQUE base-table oid touched by this
        // txn (appends ∪ deletes), nudge the disk manager to compact dead row
        // versions now that the commit is published. compact_watermark is the
        // dispatcher's visible-to-all horizon: data_table_t::compact() refuses
        // the rebuild when any version stamp is above it (another snapshot or an
        // in-flight commit still needs the history), so reclaim is deferred, not
        // forced. Agent-mailbox serialization covers the data-race side.
        //
        // Gated on !base_delete_table_oids.empty(). A commit with deletes is
        // the ONLY way this txn could push a table past the compact's 30%
        // dead-rows threshold. Proof: dead = total − committed-live. An
        // append-only commit adds rows that all commit live (committed appends are
        // visible, never dead) and reverts aborted appends physically — so it
        // produces ZERO dead rows and dead/total can only fall, never rise. Only a
        // committed DELETE turns a previously-live row dead. So when there are no
        // base deletes, no table can newly cross the threshold and the entire
        // fan-out (tables_without_indexes + maybe_cleanup_many) is provably a
        // no-op worth skipping outright.
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
            // Index gate: compact() rebuilds the row_group, shifting row
            // positions — the in-memory index engines hold POSITIONAL row refs,
            // so compacting an indexed table mid-session silently breaks every
            // subsequent index_scan. One batched query returns the subset of
            // compact_oids with NO index engine, which is the safe-to-compact set
            // (index-rebuild-on-compact is a separate task).
            std::pmr::vector<components::catalog::oid_t> safe_oids{resource_};
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_ti, tif] = actor_zeta::send(ctx->index_address,
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
                auto [_mc, mcf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::maybe_cleanup_many,
                                                   components::execution_context_t{ctx->session,
                                                                                   txn_data,
                                                                                   ctx->session_tz,
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
