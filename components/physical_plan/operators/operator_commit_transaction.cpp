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
                // FULL means "this marker is on the device". The reply used to be discarded,
                // so a refused append or a failed fsync still let the DDL commit proceed and
                // report success — a durable commit claimed over a page that never landed.
                // Nothing has been published at this point, so failing here is a clean abort.
                if (auto commit_result = co_await std::move(cf); commit_result.has_error()) {
                    set_error(commit_result.error());
                    co_return;
                }
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

        // ===================================================================
        // THE ORDER OF EVERYTHING BELOW IS THE INVARIANT, NOT A CONVENIENCE.
        //
        //   NO STEP THAT CAN FAIL MAY RUN AFTER THE FIRST STEP THAT STAMPS THE
        //   COMMIT_ID.
        //
        // The commit_id is allocated by the drain above (the dispatcher's
        // transaction_manager_t::commit() inserts it into in_flight_commits_) and is
        // removed only by the ProcArray barrier at the bottom. Every co_return in
        // between used to leave it in that set with nobody left to take it out — the
        // txn is already gone from active_, so find_transaction() answers nullptr and
        // neither a ROLLBACK nor the dispatcher's failure-release net can reach it. The
        // orphan then floors visible_to_all_locked() at commit_id - 1 for the life of
        // the process, stopping data_table_t::compact(), the DROP-GC tombstone sweep
        // and the deferred index-delete sweep — a queue that is UNBOUNDED BY
        // CONSTRUCTION, because evicting from it is the very defect it exists to
        // prevent. It was documented here as a "KNOWN leak ... accepted".
        //
        // The cure is txn_discard_msg, and what makes THAT safe is this ordering.
        // Erasing the id would publish the commit if anything already carried it, so
        // the two steps that can fail — the index insert-commit and the WAL commit
        // marker — are hoisted ABOVE every step that stamps it:
        //
        //   CAN FAIL   1. commit_inserts        (index; publishes by txn_id, not by cid)
        //   CAN FAIL   2. WAL commit marker     (the single durable commit point)
        //   ---------- the commit is now durable; nothing below may refuse it ---------
        //   stamps     3. DROP-GC tombstone remap        (txn-id space -> commit-id space)
        //   stamps     4. pg_attribute commit_id backfill
        //   stamps     5. commit_deletes                 (queues deferred_delete{.., cid})
        //   stamps     6. storage_publish_commits / _deletes
        //   barrier    7. txn_publish_msg
        //
        // WHY commit_inserts MAY MOVE UP: services/index/manager_index.cpp:1239-1246
        // takes the commit id as an UNNAMED parameter and says so — "the commit id is
        // no longer carried down" — and publishes by ctx.txn.transaction_id. It stamps
        // nothing with the cid, so its position relative to the stamping steps is free.
        //
        // WHY THE WAL MARKER MAY MOVE UP, above the storage publishes: replay
        // (services/wal/wal_reader.cpp:154-181) is a TWO-PASS scan — pass 1 collects
        // the committed transaction ids from every COMMIT marker in the log, pass 2
        // keeps every record whose transaction_id is in that set. Record order relative
        // to the marker is irrelevant, so moving the marker above the in-memory flip
        // (and the pg_attribute physical_update below it) is replay-neutral. It is also
        // strictly safer than the old order: the durable commit point now precedes
        // every reader-visible effect instead of following it.
        //
        // WHY commit_deletes MUST NOT move up with commit_inserts: it queues a
        // deferred_delete_t stamped with the cid, and the horizon sweep later turns
        // that entry into a physical index erase. Left above a marker that then failed,
        // the entry would outlive a transaction that never committed, and the discard
        // would raise the horizon right past it — erasing index entries for rows the
        // table still holds alive. That is the silent-short-answer defect the deferred
        // queue exists to prevent, arrived at from the other side.
        // ===================================================================

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

        // STEP 1 — the per-table index insert-commits, the FIRST of the two steps that
        // can fail and therefore the first thing after the drain. They flip every
        // touched table's index entries from PENDING to committed; on error we discard
        // the commit_id and co_return with NOTHING durable and NOTHING reader-visible:
        // the rows stay txn-pending (insert_id == transaction_id), invisible to every
        // snapshot; the WAL commit marker has not been written, so replay drops the
        // physicals; and the discard below means the allocated id no longer pins the
        // horizon. base_append_oids / base_delete_table_oids are the masters; copy into
        // per-send pmr-vectors so the masters survive for the publish + compact
        // consumers below.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0 && !base_append_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> append_oids{base_append_oids.begin(),
                                                                     base_append_oids.end(),
                                                                     resource_};
            auto [_ic, icf] = actor_zeta::send(
                ctx->index_address,
                &services::index::manager_index_t::commit_inserts,
                components::execution_context_t{ctx->session, txn_data, ctx->execution_context.timezone_offset},
                std::move(append_oids),
                commit_id_);
            core::error_t result = co_await std::move(icf);
            if (result.contains_error()) {
                // Clean abort: the commit_id is stamped NOWHERE (nothing above this
                // point writes it anywhere), so releasing it cannot publish anything.
                if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
                    auto [_dx, dxf] = actor_zeta::send(ctx->current_message_sender,
                                                       &services::dispatcher::manager_dispatcher_t::txn_discard_msg,
                                                       commit_id_);
                    co_await std::move(dxf);
                }
                set_error(std::move(result));
                co_return;
            }
        }

        // STEP 2 — durability. The WAL commit_txn marker is the SINGLE durable commit
        // point and the LAST step that can fail; everything below it is either void or
        // contractually infallible, which is what lets the discard above be a plain
        // erase. It precedes the in-memory MVCC flip AND the ProcArray barrier, so a
        // crash at any point after it replays into the same commit, and a crash before
        // it drops the transaction whole. Skip when the DDL-commit branch at the top
        // already emitted one.
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
            // FULL means "this marker is on the device". Discarding this reply is what let
            // a failed fsync be followed by the barrier anyway: readers would see a commit
            // that a crash one instant later would take back. Refusing HERE is now a clean
            // abort in the full sense — under the old order the storage publishes had
            // already run and the commit_id was already stamped on live row versions, so
            // the exit could only leave the txn unpublished-but-stamped and pin the horizon
            // forever. Nothing has been stamped at this point any more, so the discard is
            // sound and the transaction leaves no trace at all.
            if (auto commit_result = co_await std::move(wf); commit_result.has_error()) {
                if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
                    auto [_dx, dxf] = actor_zeta::send(ctx->current_message_sender,
                                                       &services::dispatcher::manager_dispatcher_t::txn_discard_msg,
                                                       commit_id_);
                    co_await std::move(dxf);
                }
                set_error(commit_result.error());
                co_return;
            }
        }

        // STEP 3 — DROP-GC value-space remap. DDL that drops a storage/index registers a
        // tombstone keyed by transaction_id at DROP time; the horizon-advance GC
        // compares against commit_id, so the tombstone must be remapped from
        // txn-id space into commit-id space once the real commit_id is known.
        // Triggered off the ACTUAL drops carried in the drain
        // (dropped_storage_oids, recorded by operator_dynamic_cascade_delete) —
        // decoupled from is_ddl_commit_, i.e. from which mode lowered the
        // statement: a txn that ran no DROP has an empty vector and pays nothing,
        // and a DROP that arrived through any lowering path remaps correctly.
        // Its ONE ordering constraint is "before the horizon broadcast", and that is
        // still satisfied: it is awaited here and the broadcast happens inside
        // txn_publish_msg at the bottom.
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

        // STEP 4 — patch the placeholder commit_id columns on the ALTER's pg_attribute
        // rows (swap_backfills names the (attoid, kind) pairs). Its real constraint is
        // "before storage_publish_commits", which still holds: the rows still carry
        // insert_id == transaction_id and are invisible to every concurrent snapshot,
        // so this is a metadata-only update nobody else can observe. WAL safety:
        // update_pg_attribute_commit_id_fields emits a physical_update per marker
        // paired with the matching physical_insert, so replay materializes them
        // together — and it now sits BELOW this txn's commit marker, which is
        // replay-neutral because replay gates on transaction_id, not on record order
        // relative to the marker (wal_reader.cpp's two-pass scan).
        //
        // B3c1: a dropped_at marker carries a second, later piece of the same unfinished
        // business — the physical column release. Copy those out HERE, before the move below
        // empties swap_backfills, and perform them far down, after the publish barrier.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> column_releases{resource_};
        for (const auto& b : swap_backfills) {
            if (b.kind == components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at &&
                !b.release_attname.empty() && b.release_table_oid != components::catalog::INVALID_OID) {
                column_releases.push_back(b);
            }
        }
        // The RENAME's storage half, copied out on the same principle and for the same reason:
        // it is unfinished business of an ALTER that is legal only once the commit cannot be
        // taken back. Unlike the DROP's release it patches NO commit_id column — renaming
        // preserves added_at_commit_id — so these markers must also be kept OUT of the batch
        // below: update_pg_attribute_commit_id_field_inner maps kind onto a column index
        // (added_at -> 10, anything else -> 11) and would stamp dropped_at over a LIVE row.
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

        // STEP 5 — the per-table index delete-commits. NOT the mirror of commit_inserts
        // and deliberately not hoisted with it: this handler PUBLISHES NOTHING, it
        // records (table, index, txn, commit) on manager_index_t's deferred_deletes_
        // and hands the physical erase to the horizon sweep. That queued entry carries
        // the commit_id, so it belongs strictly below the durable marker — see the
        // ordering note at the top of this block for what an orphaned entry would cost.
        //
        // The reply is awaited (rule 6) but there is no early exit behind it, and that
        // is the contract talking, not an oversight: manager_index.cpp:1314-1362 has
        // ZERO cross-actor awaits and exactly one `co_return no_error()` — restated at
        // manager_index.hpp:327-332 — because there is no IO here left to fail. Below a
        // durable commit marker an early return would be the wrong answer anyway: the
        // commit is already on the device and replay will re-derive it, so refusing to
        // publish would only make this process disagree with its own journal.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0 && !base_delete_table_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> delete_oids{base_delete_table_oids.begin(),
                                                                     base_delete_table_oids.end(),
                                                                     resource_};
            auto [_dc, dcf] = actor_zeta::send(
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

        // STEP 6 — flip MVCC state on the pg_catalog rows AND the base-table DML ranges
        // drained above: ONE publish_commits + ONE publish_deletes cover every
        // table touched between BEGIN and COMMIT. The swap (pg_catalog) and base
        // (user-table DML) sets are merged into a single send each: the manager's
        // storage_publish_commits / storage_publish_deletes partition their whole
        // argument by pool_idx_for_oid internally and the per-agent inner handlers
        // are idempotent for not-owned oids, so concatenation is value-correct and
        // order-independent within one call (4 awaited sends → 2).
        // Both are void: nothing here can refuse, which is why this is the step that
        // may stamp the commit_id on live row versions. It runs AFTER the WAL commit
        // marker now, so the in-memory flip can no longer outlive a commit the journal
        // never accepted; durability of the flip still comes from the WAL physical
        // records plus checkpoint, and replay re-derives it from the marker.
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

        // Commit-time physical COLUMN drop (B3c1) — the DROP TABLE block above, one level
        // down. operator_alter_column_drop_t only MARKED the drop: it wrote the pg_attribute
        // tombstone and named the column here, because the storage-side drop is a rebuild
        // that destroys the object knowing the column's blocks and therefore cannot be
        // undone. It is legal exactly once the tombstone can no longer be taken back — after
        // the WAL commit marker and the publish barrier above — which is the same instant the
        // block above uses for a committed DROP TABLE, and for the same reason.
        //
        // What a crash leaves, at each window: before the marker, replay drops the txn and
        // the column is untouched; between the marker and here, the tombstone is durable and
        // the column is still physically present — B3c's SAFE, resumable state (the catalog
        // hides it, the space leaks until something re-derives the drop); after the drop but
        // before the table's next checkpoint, still that same state, because the rebuild only
        // NAMES the blocks in memory and the durable root is unchanged; after that checkpoint,
        // both halves are durable. No window has the physical drop durable without the
        // tombstone, which is the one ordering that could lose a column.
        //
        // Rule 6: the reply is checked. A committed tombstone plus a storage that reports it
        // cannot drop the column is not a success — nothing re-derives this drop later.
        if (commit_id_ > 0 && !column_releases.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (const auto& release : column_releases) {
                auto [_rc, rcf] = actor_zeta::send(ctx->disk_address,
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

        // Commit-time physical COLUMN rename — the DROP block above, one statement across.
        // operator_alter_column_rename_t only MARKED it: the pg_attribute row it appended
        // carries insert_id == this txn_id, so a ROLLBACK or a crash before the marker takes
        // the new name back, while renaming the storage column is not undone by either.
        //
        // WHY IT MUST HAPPEN AT ALL: the storage keeps its OWN copy of each column's name and
        // the write path addresses columns by it — the append's column expansion matches chunk
        // aliases to storage names, drop_storage_column takes a name. Leaving that copy on the
        // old name would make the very next INSERT expand its chunk against a name the catalog
        // no longer uses.
        //
        // What it is NOT, any more: the thing that keeps the column alive across a restart.
        // manager_disk_t::rearm_dropped_column_blocks_sync used to reconcile storage columns
        // against pg_attribute BY NAME and read a storage-only name as a DROP, so skipping this
        // send physically deleted a surviving column and its data at the next start. That walk
        // now compares pg_attribute.attoid (RN-oid), which a rename does not move.
        //
        // What a crash leaves, at each window. Before the marker: replay drops the txn, and
        // both halves still carry the OLD name. Between the marker and here: the catalog
        // carries the NEW name and the storage the old one — a state no ordering can avoid,
        // since the storage's durability point (its checkpoint) is later than the catalog's
        // (the WAL marker) whichever way round the two are performed. It is no longer
        // destructive: the bootstrap walk matches the two halves on the attoid and repairs the
        // storage's stale name from the catalog.
        //
        // Rule 6: the reply is checked. A committed new attname over a storage that reports it
        // cannot be renamed is not a success — the live write path would go on using a name the
        // catalog has retired until the next restart repaired it.
        if (commit_id_ > 0 && !column_renames.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (const auto& rename : column_renames) {
                auto [_rn, rnf] = actor_zeta::send(ctx->disk_address,
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
                auto [_mc, mcf] =
                    actor_zeta::send(ctx->disk_address,
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
