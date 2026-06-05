#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <set>

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
        // branch). Patched after commit_id_ is allocated below. pmr-backed so it
        // moves straight into the batched update_pg_attribute_commit_id_fields.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> swap_backfills{resource_};
        // Explicit-txn base-table DML ranges parked by the executor commit phase.
        // Batched into storage_publish_* alongside the pg_catalog ranges, all
        // BEFORE the ProcArray publish() barrier so readers see an atomic flip.
        std::vector<components::pg_catalog_append_range_t> base_appends;
        std::set<components::catalog::oid_t> base_delete_tables;
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
            // drain carries a std::vector; copy into the pmr-backed local.
            swap_backfills.assign(drain.swap_backfills.begin(), drain.swap_backfills.end());
            base_appends = std::move(drain.base_appends);
            base_delete_tables = std::move(drain.base_delete_tables);
            commit_id_ = drain.commit_id;
        }

        // Commit back-channel: surface the just-allocated commit_id to the
        // executor tail (e.g. inline CREATE INDEX commit) via the pipeline ctx.
        ctx->committed_id = commit_id_;

        // DROP-GC value-space remap. DDL that drops a storage/index registers a
        // tombstone keyed by transaction_id at DROP time; the horizon-advance GC
        // compares against commit_id, so the tombstone must be remapped from
        // txn-id space into commit-id space once the real commit_id is known.
        // Fire-and-forget no-op message pair, kept cheap so even zero-entry DDLs
        // pay only two trivial sends. Placed right after the drain so commit_id_
        // is final.
        if (is_ddl_commit_ && txn_data.transaction_id != 0 && commit_id_ > 0) {
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
        if (!swap_backfills.empty() && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t backfill_ctx{ctx->session, txn_data, {}};
            // Log the marker count before the move empties the vector.
            const auto backfill_count = swap_backfills.size();
            auto [_b, bf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::update_pg_attribute_commit_id_fields,
                                             backfill_ctx,
                                             std::move(swap_backfills),
                                             commit_id_);
            co_await std::move(bf);
            trace(log_,
                  "operator_commit_transaction: OPTION X drained {} pg_attribute backfill markers "
                  "for txn {} commit_id {} (patched in-place)",
                  backfill_count,
                  txn_data.transaction_id,
                  commit_id_);
        }

        // Capture the UNIQUE base-table oids touched by appends / deletes BEFORE
        // the storage_publish_* block moves base_appends / base_delete_tables
        // out. These drive the per-table index commits and the MVCC-compact
        // fan-out further down, both of which run after the storage publishes.
        // pmr-backed, resource from the operator (resource_).
        std::pmr::set<components::catalog::oid_t> base_append_oids{resource_};
        for (const auto& r : base_appends) {
            base_append_oids.insert(r.table_oid);
        }
        std::pmr::set<components::catalog::oid_t> base_delete_table_oids{base_delete_tables.begin(),
                                                                         base_delete_tables.end(),
                                                                         resource_};

        // Flip MVCC state on the pg_catalog rows AND the base-table DML ranges
        // drained above: one publish_commits / publish_deletes per category
        // covers every table touched between BEGIN and COMMIT.
        if (txn_data.transaction_id != 0 && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t swap_ctx{ctx->session, txn_data, {}};
            if (!swap_appends.empty()) {
                auto [_a, af] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_publish_commits,
                                                 swap_ctx,
                                                 commit_id_,
                                                 std::move(swap_appends));
                co_await std::move(af);
            }
            if (!swap_deletes.empty()) {
                auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_publish_deletes,
                                                 swap_ctx,
                                                 commit_id_,
                                                 std::move(swap_deletes));
                co_await std::move(df);
            }
            if (!base_appends.empty()) {
                auto [_ba, baf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_publish_commits,
                                                   swap_ctx,
                                                   commit_id_,
                                                   std::move(base_appends));
                co_await std::move(baf);
            }
            if (!base_delete_tables.empty()) {
                auto [_bd, bdf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_publish_deletes,
                                                   swap_ctx,
                                                   commit_id_,
                                                   std::move(base_delete_tables));
                co_await std::move(bdf);
            }
        }

        // Per-table index commits. base_appends / base_delete_tables were
        // consumed (moved) by storage_publish_* above, so we drive these from the
        // base_append_oids / base_delete_table_oids unique-oid sets captured just
        // BEFORE the storage publish block. One batched commit_inserts /
        // commit_deletes flips every touched table's index entries from PENDING to
        // the real commit_id. Sets are already sorted+unique, so a flat
        // pmr-vector copy preserves that.
        if (ctx->index_address != actor_zeta::address_t::empty_address() &&
            txn_data.transaction_id != 0 && commit_id_ > 0) {
            std::pmr::vector<components::catalog::oid_t> append_oids{base_append_oids.begin(),
                                                                    base_append_oids.end(),
                                                                    resource_};
            std::pmr::vector<components::catalog::oid_t> delete_oids{base_delete_table_oids.begin(),
                                                                     base_delete_table_oids.end(),
                                                                     resource_};
            if (!append_oids.empty()) {
                auto [_ic, icf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::commit_inserts,
                                                   components::execution_context_t{ctx->session, txn_data, ctx->session_tz},
                                                   std::move(append_oids),
                                                   commit_id_);
                core::error_t result = co_await std::move(icf);
                if (result.contains_error()) {
                    // Latent until index commits stop being assert+abort terminal:
                    // commit_inserts currently only ever returns no_error(), so this
                    // branch is unreachable today. Once the bitcask write path can
                    // surface a real error, returning WITHOUT mark_executed makes
                    // execute_sub_plan_ surface an error cursor.
                    set_error(std::move(result));
                    co_return;
                }
            }
            if (!delete_oids.empty()) {
                auto [_dc, dcf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::commit_deletes,
                                                   components::execution_context_t{ctx->session, txn_data, ctx->session_tz},
                                                   std::move(delete_oids),
                                                   commit_id_);
                core::error_t result = co_await std::move(dcf);
                if (result.contains_error()) {
                    // Latent until index commits stop being assert+abort terminal
                    // (see commit_inserts note above).
                    set_error(std::move(result));
                    co_return;
                }
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
        // the compact gate (lowest active snapshot start_time) used below.
        uint64_t compact_gate = 0;
        if (commit_id_ > 0 && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_p, pf] = actor_zeta::send(ctx->current_message_sender,
                                             &services::dispatcher::manager_dispatcher_t::txn_publish_msg,
                                             commit_id_);
            compact_gate = co_await std::move(pf);
        }

        // MVCC-compact fan-out. For every UNIQUE base-table oid touched by this
        // txn (appends ∪ deletes), nudge the disk manager to compact dead row
        // versions now that the commit is published. compact_gate == 0 means
        // active txns still exist → agents skip the compact; it is a best-effort
        // tombstone-timing heuristic, compact CORRECTNESS rests on agent-mailbox
        // serialization, not on this gate.
        if (ctx->disk_address != actor_zeta::address_t::empty_address() && commit_id_ > 0) {
            std::pmr::vector<components::catalog::oid_t> compact_oids{base_append_oids.begin(),
                                                                      base_append_oids.end(),
                                                                      resource_};
            for (const auto& oid : base_delete_table_oids) {
                if (base_append_oids.find(oid) == base_append_oids.end()) {
                    compact_oids.push_back(oid);
                }
            }
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
                auto [_mc, mcf] = actor_zeta::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::maybe_cleanup_many,
                    components::execution_context_t{ctx->session, txn_data, ctx->session_tz, components::catalog::INVALID_OID},
                    std::move(safe_oids),
                    compact_gate);
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
