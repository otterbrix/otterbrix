#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/wal/manager_wal_replicate.hpp>

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
                // so pass 0; the canonical COMMIT record with the real commit_id
                // is written by the executor commit phase (executor.cpp).
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
        // branch). Patched after commit_id_ is allocated below.
        std::vector<components::pg_attribute_commit_id_backfill_t> swap_backfills;
        // Explicit-txn base-table DML ranges parked by the executor commit phase.
        // Batched into storage_publish_* alongside the pg_catalog ranges, all
        // BEFORE the ProcArray publish() barrier so readers see an atomic flip.
        std::vector<components::pg_catalog_append_range_t> base_appends;
        std::set<components::catalog::oid_t> base_delete_tables;
        // Null-sender guard (mirrors today's `tm ? : 0`): with no dispatcher to
        // talk to there is no txn to drain — leave commit_id_ = 0 and skip.
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
            commit_id_ = drain.commit_id;
        }

        // Patch the placeholder commit_id columns on the ALTER's pg_attribute
        // rows (swap_backfills names the (attoid, kind) pairs). Safe to do here
        // BEFORE storage_publish_commits: the rows still carry insert_id ==
        // transaction_id and are invisible to every concurrent snapshot, so this
        // is a metadata-only update nobody else can observe. WAL safety:
        // update_pg_attribute_commit_id_field emits a physical_update paired with
        // the matching physical_insert, so replay materializes them together.
        if (!swap_backfills.empty() && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t backfill_ctx{ctx->session, txn_data, {}};
            for (const auto& b : swap_backfills) {
                auto [_b, bf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::update_pg_attribute_commit_id_field,
                                                 backfill_ctx,
                                                 b.attoid,
                                                 b.kind,
                                                 commit_id_);
                co_await std::move(bf);
            }
            trace(log_,
                  "operator_commit_transaction: OPTION X drained {} pg_attribute backfill markers "
                  "for txn {} commit_id {} (patched in-place)",
                  swap_backfills.size(),
                  txn_data.transaction_id,
                  commit_id_);
        }

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

        // ProcArray publish barrier: advances published_horizon_ so subsequent
        // snapshots see this txn. MUST run after all storage_publish_* so a
        // reader can never observe a not-yet-flipped pg_catalog row. Routed to
        // the dispatcher (sole txn_manager owner) via txn_publish_msg — the
        // drain handler deliberately left this barrier un-advanced.
        if (commit_id_ > 0 && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_p, pf] = actor_zeta::send(ctx->current_message_sender,
                                             &services::dispatcher::manager_dispatcher_t::txn_publish_msg,
                                             commit_id_);
            co_await std::move(pf);
        }

        // Durability: emit the WAL commit_txn marker. Skip when the DDL-commit
        // branch above already emitted one.
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

        // No row output — like operator_checkpoint_t, the manager-level driver
        // surfaces success via the operator's executed state and reads
        // commit_id() via the typed accessor.
        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
