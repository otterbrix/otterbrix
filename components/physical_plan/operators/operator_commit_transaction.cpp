#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/disk/manager_disk.hpp>
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
        // Synchronous txn_manager work + an async disk send. Defer the entire
        // body to await_async_and_resume so the disk send participates in the
        // pipeline's await chain.
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

        // Snapshot txn_data AND swap-info BEFORE commit() purges the
        // active map; then commit; then dispatch the batched APIs against
        // the post-swap state.
        auto* tm = ctx->txn_manager;
        auto* txn_t = tm ? tm->find_transaction(ctx->session) : nullptr;

        components::table::transaction_data txn_data =
            txn_t ? txn_t->data() : components::table::transaction_data{0, 0};
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
        if (txn_t) {
            // Explicit txns park per-statement pg_catalog ranges on transaction_t
            // (executor.cpp accumulate_pg_catalog_pending); implicit txns publish
            // inline, leaving these empty so the drain is a no-op.
            txn_t->drain_pg_catalog_pending(swap_appends, swap_deletes);
            swap_backfills = txn_t->drain_pg_attribute_commit_id_backfills();
            auto drained_appends = txn_t->drain_base_appends();
            base_appends.reserve(drained_appends.size());
            for (const auto& r : drained_appends) {
                // Field-name remap: dml_append_range_t (table_oid, row_start,
                // row_count) -> pg_catalog_append_range_t (table_oid, start_row, count).
                base_appends.push_back(components::pg_catalog_append_range_t{
                    r.table_oid, r.row_start, r.row_count});
            }
            auto drained_deletes = txn_t->drain_base_deletes();
            for (const auto& d : drained_deletes) {
                // storage_publish_deletes keys by (ctx.txn.transaction_id, table_oid);
                // every drained range carries the same txn_id (the explicit
                // txn's id), so collapsing to a table set is loss-free.
                base_delete_tables.insert(d.table_oid);
            }
        }

        // commit the transaction in the txn_manager. Synchronous —
        // returns the commit_id used to flip MVCC on pg_catalog appends.
        commit_id_ = tm ? tm->commit(ctx->session) : 0;

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
        // reader can never observe a not-yet-flipped pg_catalog row.
        if (commit_id_ > 0 && tm != nullptr) {
            tm->publish(commit_id_);
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
