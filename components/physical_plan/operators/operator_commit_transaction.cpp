#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    // explicit BEGIN..COMMIT — abort-on-statement-error semantics:
    //
    // When any DML statement INSIDE an explicit txn fails (cursor returns error),
    // the executor's abort path runs storage_revert_append + revert_insert +
    // pg_catalog revert + txn_manager_->abort(session). This kills the explicit
    // txn early — subsequent statements in the same BEGIN..COMMIT block return
    // "transaction aborted" errors at parse/dispatch time.
    //
    // Accumulated pending_base_appends_ from PRIOR successful statements (before
    // the failure) remain on disk:
    //   - row insert_id == txn_id (>= TRANSACTION_ID_START)
    //   - visibility filter rejects: id >= TRANSACTION_ID_START → invisible
    //   - eventually GC'd by VACUUM / cleanup_versions / lowest_active_start_time
    //     advance
    //
    // This matches Postgres semantics (the entire txn is rolled back observably,
    // residual dead rows are VACUUM-eligible). No fallback: the executor's
    // abort path handles cleanup explicitly via revert_append +
    // revert_insert — no silent "leave data in place" hack.
    //
    // ROLLBACK (TRANS_STMT_ROLLBACK) goes through operator_abort_transaction_t
    // which has its own cleanup path; this operator only handles COMMIT.

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
                // commit_id isn't allocated yet — the DDL-commit prefix writes
                // the WAL marker BEFORE mgr.commit() returns it. Pass 0 here;
                // the canonical COMMIT record carrying
                // the real commit_id is written by the executor commit-phase
                // path (services/collection/executor.cpp).
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
        // Central accumulation: drain the explicit-txn
        // base-table DML ranges parked by the executor commit phase. These
        // are batched into storage_publish_* alongside the pg_catalog ranges
        // BEFORE the ProcArray publish() barrier so the flip is atomic from
        // any concurrent reader's point of view — publish() runs only after
        // every storage_publish_* completes.
        std::vector<components::pg_catalog_append_range_t> base_appends;
        std::set<components::catalog::oid_t> base_delete_tables;
        if (txn_t) {
            // EXTENSION — pg_catalog accumulation API.
            // drain_pg_catalog_pending wraps the move-out + clear of the
            // pg_catalog_appends / pg_catalog_delete_tables fields. The
            // explicit-txn branch in services/collection/executor.cpp parks
            // per-statement pg_catalog ranges via accumulate_pg_catalog_pending;
            // implicit txns publish inline and leave these fields empty so
            // the drain is a cheap no-op.
            txn_t->drain_pg_catalog_pending(swap_appends, swap_deletes);
            swap_backfills = txn_t->drain_pg_attribute_commit_id_backfills();
            auto drained_appends = txn_t->drain_base_appends();
            base_appends.reserve(drained_appends.size());
            for (const auto& r : drained_appends) {
                // Field-name remap only: pg_catalog_append_range_t uses
                // (table_oid, start_row, count); table::dml_append_range_t
                // uses (table_oid, row_start, row_count). Identical layout
                // in spirit, so the swap API consumes them uniformly.
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

        // operator_alter_column_{add,drop,rename} stamp pg_attribute rows
        // with added_at_commit_id = dropped_at_commit_id = 0 at execute time
        // (commit_id is not allocated until tm->commit(ctx->session) on the
        // line above). The markers in swap_backfills name the (attoid, kind)
        // pairs that need patching with the freshly-allocated commit_id_.
        //
        // Patch is safe to issue here BEFORE storage_publish_commits below:
        // the rows still carry insert_id == txn_data.transaction_id and are
        // invisible to every concurrent reader's snapshot, so writing the
        // commit_id columns is a metadata-only update on rows nobody else
        // can observe. WAL safety: update_pg_attribute_commit_id_field emits
        // a physical_update record paired with the matching physical_insert
        // (txn-local ordering); on replay the update materializes alongside
        // the row.
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

        // flip MVCC state on pg_catalog rows appended/deleted under this
        // explicit transaction. Uses the batched APIs that consume the
        // per-txn swap-info aggregated by the dispatcher. The same batched APIs
        // also flush the explicit-txn base-table DML
        // ranges drained above — one publish_commits / publish_deletes call
        // per category covers all tables touched between BEGIN and COMMIT.
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

        // ProcArray publish barrier. Moves the
        // commit_id out of in_flight_commits_ and advances published_horizon_,
        // so subsequent snapshots see this txn as visible. Must run after
        // storage_publish_commits / storage_publish_deletes so reading any
        // not-yet-flipped pg_catalog rows is impossible.
        if (commit_id_ > 0 && tm != nullptr) {
            tm->publish(commit_id_);
        }

        // explicit BEGIN..COMMIT durability: emit WAL commit_txn marker
        // for the explicit txn. DDL-commit branch already emits at lines 32-44;
        // skip if that path ran.
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
