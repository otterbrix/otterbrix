#include "operator_create_index_backfill.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <components/index/index_engine.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>

namespace components::operators {

    operator_create_index_backfill_t::operator_create_index_backfill_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string index_name,
        components::logical_plan::index_type index_type,
        std::pmr::vector<components::expressions::key_t> keys,
        components::catalog::oid_t table_oid,
        components::catalog::oid_t index_oid,
        std::string indkey)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , index_name_(std::move(index_name))
        , index_type_(index_type)
        , keys_(std::move(keys))
        , table_oid_(table_oid)
        , index_oid_(index_oid)
        , indkey_(std::move(indkey)) {}

    void operator_create_index_backfill_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_create_index_backfill_t::await_async_and_resume(pipeline::context_t* ctx) {
        // No-op when there is no index actor wired (e.g. some test harnesses).
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }

        // + 2: ensure the engine knows about the collection, then create
        // the index entry. register_collection is idempotent.
        auto [_rc, rcf] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::register_collection,
                                           ctx->session,
                                           table_oid_);
        co_await std::move(rcf);

        auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::create_index,
                                           ctx->session,
                                           table_oid_,
                                           services::index::index_name_t(index_name_),
                                           keys_,
                                           index_type_,
                                           ctx->session_tz);
        const auto id_index = co_await std::move(ixf);

        if (id_index == components::index::INDEX_ID_UNDEFINED) {
            set_error(core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"index already exists", resource_}});
            co_return;
        }

        // WAL retention guard. Capture build_start_wal_position
        // (current wal_id) and register it with manager_wal_replicate so a
        // concurrent checkpoint+truncate cannot drop records the Phase 2.5
        // catchup loop still needs. We route through the mailbox (rule 11: the
        // operator runs inside the executor actor, sync inter-actor calls are
        // forbidden). The matching unregister fires at every exit point below
        // (success after catchup, fail when catchup doesn't converge).
        // build_start_registered_ tracks whether we still owe an unregister;
        // it's only set after a successful register so accidental double-unreg
        // is impossible. No RAII helper is used: a coroutine-local guard would
        // need to capture `co_await`-able state, which the destructor cannot
        // run — explicit unregister at each exit is the cleanest pattern.
        services::wal::id_t build_start_wal_position{0};
        bool build_start_registered = false;
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_q, qf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::current_wal_id,
                                             ctx->session);
            build_start_wal_position = co_await std::move(qf);
            auto [_r, rf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::register_active_build,
                                             ctx->session,
                                             build_start_wal_position);
            co_await std::move(rf);
            build_start_registered = true;
        }

        // backfill — scan table contents and feed them into the index.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_tr, trf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::storage_total_rows,
                                               ctx->session,
                                               table_oid_);
            const auto total_rows = co_await std::move(trf);
            if (total_rows > 0) {
                auto [_ss, ssf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_scan_segment,
                                                   ctx->session,
                                                   table_oid_,
                                                   int64_t{0},
                                                   total_rows);
                auto scan_data = co_await std::move(ssf);
                if (scan_data) {
                    const auto count = scan_data->size();
                    auto [_ir, irf] = actor_zeta::send(
                        ctx->index_address,
                        &services::index::manager_index_t::insert_rows,
                        services::index::execution_context_t{ctx->session, ctx->txn, ctx->session_tz, table_oid_},
                        table_oid_,
                        std::move(scan_data),
                        uint64_t{0},
                        count);
                    co_await std::move(irf);
                    // Group 3 fix: insert_rows tags index entries with the
                    // current txn_id but leaves them PENDING. They become
                    // visible only after commit_insert is called with the
                    // commit_id. The executor's post-pipeline commit block
                    // (executor.cpp:215-232) fires commit_insert when
                    // dml_append_row_count > 0 && dml_table_oid set — record
                    // those here so backfilled index entries get committed
                    // alongside the CREATE INDEX txn. Reusing
                    // dml_append_row_count also drives storage_publish_commit
                    // (executor.cpp:217-224): that re-commits the already-
                    // committed data rows to the CREATE INDEX commit_id, which
                    // is harmless when no concurrent reader sits between the
                    // INSERT and the CREATE INDEX commits (typical case).
                    // Phase 2.5 now handles this via catchup loop below; see TODO for WAL-based path.
                    ctx->dml_append_row_start = 0;
                    ctx->dml_append_row_count = count;
                    ctx->dml_table_oid = table_oid_;
                }
            }
        }

        // c CREATE INDEX Phase 2.5 bounded-retry catchup.
        // Snapshot scan (Phase 2) may have missed rows committed concurrently with
        // the build. We scan WAL records produced after build_start_wal_position
        // (captured at Phase 2 start, retention-guarded by WAL) and re-apply
        // every PHYSICAL_{INSERT,DELETE,UPDATE} targeting this build's table_oid
        // to the in-memory index. Bounded retry guards against pathological
        // high-write workloads that never quiesce; each iteration advances
        // catchup_start_wal to the max wal_id seen, so the loop terminates as
        // soon as load() reports no new records past the watermark.
        //
        // V1.d engine apply (full): INSERT, DELETE, and UPDATE are all wired.
        //   - PHYSICAL_INSERT — forwarded with rec.physical_data directly.
        //   - PHYSICAL_DELETE — WAL records carry only row_ids, so we
        //     recover the key chunk via storage_fetch(row_ids) on the
        //     operator side, then forward the recovered chunk as
        //     physical_data to apply_wal_record_for_index. Best-effort:
        //     if storage_fetch returns null/empty (rows physically gone)
        //     we forward nullptr; manager_index logs+skips and the V1.a
        //     bounded-retry convergence guard catches persistent divergence.
        //   - PHYSICAL_UPDATE — split into TWO messages: the original
        //     PHYSICAL_UPDATE message (NEW-insert half) followed by a
        //     synthesized PHYSICAL_DELETE message carrying the recovered
        //     OLD chunk (OLD-delete half). Same fetch+forward pattern.
        constexpr int MAX_CATCHUP_ITERATIONS = 10;
        services::wal::id_t catchup_start_wal = build_start_wal_position;
        bool converged = false;
        for (int i = 0; i < MAX_CATCHUP_ITERATIONS; ++i) {
            // No WAL configured (test harness): nothing to replay, converge.
            if (ctx->wal_address == actor_zeta::address_t::empty_address()) {
                converged = true;
                break;
            }

            auto [_load, lf] = actor_zeta::send(ctx->wal_address,
                                                &services::wal::manager_wal_replicate_t::load,
                                                ctx->session,
                                                catchup_start_wal);
            auto wal_records = co_await std::move(lf);

            if (wal_records.empty()) {
                converged = true;
                break;
            }

            services::wal::id_t max_wal_id_seen = catchup_start_wal;
            // V1.d real-impl: non-const iteration so we can move
            // rec.physical_data (unique_ptr<data_chunk_t>) into the
            // apply_wal_record_for_index message. The chunk and
            // physical_row_start are required by the engine's insert_row API
            // (see services/index/manager_index.cpp). Replayed entries are
            // tagged with the CREATE INDEX txn_id so they stay PENDING until
            // the post-pipeline commit_insert publishes them alongside the
            // Phase 2 snapshot rows.
            //
            // PHYSICAL_DELETE / PHYSICAL_UPDATE handling: the WAL record
            // ships only row_ids for the deleted rows, but the engine's
            // mark_delete_row API needs the original key columns to locate
            // the bucket. We close that gap with a storage_fetch(row_ids)
            // round-trip on the operator side (read-only, MVCC-aware) and
            // forward the recovered chunk as physical_data. The cost is
            // O(deleted_rows) physical reads per catchup iteration —
            // acceptable for the bounded-retry loop. For PHYSICAL_UPDATE
            // we issue TWO messages: the original (NEW-insert half) plus a
            // synthesized PHYSICAL_DELETE (OLD-delete half).
            for (auto& rec : wal_records) {
                if (rec.id > max_wal_id_seen) {
                    max_wal_id_seen = rec.id;
                }
                if (!rec.is_valid()) {
                    continue;
                }
                if (rec.table_oid != table_oid_) {
                    continue;
                }
                if (rec.record_type != services::wal::wal_record_type::PHYSICAL_INSERT &&
                    rec.record_type != services::wal::wal_record_type::PHYSICAL_DELETE &&
                    rec.record_type != services::wal::wal_record_type::PHYSICAL_UPDATE) {
                    continue;
                }

                // For DELETE / UPDATE: recover the OLD key chunk by fetching
                // row_ids from storage. If the fetch can't run (no disk
                // address in test harness) or returns null/empty (rows
                // physically gone), we forward nullptr — the manager_index
                // handler logs+skips and the V1.a convergence guard catches
                // persistent divergence on the next iteration.
                std::unique_ptr<components::vector::data_chunk_t> old_chunk;
                const bool needs_old_chunk =
                    (rec.record_type == services::wal::wal_record_type::PHYSICAL_DELETE ||
                     rec.record_type == services::wal::wal_record_type::PHYSICAL_UPDATE) &&
                    !rec.physical_row_ids.empty() &&
                    ctx->disk_address != actor_zeta::address_t::empty_address();
                if (needs_old_chunk) {
                    components::vector::vector_t fetch_ids(resource_,
                                                           components::types::logical_type::BIGINT,
                                                           rec.physical_row_ids.size());
                    for (std::size_t k = 0; k < rec.physical_row_ids.size(); ++k) {
                        fetch_ids.data<int64_t>()[k] = rec.physical_row_ids[k];
                    }
                    auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::storage_fetch,
                                                     ctx->session,
                                                     rec.table_oid,
                                                     std::move(fetch_ids),
                                                     static_cast<uint64_t>(rec.physical_row_ids.size()));
                    old_chunk = co_await std::move(ff);
                }

                if (rec.record_type == services::wal::wal_record_type::PHYSICAL_INSERT ||
                    rec.record_type == services::wal::wal_record_type::PHYSICAL_UPDATE) {
                    // INSERT or NEW-insert half of UPDATE: forward the
                    // WAL-resident NEW chunk and the OLD row_ids (UPDATE
                    // ignores row_ids on the insert path).
                    std::pmr::vector<int64_t> row_ids(rec.physical_row_ids.begin(),
                                                     rec.physical_row_ids.end(),
                                                     resource_);
                    auto [_a, af] =
                        actor_zeta::send(ctx->index_address,
                                         &services::index::manager_index_t::apply_wal_record_for_index,
                                         ctx->session,
                                         rec.table_oid,
                                         index_oid_,
                                         rec.id,
                                         static_cast<uint8_t>(rec.record_type),
                                         std::move(row_ids),
                                         std::move(rec.physical_data),
                                         rec.physical_row_start,
                                         ctx->txn.transaction_id,
                                         rec.session_tz);
                    co_await std::move(af);
                }

                if (rec.record_type == services::wal::wal_record_type::PHYSICAL_DELETE ||
                    rec.record_type == services::wal::wal_record_type::PHYSICAL_UPDATE) {
                    // DELETE or OLD-delete half of UPDATE: send a
                    // PHYSICAL_DELETE message carrying the recovered OLD
                    // chunk + row_ids. For PHYSICAL_UPDATE we synthesize
                    // the record_type as PHYSICAL_DELETE so the handler
                    // routes through its mark_delete_row branch.
                    std::pmr::vector<int64_t> row_ids(rec.physical_row_ids.begin(),
                                                     rec.physical_row_ids.end(),
                                                     resource_);
                    auto [_a, af] = actor_zeta::send(
                        ctx->index_address,
                        &services::index::manager_index_t::apply_wal_record_for_index,
                        ctx->session,
                        rec.table_oid,
                        index_oid_,
                        rec.id,
                        static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE),
                        std::move(row_ids),
                        std::move(old_chunk),
                        rec.physical_row_start,
                        ctx->txn.transaction_id,
                        rec.session_tz);
                    co_await std::move(af);
                }
            }

            // Forward progress check: if no record in this batch carried a
            // wal_id strictly greater than the watermark, we have caught up.
            // This also guards against load() returning records at-or-below
            // the watermark (defensive — current contract is strictly-after).
            if (max_wal_id_seen == catchup_start_wal) {
                converged = true;
                break;
            }
            catchup_start_wal = max_wal_id_seen;
        }
        if (!converged) {
            // a graceful-fail path: emit a typed error
            // cursor via the operator's existing set_error helper and co_return.
            // +V1 expedited cleanup runs because ready_since=0 — index
            // never published, no snapshot ever referenced it, safe to GC.
            // release the WAL retention guard before exiting
            // so the next checkpoint can truncate freely.
            if (build_start_registered) {
                auto [_u, uf] = actor_zeta::send(ctx->wal_address,
                                                 &services::wal::manager_wal_replicate_t::unregister_active_build,
                                                 ctx->session,
                                                 build_start_wal_position);
                co_await std::move(uf);
                build_start_registered = false;
            }
            set_error(core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{
                    "CREATE INDEX failed to converge after MAX_CATCHUP_ITERATIONS "
                    "on high-write table. Retry during low-traffic window. "
                    "Future: CREATE INDEX CONCURRENTLY (WAL-based).",
                    resource_}});
            co_return;
        }

        // catchup converged — release retention guard. We do
        // this BEFORE the pg_index flip / mark_executed so a later truncate
        // after this operator returns is unblocked. The flip step below only
        // touches the catalog, not the WAL records we were protecting.
        if (build_start_registered) {
            auto [_u, uf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::unregister_active_build,
                                             ctx->session,
                                             build_start_wal_position);
            co_await std::move(uf);
            build_start_registered = false;
        }

        // flip pg_index.indisvalid → true. The metadata operator wrote
        // an indisvalid=false row earlier; replace it now that the engine is
        // populated. Skipping is safe if we have no disk actor (test harness).
        if (ctx->disk_address != actor_zeta::address_t::empty_address() &&
            index_oid_ != components::catalog::INVALID_OID) {
            constexpr components::catalog::oid_t pg_idx_oid = components::catalog::well_known_oid::pg_index_table;
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                             exec_ctx,
                                             pg_idx_oid,
                                             std::int64_t{0},
                                             index_oid_);
            co_await std::move(df);
            if (ctx->txn.transaction_id != 0)
                ctx->pg_catalog_delete_tables.insert(pg_idx_oid);

            auto valid_row = components::catalog::build_pg_index_row(resource(),
                                                                     index_oid_,
                                                                     table_oid_,
                                                                     indkey_,
                                                                     /*indisvalid=*/true);
            auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             pg_idx_oid,
                                             std::move(valid_row));
            auto rng = co_await std::move(wf);
            if (rng.count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        mark_executed();
    }

} // namespace components::operators
