#include "operator_create_index_backfill.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <components/table/column_state.hpp> // complete table_filter_t for the null-filter batched scan
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

namespace components::operators {

    // DEV_MODE test counter: batches consumed by the streaming backfill scan (proves it streams instead of
    // loading the table whole). Process-global + relaxed — coarse instrumentation, not a sync primitive.
#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_create_index_backfill_batches{0};
    } // namespace
    uint64_t create_index_backfill_batches() noexcept {
        return g_create_index_backfill_batches.load(std::memory_order_relaxed);
    }
#endif

    operator_create_index_backfill_t::operator_create_index_backfill_t(
        std::pmr::memory_resource* resource,
        log_t log,
        components::logical_plan::index_type index_type,
        std::pmr::vector<components::expressions::key_t> keys,
        components::catalog::oid_t table_oid,
        components::catalog::oid_t index_oid,
        std::string indkey)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , index_type_(index_type)
        , keys_(std::move(keys))
        , table_oid_(table_oid)
        , index_oid_(index_oid)
        , indkey_(std::move(indkey)) {}

    actor_zeta::unique_future<void> operator_create_index_backfill_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Refuses instead of the old mark_executed()+silent-success: an unwired index actor means a mis-wired
        // engine (every production topology spawns manager_index_t unconditionally), not a legitimate no-op mode.
        // Pinned by test_wave_exec_dispatcher's create_index_refuses_without_an_index_manager.
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            set_error(core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"CREATE INDEX: no index manager is wired to this executor; "
                                                     "the index was not created",
                                                     resource()}});
            co_return;
        }

        // Ensure the engine knows about the collection, then create the
        // index entry. register_collection is idempotent.
        auto [_rc, rcf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                      &services::index::manager_index_t::register_collection,
                                                      ctx->session,
                                                      table_oid_);
        co_await std::move(rcf);

        auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                      &services::index::manager_index_t::create_index,
                                                      ctx->session,
                                                      table_oid_,
                                                      index_oid_,
                                                      keys_,
                                                      index_type_,
                                                      ctx->execution_context.timezone_offset);
        // create_index answers with a core::error_t only; the index's identity below the planner is
        // index_oid_, already known here.
        auto create_error = co_await std::move(ixf);

        if (create_error.contains_error()) {
            // Report the reason the manager gave: flattening every failure to
            // "index already exists" is right for one cause and wrong for the rest,
            // including a disk index whose storage failed to open.
            set_error(create_error);
            co_return;
        }

        // CREATE back-channel (table oid + indexrelid): COMMIT publishes it, a same-txn ABORT drops it via
        // drained created_index. Gated on non-zero txn id — autocommit/bootstrap txn 0 commits inline.
        if (ctx->txn.transaction_id != 0) {
            ctx->created_indexes.push_back(components::table::created_index_t{table_oid_, index_oid_});
        }

        // WAL retention guard: pins build_start_wal_position so a concurrent checkpoint+truncate cannot drop
        // records the catchup loop still needs. build_start_registered gates the unregister at every exit
        // below (no RAII possible — a destructor cannot co_await).
        services::wal::id_t build_start_wal_position{0};
        bool build_start_registered = false;
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_q, qf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                        &services::wal::manager_wal_replicate_t::current_wal_id,
                                                        ctx->session);
            build_start_wal_position = co_await std::move(qf);
            auto [_r, rf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                        &services::wal::manager_wal_replicate_t::register_active_build,
                                                        ctx->session,
                                                        build_start_wal_position);
            co_await std::move(rf);
            build_start_registered = true;
        }

        // Every failure from here on must take the engine back out of the registry: the planner consults the
        // registry (not pg_index.indisvalid), so a half-built engine left registered ANSWERS QUERIES, and the
        // executor's undo_create_index only covers failures after this operator succeeds. frame_resource must
        // stay a real parameter — an argless coroutine lambda aborts at runtime with no allocator to extract.
        auto abandon_build = [this, ctx, &build_start_wal_position, &build_start_registered](
                                 [[maybe_unused]] std::pmr::memory_resource* frame_resource)
            -> actor_zeta::unique_future<void> {
            if (build_start_registered) {
                auto [_u, uf] =
                    actor_zeta::otterbrix::send(ctx->wal_address,
                                                &services::wal::manager_wal_replicate_t::unregister_active_build,
                                                ctx->session,
                                                build_start_wal_position);
                co_await std::move(uf);
                build_start_registered = false;
            }
            auto [_d, df] = actor_zeta::otterbrix::send(ctx->index_address,
                                                        &services::index::manager_index_t::drop_index,
                                                        ctx->session,
                                                        table_oid_,
                                                        index_oid_);
            co_await std::move(df);
            co_return;
        };

        // backfill — STREAM the table in bounded batches via the storage_fetch_next_batch cursor (cursor_id==0
        // OPENs, agent-minted id ADVANCEs, empty batch means drained), so peak scan memory is one batch + index
        // state. Index entries are stamped with each batch's TRUE physical row ids, fed one contiguous run at a
        // time (see below) — the MVCC-filtered scan skips deleted rows, so ids are gapped.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            uint64_t cursor_id = 0; // 0 == OPEN on the first fetch
            bool scan_ok = true;
            while (true) {
                auto [_fb, fbf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_fetch_next_batch,
                                                ctx->session,
                                                table_oid_,
                                                cursor_id,
                                                std::unique_ptr<components::table::table_filter_t>(nullptr),
                                                int64_t{-1},           // unbounded — index every row
                                                std::vector<size_t>{}, // empty == read all columns
                                                ctx->txn);
                auto fetch_result = co_await std::move(fbf);
                if (fetch_result.has_error()) {
                    set_error(fetch_result.error());
                    scan_ok = false;
                    break;
                }
                auto reply = std::move(fetch_result.value());
                cursor_id = reply.cursor_id;
                const uint64_t sz = reply.batch ? reply.batch->size() : 0;
                if (sz == 0) {
                    break; // drained: the agent replied an empty batch and erased the cursor
                }
#ifdef DEV_MODE
                g_create_index_backfill_batches.fetch_add(1, std::memory_order_relaxed);
#endif

                // batch->row_ids is TRUE but GAPPED (deleted rows skipped), so feed apply_wal_record_for_index one
                // maximal contiguous run at a time, based at that run's first physical id. Uses
                // apply_wal_record_for_index (index-addressed), not insert_rows(table_oid), which would fan out
                // to every index of the table and re-stage it into pre-existing indexes too
                // (test_create_index_backfill_addressing). This await's contract returns void; errors surface
                // later through commit_inserts (test_create_index_catchup_refusal).
                const auto& batch_chunk = *reply.batch;
                const auto* row_ids = batch_chunk.row_ids.data<int64_t>();
                uint64_t run_start = 0;
                while (run_start < sz) {
                    uint64_t run_len = 1;
                    while (run_start + run_len < sz &&
                           row_ids[run_start + run_len] == row_ids[run_start] + static_cast<int64_t>(run_len)) {
                        ++run_len;
                    }
                    std::pmr::vector<components::vector::data_chunk_t> idx_chunks(resource_);
                    idx_chunks.push_back(batch_chunk.partial_copy(resource_, run_start, run_len));
                    auto [_ir, irf] = actor_zeta::otterbrix::send(
                        ctx->index_address,
                        &services::index::manager_index_t::apply_wal_record_for_index,
                        ctx->session,
                        table_oid_,
                        index_oid_,
                        services::wal::id_t{0}, // no journal record: this run came from the scan
                        static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT),
                        std::pmr::vector<int64_t>(resource_),
                        std::move(idx_chunks),
                        static_cast<uint64_t>(row_ids[run_start]), // run's TRUE physical base id
                        ctx->txn.transaction_id,
                        ctx->execution_context.timezone_offset);
                    co_await std::move(irf);
                    run_start += run_len;
                }
            }

            if (!scan_ok) {
                // Streaming scan failed: the index was never published and no snapshot saw
                // it. Take the engine back out and release the WAL retention guard so the
                // next checkpoint can truncate freely.
                mark_failed();
                co_await abandon_build(resource_);
                co_return;
            }

            // No dml_append_range_t is recorded here: naming the scanned rows as "appended" would make a failed
            // CREATE INDEX UN-APPEND those real table rows via storage_revert_appends (observed: a refused
            // catchup over a 40-row table left it answering with 0 rows). The index publishes via the commit_id
            // back-channel instead; failure exits drop the engine via abandon_build.
        }

        // CREATE INDEX bounded-retry WAL catchup: re-apply every PHYSICAL_{INSERT,DELETE,UPDATE} written after
        // build_start_wal_position to catch rows committed concurrently with the snapshot scan above. Bounded
        // retry guards against write-heavy workloads that never quiesce.
        constexpr int MAX_CATCHUP_ITERATIONS = 10;
        services::wal::id_t catchup_start_wal = build_start_wal_position;
        bool converged = false;
        for (int i = 0; i < MAX_CATCHUP_ITERATIONS; ++i) {
            // No WAL configured (test harness): nothing to replay, converge.
            if (ctx->wal_address == actor_zeta::address_t::empty_address()) {
                converged = true;
                break;
            }

            auto [_load, lf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                           &services::wal::manager_wal_replicate_t::load,
                                                           ctx->session,
                                                           catchup_start_wal);
            auto wal_records_result = co_await std::move(lf);
            if (wal_records_result.has_error()) {
                // load() must REFUSE (not answer empty) on an unreadable WAL segment: an empty reply here means
                // "converged" and would silently publish the index missing every row that segment described.
                set_error(wal_records_result.error());
                mark_failed();
                co_await abandon_build(resource_);
                co_return;
            }
            auto wal_records = std::move(wal_records_result.value());

            if (wal_records.empty()) {
                converged = true;
                break;
            }

            services::wal::id_t max_wal_id_seen = catchup_start_wal;
            // Replayed entries stay PENDING until commit_inserts publishes them with the scan rows. DELETE/UPDATE
            // WAL records ship only row_ids, so mark_delete_row's key columns are recovered via
            // storage_fetch(row_ids); UPDATE replays as two messages (NEW-insert + synthesized OLD-delete).
            //
            // Four phases: (1) send every OLD-chunk fetch unawaited (mutually independent, distinct row-id
            // sets); (2) await them into per-record slots; (3) send apply_wal_record_for_index in WAL order to
            // the same manager_index mailbox (FIFO preserves order without awaiting here); (4) await completion.
            // A record's OLD-delete apply needs its own fetch done first, which the phase split guarantees.
            std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> old_chunks(resource_);
            old_chunks.resize(wal_records.size());
            std::pmr::vector<
                actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>>
                fetch_futures(resource_);
            std::pmr::vector<std::size_t> fetch_slots(resource_);
            for (std::size_t r = 0; r < wal_records.size(); ++r) {
                auto& rec = wal_records[r];
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

                // Empty OLD-chunk fetch (rows physically gone) is tolerated: manager_index_t drops the delete leg
                // and keeps the insert leg only (services/index/manager_index.cpp). A genuinely bad record instead
                // fails via catchup_failures_ refusing commit_inserts — NOT via the WAL-id convergence check below,
                // which only proves the journal went quiet.
                const bool needs_old_chunk = (rec.record_type == services::wal::wal_record_type::PHYSICAL_DELETE ||
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
                    auto [_f, ff] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::storage_fetch,
                                                                ctx->session,
                                                                rec.table_oid,
                                                                std::move(fetch_ids),
                                                                static_cast<uint64_t>(rec.physical_row_ids.size()),
                                                                // No projection: the backfill hands whole rows
                                                                // to the index engine's chunk binding.
                                                                std::vector<size_t>{},
                                                                // RAW + the real txn, not empty transaction_data:
                                                                // empty means "committed only", hiding these
                                                                // just-deleted rows.
                                                                components::table::transaction_data{},
                                                                components::table::fetch_visibility_t::RAW,
                                                                // The backfill needs every named row's old
                                                                // key columns; capping would drop keys.
                                                                /*limit=*/int64_t{-1});
                    fetch_futures.push_back(std::move(ff));
                    fetch_slots.push_back(r);
                }
            }

            // Phase 2: an ERROR reply is NOT the tolerated EMPTY batch ("rows physically gone") — an index caught
            // up from silently-empty OLD chunks diverges from the table, so this must fail loudly. First error
            // wins, but every future is still awaited so none lands on an abandoned continuation.
            core::error_t fetch_error = core::error_t::no_error();
            for (std::size_t i = 0; i < fetch_futures.size(); ++i) {
                auto fetched_r = co_await std::move(fetch_futures[i]);
                if (fetched_r.has_error()) {
                    if (!fetch_error.contains_error()) {
                        fetch_error = fetched_r.error();
                    }
                    continue;
                }
                old_chunks[fetch_slots[i]] = std::move(fetched_r.value());
            }
            if (fetch_error.contains_error()) {
                // Release the WAL retention guard before failing (mirrors the streaming-scan exit) so the
                // next checkpoint can truncate freely.
                if (build_start_registered) {
                    auto [_u, uf] =
                        actor_zeta::otterbrix::send(ctx->wal_address,
                                                    &services::wal::manager_wal_replicate_t::unregister_active_build,
                                                    ctx->session,
                                                    build_start_wal_position);
                    co_await std::move(uf);
                    build_start_registered = false;
                }
                set_error(std::move(fetch_error));
                co_return;
            }

            std::pmr::vector<actor_zeta::unique_future<void>> apply_futures(resource_);
            for (std::size_t r = 0; r < wal_records.size(); ++r) {
                auto& rec = wal_records[r];
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

                if (rec.record_type == services::wal::wal_record_type::PHYSICAL_INSERT ||
                    rec.record_type == services::wal::wal_record_type::PHYSICAL_UPDATE) {
                    // INSERT, or NEW-insert half of UPDATE: forward the WAL NEW chunk.
                    std::pmr::vector<int64_t> row_ids(rec.physical_row_ids.begin(),
                                                      rec.physical_row_ids.end(),
                                                      resource_);
                    auto [_a, af] =
                        actor_zeta::otterbrix::send(ctx->index_address,
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
                    apply_futures.push_back(std::move(af));
                }

                if (rec.record_type == services::wal::wal_record_type::PHYSICAL_DELETE ||
                    rec.record_type == services::wal::wal_record_type::PHYSICAL_UPDATE) {
                    // DELETE, or OLD-delete half of UPDATE: send the recovered OLD
                    // chunk forced to record_type PHYSICAL_DELETE so the handler
                    // routes through mark_delete_row.
                    std::pmr::vector<int64_t> row_ids(rec.physical_row_ids.begin(),
                                                      rec.physical_row_ids.end(),
                                                      resource_);
                    auto [_a, af] = actor_zeta::otterbrix::send(
                        ctx->index_address,
                        &services::index::manager_index_t::apply_wal_record_for_index,
                        ctx->session,
                        rec.table_oid,
                        index_oid_,
                        rec.id,
                        static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE),
                        std::move(row_ids),
                        std::move(old_chunks[r]),
                        rec.physical_row_start,
                        ctx->txn.transaction_id,
                        rec.session_tz);
                    apply_futures.push_back(std::move(af));
                }
            }

            // Completion-sync only: apply_wal_record_for_index returns void, so a refused apply surfaces later
            // via catchup_failures_ refusing commit_inserts, not through these futures
            // (test_create_index_catchup_refusal).
            for (auto& af : apply_futures) {
                co_await std::move(af);
            }

            // Converged if no record advanced past the watermark. Also guards
            // against load() returning records at-or-below it (defensive).
            if (max_wal_id_seen == catchup_start_wal) {
                converged = true;
                break;
            }
            catchup_start_wal = max_wal_id_seen;
        }
        if (!converged) {
            // Graceful fail: not published, so GC-able once abandon_build drops the engine and
            // releases the retention guard.
            set_error(core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"CREATE INDEX failed to converge after MAX_CATCHUP_ITERATIONS "
                                                     "on high-write table. Retry during low-traffic window. "
                                                     "Future: CREATE INDEX CONCURRENTLY (WAL-based).",
                                                     resource_}});
            mark_failed();
            co_await abandon_build(resource_);
            co_return;
        }

        // Converged: release the retention guard BEFORE the pg_index flip below
        // (which only touches the catalog) so a later truncate isn't blocked.
        if (build_start_registered) {
            auto [_u, uf] =
                actor_zeta::otterbrix::send(ctx->wal_address,
                                            &services::wal::manager_wal_replicate_t::unregister_active_build,
                                            ctx->session,
                                            build_start_wal_position);
            co_await std::move(uf);
            build_start_registered = false;
        }

        // Flip pg_index.indisvalid -> true by replacing the indisvalid=false row
        // the metadata operator wrote, now that the engine is populated.
        if (ctx->disk_address != actor_zeta::address_t::empty_address() &&
            index_oid_ != components::catalog::INVALID_OID) {
            constexpr components::catalog::oid_t pg_idx_oid = components::catalog::well_known_oid::pg_index_table;
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                        exec_ctx,
                                                        pg_idx_oid,
                                                        std::int64_t{0},
                                                        index_oid_);
            co_await std::move(df);
            if (ctx->txn.transaction_id != 0)
                ctx->pg_catalog_delete_tables.insert(pg_idx_oid);

            auto valid_row = components::catalog::build_pg_index_row(
                resource(),
                index_oid_,
                table_oid_,
                indkey_,
                /*indisvalid=*/true,
                components::logical_plan::index_type_to_indtype_code(index_type_));
            auto [_w, wf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::append_pg_catalog_row,
                                                        exec_ctx,
                                                        pg_idx_oid,
                                                        std::move(valid_row));
            auto rng_r = co_await std::move(wf);
            if (rng_r.has_error()) {
                // This row flips indisvalid; if refused, the engine stays registered (the planner
                // reads the registry, not the row) until abandon_build removes it.
                set_error(rng_r.error());
                mark_failed();
                co_await abandon_build(resource_);
                co_return;
            }
            if (rng_r.value().count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
        }

        mark_executed();
    }

} // namespace components::operators
