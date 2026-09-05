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

    // Test-observable counter of BATCHES the streaming backfill scan consumed
    // (one bump per non-empty storage_fetch_next_batch reply). Makes the batched
    // scan deterministically observable: over a table > DEFAULT_VECTOR_CAPACITY
    // the count must exceed 1, proving the loop iterated instead of materializing
    // the whole table in one scan. Process-global + relaxed:
    // coarse instrumentation, not a synchronization primitive; off every hot path.
    // DEV_MODE-only, mirroring services::collection::executor::streaming_pipeline_runs().
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
        // No-op when there is no index actor wired (e.g. some test harnesses).
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }

        // Ensure the engine knows about the collection, then create the
        // index entry. register_collection is idempotent.
        auto [_rc, rcf] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::register_collection,
                                           ctx->session,
                                           table_oid_);
        co_await std::move(rcf);

        auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::create_index,
                                           ctx->session,
                                           table_oid_,
                                           index_oid_,
                                           keys_,
                                           index_type_,
                                           ctx->execution_context.timezone_offset);
        // create_index answers with a core::error_t and nothing else. It used to hand
        // back a uint32 "index id" -- the index's POSITION in a per-table list that no
        // longer exists -- and this call site never read the number: an index's identity
        // below the planner is its indexrelid, which is right here in index_oid_.
        auto create_error = co_await std::move(ixf);

        if (create_error.contains_error()) {
            // Report the reason the manager gave: flattening every failure to
            // "index already exists" is right for one cause and wrong for the rest,
            // including a disk index whose storage failed to open.
            set_error(create_error);
            co_return;
        }

        // CREATE back-channel: record the index this statement brought into being
        // (owning table oid + indexrelid) so the COMMIT publishes it and a same-txn
        // ABORT drops the still-uncommitted index (operator_abort_transaction
        // fans manager_index_t::drop_index per drained created_index). Mirror of
        // the operator_create_collection storage back-channel; gated on a
        // non-zero txn id (autocommit/bootstrap txn 0 commits the index inline).
        if (ctx->txn.transaction_id != 0) {
            ctx->created_indexes.push_back(components::table::created_index_t{table_oid_, index_oid_});
        }

        // WAL retention guard: register build_start_wal_position so a concurrent
        // checkpoint+truncate cannot drop records the catchup loop still needs.
        // Routed via mailbox (sync inter-actor calls are forbidden inside the
        // executor actor). The matching unregister fires at every exit below;
        // build_start_registered gates it so a never-registered guard is never
        // double-unregistered. No RAII: a destructor can't co_await the unregister.
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

        // backfill — STREAM the table in bounded batches and feed each into the
        // index. Reuses the streaming storage_fetch_next_batch cursor primitive
        // (the same fetch-next source the streaming scans use; no new disk method):
        // cursor_id==0 OPENs, the agent-minted id ADVANCEs, and a drained cursor
        // replies an empty batch. Peak scan memory is one batch + index state. Index
        // entries are stamped with each batch's TRUE physical row ids (batch->row_ids, fed to
        // insert_rows one contiguous run at a time — see below), because the
        // MVCC-filtered scan skips deleted rows and its ids are gapped. Each iteration
        // does at most one fetch await followed by the insert awaits, sequential
        // across the loop; the executor coroutine's single-slot continuation is
        // republished+cleared between the awaits, so there is no lost wakeup.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            uint64_t cursor_id = 0; // 0 == OPEN on the first fetch
            uint64_t backfilled_count = 0;
            bool any_row = false;
            bool scan_ok = true;
            while (true) {
                auto [_fb, fbf] = actor_zeta::send(ctx->disk_address,
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
                any_row = true;
#ifdef DEV_MODE
                g_create_index_backfill_batches.fetch_add(1, std::memory_order_relaxed);
#endif

                // The fetched batch is an MVCC-filtered scan: deleted / invisible rows
                // are SKIPPED, so batch->row_ids carries the TRUE — possibly GAPPED —
                // physical row ids. insert_rows stamps contiguous ids from its
                // start_row_id, so feed it one maximal contiguous row-id RUN at a
                // time, based at that run's first physical id: every index entry then
                // points at its real storage row (never assume gap-free ids).
                // The extra awaits stay sequential in this operator coroutine —
                // same lost-wakeup discipline as the fetch/insert pair above.
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
                    auto [_ir, irf] =
                        actor_zeta::send(ctx->index_address,
                                         &services::index::manager_index_t::insert_rows,
                                         services::index::execution_context_t{ctx->session,
                                                                              ctx->txn,
                                                                              ctx->execution_context.timezone_offset,
                                                                              table_oid_},
                                         table_oid_,
                                         std::move(idx_chunks),
                                         static_cast<uint64_t>(row_ids[run_start]), // run's TRUE physical base id
                                         run_len);
                    auto index_error = co_await std::move(irf);
                    if (index_error.contains_error()) {
                        // A backfill run that did not reach the index leaves the new index
                        // incomplete; the CREATE INDEX must fail rather than publish it.
                        set_error(std::move(index_error));
                        co_return;
                    }
                    run_start += run_len;
                }
                backfilled_count += sz;
            }

            if (!scan_ok) {
                // Streaming scan failed: the index was never published and no snapshot
                // saw it. Release the WAL retention guard before exiting so the next
                // checkpoint can truncate freely (mirror of the non-convergence exit).
                if (build_start_registered) {
                    auto [_u, uf] = actor_zeta::send(ctx->wal_address,
                                                     &services::wal::manager_wal_replicate_t::unregister_active_build,
                                                     ctx->session,
                                                     build_start_wal_position);
                    co_await std::move(uf);
                    build_start_registered = false;
                }
                co_return;
            }

            if (any_row) {
                // insert_rows leaves entries PENDING (tagged with this txn_id). For a
                // CREATE INDEX (DDL) txn the executor does NOT route these appends
                // through the commit operator — its DDL-commit path CLEARS
                // exec_result.dml_appends (routing them would re-commit pre-existing
                // base rows). The index is published instead via the commit_id
                // back-channel (the executor's inline CREATE INDEX index-commit:
                // commit_inserts keyed by oid+commit_id, no
                // row-count). This single coalesced range is recorded for
                // symmetry/observability only; its count does not gate the commit. Rows
                // committed during the scan are caught by the catchup loop below.
                ctx->dml_appends.push_back(components::table::dml_append_range_t{table_oid_, 0, backfilled_count});
            }
        }

        // CREATE INDEX bounded-retry WAL catchup. The snapshot scan above may
        // have missed rows committed concurrently with the build, so re-apply
        // every PHYSICAL_{INSERT,DELETE,UPDATE} for table_oid written after
        // build_start_wal_position (retention-guarded above) to the in-memory
        // index. Bounded retry guards against write-heavy workloads that never
        // quiesce; each iteration advances catchup_start_wal to the max wal_id
        // seen, so it terminates once load() finds nothing past the watermark.
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
            // Non-const iteration so rec.physical_data can be moved into the
            // apply_wal_record_for_index message. Replayed entries are tagged
            // with the CREATE INDEX txn_id and stay PENDING until the
            // post-pipeline commit_inserts publishes them with the scan rows.
            // DELETE/UPDATE: the WAL record ships only row_ids, but mark_delete_row
            // needs the original key columns, so we storage_fetch(row_ids) to
            // recover the OLD chunk (O(deleted_rows) reads per iteration). UPDATE
            // is replayed as two messages (NEW-insert + synthesized OLD-delete).
            //
            // Two-phase within this WAL batch:
            //   Phase 1 sends every OLD-chunk storage_fetch (DELETE/UPDATE) to the
            //   disk mailbox without awaiting — the fetches are mutually independent
            //   (distinct row-id sets) — and also advances max_wal_id_seen.
            //   Phase 2 awaits them back into a per-record old_chunk slot.
            //   Phase 3 replays the apply_wal_record_for_index messages in WAL order
            //   to the SAME manager_index mailbox; FIFO ordering on that single
            //   mailbox preserves the replay order even though the sends are not
            //   awaited in the loop, so awaiting (phase 4) is completion-sync only.
            // A record's OLD-delete apply consumes the OLD chunk fetched for the
            // SAME record, so the fetch await (phase 2) must complete before the
            // matching apply send (phase 3) — the phase split guarantees that.
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

                // Recover the OLD key chunks for DELETE/UPDATE. If the fetch can't
                // run or returns empty (rows physically gone), the slot stays an empty
                // batch: manager_index logs+skips and the convergence guard catches any
                // persistent divergence next iteration.
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
                    auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::storage_fetch,
                                                     ctx->session,
                                                     rec.table_oid,
                                                     std::move(fetch_ids),
                                                     static_cast<uint64_t>(rec.physical_row_ids.size()),
                                                     // No projection: the backfill hands whole rows
                                                     // to the index engine's chunk binding.
                                                     std::vector<size_t>{},
                                                     // RAW, and an EMPTY transaction_data is NOT a
                                                     // substitute for it: these rows are being read
                                                     // BECAUSE they were deleted — the DELETE/UPDATE
                                                     // record's old key columns are the whole point —
                                                     // and an empty txn means "see everything
                                                     // COMMITTED", which a committed delete hides.
                                                     components::table::transaction_data{},
                                                     components::table::fetch_visibility_t::RAW);
                    fetch_futures.push_back(std::move(ff));
                    fetch_slots.push_back(r);
                }
            }

            // Phase 2: await every fetch back into its slot. An ERROR reply is NOT the
            // tolerated EMPTY batch ("the rows are physically gone", which manager_index
            // logs+skips): an index caught up from silently empty OLD chunks diverges
            // from the table, so the CREATE INDEX must fail loudly instead. The first
            // error wins, but every in-flight future is still awaited (completion-sync)
            // so no reply lands on an abandoned continuation.
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
                // Mirror the streaming-scan / non-convergence exits: the index was never
                // published and no snapshot saw it, so release the WAL retention guard
                // before failing so the next checkpoint can truncate freely.
                if (build_start_registered) {
                    auto [_u, uf] = actor_zeta::send(ctx->wal_address,
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
                    auto [_a, af] = actor_zeta::send(ctx->index_address,
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
                    auto [_a, af] =
                        actor_zeta::send(ctx->index_address,
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
            // Graceful fail: the index was never published and no snapshot saw
            // it, so it is immediately GC-able. Release the WAL retention guard
            // before exiting so the next checkpoint can truncate freely.
            if (build_start_registered) {
                auto [_u, uf] = actor_zeta::send(ctx->wal_address,
                                                 &services::wal::manager_wal_replicate_t::unregister_active_build,
                                                 ctx->session,
                                                 build_start_wal_position);
                co_await std::move(uf);
                build_start_registered = false;
            }
            set_error(core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"CREATE INDEX failed to converge after MAX_CATCHUP_ITERATIONS "
                                                     "on high-write table. Retry during low-traffic window. "
                                                     "Future: CREATE INDEX CONCURRENTLY (WAL-based).",
                                                     resource_}});
            co_return;
        }

        // Converged: release the retention guard BEFORE the pg_index flip below
        // (which only touches the catalog) so a later truncate isn't blocked.
        if (build_start_registered) {
            auto [_u, uf] = actor_zeta::send(ctx->wal_address,
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

            auto [_d, df] = actor_zeta::send(ctx->disk_address,
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
