#include "operator_create_index_backfill.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <components/table/column_state.hpp> // complete table_filter_t for the gate cursor open
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/record.hpp>

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

namespace components::operators {

    // DEV_MODE test counter: batches consumed by the RAW backfill read (proves it streams in bounded
    // batches instead of loading the table whole). Process-global + relaxed — coarse instrumentation,
    // not a sync primitive.
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

        // REGISTRATION IS THE MIRROR BOUNDARY. From the moment this message is processed, every
        // DML statement's post-append reconciliation with manager_index sees the building index
        // and stages its rows; every append that predates it lies inside the RAW read below
        // (whose coverage bound is captured strictly after). Between the two there is no window.
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

        // Every failure from here on must take the engine back out of the registry: the planner consults the
        // registry (not pg_index.indisvalid), so a half-built engine left registered ANSWERS QUERIES, and the
        // executor's undo_create_index only covers failures after this operator succeeds. frame_resource must
        // stay a real parameter — an argless coroutine lambda aborts at runtime with no allocator to extract.
        auto abandon_build = [this, ctx]([[maybe_unused]] std::pmr::memory_resource* frame_resource)
            -> actor_zeta::unique_future<void> {
            auto [_d, df] = actor_zeta::otterbrix::send(ctx->index_address,
                                                        &services::index::manager_index_t::drop_index,
                                                        ctx->session,
                                                        table_oid_,
                                                        index_oid_);
            co_await std::move(df);
            co_return;
        };

        // backfill — RAW read of EVERY physical row present when the coverage bound is captured:
        // deleted rows (a reader with an older snapshot still owns them) and other transactions'
        // uncommitted rows (their commit needs them findable) included. Visibility is the TABLE's
        // job at read time; the index only has to be a superset of what any snapshot can see.
        // Rows appended after the bound arrive through the DML mirror/reconciliation, which is in
        // force from the registration above. Journal replay is gone with it: the journal cannot
        // name a row this read plus the mirror do not already carry.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            // Compact gate: a live fetch-next cursor defers the owning agent's compact for this
            // table (has_active_scan_for_oid), and compact renumbers the physical row ids the
            // entries below are stamped with. Opened once and HELD (never advanced) across the
            // whole read, closed on every exit.
            uint64_t gate_cursor_id = 0;
            {
                auto [_g, gf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_fetch_next_batch,
                                                ctx->session,
                                                table_oid_,
                                                uint64_t{0}, // OPEN
                                                std::unique_ptr<components::table::table_filter_t>(nullptr),
                                                int64_t{-1},
                                                std::vector<size_t>{0}, // one column: the open is a pin, not a read
                                                ctx->txn);
                auto gate_r = co_await std::move(gf);
                if (gate_r.has_error()) {
                    set_error(gate_r.error());
                    mark_failed();
                    co_await abandon_build(resource_);
                    co_return;
                }
                gate_cursor_id = gate_r.value().cursor_id;
            }
            auto close_gate = [this, ctx, gate_cursor_id](
                                  [[maybe_unused]] std::pmr::memory_resource* frame_resource)
                -> actor_zeta::unique_future<void> {
                auto [_c, cf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_close_cursor,
                                                            ctx->session,
                                                            table_oid_,
                                                            gate_cursor_id);
                co_await std::move(cf);
                co_return;
            };

            // The coverage bound: every physical row id below it is read RAW here; every row id at
            // or above it is appended by a statement whose reconciliation runs after the
            // registration above and therefore mirrors.
            uint64_t total_rows = 0;
            {
                auto [_t, tf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_total_rows,
                                                            ctx->session,
                                                            table_oid_);
                auto total_r = co_await std::move(tf);
                if (total_r.has_error()) {
                    set_error(total_r.error());
                    mark_failed();
                    co_await close_gate(resource_);
                    co_await abandon_build(resource_);
                    co_return;
                }
                total_rows = total_r.value();
            }

            bool scan_ok = true;
            core::error_t scan_error = core::error_t::no_error();
            for (uint64_t base = 0; base < total_rows; base += components::vector::DEFAULT_VECTOR_CAPACITY) {
                const uint64_t count =
                    std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, total_rows - base);
                components::vector::vector_t fetch_ids(resource_, components::types::logical_type::BIGINT, count);
                for (uint64_t k = 0; k < count; ++k) {
                    fetch_ids.data<int64_t>()[k] = static_cast<int64_t>(base + k);
                }
                auto [_fb, fbf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_fetch,
                                                              ctx->session,
                                                              table_oid_,
                                                              std::move(fetch_ids),
                                                              count,
                                                              std::vector<size_t>{}, // all columns
                                                              components::table::transaction_data{},
                                                              components::table::fetch_visibility_t::RAW,
                                                              /*limit=*/int64_t{-1});
                auto fetch_result = co_await std::move(fbf);
                if (fetch_result.has_error()) {
                    scan_error = fetch_result.error();
                    scan_ok = false;
                    break;
                }
#ifdef DEV_MODE
                g_create_index_backfill_batches.fetch_add(1, std::memory_order_relaxed);
#endif

                // Feed apply_wal_record_for_index one maximal contiguous run at a time, based at the
                // run's TRUE physical id from the reply's own row_ids (a RAW fetch answers every
                // requested row, but the pairing stays by id, never by position). Index-addressed on
                // purpose: insert_rows(table_oid) would fan out to every index of the table and
                // re-stage rows pre-existing indexes already hold (test_create_index_backfill_
                // addressing). The await's contract returns void; staging errors surface through
                // commit_inserts refusing the build's transaction.
                for (auto& batch_chunk : fetch_result.value()) {
                    const uint64_t sz = batch_chunk.size();
                    if (sz == 0) {
                        continue;
                    }
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
                            services::wal::id_t{0}, // no journal record: this run came from the RAW read
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
            }

            co_await close_gate(resource_);

            if (!scan_ok) {
                // The read failed: the index was never published and no snapshot saw it. Take the
                // engine back out so the planner stops seeing it.
                set_error(std::move(scan_error));
                mark_failed();
                co_await abandon_build(resource_);
                co_return;
            }

            // No dml_append_range_t is recorded here: naming the read rows as "appended" would make a failed
            // CREATE INDEX UN-APPEND those real table rows via storage_revert_appends (observed: a refused
            // build over a 40-row table left it answering with 0 rows). The index publishes via the commit_id
            // back-channel instead; failure exits drop the engine via abandon_build.
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
