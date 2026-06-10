#include "operator_create_index_backfill.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <components/index/index_engine.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>

#include <cstddef>
#include <memory>
#include <vector>

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

        // CREATE back-channel: record the index this statement brought into being
        // (owning table oid + name) so the COMMIT publishes it and a same-txn
        // ABORT drops the still-uncommitted index (operator_abort_transaction
        // fans manager_index_t::drop_index per drained created_index). Mirror of
        // the operator_create_collection storage back-channel; gated on a
        // non-zero txn id (autocommit/bootstrap txn 0 commits the index inline).
        if (ctx->txn.transaction_id != 0) {
            ctx->created_indexes.push_back(components::table::created_index_t{table_oid_, std::string{index_name_}});
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
                    // insert_rows leaves entries PENDING (tagged with this txn_id);
                    // they become visible only when the executor's post-pipeline
                    // commit_inserts runs, which it does when dml_append_row_count > 0
                    // && dml_table_oid is set — so record those here to commit the
                    // backfilled entries alongside the CREATE INDEX txn. The reuse
                    // also re-commits the data rows to this commit_id, harmless
                    // absent a concurrent reader between the two commits. Rows
                    // committed during the scan are caught by the catchup loop below.
                    ctx->dml_append_row_start = 0;
                    ctx->dml_append_row_count = count;
                    ctx->dml_table_oid = table_oid_;
                }
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
            std::pmr::vector<std::unique_ptr<components::vector::data_chunk_t>> old_chunks(resource_);
            old_chunks.resize(wal_records.size());
            std::pmr::vector<actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>>
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

                // Recover the OLD key chunk for DELETE/UPDATE. If the fetch can't
                // run or returns empty (rows physically gone), forward nullptr:
                // manager_index logs+skips and the convergence guard catches any
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
                                                     static_cast<uint64_t>(rec.physical_row_ids.size()));
                    fetch_futures.push_back(std::move(ff));
                    fetch_slots.push_back(r);
                }
            }

            for (std::size_t i = 0; i < fetch_futures.size(); ++i) {
                old_chunks[fetch_slots[i]] = co_await std::move(fetch_futures[i]);
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
