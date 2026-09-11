#include "operator_create_index_backfill.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <components/table/column_state.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/record.hpp>

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_create_index_backfill_batches{0};
        std::atomic<uint64_t> g_create_index_backfill_partial_copies{0};
    } // namespace
    uint64_t create_index_backfill_batches() noexcept {
        return g_create_index_backfill_batches.load(std::memory_order_relaxed);
    }
    uint64_t create_index_backfill_partial_copies() noexcept {
        return g_create_index_backfill_partial_copies.load(std::memory_order_relaxed);
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
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            set_error(core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"CREATE INDEX: no index manager is wired to this executor; "
                                                     "the index was not created",
                                                     resource()}});
            co_return;
        }

        auto [_rc, rcf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                      &services::index::manager_index_t::register_collection,
                                                      ctx->session,
                                                      table_oid_);
        co_await std::move(rcf);

        uint64_t built_compact_epoch = 0;
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_ce, cef] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::storage_compact_epoch,
                                                          ctx->session,
                                                          table_oid_);
            auto epoch_r = co_await std::move(cef);
            if (epoch_r.has_error()) {
                set_error(epoch_r.error());
                co_return;
            }
            built_compact_epoch = epoch_r.value();
        }

        // Registration is the mirror boundary: appends after it are staged, appends before lie in the RAW read.
        auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                      &services::index::manager_index_t::create_index,
                                                      ctx->session,
                                                      table_oid_,
                                                      index_oid_,
                                                      keys_,
                                                      index_type_,
                                                      ctx->execution_context.timezone_offset,
                                                      built_compact_epoch);
        auto create_error = co_await std::move(ixf);

        if (create_error.contains_error()) {
            set_error(create_error);
            co_return;
        }

        if (ctx->txn.transaction_id != 0) {
            ctx->created_indexes.push_back(components::table::created_index_t{table_oid_, index_oid_});
        }

        // Registry-based; failures here must abandon_build. frame_resource must stay real (argless lambda aborts).
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

        // RAW read of every physical row at the coverage bound, deleted/uncommitted rows included —
        // the index only needs a superset. Rows after the bound arrive via the DML mirror; no journal replay.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            // Compact gate: a live cursor defers the owning agent's compact for this table (compact
            // renumbers ids stamped below); held, never advanced, across the whole read.
            uint64_t gate_cursor_id = 0;
            {
                auto [_g, gf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_fetch_next_batch,
                                                ctx->session,
                                                table_oid_,
                                                uint64_t{0},
                                                std::unique_ptr<components::table::table_filter_t>(nullptr),
                                                int64_t{-1},
                                                std::vector<size_t>{0},
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
                // create_data, but no zeroing: every one of `count` slots is written right below.
                components::vector::vector_t fetch_ids(resource_,
                                                       components::types::complex_logical_type{
                                                           components::types::logical_type::BIGINT},
                                                       /*create_data=*/true,
                                                       /*zero_data=*/false,
                                                       count);
                for (uint64_t k = 0; k < count; ++k) {
                    fetch_ids.data<int64_t>()[k] = static_cast<int64_t>(base + k);
                }
                auto [_fb, fbf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_fetch,
                                                              ctx->session,
                                                              table_oid_,
                                                              std::move(fetch_ids),
                                                              count,
                                                              std::vector<size_t>{},
                                                              components::table::transaction_data{},
                                                              components::table::fetch_visibility_t::RAW,
                                                              /*limit=*/int64_t{-1},
                                                              services::disk::k_fetch_epoch_unchecked);
                auto fetch_result = co_await std::move(fbf);
                if (fetch_result.has_error()) {
                    scan_error = fetch_result.error();
                    scan_ok = false;
                    break;
                }
#ifdef DEV_MODE
                g_create_index_backfill_batches.fetch_add(1, std::memory_order_relaxed);
#endif

                // Fed one run at a time by TRUE physical id; index-addressed so pre-existing indexes aren't re-staged.
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
                        // The whole chunk is one run -- the ordinary case for a table with no holes.
                        // partial_copy would rebuild every column here (a fresh validity mask, a
                        // copied complex_logical_type per column, an index slice for dictionaries);
                        // nothing reads batch_chunk after this, so it can simply be handed over.
                        // run_len == sz forces run_start == 0 (the scan above bounds run_len by
                        // sz - run_start), and run_start += run_len then ends the loop -- so the
                        // moved-from chunk is never read on a later turn.
                        if (run_len == sz) {
                            idx_chunks.push_back(std::move(batch_chunk));
                        } else {
#ifdef DEV_MODE
                            g_create_index_backfill_partial_copies.fetch_add(1, std::memory_order_relaxed);
#endif
                            idx_chunks.push_back(batch_chunk.partial_copy(resource_, run_start, run_len));
                        }
                        auto [_ir, irf] = actor_zeta::otterbrix::send(
                            ctx->index_address,
                            &services::index::manager_index_t::apply_wal_record_for_index,
                            ctx->session,
                            table_oid_,
                            index_oid_,
                            services::wal::id_t{0},
                            static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT),
                            std::pmr::vector<int64_t>(resource_),
                            std::move(idx_chunks),
                            static_cast<uint64_t>(row_ids[run_start]),
                            ctx->txn.transaction_id,
                            ctx->execution_context.timezone_offset);
                        co_await std::move(irf);
                        run_start += run_len;
                    }
                }
            }

            co_await close_gate(resource_);

            if (!scan_ok) {
                set_error(std::move(scan_error));
                mark_failed();
                co_await abandon_build(resource_);
                co_return;
            }

            // No dml_append_range_t: failure would UN-APPEND real rows (observed left a 40-row table at 0).
        }

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
