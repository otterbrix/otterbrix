#include "transfer_scan.hpp"

#include <services/disk/manager_disk.hpp>

#include <utility>
#include <vector>

namespace components::operators {

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 components::catalog::oid_t table_oid,
                                 logical_plan::limit_t limit,
                                 std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , table_oid_(table_oid)
        , limit_(limit)
        , projected_cols_(std::move(projected_cols)) {}

    vector::data_chunk_t transfer_scan::make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types) {
        if (projected_cols_.empty()) {
            return vector::data_chunk_t{resource_, types, 0};
        }
        // Pruned-scan contract (PR #477): pruned scans emit FULL-WIDTH chunks whose
        // non-projected columns are buffer-less placeholders, so column ordinals stay
        // stable plan-wide (expression key paths are never remapped after prune_columns).
        // The schema'd 0-row empty-guard must honor the same shape as real batches —
        // operators above index it by table ordinal. An empty `types` (the 0-column
        // drain sentinel) still degrades to a 0-column chunk here.
        return vector::data_chunk_t{resource_, types, projected_cols_, 0};
    }

    // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
    // FIRST call OPENs a position-only cursor (no filter); subsequent calls ADVANCE it, one batch
    // each. At most one cross-actor fetch await per call, sequential across calls in this nested
    // operator coroutine — no lost-wakeup. Peak scan memory = one batch.
    actor_zeta::unique_future<void> transfer_scan::release_cursor(pipeline::context_t* ctx) {
        if (cursor_id_ == 0 || drained_) {
            co_return;
        }
        const uint64_t id = cursor_id_;
        cursor_id_ = 0;
        drained_ = true;
        auto [_s, cf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_close_cursor,
                                         ctx->session,
                                         table_oid_,
                                         id);
        co_await std::move(cf);
        co_return;
    }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    transfer_scan::source_next(pipeline::context_t* ctx) {
        if (drained_) {
            co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
        }

        // No-table sentinel (no-FROM SELECT): emit ONE synthetic single-row batch that
        // carries one placeholder column (so it is not the 0-column drain sentinel),
        // then drain. operator_select_t projects its constant/arithmetic columns over
        // this one row to produce the single constants row (the placeholder is ignored),
        // matching the legacy virtual-row path. No disk round-trip.
        if (table_oid_ == components::catalog::INVALID_OID) {
            drained_ = true;
            std::pmr::vector<types::complex_logical_type> types(resource_);
            types.emplace_back(types::logical_type::BOOLEAN);
            vector::data_chunk_t row{resource_, types, 1};
            row.set_cardinality(1);
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(row));
        }

        if (!opened_) {
            opened_ = true;
            // The read-cap (offset+limit head cap) is pushed down as the scan's row-count cap.
            // SELECT OFFSET is applied by operator_limit above, so every scan receives offset()==0
            // and head_cap() == limit here.
            const int64_t scan_limit = limit_.head_cap();
            auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_fetch_next_batch,
                                             ctx->session,
                                             table_oid_,
                                             cursor_id_, // 0 == OPEN
                                             std::unique_ptr<table::table_filter_t>(nullptr),
                                             scan_limit,
                                             projected_cols_,
                                             ctx->txn);
            auto fetch_result = co_await std::move(sf);
            if (fetch_result.has_error()) {
                set_error(fetch_result.error());
                mark_failed();
                co_return fetch_result.convert_error<vector::data_chunk_t>();
            }
            auto reply = std::move(fetch_result.value());
            cursor_id_ = reply.cursor_id;
            co_return co_await emit_or_skip(ctx, std::move(reply.batch));
        }

        // ADVANCE.
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_fetch_next_batch,
                                         ctx->session,
                                         table_oid_,
                                         cursor_id_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         int64_t{-1},
                                         projected_cols_,
                                         ctx->txn);
        auto fetch_result = co_await std::move(sf);
        if (fetch_result.has_error()) {
            set_error(fetch_result.error());
            mark_failed();
            co_return fetch_result.convert_error<vector::data_chunk_t>();
        }
        auto reply = std::move(fetch_result.value());
        co_return co_await emit_or_skip(ctx, std::move(reply.batch));
    }

    // The drained empty-guard for one fetched batch. transfer_scan fetches the guard schema lazily
    // (storage_types) only on the drained-with-zero-rows path (it has no upfront filter to type).
    // (OFFSET is applied by operator_limit above; every scan receives offset()==0, so no skip.)
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    transfer_scan::emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch) {
        const uint64_t sz = batch ? batch->size() : 0;

        if (sz == 0) {
            drained_ = true;
            if (!emitted_any_) {
                emitted_any_ = true;
                if (!guard_types_loaded_) {
                    guard_types_loaded_ = true;
                    auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::storage_types,
                                                     ctx->session,
                                                     table_oid_);
                    auto types_result = co_await std::move(tf);
                    if (types_result.has_error()) {
                        // The guard chunk carries the table's schema to operators above; a
                        // refused read cannot supply one, and an empty list would pass a
                        // 0-column chunk off as this table's shape.
                        set_error(types_result.error());
                        mark_failed();
                        co_return types_result.convert_error<vector::data_chunk_t>();
                    }
                    guard_types_ = std::move(types_result.value());
                }
                co_return make_drain_chunk(guard_types_);
            }
            co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
        }

        emitted_any_ = true;
        co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(*batch));
    }

} // namespace components::operators