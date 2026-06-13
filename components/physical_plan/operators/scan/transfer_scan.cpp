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

    void transfer_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (table_oid_ == components::catalog::INVALID_OID)
            return;
        async_wait();
    }

    actor_zeta::unique_future<void> transfer_scan::await_async_and_resume(pipeline::context_t* ctx) {
        int64_t offset_val = limit_.offset();
        int64_t limit_val = limit_.limit();
        // Push LIMIT+OFFSET down to disk: the scan reads at most (offset+limit) rows;
        // the offset_val rows at the head are skipped in this operator below.
        int64_t scan_limit = (limit_val < 0) ? limit_val : limit_val + offset_val;
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan_batched,
                                         ctx->session,
                                         table_oid_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         scan_limit,
                                         projected_cols_,
                                         ctx->txn);
        auto batches = co_await std::move(sf);

        // Skip offset rows across batches. Partial-copy the boundary batch.
        if (offset_val > 0) {
            uint64_t remaining = static_cast<uint64_t>(offset_val);
            size_t skip_count = 0;
            for (; skip_count < batches.size() && remaining > 0; ++skip_count) {
                auto sz = batches[skip_count].size();
                if (sz <= remaining) {
                    remaining -= sz;
                    continue;
                }
                batches[skip_count] = batches[skip_count].partial_copy(resource_, remaining, sz - remaining);
                remaining = 0;
                break;
            }
            if (skip_count > 0) {
                batches.erase(batches.begin(), batches.begin() + static_cast<std::ptrdiff_t>(skip_count));
            }
        }

        // Maintain the operator_data_t invariant: at least one (possibly empty)
        // chunk. storage_scan_batched can return an empty vector when the disk
        // service get_storage(table_oid) hits an oid-resolution race (observed
        // at SSB-scale on comma-join cross-products). Without this guard,
        // operator_join.cpp:125 asserts. Fetch types only on the empty path so
        // the steady-state scan keeps a single async round-trip.
        if (batches.empty()) {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            auto tbl_types = co_await std::move(tf);
            std::pmr::vector<types::complex_logical_type> projected_types(resource_);
            if (projected_cols_.empty()) {
                projected_types = tbl_types;
            } else {
                projected_types.reserve(projected_cols_.size());
                for (auto idx : projected_cols_) {
                    if (idx < tbl_types.size()) {
                        projected_types.push_back(tbl_types[idx]);
                    }
                }
            }
            batches.emplace_back(resource_, projected_types, 0);
        }

        output_ = make_operator_data(resource_, std::move(batches));
        mark_executed();
        co_return;
    }

} // namespace components::operators