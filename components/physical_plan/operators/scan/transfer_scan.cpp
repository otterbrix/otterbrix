#include "transfer_scan.hpp"

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 collection_full_name_t name,
                                 logical_plan::limit_t limit)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , name_(std::move(name))
        , limit_(limit) {}

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 collection_full_name_t name,
                                 logical_plan::limit_t limit,
                                 std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , name_(std::move(name))
        , limit_(limit)
        , projected_cols_(std::move(projected_cols)) {}

    void transfer_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (name_.empty())
            return;
        async_wait();
    }

    actor_zeta::unique_future<void> transfer_scan::await_async_and_resume(pipeline::context_t* ctx) {
        int64_t offset_val = limit_.offset();
        int64_t limit_val = limit_.limit();
        int64_t scan_limit = (limit_val < 0) ? limit_val : limit_val + offset_val;
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan_batched,
                                         ctx->session,
                                         name_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         scan_limit,
                                         projected_cols_,
                                         ctx->txn);
        auto batches = co_await std::move(sf);

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

        output_ = make_operator_data(resource_, std::move(batches));
        mark_executed();
        co_return;
    }

} // namespace components::operators
