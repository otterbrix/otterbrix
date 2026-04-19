#include "transfer_scan.hpp"

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 collection_full_name_t name,
                                 logical_plan::limit_t limit)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , name_(std::move(name))
        , limit_(limit) {}

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
                                         &services::disk::manager_disk_t::storage_scan,
                                         ctx->session,
                                         name_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         scan_limit,
                                         ctx->txn);
        auto data = co_await std::move(sf);

        if (data) {
            if (offset_val > 0 && static_cast<uint64_t>(offset_val) < data->size()) {
                *data = data->partial_copy(resource_,
                                           static_cast<uint64_t>(offset_val),
                                           data->size() - static_cast<uint64_t>(offset_val));
            } else if (offset_val > 0) {
                data->set_cardinality(0);
            }
            output_ = make_operator_data(resource_, std::move(*data));
        } else {
            output_ = make_operator_data(resource_, std::pmr::vector<types::complex_logical_type>{resource_});
        }
        mark_executed();
        co_return;
    }

} // namespace components::operators
