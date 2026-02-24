#include "primary_key_scan.hpp"

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    primary_key_scan::primary_key_scan(std::pmr::memory_resource* resource, collection_full_name_t name)
        : read_only_operator_t(resource, log_t{}, operator_type::primary_key_scan)
        , name_(std::move(name))
        , rows_(resource, types::logical_type::BIGINT) {}

    void primary_key_scan::append(size_t id) {
        rows_.set_value(size_++, types::logical_value_t(resource(), static_cast<int64_t>(id)));
    }

    void primary_key_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (name_.empty() || size_ == 0)
            return;
        async_wait();
    }

    actor_zeta::unique_future<void> primary_key_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (size_ > 0) {
            // Copy rows vector for send
            vector::vector_t row_ids_copy(resource_, types::logical_type::BIGINT, size_);
            for (size_t i = 0; i < size_; i++) {
                row_ids_copy.set_value(i, types::logical_value_t{resource_, rows_.data<int64_t>()[i]});
            }

            auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_fetch,
                                             ctx->session,
                                             name_,
                                             std::move(row_ids_copy),
                                             size_);
            auto data = co_await std::move(ff);

            if (data) {
                output_ = make_operator_data(resource_, std::move(*data));
            } else {
                auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_types,
                                                 ctx->session,
                                                 name_);
                auto types = co_await std::move(tf);
                output_ = make_operator_data(resource_, types);
            }
        } else {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             name_);
            auto types = co_await std::move(tf);
            output_ = make_operator_data(resource_, types);
        }

        mark_executed();
        co_return;
    }

} // namespace components::operators
