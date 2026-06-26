#include "primary_key_scan.hpp"

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    primary_key_scan::primary_key_scan(std::pmr::memory_resource* resource, components::catalog::oid_t table_oid)
        : read_only_operator_t(resource, log_t{}, operator_type::primary_key_scan)
        , table_oid_(table_oid)
        , rows_(resource, types::logical_type::BIGINT) {}

    void primary_key_scan::append(size_t id) { rows_.set_value<int64_t>(size_++, static_cast<int64_t>(id)); }

    void primary_key_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (table_oid_ == components::catalog::INVALID_OID || size_ == 0)
            return;
        async_wait();
    }

    actor_zeta::unique_future<void> primary_key_scan::await_async_and_resume(pipeline::context_t* ctx) {
        // One fetch for all row-ids; the disk layer returns the rows as a vector of
        // ≤DEFAULT_VECTOR_CAPACITY chunks.
        vector::vector_t ids(resource_, types::logical_type::BIGINT, size_);
        for (size_t i = 0; i < size_; i++) {
            ids.set_value(i, rows_.data<int64_t>()[i]);
        }
        auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_fetch,
                                         ctx->session,
                                         table_oid_,
                                         std::move(ids),
                                         size_);
        auto batches = co_await std::move(ff);

        if (!batches.empty()) {
            output_ = make_operator_data(resource_, std::move(batches));
        } else {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            auto types = co_await std::move(tf);
            output_ = make_operator_data(resource_, types);
        }

        mark_executed();
        co_return;
    }

} // namespace components::operators
