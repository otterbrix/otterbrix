#include "operator_primitive_write.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_primitive_write_t::operator_primitive_write_t(std::pmr::memory_resource* resource,
                                                           log_t log,
                                                           components::catalog::oid_t catalog_table_oid,
                                                           vector::data_chunk_t row)
        : read_write_operator_t(resource, std::move(log), operator_type::primitive_write)
        , catalog_table_oid_(catalog_table_oid)
        , row_(std::move(row)) {}

    void operator_primitive_write_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_primitive_write_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         catalog_table_oid_,
                                         std::move(row_));
        auto rng = co_await std::move(fut);
        if (rng.count > 0)
            ctx->pg_catalog_appends.push_back(std::move(rng));
        mark_executed();
    }

} // namespace components::operators
