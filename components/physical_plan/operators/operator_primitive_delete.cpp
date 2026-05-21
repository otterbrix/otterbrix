#include "operator_primitive_delete.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_primitive_delete_t::operator_primitive_delete_t(std::pmr::memory_resource* resource,
                                                             log_t log,
                                                             components::catalog::oid_t catalog_table_oid,
                                                             std::int64_t oid_col_idx,
                                                             components::catalog::oid_t target_oid)
        : read_write_operator_t(resource, std::move(log), operator_type::primitive_delete)
        , catalog_table_oid_(catalog_table_oid)
        , oid_col_idx_(oid_col_idx)
        , target_oid_(target_oid) {}

    void operator_primitive_delete_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_primitive_delete_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                         exec_ctx,
                                         catalog_table_oid_,
                                         oid_col_idx_,
                                         target_oid_);
        co_await std::move(fut);
        if (ctx->txn.transaction_id != 0)
            ctx->pg_catalog_delete_tables.insert(catalog_table_oid_);
        mark_executed();
    }

} // namespace components::operators
