#include "operator_create_index_metadata.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_create_index_metadata_t::operator_create_index_metadata_t(std::pmr::memory_resource* resource,
                                                                       log_t log,
                                                                       std::vector<catalog_write_t> catalog_writes)
        // Reuse operator_type::create_collection because the executor's
        // generic-DDL path already treats these write-only operators correctly
        // (no scan/dml side-effects, root output is a success cursor). Adding a
        // dedicated enum entry would require touching the executor switch and
        // every is_dml-style helper; the type is internally informational.
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , catalog_writes_(std::move(catalog_writes)) {}

    void operator_create_index_metadata_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_create_index_metadata_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        for (auto& [tbl, row] : catalog_writes_) {
            auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             tbl,
                                             std::move(row));
            auto rng = co_await std::move(fut);
            if (rng.count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng));
        }
        mark_executed();
    }

} // namespace components::operators
