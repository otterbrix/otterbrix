#include "operator_create_index_metadata.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <vector>

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

    actor_zeta::unique_future<void> operator_create_index_metadata_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Two-phase: every append is independent (no iteration consumes the
        // previous result), so send all rows first then await in order.
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        std::pmr::vector<actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>>
            append_futures(resource_);
        append_futures.reserve(catalog_writes_.size());
        for (auto& [tbl, row] : catalog_writes_) {
            auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             tbl,
                                             std::move(row));
            append_futures.push_back(std::move(fut));
        }
        // Drain all, first error wins. A pg_index row that was refused means CREATE INDEX
        // built nothing the catalog can find, so the statement must fail rather than leave a
        // backfill running against metadata that does not exist.
        core::error_t append_error = core::error_t::no_error();
        for (auto& fut : append_futures) {
            auto rng_r = co_await std::move(fut);
            if (rng_r.has_error()) {
                if (!append_error.contains_error()) {
                    append_error = rng_r.error();
                }
                continue;
            }
            if (rng_r.value().count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
        }
        if (append_error.contains_error()) {
            set_error(std::move(append_error));
            mark_failed();
            co_return;
        }
        mark_executed();
    }

} // namespace components::operators
