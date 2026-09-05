#include "operator_create_matview.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <vector>

namespace components::operators {

    operator_create_matview_t::operator_create_matview_t(std::pmr::memory_resource* resource,
                                                         log_t log,
                                                         components::catalog::oid_t mv_oid,
                                                         components::catalog::oid_t namespace_oid,
                                                         std::vector<table::column_definition_t> columns,
                                                         std::vector<catalog_write_t> catalog_writes)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , mv_oid_(mv_oid)
        , namespace_oid_(namespace_oid)
        , columns_(std::move(columns))
        , catalog_writes_(std::move(catalog_writes)) {}

    actor_zeta::unique_future<void> operator_create_matview_t::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

        // Create physical heap storage: always disk-backed (plan-gen guarantees
        // non-empty inferred columns for a matview — create_plan_create_matview
        // refuses an empty set). A matview is relkind='m', NEVER computed — passed
        // explicitly so even a degenerate zero-column matview cannot come up as a
        // dynamic-schema table.
        {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_disk,
                                           ctx->session,
                                           mv_oid_,
                                           namespace_oid_,
                                           std::move(columns_),
                                           /*is_computed=*/false);
            co_await std::move(f);
        }

        // CREATE back-channel: record the matview's heap storage oid so COMMIT
        // publishes it and a same-txn ABORT drops it (mirror of the
        // operator_create_collection back-channel; same non-zero-txn gate).
        if (ctx->txn.transaction_id != 0) {
            ctx->created_storage_oids.push_back(mv_oid_);
        }

        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_, f] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::register_collection,
                                           ctx->session,
                                           mv_oid_);
            co_await std::move(f);
        }

        // Write pg_catalog rows (pg_class + pg_attribute + pg_rewrite + pg_depend).
        // Two-phase: every append is independent (no iteration consumes the
        // previous result), so send all rows first then await in order.
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        std::pmr::vector<actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>>
            append_futures(resource_);
        append_futures.reserve(catalog_writes_.size());
        for (auto& [tbl_oid, row] : catalog_writes_) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::append_pg_catalog_row,
                                           exec_ctx,
                                           tbl_oid,
                                           std::move(row));
            append_futures.push_back(std::move(f));
        }
        // EVERY future is drained before the first refusal is acted on: abandoning the
        // rest would leave replies for a statement that no longer exists. First error wins.
        core::error_t append_error = core::error_t::no_error();
        for (auto& f : append_futures) {
            auto rng_r = co_await std::move(f);
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
            // A catalog row that never landed leaves the view existing in name only.
            set_error(std::move(append_error));
            mark_failed();
            co_return;
        }

        // Created empty. This operator only ever implements WITH NO DATA — the
        // transformer refuses the form that would need populating — so there is
        // nothing left to do here. See operator_create_matview.hpp.
        mark_executed();
    }

} // namespace components::operators
