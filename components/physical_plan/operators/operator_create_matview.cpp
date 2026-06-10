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
                                                         bool is_disk_storage,
                                                         std::vector<catalog_write_t> catalog_writes,
                                                         operator_ptr body_op)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , mv_oid_(mv_oid)
        , namespace_oid_(namespace_oid)
        , columns_(std::move(columns))
        , is_disk_storage_(is_disk_storage)
        , catalog_writes_(std::move(catalog_writes))
        , body_op_(std::move(body_op)) {}

    void operator_create_matview_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_create_matview_t::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

        // Create physical heap storage.
        if (columns_.empty()) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage,
                                           ctx->session,
                                           mv_oid_,
                                           namespace_oid_);
            co_await std::move(f);
        } else if (is_disk_storage_) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_disk,
                                           ctx->session,
                                           mv_oid_,
                                           namespace_oid_,
                                           std::move(columns_));
            co_await std::move(f);
        } else {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_with_columns,
                                           ctx->session,
                                           mv_oid_,
                                           namespace_oid_,
                                           std::move(columns_));
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
        std::pmr::vector<actor_zeta::unique_future<components::pg_catalog_append_range_t>> append_futures(resource_);
        append_futures.reserve(catalog_writes_.size());
        for (auto& [tbl_oid, row] : catalog_writes_) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::append_pg_catalog_row,
                                           exec_ctx,
                                           tbl_oid,
                                           std::move(row));
            append_futures.push_back(std::move(f));
        }
        for (auto& f : append_futures) {
            auto rng = co_await std::move(f);
            if (rng.count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        // Created empty (WITH NO DATA semantics): driving body_op_ here to
        // populate hits an actor_zeta nested-await bug (full_scan crashes when
        // driven from inside this operator's outer await). REFRESH MATERIALIZED
        // VIEW populates later via its own INSERT-SELECT plan. body_op_ holds
        // the compiled body plan but is intentionally not driven here.
        (void) body_op_;
        mark_executed();
    }

} // namespace components::operators
