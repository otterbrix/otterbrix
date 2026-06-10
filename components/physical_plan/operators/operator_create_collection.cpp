#include "operator_create_collection.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <vector>

namespace components::operators {

    operator_create_collection_t::operator_create_collection_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               components::catalog::oid_t table_oid,
                                                               components::catalog::oid_t database_oid,
                                                               std::vector<table::column_definition_t> columns,
                                                               bool is_disk_storage,
                                                               std::vector<catalog_write_t> catalog_writes)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , table_oid_(table_oid)
        , database_oid_(database_oid)
        , columns_(std::move(columns))
        , is_disk_storage_(is_disk_storage)
        , catalog_writes_(std::move(catalog_writes)) {}

    void operator_create_collection_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_create_collection_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (columns_.empty()) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage,
                                           ctx->session,
                                           table_oid_,
                                           database_oid_);
            co_await std::move(f);
        } else if (is_disk_storage_) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_disk,
                                           ctx->session,
                                           table_oid_,
                                           database_oid_,
                                           std::move(columns_));
            co_await std::move(f);
        } else {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_with_columns,
                                           ctx->session,
                                           table_oid_,
                                           database_oid_,
                                           std::move(columns_));
            co_await std::move(f);
        }

        // CREATE back-channel: record the storage oid this statement brought into
        // being so the COMMIT can publish it and a same-txn ABORT can drop it (a
        // CREATE inside a txn must be revertible until COMMIT). Mirror of the
        // operator_dynamic_cascade_delete drop back-channel; gated on a non-zero
        // txn id (autocommit/bootstrap txn 0 publishes inline, never accumulates).
        if (ctx->txn.transaction_id != 0) {
            ctx->created_storage_oids.push_back(table_oid_);
        }

        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_, f] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::register_collection,
                                           ctx->session,
                                           table_oid_);
            co_await std::move(f);
        }

        // Write pg_catalog rows (pg_class, pg_attribute, pg_depend).
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

        mark_executed();
    }

} // namespace components::operators
