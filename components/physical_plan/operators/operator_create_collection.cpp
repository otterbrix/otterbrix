#include "operator_create_collection.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

namespace components::operators {

    operator_create_collection_t::operator_create_collection_t(
        std::pmr::memory_resource*             resource,
        log_t                                  log,
        collection_full_name_t                 collection,
        std::vector<table::column_definition_t> columns,
        bool                                   is_disk_storage,
        std::vector<catalog_write_t>           catalog_writes)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , collection_(std::move(collection))
        , columns_(std::move(columns))
        , is_disk_storage_(is_disk_storage)
        , catalog_writes_(std::move(catalog_writes)) {}

    void operator_create_collection_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_create_collection_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Step 1: Create physical storage
        if (columns_.empty()) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage,
                                           ctx->session, collection_);
            co_await std::move(f);
        } else if (is_disk_storage_) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_disk,
                                           ctx->session, collection_, std::move(columns_));
            co_await std::move(f);
        } else {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::create_storage_with_columns,
                                           ctx->session, collection_, std::move(columns_));
            co_await std::move(f);
        }

        // Step 2: Register with index manager
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_, f] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::register_collection,
                                           ctx->session, collection_);
            co_await std::move(f);
        }

        // Step 3: Write pg_catalog rows (pg_class, pg_attribute, pg_depend)
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        for (auto& [tbl, row] : catalog_writes_) {
            auto [_, f] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::append_pg_catalog_row,
                                           exec_ctx, tbl, std::move(row));
            auto rng = co_await std::move(f);
            if (rng.count > 0) ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        mark_executed();
    }

} // namespace components::operators