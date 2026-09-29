#include "operator_resolve_server.hpp"

#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string_view>

namespace components::operators {

    namespace catalog = components::catalog;

    actor_zeta::unique_future<core::result_wrapper_t<foreign_server_row_t>>
    read_foreign_server(std::pmr::memory_resource* resource,
                        actor_zeta::address_t disk_address,
                        components::execution_context_t exec_ctx,
                        std::string name) {
        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(catalog::pg_foreign_server_col::srvname);
        auto [_s, sf] = actor_zeta::otterbrix::send(disk_address,
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    exec_ctx,
                                                    catalog::well_known_oid::pg_foreign_server_table,
                                                    std::move(keys),
                                                    make_key_chunk(resource, std::string_view{name}),
                                                    std::pmr::vector<std::uint64_t>{resource});
        auto batches_r = co_await std::move(sf);
        if (batches_r.has_error()) {
            co_return batches_r.error();
        }
        foreign_server_row_t row;
        for (const auto& batch : batches_r.value()) {
            if (batch.size() != 0 && batch.column_count() > catalog::pg_foreign_server_col::srvtype &&
                !batch.is_null(catalog::pg_foreign_server_col::oid, 0) &&
                !batch.is_null(catalog::pg_foreign_server_col::srvtype, 0)) {
                row.oid =
                    static_cast<catalog::oid_t>(batch.get_value<std::uint32_t>(catalog::pg_foreign_server_col::oid, 0));
                row.type.assign(batch.get_value<std::string_view>(catalog::pg_foreign_server_col::srvtype, 0));
                break;
            }
        }
        co_return row;
    }

    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    read_foreign_namespace(std::pmr::memory_resource* resource,
                           actor_zeta::address_t disk_address,
                           components::execution_context_t exec_ctx,
                           catalog::oid_t server_oid,
                           std::string remote_db,
                           std::string remote_schema) {
        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(catalog::pg_foreign_namespace_col::nspserver);
        keys.emplace_back(catalog::pg_foreign_namespace_col::nspdb);
        keys.emplace_back(catalog::pg_foreign_namespace_col::nspname);
        auto [_n, nf] = actor_zeta::otterbrix::send(
            disk_address,
            &services::disk::manager_disk_t::read_chunks_by_key,
            exec_ctx,
            catalog::well_known_oid::pg_foreign_namespace_table,
            std::move(keys),
            make_key_chunk(resource,
                           static_cast<std::uint32_t>(server_oid),
                           std::string_view{remote_db},
                           std::string_view{remote_schema}),
            std::pmr::vector<std::uint64_t>{resource});
        auto batches_r = co_await std::move(nf);
        if (batches_r.has_error()) {
            co_return batches_r.error();
        }
        for (const auto& batch : batches_r.value()) {
            if (batch.size() != 0 && batch.column_count() > catalog::pg_foreign_namespace_col::oid &&
                !batch.is_null(catalog::pg_foreign_namespace_col::oid, 0)) {
                co_return static_cast<catalog::oid_t>(
                    batch.get_value<std::uint32_t>(catalog::pg_foreign_namespace_col::oid, 0));
            }
        }
        co_return catalog::INVALID_OID;
    }

    operator_resolve_server_t::operator_resolve_server_t(std::pmr::memory_resource* resource,
                                                         log_t log,
                                                         components::logical_plan::node_catalog_resolve_t* node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_server)
        , node_(node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("server_oid");
    }

    actor_zeta::unique_future<void> operator_resolve_server_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        for (auto& entry : node_->entries()) {
            if (ctx->disk_address == actor_zeta::address_t::empty_address() || entry.relname.empty()) {
                continue;
            }
            auto server_r = co_await read_foreign_server(resource_, ctx->disk_address, exec_ctx, entry.relname);
            if (server_r.has_error()) {
                set_error(server_r.error());
                co_return;
            }
            entry.server_oid = server_r.value().oid;
            entry.server_type = std::move(server_r.value().type);
        }

        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
