#include "operator_resolve_type.hpp"

#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace components::operators {

    namespace catalog = components::catalog;
    namespace types = components::types;

    operator_resolve_type_t::operator_resolve_type_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     components::logical_plan::node_catalog_resolve_t* node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type)
        , node_(node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("type_oid");
    }

    actor_zeta::unique_future<void> operator_resolve_type_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgType = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        for (auto& entry : node_->entries()) {
            // Resolve the entry's namespace first. "public" / "pg_catalog" are
            // well-known constants and don't need a disk roundtrip; arbitrary names
            // go through pg_namespace.
            auto namespace_oid = catalog::INVALID_OID;
            if (entry.dbname == "public" || entry.dbname.empty()) {
                namespace_oid = catalog::well_known_oid::public_namespace;
            } else if (entry.dbname == "pg_catalog") {
                namespace_oid = catalog::well_known_oid::pg_catalog_namespace;
            } else if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<std::string> ns_keys(resource_);
                ns_keys.emplace_back("nspname");
                auto [_n, nf] =
                    actor_zeta::send(ctx->disk_address,
                                     &services::disk::manager_disk_t::read_chunks_by_key,
                                     exec_ctx,
                                     kPgNamespace,
                                     std::move(ns_keys),
                                     components::operators::make_key_chunk(resource_, std::string_view{entry.dbname}));
                auto ns_batches = co_await std::move(nf);
                if (!ns_batches.empty() && ns_batches[0].size() != 0 && ns_batches[0].column_count() >= 1 &&
                    !ns_batches[0].is_null(0, 0)) {
                    namespace_oid = static_cast<catalog::oid_t>(ns_batches[0].get_value<std::uint32_t>(0, 0));
                }
            }
            if (namespace_oid == catalog::INVALID_OID || ctx->disk_address == actor_zeta::address_t::empty_address()) {
                continue;
            }

            // pg_type columns, by the order system_table_schemas.cpp persists them:
            //   0 oid, 1 typname, 2 typnamespace, 3 typdefspec.
            std::pmr::vector<std::string> typ_keys(resource_);
            typ_keys.emplace_back("typname");
            typ_keys.emplace_back("typnamespace");
            auto [_t, tf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_chunks_by_key,
                exec_ctx,
                kPgType,
                std::move(typ_keys),
                components::operators::make_key_chunk(resource_, std::string_view{entry.type_name}, namespace_oid));
            auto batches = co_await std::move(tf);

            // A miss leaves the entry at INVALID_OID with no type_md — that is how
            // "type is not registered" is reported.
            if (batches.empty() || batches.front().size() == 0 || batches.front().column_count() < 4) {
                continue;
            }
            const auto& batch = batches.front();
            if (batch.is_null(0, 0) || batch.is_null(1, 0) || batch.is_null(2, 0)) {
                continue;
            }

            components::logical_plan::resolved_type_metadata_t md;
            md.type_oid = static_cast<catalog::oid_t>(batch.get_value<std::uint32_t>(0, 0));
            md.name = std::string(batch.get_value<std::string_view>(1, 0));
            md.namespace_oid = static_cast<catalog::oid_t>(batch.get_value<std::uint32_t>(2, 0));
            if (!batch.is_null(3, 0)) {
                md.typdefspec = std::string(batch.get_value<std::string_view>(3, 0));
            }
            if (!md.typdefspec.empty()) {
                md.type = catalog::decode_type_spec(resource_, md.typdefspec);
            } else {
                const auto lt = catalog::oid_to_builtin_type(md.type_oid);
                if (lt != types::logical_type::UNKNOWN) {
                    md.type = types::complex_logical_type{lt};
                }
            }
            entry.type_oid = md.type_oid;
            entry.type_md = std::move(md);
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
