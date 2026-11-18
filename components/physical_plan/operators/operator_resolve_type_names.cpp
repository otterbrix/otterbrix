#include "operator_resolve_type_names.hpp"

#include "catalog_name_lookup.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/place.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;
    namespace types = components::types;

    operator_resolve_type_names_t::operator_resolve_type_names_t(std::pmr::memory_resource* resource,
                                                                 log_t log,
                                                                 components::logical_plan::node_catalog_resolve_t* node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type_names)
        , node_(node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("type_oid");
    }

    namespace {

        // PostgreSQL 18 quote_identifier, for the names the catalog keeps (lower case unless written quoted): one
        // that is not lower-case letters, digits and underscores after a letter or underscore is double-quoted.
        std::string quoted_identifier(std::string_view name) {
            bool safe = !name.empty() && ((name.front() >= 'a' && name.front() <= 'z') || name.front() == '_');
            for (const char symbol : name) {
                safe = safe && ((symbol >= 'a' && symbol <= 'z') || (symbol >= '0' && symbol <= '9') || symbol == '_');
            }
            if (safe) {
                return std::string{name};
            }
            std::string out{"\""};
            for (const char symbol : name) {
                out.push_back(symbol);
                if (symbol == '"') {
                    out.push_back('"');
                }
            }
            out.push_back('"');
            return out;
        }

    } // namespace

    actor_zeta::unique_future<void> operator_resolve_type_names_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            const components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            for (auto& entry : node_->entries()) {
                // An earlier pass resolved it: pg_catalog's types are there whatever the place.
                if (!entry.type_names.empty()) {
                    continue;
                }
                // The place: the database whose schemas are seen, and the one schema seen without a qualifier.
                auto place = co_await lookup_schema(resource_, ctx, catalog::complete_name(ctx->place, {}, {}, {}));
                if (place.has_error()) {
                    set_error(place.error());
                    co_return;
                }
                std::unordered_map<catalog::oid_t, std::string> schemas;
                std::pmr::vector<catalog::oid_t> namespace_oids(resource_);
                schemas.emplace(catalog::well_known_oid::pg_catalog_namespace, "pg_catalog");
                namespace_oids.push_back(catalog::well_known_oid::pg_catalog_namespace);
                if (place.value().database_oid != catalog::INVALID_OID) {
                    std::pmr::vector<std::uint64_t> keys(resource_);
                    keys.emplace_back(catalog::pg_namespace_col::nspdatabase);
                    auto [_n, nf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                                exec_ctx,
                                                                catalog::well_known_oid::pg_namespace_table,
                                                                std::move(keys),
                                                                make_key_chunk(resource_, place.value().database_oid),
                                                                std::pmr::vector<std::uint64_t>{resource_});
                    auto namespaces = co_await std::move(nf);
                    if (namespaces.has_error()) {
                        set_error(namespaces.error());
                        co_return;
                    }
                    for (const auto& chunk : namespaces.value()) {
                        for (uint64_t row = 0; row < chunk.size(); ++row) {
                            const auto oid = chunk.get_value<catalog::oid_t>(catalog::pg_namespace_col::oid, row);
                            if (schemas
                                    .emplace(
                                        oid,
                                        std::string{
                                            chunk.get_value<std::string_view>(catalog::pg_namespace_col::nspname, row)})
                                    .second) {
                                namespace_oids.push_back(oid);
                            }
                        }
                    }
                }

                std::pmr::vector<std::uint64_t> keys(resource_);
                keys.emplace_back(catalog::pg_type_col::typnamespace);
                auto [_t, tf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::read_chunks_by_keys,
                                                            exec_ctx,
                                                            catalog::well_known_oid::pg_type_table,
                                                            std::move(keys),
                                                            make_keys_batch(resource_, namespace_oids),
                                                            std::pmr::vector<std::uint64_t>{resource_});
                auto found = co_await std::move(tf);
                if (found.has_error()) {
                    set_error(found.error());
                    co_return;
                }
                const auto place_schema = place.value().namespace_oid;
                for (const auto& batches : found.value()) {
                    for (const auto& chunk : batches) {
                        for (uint64_t row = 0; row < chunk.size(); ++row) {
                            namespace col = catalog::pg_type_col;
                            const auto type_namespace = chunk.get_value<catalog::oid_t>(col::typnamespace, row);
                            const std::string type_name =
                                quoted_identifier(chunk.get_value<std::string_view>(col::typname, row));
                            const bool visible = type_namespace == catalog::well_known_oid::pg_catalog_namespace ||
                                                 type_namespace == place_schema;
                            components::logical_plan::resolved_type_name_t named;
                            named.oid = chunk.get_value<catalog::oid_t>(col::oid, row);
                            named.name =
                                visible ? type_name : quoted_identifier(schemas[type_namespace]) + "." + type_name;
                            const auto element = chunk.get_value<catalog::oid_t>(col::typelem, row);
                            if (element != catalog::INVALID_OID &&
                                chunk.get_value<std::string_view>(col::typcategory, row) == "A") {
                                named.element = element;
                            }
                            entry.type_names.push_back(std::move(named));
                        }
                    }
                }
            }
        }

        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
