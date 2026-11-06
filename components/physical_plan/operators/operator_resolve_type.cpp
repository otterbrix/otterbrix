#include "operator_resolve_type.hpp"

#include "catalog_name_lookup.hpp"
#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
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

    namespace {
        // pg_type columns, by the order system_table_schemas.cpp persists them:
        //   0 oid, 1 typname, 2 typnamespace, 3 typdefspec.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>
        read_pg_type(std::pmr::memory_resource* resource,
                     pipeline::context_t* ctx,
                     catalog::oid_t namespace_oid,
                     std::string type_name) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            std::pmr::vector<std::uint64_t> typ_keys(resource);
            typ_keys.emplace_back(catalog::pg_type_col::typname);
            typ_keys.emplace_back(catalog::pg_type_col::typnamespace);
            auto [_t, tf] =
                actor_zeta::otterbrix::send(ctx->disk_address,
                                            &services::disk::manager_disk_t::read_chunks_by_key,
                                            exec_ctx,
                                            catalog::well_known_oid::pg_type_table,
                                            std::move(typ_keys),
                                            make_key_chunk(resource, std::string_view{type_name}, namespace_oid),
                                            std::pmr::vector<std::uint64_t>{resource});
            co_return co_await std::move(tf);
        }

        bool holds_a_type(const std::pmr::vector<vector::data_chunk_t>& batches) {
            return !batches.empty() && batches.front().size() != 0 && batches.front().column_count() >= 4;
        }
    } // namespace

    actor_zeta::unique_future<void> operator_resolve_type_t::await_async_and_resume(pipeline::context_t* ctx) {
        for (auto& entry : node_->entries()) {
            if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
                break;
            }
            if (entry.type_md.has_value()) {
                continue;
            }
            std::pmr::vector<vector::data_chunk_t> batches{resource_};
            // A bare type name looks in pg_catalog before the place, as a bare relation name does.
            if (entry.database.empty() && entry.schema.empty()) {
                auto in_catalog = co_await read_pg_type(resource_,
                                                        ctx,
                                                        catalog::well_known_oid::pg_catalog_namespace,
                                                        entry.type_name);
                if (in_catalog.has_error()) {
                    set_error(in_catalog.error());
                    co_return;
                }
                batches = std::move(in_catalog.value());
            }
            if (!holds_a_type(batches)) {
                auto schema =
                    co_await lookup_schema(resource_,
                                           ctx,
                                           catalog::complete_name(ctx->place, {}, entry.database, entry.schema));
                if (schema.has_error()) {
                    // A failed pg_namespace read is not a miss; saying "not found" here hides it.
                    set_error(schema.error());
                    co_return;
                }
                entry.database_oid = schema.value().database_oid;
                entry.namespace_oid = schema.value().namespace_oid;
                if (entry.namespace_oid == catalog::INVALID_OID) {
                    continue;
                }
                auto in_schema = co_await read_pg_type(resource_, ctx, entry.namespace_oid, entry.type_name);
                if (in_schema.has_error()) {
                    set_error(in_schema.error());
                    co_return;
                }
                batches = std::move(in_schema.value());
            }

            // A miss leaves the entry at INVALID_OID with no type_md — that is how
            // "type is not registered" is reported.
            if (!holds_a_type(batches)) {
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
                auto md_type_r = catalog::decode_type_spec(resource_, md.typdefspec);
                if (md_type_r.has_error()) {
                    // An unreadable typdefspec is catalog corruption; answering a guessed
                    // type would let every use of this UDT reinterpret bytes silently.
                    set_error(md_type_r.error());
                    co_return;
                }
                md.type = std::move(md_type_r.value());
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
