#include "operator_resolve_type.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;
    namespace types   = components::types;

    operator_resolve_type_t::operator_resolve_type_t(std::pmr::memory_resource* resource,
                                                       log_t                      log,
                                                       catalog::oid_t             namespace_oid,
                                                       std::string                name)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type)
        , namespace_oid_(namespace_oid)
        , name_(std::move(name)) {}

    operator_resolve_type_t::operator_resolve_type_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string dbname,
        std::string name,
        components::logical_plan::node_catalog_resolve_type_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type)
        , namespace_oid_(catalog::INVALID_OID)
        , dbname_(std::move(dbname))
        , name_(std::move(name))
        , target_node_(target_node) {}

    void operator_resolve_type_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_resolve_type_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgType      = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(4);
        out_types.emplace_back(types::logical_type::UINTEGER);       // oid
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // typname
        out_types.emplace_back(types::logical_type::UINTEGER);       // typnamespace
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // typdefspec

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // M4.F: dbname-form (back-pointer ctor) — resolve namespace_oid from
        // dbname before pg_type scan. "public" / "pg_catalog" are well-known
        // constants and don't need a disk roundtrip; arbitrary names go
        // through pg_namespace.
        if (target_node_ && namespace_oid_ == catalog::INVALID_OID) {
            if (dbname_ == "public" || dbname_.empty()) {
                namespace_oid_ = catalog::well_known_oid::public_namespace;
            } else if (dbname_ == "pg_catalog") {
                namespace_oid_ = catalog::well_known_oid::pg_catalog_namespace;
            } else if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                types::logical_value_t db_lv(resource_, std::string_view{dbname_});
                auto [_n, nf] = actor_zeta::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::read_rows_by_key,
                    exec_ctx, kPgNamespace,
                    std::vector<std::string>{"nspname"},
                    std::vector<types::logical_value_t>{db_lv});
                auto ns_rows = co_await std::move(nf);
                if (!ns_rows.empty() && !ns_rows[0].empty() && !ns_rows[0][0].is_null()) {
                    namespace_oid_ = static_cast<catalog::oid_t>(
                        ns_rows[0][0].value<std::uint32_t>());
                }
            }
        }

        vector::data_chunk_t chunk(resource_, out_types, /*capacity=*/1);

        if (namespace_oid_ != catalog::INVALID_OID &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            types::logical_value_t name_lv(resource_, std::string_view{name_});
            types::logical_value_t ns_lv(resource_, namespace_oid_);
            auto [_t, tf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgType,
                std::vector<std::string>{"typname", "typnamespace"},
                std::vector<types::logical_value_t>{name_lv, ns_lv});
            auto rows = co_await std::move(tf);

            if (!rows.empty() && rows.front().size() >= 4) {
                const auto& row = rows.front();
                if (!row[0].is_null() && !row[1].is_null() && !row[2].is_null()) {
                    chunk.set_value(0, 0, row[0]);
                    chunk.set_value(1, 0, row[1]);
                    chunk.set_value(2, 0, row[2]);
                    if (!row[3].is_null()) {
                        chunk.set_value(3, 0, row[3]);
                    } else {
                        chunk.set_value(3, 0, types::logical_value_t(resource_, std::string{}));
                    }
                    chunk.set_cardinality(1);

                    // M4.F: stamp resolved metadata on the logical node so
                    // enrich / validate / resolve_type.cpp can read it via
                    // plan_resolve_index.
                    if (target_node_) {
                        components::logical_plan::resolved_type_metadata_t md;
                        md.type_oid = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                        md.namespace_oid = static_cast<catalog::oid_t>(row[2].value<std::uint32_t>());
                        md.name = std::string(row[1].value<std::string_view>());
                        if (!row[3].is_null()) {
                            md.typdefspec = std::string(row[3].value<std::string_view>());
                        }
                        if (!md.typdefspec.empty()) {
                            md.type = catalog::decode_type_spec(resource_, md.typdefspec);
                        } else {
                            const auto lt = catalog::oid_to_builtin_type(md.type_oid);
                            if (lt != types::logical_type::UNKNOWN) {
                                md.type = types::complex_logical_type{lt};
                            }
                        }
                        target_node_->set_type_oid(md.type_oid);
                        target_node_->set_resolved_metadata(std::move(md));
                    }
                }
            }
        }

        set_output(make_operator_data(resource_, std::move(chunk)));
        mark_executed();
    }

} // namespace components::operators