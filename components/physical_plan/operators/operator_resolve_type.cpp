#include "operator_resolve_type.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;
    namespace types = components::types;

    namespace {
        // File-local typed setters — pg_type schema is statically known here,
        // so we bypass the logical_value_t variant dispatch and write directly
        // into the column's typed buffer.
        inline void set_uint32(vector::data_chunk_t& c, size_t col, size_t row, std::uint32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::uint32_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void
        set_str(vector::data_chunk_t& c, size_t col, size_t row, std::string_view v, std::pmr::memory_resource* r) {
            auto& vec = c.data[col];
            if (!vec.auxiliary()) {
                vec.set_auxiliary(std::make_shared<vector::string_vector_buffer_t>(r));
            }
            auto* sb = static_cast<vector::string_vector_buffer_t*>(vec.auxiliary().get());
            auto* ptr = sb->insert(v);
            reinterpret_cast<std::string_view*>(vec.template data<std::byte>())[row] =
                std::string_view(static_cast<const char*>(ptr), v.size());
            vec.validity().set(row, true);
        }
    } // namespace

    operator_resolve_type_t::operator_resolve_type_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     catalog::oid_t namespace_oid,
                                                     std::string name)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type)
        , namespace_oid_(namespace_oid)
        , name_(std::move(name)) {}

    operator_resolve_type_t::operator_resolve_type_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     std::string dbname,
                                                     std::string name,
                                                     components::logical_plan::node_catalog_resolve_type_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type)
        , namespace_oid_(catalog::INVALID_OID)
        , dbname_(std::move(dbname))
        , name_(std::move(name))
        , target_node_(target_node) {}

    void operator_resolve_type_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_resolve_type_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgType = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(4);
        out_types.emplace_back(types::logical_type::UINTEGER);       // oid
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // typname
        out_types.emplace_back(types::logical_type::UINTEGER);       // typnamespace
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // typdefspec

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Dbname-form (back-pointer ctor) — resolve namespace_oid from
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
                std::pmr::vector<std::string> ns_keys(resource_);
                ns_keys.emplace_back("nspname");
                std::pmr::vector<types::logical_value_t> ns_vals(resource_);
                ns_vals.emplace_back(db_lv);
                auto [_n, nf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::read_chunks_by_key,
                                                 exec_ctx,
                                                 kPgNamespace,
                                                 std::move(ns_keys),
                                                 components::operators::make_key_chunk(resource_, std::move(ns_vals)));
                auto ns_batches = co_await std::move(nf);
                if (!ns_batches.empty() && ns_batches[0].size() != 0 && ns_batches[0].column_count() >= 1) {
                    auto ns_oid_v = ns_batches[0].value(0, 0);
                    if (!ns_oid_v.is_null()) {
                        namespace_oid_ = static_cast<catalog::oid_t>(ns_oid_v.value<std::uint32_t>());
                    }
                }
            }
        }

        vector::data_chunk_t chunk(resource_, out_types, /*capacity=*/1);

        if (namespace_oid_ != catalog::INVALID_OID && ctx->disk_address != actor_zeta::address_t::empty_address()) {
            types::logical_value_t name_lv(resource_, std::string_view{name_});
            types::logical_value_t ns_lv(resource_, namespace_oid_);
            std::pmr::vector<std::string> typ_keys(resource_);
            typ_keys.emplace_back("typname");
            typ_keys.emplace_back("typnamespace");
            std::pmr::vector<types::logical_value_t> typ_vals(resource_);
            typ_vals.emplace_back(name_lv);
            typ_vals.emplace_back(ns_lv);
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::read_chunks_by_key,
                                             exec_ctx,
                                             kPgType,
                                             std::move(typ_keys),
                                             components::operators::make_key_chunk(resource_, std::move(typ_vals)));
            auto batches = co_await std::move(tf);

            if (!batches.empty() && batches.front().size() != 0 && batches.front().column_count() >= 4) {
                const auto& batch = batches.front();
                auto v0 = batch.value(0, 0);
                auto v1 = batch.value(1, 0);
                auto v2 = batch.value(2, 0);
                auto v3 = batch.value(3, 0);
                if (!v0.is_null() && !v1.is_null() && !v2.is_null()) {
                    set_uint32(chunk, 0, 0, v0.value<std::uint32_t>());
                    set_str(chunk, 1, 0, v1.value<std::string_view>(), resource_);
                    set_uint32(chunk, 2, 0, v2.value<std::uint32_t>());
                    // Preserve original behaviour: when typdefspec is null,
                    // still write an empty string (not a null cell) so any
                    // downstream consumer sees the same shape it used to.
                    if (!v3.is_null()) {
                        set_str(chunk, 3, 0, v3.value<std::string_view>(), resource_);
                    } else {
                        set_str(chunk, 3, 0, std::string_view{}, resource_);
                    }
                    chunk.set_cardinality(1);

                    // Stamp resolved metadata on the logical node so
                    // enrich / validate / resolve_type.cpp can read it via
                    // plan_resolve_index.
                    if (target_node_) {
                        components::logical_plan::resolved_type_metadata_t md;
                        md.type_oid = static_cast<catalog::oid_t>(v0.value<std::uint32_t>());
                        md.namespace_oid = static_cast<catalog::oid_t>(v2.value<std::uint32_t>());
                        md.name = std::string(v1.value<std::string_view>());
                        if (!v3.is_null()) {
                            md.typdefspec = std::string(v3.value<std::string_view>());
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