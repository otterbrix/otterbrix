#include "resolve_type.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>

namespace services::dispatcher {

    bool resolve_builtin(components::types::complex_logical_type& ct) {
        const auto lt = components::catalog::pg_name_to_logical_type(ct.type_name());
        if (lt == components::types::logical_type::UNKNOWN) return false;
        const std::string alias = ct.has_alias() ? ct.alias() : std::string{};
        ct = components::types::complex_logical_type{lt};
        if (!alias.empty()) ct.set_alias(alias);
        return true;
    }

    void apply_resolved(components::types::complex_logical_type& ct,
                        const resolved_type_t* rt) {
        if (!rt) return;
        const std::string alias = ct.has_alias() ? ct.alias() : std::string{};
        ct = rt->type;
        if (!alias.empty()) ct.set_alias(alias);
    }

    actor_zeta::unique_future<void>
    resolve_one_type(components::types::complex_logical_type& ct,
                     catalog_view_t& view,
                     components::execution_context_t ctx) {
        using namespace components::catalog;
        if (ct.type() != components::types::logical_type::UNKNOWN) co_return;
        if (resolve_builtin(ct)) co_return;
        co_await view.get_type(ctx, well_known_oid::public_namespace, std::string(ct.type_name()));
        co_await view.get_type(ctx, well_known_oid::pg_catalog_namespace, std::string(ct.type_name()));
        const auto* rt = view.try_get_type(well_known_oid::public_namespace,
                                            std::string_view(ct.type_name()));
        if (!rt) rt = view.try_get_type(well_known_oid::pg_catalog_namespace,
                                         std::string_view(ct.type_name()));
        apply_resolved(ct, rt);
    }

    actor_zeta::unique_future<void>
    resolve_column_definitions(std::vector<components::table::column_definition_t>& cols,
                                catalog_view_t& view,
                                components::execution_context_t ctx) {
        for (auto& col : cols) {
            auto& ct = col.type();
            co_await resolve_one_type(ct, view, ctx);
            if (ct.type() == components::types::logical_type::STRUCT) {
                for (auto& field : ct.child_types()) {
                    co_await resolve_one_type(field, view, ctx);
                }
            }
            if (ct.type() == components::types::logical_type::ARRAY) {
                const auto* arr_ext =
                    static_cast<const components::types::array_logical_type_extension*>(ct.extension());
                auto inner = arr_ext->internal_type();
                const size_t sz = arr_ext->size();
                if (inner.type() == components::types::logical_type::UNKNOWN) {
                    co_await resolve_one_type(inner, view, ctx);
                    std::string alias = ct.has_alias() ? ct.alias() : std::string{};
                    ct = components::types::complex_logical_type::create_array(inner, sz);
                    if (!alias.empty()) ct.set_alias(alias);
                }
            }
        }
    }

} // namespace services::dispatcher