#include "resolve_type.hpp"

#include "plan_resolve_index.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>

namespace services::dispatcher {

    bool resolve_builtin(components::types::complex_logical_type& ct) {
        const auto lt = components::catalog::pg_name_to_logical_type(ct.type_name());
        if (lt == components::types::logical_type::UNKNOWN)
            return false;
        const std::string alias = ct.has_alias() ? ct.alias() : std::string{};
        ct = components::types::complex_logical_type{lt};
        if (!alias.empty())
            ct.set_alias(alias);
        return true;
    }

    // Sync — reads UDT metadata from the supplied plan-tree idx exclusively.
    // transform_create_table / transform_types emit resolve_type per UDT
    // before Pass 1; we consume what Pass 1 stamped. Misses leave the type
    // as UNKNOWN — validate_types_sync surfaces "type not registered".
    void resolve_one_type(components::types::complex_logical_type& ct, const impl::plan_resolve_index_t* idx) {
        if (ct.type() != components::types::logical_type::UNKNOWN)
            return;
        if (resolve_builtin(ct))
            return;
        const auto* md = impl::type_md_for(idx, "public", std::string_view(ct.type_name()));
        if (!md) {
            md = impl::type_md_for(idx, "pg_catalog", std::string_view(ct.type_name()));
        }
        if (!md)
            return;
        const std::string alias = ct.has_alias() ? ct.alias() : std::string{};
        ct = md->type;
        if (!alias.empty())
            ct.set_alias(alias);
    }

    void resolve_column_definitions(std::vector<components::table::column_definition_t>& cols,
                                    const impl::plan_resolve_index_t* idx) {
        for (auto& col : cols) {
            auto& ct = col.type();
            resolve_one_type(ct, idx);
            if (ct.type() == components::types::logical_type::STRUCT) {
                for (auto& field : ct.child_types()) {
                    resolve_one_type(field, idx);
                }
            }
            if (ct.type() == components::types::logical_type::ARRAY) {
                const auto* arr_ext =
                    static_cast<const components::types::array_logical_type_extension*>(ct.extension());
                auto inner = arr_ext->internal_type();
                const size_t sz = arr_ext->size();
                if (inner.type() == components::types::logical_type::UNKNOWN) {
                    resolve_one_type(inner, idx);
                    std::string alias = ct.has_alias() ? ct.alias() : std::string{};
                    ct = components::types::complex_logical_type::create_array(inner, sz);
                    if (!alias.empty())
                        ct.set_alias(alias);
                }
            }
        }
    }

} // namespace services::dispatcher