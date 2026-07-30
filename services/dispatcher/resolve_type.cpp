#include "resolve_type.hpp"

#include "plan_resolve_index.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>

namespace services::dispatcher {

    bool resolve_builtin(components::types::complex_logical_type& ct) {
        const auto lt = components::catalog::pg_name_to_logical_type(ct.type_name());
        if (lt == components::types::logical_type::UNKNOWN)
            return false;
        // Replacing the type would drop the FIELD name it carries when `ct` is a composite
        // type's field — the one name that is a property of the type, because it is what
        // addresses the field. A COLUMN's name was never here to be preserved after M3-B5:
        // it lives on the column_definition_t whose type this is.
        const std::string field_name = ct.field_name();
        ct = components::types::complex_logical_type{lt};
        if (!field_name.empty())
            ct.set_field_name(field_name);
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
        const std::string field_name = ct.field_name();
        ct = md->type;
        if (!field_name.empty())
            ct.set_field_name(field_name);
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
                    std::string field_name = ct.field_name();
                    ct = components::types::complex_logical_type::create_array(inner, sz);
                    if (!field_name.empty())
                        ct.set_field_name(field_name);
                }
            }
            if (ct.type() == components::types::logical_type::LIST) {
                const auto* list_ext =
                    static_cast<const components::types::list_logical_type_extension*>(ct.extension());
                auto inner = list_ext->node();
                if (inner.type() == components::types::logical_type::UNKNOWN) {
                    resolve_one_type(inner, idx);
                    std::string field_name = ct.field_name();
                    ct = components::types::complex_logical_type::create_list(inner);
                    if (!field_name.empty())
                        ct.set_field_name(field_name);
                }
            }
        }
    }

} // namespace services::dispatcher