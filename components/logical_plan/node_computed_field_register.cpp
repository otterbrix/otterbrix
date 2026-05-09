#include "node_computed_field_register.hpp"

namespace components::logical_plan {

    node_computed_field_register_t::node_computed_field_register_t(
        std::pmr::memory_resource* resource,
        collection_full_name_t      collection,
        components::catalog::oid_t  table_oid,
        std::vector<components::table::column_definition_t> columns)
        : node_t(resource, node_type::computed_field_register_t, std::move(collection))
        , table_oid_(table_oid)
        , columns_(std::move(columns)) {}

    hash_t node_computed_field_register_t::hash_impl() const { return 0; }

    std::string node_computed_field_register_t::to_string_impl() const {
        std::string out = "$computed_field_register[";
        bool first = true;
        for (const auto& c : columns_) {
            if (!first) out.push_back(',');
            out.append(c.name());
            first = false;
        }
        out.push_back(']');
        return out;
    }

} // namespace components::logical_plan
