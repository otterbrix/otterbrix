#include "node_computed_field_register.hpp"

namespace components::logical_plan {

    node_computed_field_register_t::node_computed_field_register_t(
        std::pmr::memory_resource* resource,
        std::string dbname,
        std::string relname,
        components::catalog::oid_t table_oid,
        std::vector<components::table::column_definition_t> columns)
        : node_t(resource, node_type::computed_field_register_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , columns_(std::move(columns)) {
        set_table_oid(table_oid);
    }

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
