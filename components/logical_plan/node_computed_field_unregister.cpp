#include "node_computed_field_unregister.hpp"

namespace components::logical_plan {

    node_computed_field_unregister_t::node_computed_field_unregister_t(
        std::pmr::memory_resource* resource,
        std::string dbname,
        std::string relname,
        components::catalog::oid_t table_oid,
        std::string column_name)
        : node_t(resource, node_type::computed_field_unregister_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , column_name_(std::move(column_name)) {
        set_table_oid(table_oid);
    }

    hash_t node_computed_field_unregister_t::hash_impl() const { return 0; }

    std::string node_computed_field_unregister_t::to_string_impl() const {
        return "$computed_field_unregister[" + column_name_ + "]";
    }

} // namespace components::logical_plan
