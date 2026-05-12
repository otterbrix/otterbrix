#include "node_alter_column_add.hpp"

namespace components::logical_plan {

    node_alter_column_add_t::node_alter_column_add_t(std::pmr::memory_resource* resource,
                                                     std::string dbname,
                                                     std::string relname,
                                                     components::catalog::oid_t table_oid,
                                                     components::table::column_definition_t column)
        : node_t(resource, node_type::alter_column_add_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , column_(std::move(column)) {
        set_table_oid(table_oid);
    }

    hash_t node_alter_column_add_t::hash_impl() const { return 0; }

    std::string node_alter_column_add_t::to_string_impl() const {
        return "$alter_column_add[" + column_.name() + "]";
    }

} // namespace components::logical_plan
