#include "node_alter_column_rename.hpp"

namespace components::logical_plan {

    node_alter_column_rename_t::node_alter_column_rename_t(std::pmr::memory_resource* resource,
                                                           std::string dbname,
                                                           std::string relname,
                                                           components::catalog::oid_t table_oid,
                                                           std::string old_name,
                                                           std::string new_name)
        : node_t(resource, node_type::alter_column_rename_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , old_name_(std::move(old_name))
        , new_name_(std::move(new_name)) {
        set_table_oid(table_oid);
    }

    hash_t node_alter_column_rename_t::hash_impl() const { return 0; }

    std::string node_alter_column_rename_t::to_string_impl() const {
        return "$alter_column_rename[" + old_name_ + " -> " + new_name_ + "]";
    }

} // namespace components::logical_plan
