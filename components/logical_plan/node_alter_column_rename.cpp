#include "node_alter_column_rename.hpp"

namespace components::logical_plan {

    node_alter_column_rename_t::node_alter_column_rename_t(std::pmr::memory_resource* resource,
                                                           components::catalog::oid_t table_oid,
                                                           core::columnname_t old_name,
                                                           core::columnname_t new_name)
        : node_t(resource, node_type::alter_column_rename_t)
        , old_name_(std::move(static_cast<std::string&>(old_name)))
        , new_name_(std::move(static_cast<std::string&>(new_name))) {
        set_table_oid(table_oid);
    }

    hash_t node_alter_column_rename_t::hash_impl() const { return 0; }

    std::string node_alter_column_rename_t::to_string_impl() const {
        return "$alter_column_rename[" + old_name_ + " -> " + new_name_ + "]";
    }

} // namespace components::logical_plan
