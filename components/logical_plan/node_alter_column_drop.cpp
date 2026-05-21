#include "node_alter_column_drop.hpp"

namespace components::logical_plan {

    node_alter_column_drop_t::node_alter_column_drop_t(std::pmr::memory_resource* resource,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t namespace_oid,
                                                       core::columnname_t column_name,
                                                       components::catalog::drop_behavior_t behavior)
        : node_t(resource, node_type::alter_column_drop_t)
        , namespace_oid_(namespace_oid)
        , column_name_(std::move(static_cast<std::string&>(column_name)))
        , behavior_(behavior) {
        set_table_oid(table_oid);
    }

    hash_t node_alter_column_drop_t::hash_impl() const { return 0; }

    std::string node_alter_column_drop_t::to_string_impl() const { return "$alter_column_drop[" + column_name_ + "]"; }

} // namespace components::logical_plan
