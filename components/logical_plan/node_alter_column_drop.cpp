#include "node_alter_column_drop.hpp"

namespace components::logical_plan {

    node_alter_column_drop_t::node_alter_column_drop_t(
        std::pmr::memory_resource*           resource,
        collection_full_name_t               collection,
        components::catalog::oid_t           table_oid,
        components::catalog::oid_t           namespace_oid,
        std::string                          column_name,
        components::catalog::drop_behavior_t behavior)
        : node_t(resource, node_type::alter_column_drop_t, std::move(collection))
        , table_oid_(table_oid)
        , namespace_oid_(namespace_oid)
        , column_name_(std::move(column_name))
        , behavior_(behavior) {}

    hash_t node_alter_column_drop_t::hash_impl() const { return 0; }

    std::string node_alter_column_drop_t::to_string_impl() const {
        return "$alter_column_drop[" + column_name_ + "]";
    }

} // namespace components::logical_plan
