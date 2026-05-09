#include "node_alter_column_rename.hpp"

namespace components::logical_plan {

    node_alter_column_rename_t::node_alter_column_rename_t(std::pmr::memory_resource* resource,
                                                            collection_full_name_t      collection,
                                                            components::catalog::oid_t  table_oid,
                                                            std::string                 old_name,
                                                            std::string                 new_name)
        : node_t(resource, node_type::alter_column_rename_t, std::move(collection))
        , table_oid_(table_oid)
        , old_name_(std::move(old_name))
        , new_name_(std::move(new_name)) {}

    hash_t node_alter_column_rename_t::hash_impl() const { return 0; }

    std::string node_alter_column_rename_t::to_string_impl() const {
        return "$alter_column_rename[" + old_name_ + " -> " + new_name_ + "]";
    }

} // namespace components::logical_plan
