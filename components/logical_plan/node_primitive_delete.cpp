#include "node_primitive_delete.hpp"

namespace components::logical_plan {

    node_primitive_delete_t::node_primitive_delete_t(std::pmr::memory_resource* resource,
                                                      collection_full_name_t     catalog_table,
                                                      std::int64_t               oid_col_idx,
                                                      components::catalog::oid_t target_oid)
        : node_t(resource, node_type::primitive_delete_t, catalog_table)
        , catalog_table_(std::move(catalog_table))
        , oid_col_idx_(oid_col_idx)
        , target_oid_(target_oid) {}

    hash_t node_primitive_delete_t::hash_impl() const { return 0; }

    std::string node_primitive_delete_t::to_string_impl() const {
        return "$primitive_delete[" + catalog_table_.collection + "]";
    }

} // namespace components::logical_plan