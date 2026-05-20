#include "node_primitive_delete.hpp"

namespace components::logical_plan {

    node_primitive_delete_t::node_primitive_delete_t(std::pmr::memory_resource* resource,
                                                     components::catalog::oid_t catalog_table_oid,
                                                     std::int64_t oid_col_idx,
                                                     components::catalog::oid_t target_oid)
        : node_t(resource, node_type::primitive_delete_t)
        , catalog_table_oid_(catalog_table_oid)
        , oid_col_idx_(oid_col_idx)
        , target_oid_(target_oid) {
        set_table_oid(catalog_table_oid);
    }

    hash_t node_primitive_delete_t::hash_impl() const { return 0; }

    std::string node_primitive_delete_t::to_string_impl() const {
        return "$primitive_delete[oid=" + std::to_string(catalog_table_oid_) + "]";
    }

} // namespace components::logical_plan
