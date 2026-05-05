#include "node_primitive_delete.hpp"

#include <sstream>

namespace components::logical_plan {

    node_primitive_delete_t::node_primitive_delete_t(std::pmr::memory_resource*    resource,
                                                      const collection_full_name_t& collection,
                                                      components::catalog::oid_t    table_oid,
                                                      std::size_t                   oid_col_index,
                                                      components::catalog::oid_t    match_oid)
        : node_t(resource, node_type::primitive_delete_t, collection)
        , table_oid_(table_oid)
        , oid_col_index_(oid_col_index)
        , match_oid_(match_oid) {}

    hash_t      node_primitive_delete_t::hash_impl()      const { return 0; }
    std::string node_primitive_delete_t::to_string_impl() const {
        std::ostringstream s;
        s << "$primitive_delete: " << collection_name()
          << " table_oid=" << table_oid_
          << " match_oid=" << match_oid_;
        return s.str();
    }

} // namespace components::logical_plan
