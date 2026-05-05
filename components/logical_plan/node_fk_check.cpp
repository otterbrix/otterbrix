#include "node_fk_check.hpp"

#include <sstream>

namespace components::logical_plan {

    node_fk_check_t::node_fk_check_t(std::pmr::memory_resource*         resource,
                                       const collection_full_name_t&      collection,
                                       components::catalog::resolved_fk_t fk)
        : node_t(resource, node_type::fk_check_t, collection)
        , fk_(std::move(fk)) {}

    hash_t      node_fk_check_t::hash_impl()      const { return 0; }
    std::string node_fk_check_t::to_string_impl() const {
        std::ostringstream s;
        s << "$fk_check: " << collection_name() << " con=" << fk_.con_oid;
        return s.str();
    }

} // namespace components::logical_plan
