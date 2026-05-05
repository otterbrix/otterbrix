#include "node_check_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_check_constraint_t::node_check_constraint_t(
        std::pmr::memory_resource*            resource,
        const collection_full_name_t&         collection,
        components::catalog::row_predicate_fn pred,
        std::string                           conexpr)
        : node_t(resource, node_type::check_constraint_t, collection)
        , pred_(std::move(pred))
        , conexpr_(std::move(conexpr)) {}

    hash_t      node_check_constraint_t::hash_impl()      const { return 0; }
    std::string node_check_constraint_t::to_string_impl() const {
        std::ostringstream s;
        s << "$check_constraint: " << collection_name() << " [" << conexpr_ << "]";
        return s.str();
    }

} // namespace components::logical_plan
