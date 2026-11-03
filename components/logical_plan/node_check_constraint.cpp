#include "node_check_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_check_constraint_t::node_check_constraint_t(std::pmr::memory_resource* resource,
                                                     core::schema_t schema,
                                                     core::relname_t relname,
                                                     std::vector<std::pair<std::string, uint64_t>> array_size_reqs)
        : node_t(resource, node_type::check_constraint_t, qualified_name_t{std::move(schema), std::move(relname)})
        , array_size_reqs_(std::move(array_size_reqs)) {}

    hash_t node_check_constraint_t::hash_impl() const { return 0; }

    std::string node_check_constraint_t::to_string_impl() const {
        std::ostringstream s;
        s << "$check_constraint: " << target_.collection;
        return s.str();
    }

} // namespace components::logical_plan
