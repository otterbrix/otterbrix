#include "node_check_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_check_constraint_t::node_check_constraint_t(
        std::pmr::memory_resource*      resource,
        const collection_full_name_t&   collection,
        std::vector<std::string>        not_null_columns,
        std::vector<check_entry_t>      checks)
        : node_t(resource, node_type::check_constraint_t, collection)
        , not_null_columns_(std::move(not_null_columns))
        , checks_(std::move(checks)) {}

    hash_t node_check_constraint_t::hash_impl() const { return 0; }

    std::string node_check_constraint_t::to_string_impl() const {
        std::ostringstream s;
        s << "$check_constraint: " << collection_name()
          << " [nn=" << not_null_columns_.size()
          << " chk=" << checks_.size() << "]";
        return s.str();
    }

} // namespace components::logical_plan
