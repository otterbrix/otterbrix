#include "node_check_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_check_constraint_t::node_check_constraint_t(std::pmr::memory_resource* resource,
                                                     core::dbname_t dbname,
                                                     core::relname_t relname,
                                                     std::vector<std::string> not_null_columns,
                                                     std::vector<std::pair<std::string, std::string>> check_exprs)
        : node_t(resource, node_type::check_constraint_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , not_null_columns_(std::move(not_null_columns))
        , check_exprs_(std::move(check_exprs)) {}

    hash_t node_check_constraint_t::hash_impl() const { return 0; }

    std::string node_check_constraint_t::to_string_impl() const {
        std::ostringstream s;
        s << "$check_constraint: " << relname_ << " [nn=" << not_null_columns_.size() << "]";
        return s.str();
    }

} // namespace components::logical_plan
