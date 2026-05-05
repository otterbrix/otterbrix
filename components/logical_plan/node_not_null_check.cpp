#include "node_not_null_check.hpp"

#include <sstream>

namespace components::logical_plan {

    node_not_null_check_t::node_not_null_check_t(std::pmr::memory_resource*    resource,
                                                  const collection_full_name_t& collection,
                                                  std::vector<std::string>      not_null_columns)
        : node_t(resource, node_type::not_null_check_t, collection)
        , not_null_columns_(std::move(not_null_columns)) {}

    hash_t node_not_null_check_t::hash_impl() const { return 0; }

    std::string node_not_null_check_t::to_string_impl() const {
        std::ostringstream s;
        s << "$not_null_check: " << collection_name();
        return s.str();
    }

} // namespace components::logical_plan
