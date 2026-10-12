#include "node_match.hpp"

#include <sstream>

namespace components::logical_plan {

    node_match_t::node_match_t(std::pmr::memory_resource* resource, qualified_name_t target)
        : node_t(resource, node_type::match_t, std::move(target)) {}

    hash_t node_match_t::hash_impl() const { return 0; }

    std::string node_match_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$match: {";
        bool is_first = true;
        for (const auto& expr : expressions_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   qualified_name_t target,
                                   const expressions::expression_ptr& match) {
        node_match_ptr node = new node_match_t{resource, std::move(target)};
        if (match) {
            node->append_expression(match);
        }
        return node;
    }

} // namespace components::logical_plan
