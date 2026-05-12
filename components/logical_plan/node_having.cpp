#include "node_having.hpp"

#include <sstream>

namespace components::logical_plan {

    node_having_t::node_having_t(std::pmr::memory_resource* resource, std::string dbname, std::string relname)
        : node_t(resource, node_type::having_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname)) {}

    hash_t node_having_t::hash_impl() const { return 0; }

    std::string node_having_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$having: {";
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

    node_having_ptr make_node_having(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     const expressions::expression_ptr& expr) {
        node_having_ptr node = new node_having_t{resource, std::move(dbname), std::move(relname)};
        if (expr) {
            node->append_expression(expr);
        }
        return node;
    }

} // namespace components::logical_plan
