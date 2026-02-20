#include "node_having.hpp"

#include <sstream>

namespace components::logical_plan {

    node_having_t::node_having_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::having_t, collection) {}

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

    void node_having_t::serialize_impl(serializer::msgpack_serializer_t* /*serializer*/) const {
        // HAVING is a runtime-only node, no serialization needed
    }

    node_having_ptr make_node_having(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const expressions::expression_ptr& expr) {
        node_having_ptr node = new node_having_t{resource, collection};
        if (expr) {
            node->append_expression(expr);
        }
        return node;
    }

} // namespace components::logical_plan
