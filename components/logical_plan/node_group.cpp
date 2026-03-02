#include "node_group.hpp"

#include <sstream>

namespace components::logical_plan {

    node_group_t::node_group_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::group_t, collection) {}

    hash_t node_group_t::hash_impl() const { return 0; }

    std::string node_group_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$group: {";
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

    node_group_ptr make_node_group(std::pmr::memory_resource* resource, const collection_full_name_t& collection) {
        return {new node_group_t{resource, collection}};
    }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::vector<expression_ptr>& expressions) {
        auto node = new node_group_t{resource, collection};
        node->append_expressions(expressions);
        return node;
    }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::pmr::vector<expression_ptr>& expressions) {
        auto node = new node_group_t{resource, collection};
        node->append_expressions(expressions);
        return node;
    }

} // namespace components::logical_plan