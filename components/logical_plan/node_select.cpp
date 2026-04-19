#include "node_select.hpp"

#include <sstream>

namespace components::logical_plan {

    node_select_t::node_select_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::select_t, collection) {}

    hash_t node_select_t::hash_impl() const { return 0; }

    std::string node_select_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$select: {";
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

    node_select_ptr make_node_select(std::pmr::memory_resource* resource, const collection_full_name_t& collection) {
        return {new node_select_t(resource, collection)};
    }

} // namespace components::logical_plan
