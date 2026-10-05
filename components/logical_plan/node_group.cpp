#include "node_group.hpp"

#include <sstream>

namespace components::logical_plan {

    node_group_t::node_group_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::group_t)
        , input_types_(resource) {}

    void node_group_t::set_pushdown(bool pushdown) noexcept { pushdown_ = pushdown; }

    bool node_group_t::pushdown() const noexcept { return pushdown_; }

    // Intentionally still 0: pushdown_ is a static-rollout-gated annotation and is
    // NOT folded in here (see the note on set_pushdown() in the header).
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

    node_group_ptr make_node_group(std::pmr::memory_resource* resource) { return {new node_group_t{resource}}; }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const std::vector<expression_ptr>& expressions) {
        auto node = new node_group_t{resource};
        node->append_expressions(expressions);
        return node;
    }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const std::pmr::vector<expression_ptr>& expressions) {
        auto node = new node_group_t{resource};
        node->append_expressions(expressions);
        return node;
    }

} // namespace components::logical_plan
