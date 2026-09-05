#include "node_update.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <sstream>

namespace components::logical_plan {

    node_update_t::node_update_t(std::pmr::memory_resource* resource,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit,
                                 const std::pmr::vector<expressions::expression_ptr>& updates,
                                 bool upsert)
        : node_t(resource, node_type::update_t)
        // Allocator-extended copy: a std::pmr::vector's plain copy constructor does NOT propagate
        // the source's allocator, so `update_expressions_(updates)` would put the node's own
        // expression list on the default resource instead of the node's arena.
        , update_expressions_(updates, resource)
        , returning_(resource)
        , upsert_(upsert) {
        append_child(match);
        append_child(limit);
    }

    const std::pmr::vector<expressions::expression_ptr>& node_update_t::updates() const { return update_expressions_; }

    std::pmr::vector<expressions::expression_ptr>& node_update_t::updates() { return update_expressions_; }

    bool node_update_t::upsert() const { return upsert_; }

    std::pmr::vector<expressions::expression_ptr>& node_update_t::returning() { return returning_; }
    const std::pmr::vector<expressions::expression_ptr>& node_update_t::returning() const { return returning_; }

    hash_t node_update_t::hash_impl() const { return 0; }

    std::string node_update_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$update: <oid:" << static_cast<std::uint64_t>(table_oid()) << "> {";
        stream << "$upsert: " << upsert_;
        for (const auto& child : children()) {
            stream << ", " << child->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::expression_ptr>& updates,
                                     bool upsert) {
        return {new node_update_t{resource, match, limit, updates, upsert}};
    }

} // namespace components::logical_plan
