#include "node_insert_on_conflict.hpp"

#include <sstream>

namespace components::logical_plan {

    insert_on_conflict_t::insert_on_conflict_t(std::pmr::memory_resource* resource)
        : target_columns(resource)
        , constraint_name(resource) {}

    node_insert_on_conflict_t::node_insert_on_conflict_t(std::pmr::memory_resource* resource, node_insert_ptr insert)
        : node_t(resource, node_type::insert_on_conflict_t)
        , on_conflict_(resource) {
        append_child(std::move(insert));
    }

    node_insert_t* node_insert_on_conflict_t::insert() const {
        return static_cast<node_insert_t*>(children_.front().get());
    }

    hash_t node_insert_on_conflict_t::hash_impl() const { return 0; }

    std::string node_insert_on_conflict_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert_on_conflict: "
               << (on_conflict_.action == on_conflict_action_t::do_nothing ? "do_nothing" : "do_update") << " {"
               << children_.front()->to_string() << "}";
        return stream.str();
    }

} // namespace components::logical_plan
