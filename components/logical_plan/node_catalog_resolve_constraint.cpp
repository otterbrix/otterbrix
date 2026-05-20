#include "node_catalog_resolve_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_catalog_resolve_constraint_t::node_catalog_resolve_constraint_t(std::pmr::memory_resource* resource,
                                                                         node_catalog_resolve_table_t* target,
                                                                         direction_t direction)
        : node_t(resource, node_type::catalog_resolve_constraint_t)
        , target_(target)
        , direction_(direction) {}

    hash_t node_catalog_resolve_constraint_t::hash_impl() const { return 0; }

    std::string node_catalog_resolve_constraint_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$catalog_resolve_constraint: ";
        stream << (direction_ == direction_t::outgoing ? "outgoing" : "referencing");
        if (target_) {
            stream << " target=" << target_->dbname() << "." << target_->relname();
        }
        return stream.str();
    }

    node_catalog_resolve_constraint_ptr
    make_node_catalog_resolve_constraint(std::pmr::memory_resource* resource,
                                         node_catalog_resolve_table_t* target,
                                         node_catalog_resolve_constraint_t::direction_t direction) {
        return {new node_catalog_resolve_constraint_t{resource, target, direction}};
    }

} // namespace components::logical_plan