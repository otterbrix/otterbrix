#include "node_drop_macro.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_macro_t::node_drop_macro_t(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& name)
        : node_t(resource, node_type::drop_macro_t, name) {}

    hash_t node_drop_macro_t::hash_impl() const { return 0; }

    std::string node_drop_macro_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_macro: " << database_name() << "." << collection_name();
        return stream.str();
    }

    node_drop_macro_ptr make_node_drop_macro(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& name) {
        return {new node_drop_macro_t{resource, name}};
    }

} // namespace components::logical_plan
