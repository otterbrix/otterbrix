#include "node_drop_type.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_type_t::node_drop_type_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::drop_type_t) {}

    hash_t node_drop_type_t::hash_impl() const { return static_cast<hash_t>(type_oid_); }

    std::string node_drop_type_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_type: <oid:" << static_cast<std::uint64_t>(type_oid_) << ">";
        return stream.str();
    }

    node_drop_type_ptr make_node_drop_type(std::pmr::memory_resource* resource) {
        return {new node_drop_type_t{resource}};
    }

} // namespace components::logical_plan
