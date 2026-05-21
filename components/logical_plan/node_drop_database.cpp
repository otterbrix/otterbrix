#include "node_drop_database.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_database_t::node_drop_database_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::drop_database_t) {}

    hash_t node_drop_database_t::hash_impl() const { return static_cast<hash_t>(namespace_oid_); }

    std::string node_drop_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_database: <oid:" << static_cast<std::uint64_t>(namespace_oid_) << ">";
        return stream.str();
    }

    node_drop_database_ptr make_node_drop_database(std::pmr::memory_resource* resource) {
        return {new node_drop_database_t{resource}};
    }

} // namespace components::logical_plan
