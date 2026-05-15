#include "node_drop_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_collection_t::node_drop_collection_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::drop_collection_t) {}

    hash_t node_drop_collection_t::hash_impl() const {
        return static_cast<hash_t>(namespace_oid_) ^ static_cast<hash_t>(table_oid());
    }

    std::string node_drop_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_collection: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
        return stream.str();
    }

    node_drop_collection_ptr make_node_drop_collection(std::pmr::memory_resource* resource) {
        return {new node_drop_collection_t{resource}};
    }

} // namespace components::logical_plan
