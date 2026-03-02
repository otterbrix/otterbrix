#include "node_drop_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_collection_t::node_drop_collection_t(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& collection)
        : node_t(resource, node_type::drop_collection_t, collection) {}

    hash_t node_drop_collection_t::hash_impl() const { return 0; }

    std::string node_drop_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    node_drop_collection_ptr make_node_drop_collection(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection) {
        return {new node_drop_collection_t{resource, collection}};
    }

} // namespace components::logical_plan
