#include "node_drop_view.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_view_t::node_drop_view_t(std::pmr::memory_resource* resource,
                                       const collection_full_name_t& name)
        : node_t(resource, node_type::drop_view_t, name) {}

    hash_t node_drop_view_t::hash_impl() const { return 0; }

    std::string node_drop_view_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_view: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_drop_view_t::serialize_impl(serializer::msgpack_serializer_t* /*serializer*/) const {}

    node_drop_view_ptr make_node_drop_view(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& name) {
        return {new node_drop_view_t{resource, name}};
    }

} // namespace components::logical_plan
