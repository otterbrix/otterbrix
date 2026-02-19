#include "node_create_view.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_view_t::node_create_view_t(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& name,
                                           std::string query_sql)
        : node_t(resource, node_type::create_view_t, name)
        , query_sql_(std::move(query_sql)) {}

    hash_t node_create_view_t::hash_impl() const { return 0; }

    std::string node_create_view_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_view: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_create_view_t::serialize_impl(serializer::msgpack_serializer_t* /*serializer*/) const {}

    node_create_view_ptr make_node_create_view(std::pmr::memory_resource* resource,
                                               const collection_full_name_t& name,
                                               std::string query_sql) {
        return {new node_create_view_t{resource, name, std::move(query_sql)}};
    }

} // namespace components::logical_plan
