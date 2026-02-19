#include "node_create_macro.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_macro_t::node_create_macro_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& name,
                                             std::vector<std::string> parameters,
                                             std::string body_sql)
        : node_t(resource, node_type::create_macro_t, name)
        , parameters_(std::move(parameters))
        , body_sql_(std::move(body_sql)) {}

    hash_t node_create_macro_t::hash_impl() const { return 0; }

    std::string node_create_macro_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_macro: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_create_macro_t::serialize_impl(serializer::msgpack_serializer_t* /*serializer*/) const {}

    node_create_macro_ptr make_node_create_macro(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& name,
                                                 std::vector<std::string> parameters,
                                                 std::string body_sql) {
        return {new node_create_macro_t{resource, name, std::move(parameters), std::move(body_sql)}};
    }

} // namespace components::logical_plan
