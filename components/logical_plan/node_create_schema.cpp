#include "node_create_schema.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_schema_t::node_create_schema_t(std::pmr::memory_resource* resource,
                                               qualified_name_t schema,
                                               bool if_not_exists)
        : node_t(resource, node_type::create_schema_t, std::move(schema))
        , if_not_exists_(if_not_exists) {}

    hash_t node_create_schema_t::hash_impl() const { return 0; }

    std::string node_create_schema_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_schema" << (if_not_exists_ ? "_if_not_exists" : "") << ": " << target_.schema;
        return stream.str();
    }

    node_create_schema_ptr
    make_node_create_schema(std::pmr::memory_resource* resource, qualified_name_t schema, bool if_not_exists) {
        return {new node_create_schema_t{resource, std::move(schema), if_not_exists}};
    }

} // namespace components::logical_plan
