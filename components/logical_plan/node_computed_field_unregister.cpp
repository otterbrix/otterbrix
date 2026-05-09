#include "node_computed_field_unregister.hpp"

namespace components::logical_plan {

    node_computed_field_unregister_t::node_computed_field_unregister_t(
        std::pmr::memory_resource* resource,
        collection_full_name_t      collection,
        components::catalog::oid_t  table_oid,
        std::string                 column_name)
        : node_t(resource, node_type::computed_field_unregister_t, std::move(collection))
        , table_oid_(table_oid)
        , column_name_(std::move(column_name)) {}

    hash_t node_computed_field_unregister_t::hash_impl() const { return 0; }

    std::string node_computed_field_unregister_t::to_string_impl() const {
        return "$computed_field_unregister[" + column_name_ + "]";
    }

} // namespace components::logical_plan
