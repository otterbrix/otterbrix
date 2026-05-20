#include "node_computed_field_unregister.hpp"

namespace components::logical_plan {

    node_computed_field_unregister_t::node_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                                                       core::dbname_t dbname,
                                                                       core::relname_t relname,
                                                                       components::catalog::oid_t table_oid,
                                                                       core::columnname_t column_name)
        : node_t(resource, node_type::computed_field_unregister_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , column_name_(std::move(static_cast<std::string&>(column_name))) {
        set_table_oid(table_oid);
    }

    hash_t node_computed_field_unregister_t::hash_impl() const { return 0; }

    std::string node_computed_field_unregister_t::to_string_impl() const {
        return "$computed_field_unregister[" + column_name_ + "]";
    }

} // namespace components::logical_plan
