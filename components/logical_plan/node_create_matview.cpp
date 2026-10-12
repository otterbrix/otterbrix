#include "node_create_matview.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_matview_t::node_create_matview_t(std::pmr::memory_resource* resource,
                                                 core::matviewname_t matviewname,
                                                 components::catalog::oid_t namespace_oid,
                                                 components::catalog::oid_t matview_oid,
                                                 std::pmr::vector<table::column_definition_t> columns,
                                                 std::vector<components::catalog::catalog_write_t> catalog_writes)
        : node_t(resource, node_type::create_matview_t)
        , matviewname_(std::move(matviewname))
        , namespace_oid_(namespace_oid)
        , matview_oid_(matview_oid)
        , columns_(std::move(columns))
        , catalog_writes_(std::move(catalog_writes)) {}

    hash_t node_create_matview_t::hash_impl() const { return 0; }

    std::string node_create_matview_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_matview: " << matviewname_;
        return stream.str();
    }

    node_create_matview_ptr make_node_create_matview(std::pmr::memory_resource* resource,
                                                     core::matviewname_t matviewname,
                                                     components::catalog::oid_t namespace_oid,
                                                     components::catalog::oid_t matview_oid,
                                                     std::pmr::vector<table::column_definition_t> columns,
                                                     std::vector<components::catalog::catalog_write_t> catalog_writes) {
        return {new node_create_matview_t{resource,
                                          std::move(matviewname),
                                          namespace_oid,
                                          matview_oid,
                                          std::move(columns),
                                          std::move(catalog_writes)}};
    }

} // namespace components::logical_plan
