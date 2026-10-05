#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/catalog_write.hpp>
#include <components/table/column_definition.hpp>

#include <memory_resource>
#include <vector>

namespace components::logical_plan {

    // What the planner lowers a CREATE MATERIALIZED VIEW (node_create_view_t, materialized) to: the matview's heap
    // with the view's columns, and the catalog rows of a view with relkind 'm', written in one operator.
    class node_create_matview_t final : public node_t {
    public:
        node_create_matview_t(std::pmr::memory_resource* resource,
                              core::matviewname_t matviewname,
                              components::catalog::oid_t namespace_oid,
                              components::catalog::oid_t matview_oid,
                              std::pmr::vector<table::column_definition_t> columns,
                              std::vector<components::catalog::catalog_write_t> catalog_writes);

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        components::catalog::oid_t matview_oid() const noexcept { return matview_oid_; }
        std::pmr::vector<table::column_definition_t> take_columns() { return std::move(columns_); }
        std::vector<components::catalog::catalog_write_t> take_catalog_writes() { return std::move(catalog_writes_); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        core::matviewname_t matviewname_;
        components::catalog::oid_t namespace_oid_;
        components::catalog::oid_t matview_oid_;
        std::pmr::vector<table::column_definition_t> columns_;
        std::vector<components::catalog::catalog_write_t> catalog_writes_;
    };

    using node_create_matview_ptr = boost::intrusive_ptr<node_create_matview_t>;

    node_create_matview_ptr make_node_create_matview(std::pmr::memory_resource* resource,
                                                     core::matviewname_t matviewname,
                                                     components::catalog::oid_t namespace_oid,
                                                     components::catalog::oid_t matview_oid,
                                                     std::pmr::vector<table::column_definition_t> columns,
                                                     std::vector<components::catalog::catalog_write_t> catalog_writes);

} // namespace components::logical_plan
