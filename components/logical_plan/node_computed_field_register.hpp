#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

#include <vector>

namespace components::logical_plan {

    class node_computed_field_register_t final : public node_t {
    public:
        node_computed_field_register_t(std::pmr::memory_resource* resource,
                                       std::string dbname,
                                       std::string relname,
                                       components::catalog::oid_t table_oid,
                                       std::vector<components::table::column_definition_t> columns);

        const std::vector<components::table::column_definition_t>& columns() const noexcept { return columns_; }
        std::vector<components::table::column_definition_t>& columns() noexcept { return columns_; }

        // Phase 9.W/10.D: role-named accessors.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::vector<components::table::column_definition_t> columns_;
    };

    using node_computed_field_register_ptr = boost::intrusive_ptr<node_computed_field_register_t>;

} // namespace components::logical_plan
