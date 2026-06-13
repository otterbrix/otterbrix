#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

namespace components::logical_plan {

    // Planner-emitted DDL leaf for `ALTER TABLE ... ADD COLUMN`.
    class node_alter_column_add_t final : public node_t {
    public:
        node_alter_column_add_t(std::pmr::memory_resource* resource,
                                components::catalog::oid_t table_oid,
                                components::table::column_definition_t column);

        const components::table::column_definition_t& column() const noexcept { return column_; }
        components::table::column_definition_t& column() noexcept { return column_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::table::column_definition_t column_;
    };

    using node_alter_column_add_ptr = boost::intrusive_ptr<node_alter_column_add_t>;

} // namespace components::logical_plan
