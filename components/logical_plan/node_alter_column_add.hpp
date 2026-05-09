#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

namespace components::logical_plan {

    // Planner-emitted DDL leaf for `ALTER TABLE ... ADD COLUMN`.
    //
    // Built by planner::rewrite_alter_table from a single add_column subcommand of
    // node_alter_table_t. The operator (operator_alter_column_add_t) does the
    // pg_attribute scan + attoid allocation + pg_attribute write + in-memory schema
    // update at execution time.
    class node_alter_column_add_t final : public node_t {
    public:
        node_alter_column_add_t(std::pmr::memory_resource* resource,
                                 collection_full_name_t      collection,
                                 components::catalog::oid_t  table_oid,
                                 components::table::column_definition_t column);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const components::table::column_definition_t& column() const noexcept { return column_; }
        components::table::column_definition_t&       column()       noexcept { return column_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t              table_oid_;
        components::table::column_definition_t  column_;
    };

    using node_alter_column_add_ptr = boost::intrusive_ptr<node_alter_column_add_t>;

} // namespace components::logical_plan
