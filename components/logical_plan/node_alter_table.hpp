#pragma once

#include "node.hpp"
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

#include <string>
#include <variant>

namespace components::logical_plan {

    enum class alter_table_kind : std::uint8_t
    {
        add_column,
        drop_column,
        rename_column,
    };

    // Single ALTER TABLE subcommand. Multi-clause ALTER TABLE flattens into a vector
    // of these; executor iterates the list under one transaction so partial failures roll back.
    struct alter_table_subcommand_t {
        alter_table_kind kind{alter_table_kind::drop_column};
        std::string column_name;
        std::string new_column_name; // rename_column only
        components::table::column_definition_t column;
        alter_table_subcommand_t()
            : column("", components::types::complex_logical_type{components::types::logical_type::UNKNOWN}) {}
    };

    // node_alter_table holds one or more subcommands. Single-clause forms (existing factories)
    // produce a node with subcommands_.size()==1. The kind()/column_name()/etc accessors
    // return the FIRST subcommand for backward compatibility; iterate subcommands() for full set.
    class node_alter_table_t final : public node_t {
    public:
        // ADD COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           const collection_full_name_t& collection,
                           components::table::column_definition_t column);
        // DROP COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           const collection_full_name_t& collection,
                           alter_table_kind kind,
                           std::string column_name);
        // RENAME COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           const collection_full_name_t& collection,
                           std::string old_name,
                           std::string new_name);
        // Multi-clause form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           const collection_full_name_t& collection,
                           std::vector<alter_table_subcommand_t> subcommands);

        // Backward-compatible accessors — return data from FIRST subcommand.
        alter_table_kind kind() const noexcept { return subcommands_.front().kind; }
        const std::string& column_name() const noexcept { return subcommands_.front().column_name; }
        const std::string& new_column_name() const noexcept { return subcommands_.front().new_column_name; }
        const components::table::column_definition_t& column() const { return subcommands_.front().column; }

        const std::vector<alter_table_subcommand_t>& subcommands() const noexcept { return subcommands_; }

        // Stamped by enrich_plan once the catalog_view has cached the target table.
        // The planner (rewrite_alter_table) consumes this to build per-clause primitives.
        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        void set_table_oid(components::catalog::oid_t oid) noexcept { table_oid_ = oid; }

        // Stamped by enrich_plan from pg_class.relkind. The planner uses this on
        // drop_column subcommands to choose between the static-schema path
        // (alter_column_drop → pg_attribute tombstone) and the dynamic-schema
        // path (computed_field_unregister → pg_computed_column refcount-decrement).
        // Defaults to 'r' (regular table) so untouched static paths behave as before.
        char relkind() const noexcept { return relkind_; }
        void set_relkind(char rk) noexcept { relkind_ = rk; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<alter_table_subcommand_t> subcommands_;
        components::catalog::oid_t            table_oid_{components::catalog::INVALID_OID};
        char                                  relkind_{components::catalog::relkind::regular};
    };

    using node_alter_table_ptr = boost::intrusive_ptr<node_alter_table_t>;

    node_alter_table_ptr make_node_alter_table_add_column(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           components::table::column_definition_t column);
    node_alter_table_ptr make_node_alter_table_drop_column(std::pmr::memory_resource* resource,
                                                            const collection_full_name_t& collection,
                                                            std::string column_name);
    node_alter_table_ptr make_node_alter_table_rename_column(std::pmr::memory_resource* resource,
                                                              const collection_full_name_t& collection,
                                                              std::string old_name,
                                                              std::string new_name);
    node_alter_table_ptr make_node_alter_table_multi(std::pmr::memory_resource* resource,
                                                      const collection_full_name_t& collection,
                                                      std::vector<alter_table_subcommand_t> subcommands);

} // namespace components::logical_plan
