#pragma once

#include "node.hpp"
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/table/column_definition.hpp>

#include <string>

namespace components::logical_plan {

    enum class alter_table_kind : std::uint8_t
    {
        add_column,
        drop_column,
        rename_column,
        drop_constraint,
    };

    struct alter_table_subcommand_t {
        alter_table_kind kind{alter_table_kind::drop_column};
        std::string column_name;
        std::string new_column_name; // rename_column only
        // drop_constraint only: the executor resolves constraint_name to constraint_oid
        std::string constraint_name;
        components::catalog::oid_t constraint_oid{components::catalog::INVALID_OID};
        // drop_column only: RESTRICT (default or written) or CASCADE; see
        // operator_alter_column_drop_t, which refuses a dependent-blocked drop under restrict_
        components::catalog::drop_behavior_t behavior{components::catalog::drop_behavior_t::restrict_};
        // drop_column / drop_constraint only: DROP ... IF EXISTS (AlterTableCmd.missing_ok). A missing column /
        // constraint skips this subcommand alone and the others still apply; the operators always refuse a missing
        // target.
        bool if_exists{false};
        components::table::column_definition_t column;
        alter_table_subcommand_t()
            : column("", components::types::complex_logical_type{components::types::logical_type::UNKNOWN}) {}
    };

    class node_alter_table_t final : public node_t {
    public:
        // ADD COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource, components::table::column_definition_t column);
        // DROP COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource, alter_table_kind kind, std::string column_name);
        // RENAME COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource, std::string old_name, std::string new_name);
        // Multi-clause form.
        node_alter_table_t(std::pmr::memory_resource* resource, std::vector<alter_table_subcommand_t> subcommands);

        alter_table_kind kind() const noexcept { return subcommands_.front().kind; }
        const std::string& column_name() const noexcept { return subcommands_.front().column_name; }
        const std::string& new_column_name() const noexcept { return subcommands_.front().new_column_name; }
        const components::table::column_definition_t& column() const { return subcommands_.front().column; }

        const std::vector<alter_table_subcommand_t>& subcommands() const noexcept { return subcommands_; }
        // Mutable: the executor stamps constraint_oid onto drop_constraint subcommands and drops skipped ones.
        std::vector<alter_table_subcommand_t>& subcommands() noexcept { return subcommands_; }

        char relkind() const noexcept { return relkind_; }
        void set_relkind(char rk) noexcept { relkind_ = rk; }

        // ALTER TABLE IF EXISTS (AlterTableStmt.missing_ok): a missing table is an empty success instead of the
        // refusal; every other refusal stays one. The clause-level flag is each subcommand's own.
        bool if_exists() const noexcept { return if_exists_; }
        void set_if_exists(bool v) noexcept { if_exists_ = v; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<alter_table_subcommand_t> subcommands_;
        char relkind_{components::catalog::relkind::regular};
        bool if_exists_{false};
    };

    using node_alter_table_ptr = boost::intrusive_ptr<node_alter_table_t>;

    node_alter_table_ptr make_node_alter_table_add_column(std::pmr::memory_resource* resource,
                                                          components::table::column_definition_t column);
    node_alter_table_ptr make_node_alter_table_drop_column(std::pmr::memory_resource* resource,
                                                           std::string column_name);
    node_alter_table_ptr make_node_alter_table_rename_column(std::pmr::memory_resource* resource,
                                                             std::string old_name,
                                                             std::string new_name);
    node_alter_table_ptr make_node_alter_table_multi(std::pmr::memory_resource* resource,
                                                     std::vector<alter_table_subcommand_t> subcommands);

} // namespace components::logical_plan
