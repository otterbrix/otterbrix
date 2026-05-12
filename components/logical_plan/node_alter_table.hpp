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

    struct alter_table_subcommand_t {
        alter_table_kind kind{alter_table_kind::drop_column};
        std::string column_name;
        std::string new_column_name; // rename_column only
        components::table::column_definition_t column;
        alter_table_subcommand_t()
            : column("", components::types::complex_logical_type{components::types::logical_type::UNKNOWN}) {}
    };

    class node_alter_table_t final : public node_t {
    public:
        // Phase 10.D: ctors take role-named strings instead of cfn struct.
        // ADD COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           std::string dbname,
                           std::string relname,
                           components::table::column_definition_t column);
        // DROP COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           std::string dbname,
                           std::string relname,
                           alter_table_kind kind,
                           std::string column_name);
        // RENAME COLUMN form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           std::string dbname,
                           std::string relname,
                           std::string old_name,
                           std::string new_name);
        // Multi-clause form.
        node_alter_table_t(std::pmr::memory_resource* resource,
                           std::string dbname,
                           std::string relname,
                           std::vector<alter_table_subcommand_t> subcommands);

        // Backward-compatible accessors — return data from FIRST subcommand.
        alter_table_kind kind() const noexcept { return subcommands_.front().kind; }
        const std::string& column_name() const noexcept { return subcommands_.front().column_name; }
        const std::string& new_column_name() const noexcept { return subcommands_.front().new_column_name; }
        const components::table::column_definition_t& column() const { return subcommands_.front().column; }

        const std::vector<alter_table_subcommand_t>& subcommands() const noexcept { return subcommands_; }

        char relkind() const noexcept { return relkind_; }
        void set_relkind(char rk) noexcept { relkind_ = rk; }

        // Phase 9.W/10.D: role-named accessors. ALTER TABLE target identifiers; routing
        // uses table_oid stamped by enrich.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::vector<alter_table_subcommand_t> subcommands_;
        char relkind_{components::catalog::relkind::regular};
    };

    using node_alter_table_ptr = boost::intrusive_ptr<node_alter_table_t>;

    node_alter_table_ptr make_node_alter_table_add_column(std::pmr::memory_resource* resource,
                                                           std::string dbname,
                                                           std::string relname,
                                                           components::table::column_definition_t column);
    node_alter_table_ptr make_node_alter_table_drop_column(std::pmr::memory_resource* resource,
                                                            std::string dbname,
                                                            std::string relname,
                                                            std::string column_name);
    node_alter_table_ptr make_node_alter_table_rename_column(std::pmr::memory_resource* resource,
                                                              std::string dbname,
                                                              std::string relname,
                                                              std::string old_name,
                                                              std::string new_name);
    node_alter_table_ptr make_node_alter_table_multi(std::pmr::memory_resource* resource,
                                                      std::string dbname,
                                                      std::string relname,
                                                      std::vector<alter_table_subcommand_t> subcommands);

} // namespace components::logical_plan
