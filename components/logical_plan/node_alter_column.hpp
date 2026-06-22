#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/table/column_definition.hpp>

#include <memory_resource>
#include <string>

namespace components::logical_plan {

    enum class alter_column_op : uint8_t
    {
        add,
        rename,
        drop
    };

    // Flat ALTER-COLUMN primitive carrying the clause op() plus the role-named
    // payload each clause uses. node_alter_table_t (the parser-level multi-clause
    // node) STAYS; this node is what rewrite_alter_table lowers each subcommand into.
    //
    // Field usage by op:
    //   add    — column_
    //   rename — old_name_ / new_name_ / attoid_
    //   drop   — namespace_oid_ / column_name_ / behavior_ / attoid_
    //   (base) — table_oid() set at construction time by the planner.
    //
    // computed_ marks the relkind='g' (Mongo-style dynamic schema) variants.
    // When computed_==true:
    //   op=add  — registered_cols_ carries the columns to (re)register in
    //             pg_computed_column (the data the INSERT chunk surfaced);
    //             create_plan routes to operator_computed_field_register_t.
    //   op=drop — column_name_ / attoid_ identify the tombstoned column;
    //             create_plan routes to operator_computed_field_unregister_t.
    // computed_==false routes to the plain operator_alter_column_* operators.
    class node_alter_column_t final : public node_t {
    public:
        node_alter_column_t(std::pmr::memory_resource* resource, alter_column_op op);

        alter_column_op op() const noexcept { return op_; }

        // add
        const components::table::column_definition_t& column() const noexcept { return column_; }
        components::table::column_definition_t& column() noexcept { return column_; }
        void set_column(components::table::column_definition_t column) { column_ = std::move(column); }

        // rename
        const std::string& old_name() const noexcept { return old_name_; }
        void set_old_name(core::columnname_t name) { old_name_ = std::move(static_cast<std::string&>(name)); }
        const std::string& new_name() const noexcept { return new_name_; }
        void set_new_name(core::columnname_t name) { new_name_ = std::move(static_cast<std::string&>(name)); }

        // drop (column_name shared with computed unregister; attoid shared with rename)
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }
        const std::string& column_name() const noexcept { return column_name_; }
        void set_column_name(core::columnname_t name) { column_name_ = std::move(static_cast<std::string&>(name)); }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        void set_behavior(components::catalog::drop_behavior_t b) noexcept { behavior_ = b; }

        // rename + drop
        components::catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(components::catalog::oid_t a) noexcept { attoid_ = a; }

        // computed (relkind='g')
        bool computed() const noexcept { return computed_; }
        void set_computed(bool v) noexcept { computed_ = v; }
        const std::pmr::vector<components::table::column_definition_t>& registered_cols() const noexcept {
            return registered_cols_;
        }
        void set_registered_cols(std::pmr::vector<components::table::column_definition_t> cols) {
            registered_cols_ = std::move(cols);
        }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const alter_column_op op_;
        // add
        components::table::column_definition_t column_;
        // rename
        std::string old_name_;
        std::string new_name_;
        // drop
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        std::string column_name_;
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
        // rename + drop
        components::catalog::oid_t attoid_{components::catalog::INVALID_OID};
        // computed (relkind='g')
        bool computed_{false};
        std::pmr::vector<components::table::column_definition_t> registered_cols_;
    };

    using node_alter_column_ptr = boost::intrusive_ptr<node_alter_column_t>;
    node_alter_column_ptr make_node_alter_column(std::pmr::memory_resource* resource, alter_column_op op);

} // namespace components::logical_plan
