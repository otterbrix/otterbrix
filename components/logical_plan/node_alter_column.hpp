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

    // Lowered from node_alter_table_t by rewrite_alter_table; computed_ (relkind='g') routes add/drop through
    // operator_computed_field_register_t/unregister_t instead of the plain operators.
    class node_alter_column_t final : public node_t {
    public:
        node_alter_column_t(std::pmr::memory_resource* resource, alter_column_op op);

        alter_column_op op() const noexcept { return op_; }

        const components::table::column_definition_t& column() const noexcept { return column_; }
        components::table::column_definition_t& column() noexcept { return column_; }
        void set_column(components::table::column_definition_t column) { column_ = std::move(column); }

        const std::string& old_name() const noexcept { return old_name_; }
        void set_old_name(core::columnname_t name) { old_name_ = std::move(static_cast<std::string&>(name)); }
        const std::string& new_name() const noexcept { return new_name_; }
        void set_new_name(core::columnname_t name) { new_name_ = std::move(static_cast<std::string&>(name)); }

        // column_name_ is shared with the computed-unregister route.
        const std::string& column_name() const noexcept { return column_name_; }
        void set_column_name(core::columnname_t name) { column_name_ = std::move(static_cast<std::string&>(name)); }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        void set_behavior(components::catalog::drop_behavior_t b) noexcept { behavior_ = b; }
        // DROP COLUMN IF EXISTS: carried for both drop routes so a missing column is a no-op, not an error.
        bool missing_ok() const noexcept { return missing_ok_; }
        void set_missing_ok(bool v) noexcept { missing_ok_ = v; }

        components::catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(components::catalog::oid_t a) noexcept { attoid_ = a; }

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
        components::table::column_definition_t column_;
        std::string old_name_;
        std::string new_name_;
        std::string column_name_;
        // unwritten form defaults to RESTRICT (PostgreSQL parity); see node_alter_table.hpp
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::restrict_};
        bool missing_ok_{false};
        components::catalog::oid_t attoid_{components::catalog::INVALID_OID};
        bool computed_{false};
        std::pmr::vector<components::table::column_definition_t> registered_cols_;
    };

    using node_alter_column_ptr = boost::intrusive_ptr<node_alter_column_t>;
    node_alter_column_ptr make_node_alter_column(std::pmr::memory_resource* resource, alter_column_op op);

} // namespace components::logical_plan
