#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/expression.hpp>

#include <string>

namespace components::logical_plan {

    enum class constraint_kind : char
    {
        primary_key = 'p',
        foreign_key = 'f',
        unique = 'u',
        check = 'c',
        not_null = 'n',
    };

    class node_create_constraint_t final : public node_t {
    public:
        node_create_constraint_t(std::pmr::memory_resource* resource,
                                 qualified_name_t target,
                                 core::constraint_name_t name,
                                 constraint_kind kind,
                                 qualified_name_t ref = {});

        const core::constraint_name_t& name() const noexcept { return name_; }
        constraint_kind kind() const noexcept { return kind_; }
        // The FK's referenced table, as written: enrich binds ref_table_oid() by it, a lookup of its own beside the
        // constrained table's.
        const qualified_name_t& ref() const noexcept { return ref_; }
        void set_ref(qualified_name_t ref) { ref_ = std::move(ref); }

        const std::vector<std::string>& local_col_names() const noexcept { return local_col_names_; }
        const std::vector<std::string>& ref_col_names() const noexcept { return ref_col_names_; }
        void set_local_col_names(std::vector<std::string> v) noexcept { local_col_names_ = std::move(v); }
        void set_ref_col_names(std::vector<std::string> v) noexcept { ref_col_names_ = std::move(v); }

        char match_type() const noexcept { return match_type_; }
        char del_action() const noexcept { return del_action_; }
        char upd_action() const noexcept { return upd_action_; }
        void set_match_type(char c) noexcept { match_type_ = c; }
        void set_del_action(char c) noexcept { del_action_ = c; }
        void set_upd_action(char c) noexcept { upd_action_ = c; }

        // parsed to expression, so we can validate it
        const expressions::expression_ptr& check_expression() const noexcept { return check_expression_; }
        void set_check_expression(expressions::expression_ptr expr) { check_expression_ = std::move(expr); }

        const std::string& check_expression_sql() const noexcept { return check_expression_sql_; }
        void set_check_expression_sql(std::string sql) { check_expression_sql_ = std::move(sql); }

        const std::vector<components::catalog::oid_t>& check_col_attoids() const noexcept { return check_col_attoids_; }

        components::catalog::oid_t ref_table_oid() const noexcept { return ref_table_oid_; }
        void set_ref_table_oid(components::catalog::oid_t oid) noexcept { ref_table_oid_ = oid; }

        const std::vector<components::catalog::oid_t>& fk_col_attoids() const noexcept { return fk_col_attoids_; }
        void set_fk_col_attoids(std::vector<components::catalog::oid_t> v) noexcept { fk_col_attoids_ = std::move(v); }

        const std::vector<components::catalog::oid_t>& ref_col_attoids() const noexcept { return ref_col_attoids_; }
        void set_ref_col_attoids(std::vector<components::catalog::oid_t> v) noexcept {
            ref_col_attoids_ = std::move(v);
        }

        // set when this node is a child of node_create_collection_t: its table doesn't
        // exist yet, so the parent's enrich case checks names and rewrite_create_table
        // mints the attoids; this node's own enrich case is skipped
        bool inline_with_table() const noexcept { return inline_with_table_; }
        void set_inline_with_table(bool value) noexcept { inline_with_table_ = value; }

        // inline FK referencing the table being created (`CREATE TABLE t (... REFERENCES
        // t (id))`); no catalog entry to resolve against, both oids minted by the same rewrite
        bool self_reference() const noexcept { return self_reference_; }
        void set_self_reference(bool value) noexcept { self_reference_ = value; }

        // ALTER TABLE IF EXISTS ... ADD CONSTRAINT (AlterTableStmt.missing_ok): a missing table is an empty success
        // instead of the refusal; every other refusal stays one.
        bool if_exists() const noexcept { return if_exists_; }
        void set_if_exists(bool value) noexcept { if_exists_ = value; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        core::constraint_name_t name_;
        constraint_kind kind_;
        qualified_name_t ref_;
        std::vector<std::string> local_col_names_;
        std::vector<std::string> ref_col_names_;
        char match_type_{'s'};
        char del_action_{'a'};
        char upd_action_{'a'};
        expressions::expression_ptr check_expression_;
        std::string check_expression_sql_;
        std::vector<components::catalog::oid_t> check_col_attoids_;
        components::catalog::oid_t ref_table_oid_{components::catalog::INVALID_OID};
        std::vector<components::catalog::oid_t> fk_col_attoids_;
        std::vector<components::catalog::oid_t> ref_col_attoids_;
        bool inline_with_table_{false};
        bool self_reference_{false};
        bool if_exists_{false};
    };

    using node_create_constraint_ptr = boost::intrusive_ptr<node_create_constraint_t>;

    node_create_constraint_ptr make_node_create_constraint(std::pmr::memory_resource* resource,
                                                           qualified_name_t target,
                                                           core::constraint_name_t name,
                                                           constraint_kind kind,
                                                           qualified_name_t ref = {});

} // namespace components::logical_plan
