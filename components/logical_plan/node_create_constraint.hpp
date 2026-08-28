#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

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
                                 std::string dbname,
                                 std::string relname,
                                 core::constraint_name_t name,
                                 constraint_kind kind,
                                 std::string ref_dbname = {});

        const std::string& name() const noexcept { return name_; }
        constraint_kind kind() const noexcept { return kind_; }
        // The FK's referenced table, as written. With ref_dbname() it is how enrich
        // binds ref_table_oid() to a resolved entry by name — the local table and the
        // referenced table are two independent lookups, not "the first and second".
        const std::string& ref_dbname() const noexcept { return ref_dbname_; }
        const std::string& ref_relname() const noexcept { return ref_relname_; }
        void set_ref_relname(std::string relname) { ref_relname_ = std::move(relname); }

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
        void set_check_col_attoids(std::vector<components::catalog::oid_t> v) noexcept {
            check_col_attoids_ = std::move(v);
        }

        components::catalog::oid_t ref_table_oid() const noexcept { return ref_table_oid_; }
        void set_ref_table_oid(components::catalog::oid_t oid) noexcept { ref_table_oid_ = oid; }

        const std::vector<components::catalog::oid_t>& fk_col_attoids() const noexcept { return fk_col_attoids_; }
        void set_fk_col_attoids(std::vector<components::catalog::oid_t> v) noexcept { fk_col_attoids_ = std::move(v); }

        const std::vector<components::catalog::oid_t>& ref_col_attoids() const noexcept { return ref_col_attoids_; }
        void set_ref_col_attoids(std::vector<components::catalog::oid_t> v) noexcept {
            ref_col_attoids_ = std::move(v);
        }

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::string ref_relname_;
        std::string name_;
        constraint_kind kind_;
        std::string ref_dbname_;
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
    };

    using node_create_constraint_ptr = boost::intrusive_ptr<node_create_constraint_t>;

    node_create_constraint_ptr make_node_create_constraint(std::pmr::memory_resource* resource,
                                                           std::string dbname,
                                                           std::string relname,
                                                           core::constraint_name_t name,
                                                           constraint_kind kind,
                                                           std::string ref_dbname = {});

} // namespace components::logical_plan
