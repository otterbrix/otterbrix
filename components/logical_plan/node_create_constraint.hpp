#pragma once

#include "node.hpp"

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

    // node_create_constraint — single CREATE CONSTRAINT or table-bound constraint clause.
    // FK form carries ref_collection (database+collection) for the referenced table; the
    // executor resolves it to ref_table_oid via resolve_table before calling ddl_create_constraint.
    // Non-FK forms leave ref_collection empty.
    class node_create_constraint_t final : public node_t {
    public:
        node_create_constraint_t(std::pmr::memory_resource* resource,
                                  const collection_full_name_t& collection,
                                  std::string name,
                                  constraint_kind kind,
                                  collection_full_name_t ref_collection);

        const std::string& name() const noexcept { return name_; }
        constraint_kind kind() const noexcept { return kind_; }
        const collection_full_name_t& ref_collection() const noexcept { return ref_collection_; }
        const std::vector<std::string>& columns() const noexcept { return columns_; }
        const std::vector<std::string>& ref_columns() const noexcept { return ref_columns_; }
        std::vector<std::string>& columns() noexcept { return columns_; }
        std::vector<std::string>& ref_columns() noexcept { return ref_columns_; }

        // FK semantic flags — meaningful only when kind_ == foreign_key.
        // Defaults: 's' MATCH SIMPLE, 'a' NO ACTION on delete/update.
        char match_type() const noexcept { return match_type_; }
        char del_action() const noexcept { return del_action_; }
        char upd_action() const noexcept { return upd_action_; }
        void set_match_type(char c) noexcept { match_type_ = c; }
        void set_del_action(char c) noexcept { del_action_ = c; }
        void set_upd_action(char c) noexcept { upd_action_ = c; }

        // CHECK constraint expression SQL text — meaningful only when kind_ == check.
        const std::string& check_expr() const noexcept { return check_expr_; }
        void set_check_expr(std::string expr) { check_expr_ = std::move(expr); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        constraint_kind kind_;
        collection_full_name_t ref_collection_;
        std::vector<std::string> columns_;     // local column names participating in constraint
        std::vector<std::string> ref_columns_; // referenced column names (FK only)
        char match_type_{'s'};                 // 's' SIMPLE / 'f' FULL / 'p' PARTIAL
        char del_action_{'a'};                 // 'a' NO ACTION / 'r' RESTRICT / 'c' CASCADE / 'n' SET NULL / 'd' SET DEFAULT
        char upd_action_{'a'};                 // same alphabet
        std::string check_expr_;               // CHECK constraint SQL text (empty for non-CHECK)
    };

    using node_create_constraint_ptr = boost::intrusive_ptr<node_create_constraint_t>;

    node_create_constraint_ptr
    make_node_create_constraint(std::pmr::memory_resource* resource,
                                  const collection_full_name_t& collection,
                                  std::string name,
                                  constraint_kind kind,
                                  collection_full_name_t ref_collection = {});

} // namespace components::logical_plan
