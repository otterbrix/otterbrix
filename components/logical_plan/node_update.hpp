#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/fk_info.hpp>
#include <components/expressions/update_expression.hpp>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               std::string dbname_to,
                               std::string relname_to,
                               std::string dbname_from,
                               std::string relname_from,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const std::pmr::vector<expressions::update_expr_ptr>& updates,
                               bool upsert = false);

        const std::pmr::vector<expressions::update_expr_ptr>& updates() const;
        bool upsert() const;
        const std::string& dbname_from() const noexcept { return dbname_from_; }
        const std::string& relname_from() const noexcept { return relname_from_; }

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

        // Phase 9.W/10.D: role-named accessors. UPDATE target table identity at parser stage;
        // routing in resolved-stage code uses table_oid().
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        std::string dbname_from_;
        std::string relname_from_;
        std::pmr::vector<expressions::update_expr_ptr> update_expressions_;
        bool upsert_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
    };

    using node_update_ptr = boost::intrusive_ptr<node_update_t>;

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          std::string dbname,
                                          std::string relname,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          std::string dbname_to,
                                          std::string relname_to,
                                          std::string dbname_from,
                                          std::string relname_from,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         std::string dbname,
                                         std::string relname,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         std::string dbname_to,
                                         std::string relname_to,
                                         std::string dbname_from,
                                         std::string relname_from,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     std::string dbname_to,
                                     std::string relname_to,
                                     std::string dbname_from,
                                     std::string relname_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

} // namespace components::logical_plan
