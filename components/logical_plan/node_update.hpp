#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/results/fk_result.hpp>
#include <components/expressions/update_expression.hpp>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection_to,
                               const collection_full_name_t& collection_from,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const std::pmr::vector<expressions::update_expr_ptr>& updates,
                               bool upsert = false);

        const std::pmr::vector<expressions::update_expr_ptr>& updates() const;
        bool upsert() const;
        const collection_full_name_t& collection_from() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_outgoing_fks(std::vector<catalog::resolved_fk_t> v) { outgoing_fks_ = std::move(v); }
        void set_check_exprs(std::vector<std::string> v)              { check_exprs_  = std::move(v); }
        void set_not_null_cols(std::vector<std::string> v)            { not_null_cols_ = std::move(v); }

        const std::vector<catalog::resolved_fk_t>& outgoing_fks()  const { return outgoing_fks_; }
        const std::vector<std::string>&             check_exprs()   const { return check_exprs_; }
        const std::vector<std::string>&             not_null_cols() const { return not_null_cols_; }

    private:
        collection_full_name_t collection_from_;
        std::pmr::vector<expressions::update_expr_ptr> update_expressions_;
        bool upsert_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<catalog::resolved_fk_t> outgoing_fks_;
        std::vector<std::string>            check_exprs_;
        std::vector<std::string>            not_null_cols_;
    };

    using node_update_ptr = boost::intrusive_ptr<node_update_t>;

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection_to,
                                          const collection_full_name_t& collection_from,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection_to,
                                         const collection_full_name_t& collection_from,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection_to,
                                     const collection_full_name_t& collection_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

} // namespace components::logical_plan