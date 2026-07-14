#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/fk_info.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory>
#include <utility>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const std::pmr::vector<expressions::update_expr_ptr>& updates,
                               bool upsert = false);

        const std::pmr::vector<expressions::update_expr_ptr>& updates() const;
        bool upsert() const;

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

        // UNIQUE / PRIMARY KEY column groups (contype 'u'/'p'), one ordered
        // column-name list per constraint. Stamped by the dispatcher's enrich pass;
        // the planner forwards these onto the node_check_constraint_t wrapper so
        // operator_unique_constraint_t enforces them on the UPDATE write-set.
        void set_unique_groups(std::vector<std::vector<std::string>> v) { unique_groups_ = std::move(v); }
        const std::vector<std::vector<std::string>>& unique_groups() const { return unique_groups_; }

        // Decoded column DEFAULTs as a one-row data_chunk_t for the constraint operators
        // (see node_insert_t::column_defaults for the shape). Stamped by enrich; the
        // planner deep-copies it onto the node_check_constraint_t wrapper.
        void set_column_defaults(std::unique_ptr<vector::data_chunk_t> v) { column_defaults_ = std::move(v); }
        const vector::data_chunk_t* column_defaults() const noexcept { return column_defaults_.get(); }

    private:
        std::pmr::vector<expressions::update_expr_ptr> update_expressions_;
        std::pmr::vector<expressions::expression_ptr> returning_;
        bool upsert_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
        std::vector<std::vector<std::string>> unique_groups_;   // UNIQUE / PK column groups
        std::unique_ptr<vector::data_chunk_t> column_defaults_; // one-row chunk; nullptr = no defaults
    };

    using node_update_ptr = boost::intrusive_ptr<node_update_t>;

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

} // namespace components::logical_plan
