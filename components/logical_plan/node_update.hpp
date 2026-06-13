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
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const std::pmr::vector<expressions::update_expr_ptr>& updates,
                               bool upsert = false);

        const std::pmr::vector<expressions::update_expr_ptr>& updates() const;
        bool upsert() const;

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // UPDATE ... FROM source-side table_oid (the USING-clause table).
        // enrich_logical_plan stamps this from the sibling resolve_table for the
        // FROM target. Default INVALID_OID — caller must check before using.
        components::catalog::oid_t table_oid_from() const noexcept { return table_oid_from_; }
        void set_table_oid_from(components::catalog::oid_t oid) noexcept { table_oid_from_ = oid; }

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

    private:
        std::pmr::vector<expressions::update_expr_ptr> update_expressions_;
        std::pmr::vector<expressions::expression_ptr> returning_;
        bool upsert_;
        components::catalog::oid_t table_oid_from_{components::catalog::INVALID_OID};

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
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
