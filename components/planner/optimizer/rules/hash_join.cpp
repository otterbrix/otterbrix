#include "hash_join.hpp"

#include <optional>
#include <utility>
#include <variant>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace ce = components::expressions;
        namespace lp = components::logical_plan;

        // Detect a single equi-comparison `eq(left.key, right.key)` and return the
        // (left_col, right_col) column indices into each side's input chunk. The
        // validator stamps key.side()/key.path() during JOIN validation, so we rely on
        // those. Returns nullopt for anything else — non-eq comparisons, AND/union
        // conditions (group() == compare but type() != eq), const operands, or two keys
        // on the same side — and the join is left as a nested-loop join.
        std::optional<std::pair<size_t, size_t>> detect_equi_columns(const ce::expression_ptr& expr) {
            if (!expr || expr->group() != ce::expression_group::compare) {
                return std::nullopt;
            }
            const auto* cmp = static_cast<const ce::compare_expression_t*>(expr.get());
            if (cmp->type() != ce::compare_type::eq) {
                return std::nullopt;
            }
            if (!std::holds_alternative<ce::key_t>(cmp->left()) || !std::holds_alternative<ce::key_t>(cmp->right())) {
                return std::nullopt;
            }
            const auto& lk = std::get<ce::key_t>(cmp->left());
            const auto& rk = std::get<ce::key_t>(cmp->right());
            // Only a single top-level column maps to a hash-table probe. A multi-element
            // path is a nested-struct/UDT field access (e.g. `(custom_type).f1`); path()[0]
            // would address the whole struct column, not the scalar being compared, so we
            // leave those to the nested-loop join, which evaluates the full path correctly.
            if (lk.path().size() != 1 || rk.path().size() != 1) {
                return std::nullopt;
            }
            if (lk.side() == ce::side_t::left && rk.side() == ce::side_t::right) {
                return std::make_pair(lk.path()[0], rk.path()[0]);
            }
            if (lk.side() == ce::side_t::right && rk.side() == ce::side_t::left) {
                return std::make_pair(rk.path()[0], lk.path()[0]);
            }
            return std::nullopt;
        }

        // CROSS has no equi-condition; INVALID is rejected during planning. The hash
        // path implements inner / left / right / full only.
        bool is_equi_joinable(lp::join_type t) {
            using jt = lp::join_type;
            return t == jt::inner || t == jt::left || t == jt::right || t == jt::full;
        }

        // If `node` is a hash-eligible join, return its node_hash_join_t replacement
        // (carrying the original children, ON-condition expressions, oid and alias);
        // otherwise return `node` unchanged.
        lp::node_ptr try_rewrite_join(const lp::node_ptr& node) {
            if (node->type() != lp::node_type::join_t) {
                return node;
            }
            const auto* join = static_cast<const lp::node_join_t*>(node.get());
            if (!is_equi_joinable(join->type()) || node->expressions().empty()) {
                return node;
            }
            auto equi = detect_equi_columns(node->expressions().front());
            if (!equi) {
                return node;
            }
            // Join nodes carry no dbname/relname (always empty — see transform_select);
            // the equi-key indices are all the hash-join operator needs.
            auto hj = lp::make_node_hash_join(node->resource(),
                                              core::dbname_t{},
                                              core::relname_t{},
                                              join->type(),
                                              equi->first,
                                              equi->second);
            for (const auto& child : node->children()) {
                hj->append_child(child);
            }
            hj->append_expressions(node->expressions());
            hj->set_table_oid(node->table_oid());
            hj->set_result_alias(node->result_alias());
            return hj;
        }

        lp::node_ptr walk(const lp::node_ptr& node) {
            if (!node) {
                return node;
            }
            for (auto& child : node->children()) {
                child = walk(child);
            }
            return try_rewrite_join(node);
        }
    } // namespace

    logical_plan::node_ptr rewrite_hash_joins(std::pmr::memory_resource* /*resource*/, logical_plan::node_ptr root) {
        return walk(root);
    }

} // namespace components::planner::optimizer
