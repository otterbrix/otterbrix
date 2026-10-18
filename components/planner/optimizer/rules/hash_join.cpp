#include "hash_join.hpp"

#include "conjunct_utils.hpp"

#include <cstdint>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace ce = components::expressions;
        namespace lp = components::logical_plan;

        constexpr size_t left_input = 0;
        constexpr size_t right_input = 1;
        constexpr size_t no_input = SIZE_MAX;

        // A multi-element path is a nested-struct field access; those keys stay on the nested-loop join.
        size_t operand_side(ce::param_storage& operand) {
            return operand_input(
                operand,
                [](const ce::key_t& key) {
                    if (key.path().size() != 1) {
                        return no_input;
                    }
                    switch (key.side()) {
                        case ce::side_t::left:
                            return left_input;
                        case ce::side_t::right:
                            return right_input;
                        default:
                            return no_input;
                    }
                },
                no_input);
        }

        // A single eq whose operands each read one side, one per side; the left side's operand is moved first.
        bool is_hash_condition(const ce::expression_ptr& expr) {
            if (!expr || expr->group() != ce::expression_group::compare) {
                return false;
            }
            auto* cmp = static_cast<ce::compare_expression_t*>(expr.get());
            if (cmp->type() != ce::compare_type::eq) {
                return false;
            }
            const size_t l = operand_side(cmp->left());
            const size_t r = operand_side(cmp->right());
            if (l == no_input || r == no_input || l == r) {
                return false;
            }
            if (l == right_input) {
                ce::param_storage left = std::move(cmp->left());
                cmp->left() = std::move(cmp->right());
                cmp->right() = std::move(left);
            }
            return true;
        }

        // CROSS has no equi-condition; INVALID is rejected during planning. The hash
        // path implements inner / left / right / full only.
        bool is_equi_joinable(lp::join_type t) {
            using jt = lp::join_type;
            return t == jt::inner || t == jt::left || t == jt::right || t == jt::full;
        }

        lp::node_ptr try_rewrite_join(const lp::node_ptr& node) {
            if (node->type() != lp::node_type::join_t) {
                return node;
            }
            auto* join = static_cast<lp::node_join_t*>(node.get());
            if (!is_equi_joinable(join->type()) || node->expressions().empty()) {
                return node;
            }
            if (is_hash_condition(node->expressions().front())) {
                join->set_algo(lp::node_join_t::join_algo::hash);
            }
            return node;
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
