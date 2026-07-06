#pragma once

#include <memory_resource>
#include <vector>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>

namespace components::planner::optimizer {

    // Split a predicate tree into its top-level AND-conjuncts. A `union_and` node
    // flattens (recursively) into its children; any other expression is a single
    // conjunct. The result vector is built on `resource` (rule 8). Shared by
    // pushdown_filter and promote_cross_join.
    inline std::pmr::vector<components::expressions::expression_ptr>
    split_conjuncts(std::pmr::memory_resource* resource, const components::expressions::expression_ptr& expr) {
        using namespace components::expressions;
        std::pmr::vector<expression_ptr> result{resource};
        if (!expr) {
            return result;
        }
        if (expr->group() == expression_group::compare) {
            auto* cmp = static_cast<compare_expression_t*>(expr.get());
            if (cmp->type() == compare_type::union_and) {
                for (const auto& child : cmp->children()) {
                    auto sub = split_conjuncts(resource, child);
                    result.insert(result.end(), sub.begin(), sub.end());
                }
                return result;
            }
        }
        result.push_back(expr);
        return result;
    }

    // Recombine conjuncts into one predicate: nullptr when empty, the single element
    // when size 1, otherwise a `union_and` over all of them. Built on `resource`
    // (rule 8).
    inline components::expressions::expression_ptr
    rebuild_conjunction(std::pmr::memory_resource* resource,
                        const std::pmr::vector<components::expressions::expression_ptr>& conjuncts) {
        using namespace components::expressions;
        if (conjuncts.empty()) {
            return nullptr;
        }
        if (conjuncts.size() == 1) {
            return conjuncts.front();
        }
        auto conj = make_compare_union_expression(resource, compare_type::union_and);
        for (const auto& c : conjuncts) {
            conj->append_child(c);
        }
        return conj;
    }

} // namespace components::planner::optimizer
