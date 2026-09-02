#include "create_plan_sort.hpp"

#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

namespace services::planner::impl {

    namespace {
        components::sort::null_order resolve_null_order(components::expressions::sort_null_order requested,
                                                        components::sort::order ord) {
            switch (requested) {
                case components::expressions::sort_null_order::nulls_first:
                    return components::sort::null_order::first;
                case components::expressions::sort_null_order::nulls_last:
                    return components::sort::null_order::last;
                default:
                    return ord == components::sort::order::ascending ? components::sort::null_order::last
                                                                     : components::sort::null_order::first;
            }
        }
    } // namespace

    components::operators::operator_ptr create_plan_sort(const context_storage_t& context,
                                                         const components::logical_plan::node_ptr& node,
                                                         components::logical_plan::limit_t limit) {
        auto table_oid = node->table_oid();
        bool known = context.has_table_oid(table_oid);
        auto plan_resource = known ? context.resource : node->resource();
        auto sort =
            known ? boost::intrusive_ptr(new components::operators::operator_sort_t(plan_resource, context.log.clone()))
                  : boost::intrusive_ptr(new components::operators::operator_sort_t(node->resource(), log_t{}));

        for (const auto& expr : node->expressions()) {
            if (expr->group() != components::expressions::expression_group::sort) {
                continue;
            }
            const auto* sort_expr = static_cast<components::expressions::sort_expression_t*>(expr.get());
            const auto ord = components::operators::operator_sort_t::order(sort_expr->order());
            const auto nulls = resolve_null_order(sort_expr->null_order(), ord);
            if (components::expressions::is_key(sort_expr->operand())) {
                const auto& path = components::expressions::as_key(sort_expr->operand()).path();
                if (path.empty()) {
                    // Defensive guard (validation resolves the path so this never fires): return
                    // nullptr -> executor surfaces the error (rule 9: no throw on the operator-build path).
                    return nullptr;
                }
                sort->add(path, ord, nulls);
                continue;
            }
            components::operators::sort_key_spec_t computed(plan_resource);
            computed.expression = std::get<components::expressions::expression_ptr>(sort_expr->operand());
            computed.order_ = ord;
            computed.null_order_ = nulls;
            sort->add_computed(std::move(computed));
        }
        sort->set_limit(limit);
        return sort;
    }

} // namespace services::planner::impl
