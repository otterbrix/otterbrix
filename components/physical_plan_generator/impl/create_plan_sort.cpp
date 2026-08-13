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
            if (expr->group() == components::expressions::expression_group::sort) {
                // Regular sort key: path was resolved by validate_logical_plan
                const auto* sort_expr = static_cast<components::expressions::sort_expression_t*>(expr.get());
                const auto& path = sort_expr->key().path();
                if (path.empty()) {
                    // Defensive guard (validation resolves the path so this never fires): return
                    // nullptr -> executor surfaces the error (rule 9: no throw on the operator-build path).
                    return nullptr;
                }
                const auto ord = components::operators::operator_sort_t::order(sort_expr->order());
                sort->add(path, ord, resolve_null_order(sort_expr->null_order(), ord));
            } else if (expr->group() == components::expressions::expression_group::scalar) {
                // Computed arithmetic sort key (from ORDER BY arithmetic expression).
                // Sort order is encoded in key.path()[0]: 0 = ascending, 1 = descending.
                const auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                components::operators::sort_key_spec_t ck(plan_resource);
                ck.expression = expr;
                const auto& sort_path = scalar_expr->key().path();
                bool is_desc = !sort_path.empty() && sort_path[0] == size_t(1);
                ck.order_ = is_desc ? components::sort::order::descending : components::sort::order::ascending;
                // path[1] (when present) encodes the SQL NULLS placement: 0 default, 1 first, 2 last.
                auto requested_nulls = sort_path.size() > 1
                                           ? static_cast<components::expressions::sort_null_order>(sort_path[1])
                                           : components::expressions::sort_null_order::nulls_default;
                ck.null_order_ = resolve_null_order(requested_nulls, ck.order_);
                sort->add_computed(std::move(ck));
            }
        }
        sort->set_limit(limit);
        return sort;
    }

} // namespace services::planner::impl
