#include "create_plan_sort.hpp"

#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_sort(const context_storage_t& context,
                                                         const components::logical_plan::node_ptr& node) {
        auto coll_name = node->collection_full_name();
        bool known = context.has_collection(coll_name);
        auto plan_resource = known ? context.resource : node->resource();
        auto sort = known ? boost::intrusive_ptr(
                                new components::operators::operator_sort_t(context.resource, context.log.clone()))
                          : boost::intrusive_ptr(new components::operators::operator_sort_t(node->resource(), log_t{}));

        for (const auto& expr : node->expressions()) {
            if (expr->group() == components::expressions::expression_group::sort) {
                // Regular sort key: path was resolved by validate_logical_plan
                const auto* sort_expr = static_cast<components::expressions::sort_expression_t*>(expr.get());
                const auto& path = sort_expr->key().path();
                if (path.empty()) {
                    throw std::logic_error("Sort key has unresolved path: " + sort_expr->key().as_string());
                }
                sort->add(path, components::operators::operator_sort_t::order(sort_expr->order()));
            } else if (expr->group() == components::expressions::expression_group::scalar) {
                // Computed arithmetic sort key (from ORDER BY arithmetic expression).
                // Sort order is encoded in key.path()[0]: 0 = ascending, 1 = descending.
                const auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                components::operators::computed_sort_key_t ck(plan_resource);
                ck.op = scalar_expr->type();
                ck.operands = scalar_expr->params();
                bool is_desc = !scalar_expr->key().path().empty() && scalar_expr->key().path()[0] == size_t(1);
                ck.order_ = is_desc ? components::sort::order::descending : components::sort::order::ascending;
                sort->add_computed(std::move(ck));
            }
        }
        return sort;
    }

} // namespace services::planner::impl
