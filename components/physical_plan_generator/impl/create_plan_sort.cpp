#include "create_plan_sort.hpp"

#include <components/expressions/sort_expression.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_sort(const context_storage_t& context,
                                                               const components::logical_plan::node_ptr& node) {
        auto sort = boost::intrusive_ptr(
            new components::operators::operator_sort_t(context.at(node->collection_full_name())));
        std::for_each(node->expressions().begin(),
                      node->expressions().end(),
                      [&sort](const components::expressions::expression_ptr& expr) {
                          const auto* sort_expr = static_cast<components::expressions::sort_expression_t*>(expr.get());
                          sort->add(sort_expr->key().as_string(),
                                    components::operators::operator_sort_t::order(sort_expr->order()));
                      });
        return sort;
    }

} // namespace services::planner::impl