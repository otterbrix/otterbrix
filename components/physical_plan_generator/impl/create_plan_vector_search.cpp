#include "create_plan_vector_search.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_vector_search.hpp>
#include <components/physical_plan/operators/operator_vector_search.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_vector_search(const context_storage_t& context,
                                                                  const components::logical_plan::node_ptr& node) {
        auto vs_node = static_cast<const components::logical_plan::node_vector_search_t*>(node.get());

        components::expressions::compare_expression_ptr filter;
        if (!node->expressions().empty()) {
            filter = boost::dynamic_pointer_cast<components::expressions::compare_expression_t>(
                node->expressions()[0]);
        }

        if (context.has_table_oid(node->table_oid())) {
            return boost::intrusive_ptr(
                new components::operators::operator_vector_search_t(context.resource,
                                                                    context.log.clone(),
                                                                    node->table_oid(),
                                                                    vs_node->column_name(),
                                                                    vs_node->query_vector(),
                                                                    vs_node->k(),
                                                                    vs_node->metric(),
                                                                    filter,
                                                                    vs_node->strategy(),
                                                                    vs_node->descending()));
        } else {
            return boost::intrusive_ptr(
                new components::operators::operator_vector_search_t(nullptr,
                                                                    log_t{},
                                                                    node->table_oid(),
                                                                    vs_node->column_name(),
                                                                    vs_node->query_vector(),
                                                                    vs_node->k(),
                                                                    vs_node->metric(),
                                                                    filter,
                                                                    vs_node->strategy(),
                                                                    vs_node->descending()));
        }
    }

} // namespace services::planner::impl
