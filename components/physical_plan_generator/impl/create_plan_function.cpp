#include "create_plan_function.hpp"

#include <components/expressions/compare_expression.hpp> // is_key / as_key
#include <components/logical_plan/node_function.hpp>
#include <components/physical_plan/operators/operator_function.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_function(const context_storage_t& context,
                                                             const components::logical_plan::node_ptr& node) {
        const auto* function_node = static_cast<const components::logical_plan::node_function_t*>(node.get());

        auto* resource = context.has_table_oid(node->table_oid()) ? context.resource : node->resource();
        auto log = context.has_table_oid(node->table_oid()) ? context.log.clone() : log_t{};

        // This vector is MOVED into the operator below, which lives on `resource` and reads its
        // args during execution — after the logical plan, and node->resource() with it, is gone.
        // So the vector and every key inside it are placed on `resource`. A parameter_id_t arg
        // is a scalar with no arena; an expression arg is an intrusive pointer whose pointee
        // still lives on the node's arena (a separate question, untouched here).
        std::pmr::vector<components::expressions::param_storage> args(resource);
        args.reserve(function_node->args().size());
        for (const auto& arg : function_node->args()) {
            if (components::expressions::is_key(arg)) {
                args.emplace_back(components::expressions::key_t{components::expressions::as_key(arg), resource});
            } else {
                args.emplace_back(arg);
            }
        }

        const std::string& alias =
            function_node->result_alias().empty() ? function_node->name() : function_node->result_alias();

        return boost::intrusive_ptr(new components::operators::operator_function_t(resource,
                                                                                   std::move(log),
                                                                                   function_node->function_uid(),
                                                                                   std::move(args),
                                                                                   alias));
    }

} // namespace services::planner::impl
