#include "create_plan_function.hpp"

#include <components/expressions/compare_expression.hpp> // is_key / as_key
#include <components/logical_plan/node_function.hpp>
#include <components/physical_plan/operators/operator_function.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_function(const context_storage_t& context,
                         const components::compute::function_registry_t& function_registry,
                         const components::logical_plan::node_ptr& node) {
        const auto* function_node = static_cast<const components::logical_plan::node_function_t*>(node.get());

        auto* resource = context.resource;
        auto log = context.log.clone();

        // Moved into the operator (lives on `resource`) after the logical plan's own arena is gone,
        // so every key must be placed on `resource` too.
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

        // Validation resolved the uid against this same registry, so the operator owns a copy and runs without one.
        const auto* function = function_registry.get_function(function_node->function_uid());
        return boost::intrusive_ptr(new components::operators::operator_function_t(
            resource,
            std::move(log),
            function == nullptr ? components::compute::no_function() : function->get_copy(resource),
            std::move(args),
            alias));
    }

} // namespace services::planner::impl
