#include "create_plan_function.hpp"

#include <components/logical_plan/node_function.hpp>
#include <components/physical_plan/operators/operator_function.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_function(const context_storage_t& context,
                                                             const components::logical_plan::node_ptr& node) {
        const auto* function_node = static_cast<const components::logical_plan::node_function_t*>(node.get());

        auto* resource = context.has_table_oid(node->table_oid()) ? context.resource : node->resource();
        auto log = context.has_table_oid(node->table_oid()) ? context.log.clone() : log_t{};

        std::pmr::vector<components::expressions::param_storage> args(node->resource());
        args.reserve(function_node->args().size());
        for (const auto& arg : function_node->args()) {
            args.emplace_back(arg);
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
