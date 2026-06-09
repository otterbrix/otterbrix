#include "create_plan_cte_scan.hpp"

#include <components/logical_plan/node_cte_scan.hpp>
#include <components/physical_plan/operators/operator_cte_scan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_cte_scan(const context_storage_t& context,
                         const components::compute::function_registry_t& /*function_registry*/,
                         const components::logical_plan::node_ptr& node,
                         components::logical_plan::limit_t /*limit*/,
                         const components::logical_plan::storage_parameters* /*params*/) {
        const auto* scan_node = static_cast<const components::logical_plan::node_cte_scan_t*>(node.get());
        auto it = context.cte_working_sets.find(scan_node->cte_name());
        if (it == context.cte_working_sets.end()) {
            return boost::intrusive_ptr(
                new components::operators::operator_cte_scan_t(context.resource, context.log.clone(), nullptr));
        }
        return boost::intrusive_ptr(
            new components::operators::operator_cte_scan_t(context.resource, context.log.clone(), it->second));
    }

} // namespace services::planner::impl
