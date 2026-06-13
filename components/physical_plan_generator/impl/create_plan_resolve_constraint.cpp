#include "create_plan_resolve_constraint.hpp"

#include <components/logical_plan/node_catalog_resolve_constraint.hpp>
#include <components/physical_plan/operators/operator_resolve_constraint.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_constraint(const context_storage_t& context,
                                                                       const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_constraint_t*>(node.get());
        return boost::intrusive_ptr(
            new components::operators::operator_resolve_constraint_t(context.resource, context.log.clone(), n));
    }

} // namespace services::planner::impl
