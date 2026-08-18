#include "create_plan_resolve_constraint.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/physical_plan/operators/operator_resolve_constraint.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_constraint(const context_storage_t& context,
                                                                       const components::logical_plan::node_ptr& node) {
        const auto* tables = context.catalog_resolves ? context.catalog_resolves->tables.get() : nullptr;
        return boost::intrusive_ptr(new components::operators::operator_resolve_constraint_t(
            context.resource,
            context.log.clone(),
            static_cast<components::logical_plan::node_catalog_resolve_t*>(node.get()),
            tables));
    }

} // namespace services::planner::impl
