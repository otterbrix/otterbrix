#include "create_plan_resolve_type.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/physical_plan/operators/operator_resolve_type.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_type(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node) {
        return boost::intrusive_ptr(new components::operators::operator_resolve_type_t(
            context.resource,
            context.log.clone(),
            static_cast<components::logical_plan::node_catalog_resolve_t*>(node.get())));
    }

} // namespace services::planner::impl
