#include "create_plan_allocate_oids.hpp"

#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/physical_plan/operators/operator_allocate_oids.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_allocate_oids(const context_storage_t& context,
                                                                  const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_allocate_oids_t*>(node.get());
        return boost::intrusive_ptr(
            new components::operators::operator_allocate_oids_t(context.resource, context.log.clone(), n->count(), n));
    }

} // namespace services::planner::impl