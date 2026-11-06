#pragma once

#include <components/physical_plan_generator/create_plan.hpp>

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // The node's (source, target) types, resolved by the executor, are mapped to their pg_type oids here.
    components::operators::operator_ptr create_plan_register_cast(const context_storage_t& context,
                                                                  const components::logical_plan::node_ptr& node);

    // Lower a node_unregister_cast_t into operator_unregister_cast_t.
    components::operators::operator_ptr create_plan_unregister_cast(const context_storage_t& context,
                                                                    const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl