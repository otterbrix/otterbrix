#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::compute::function_registry_t& function_registry,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit,
                          const components::logical_plan::storage_parameters* params = nullptr);

    // Aggregate-pushdown POD spec-build (exposed for unit tests). Populates `out`
    // from the group node's keys/aggregates + the aggregate node's output_types, or returns
    // false when the shape is NOT faithfully POD-representable (HAVING, a coalesce/case_when/
    // arithmetic group key, a distinct/multi-arg/expression/unresolved/UDF aggregate argument)
    // — in which case the coordinator aggregate stands (R6 capability select). The spec carries
    // NO node_ptr / expression_ptr (R10/R14): it is trivially mailbox-disjoint by construction.
    bool build_pushed_spec(const components::logical_plan::node_group_t* group,
                           const components::logical_plan::node_ptr& agg_node,
                           std::pmr::memory_resource* resource,
                           components::operators::pushed_aggregate_spec_t& out);
}
