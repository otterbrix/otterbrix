#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Lower a node_get_schema_t into operator_get_schema_t. The logical node
    // carries the (database, collection) ids; the operator self-resolves
    // namespace / table / columns at execution time. Wired from create_plan.cpp
    // dispatch on node_type::get_schema_t.
    components::operators::operator_ptr
    create_plan_get_schema(const context_storage_t& context,
                            const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl
