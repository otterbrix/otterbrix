#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Bridge node_catalog_resolve_constraint_t → operator_resolve_constraint_t.
    // The operator reads the parent table_oid from the back-pointed
    // resolve_table node (filled by an earlier Pass 1 resolve_table
    // operator), so this generator just forwards the back-pointer.
    components::operators::operator_ptr create_plan_resolve_constraint(const context_storage_t& context,
                                                                       const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl