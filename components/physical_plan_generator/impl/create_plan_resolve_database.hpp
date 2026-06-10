#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Translate a logical CATALOG_RESOLVE_DATABASE leaf into its
    // physical counterpart (operator_resolve_database_t).
    components::operators::operator_ptr create_plan_resolve_database(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl
