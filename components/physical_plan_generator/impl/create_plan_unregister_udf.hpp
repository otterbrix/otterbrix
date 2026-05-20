#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Lower a node_unregister_udf_t into operator_unregister_udf_t. No external
    // address list is required — the operator probes the global default
    // function_registry_t and pg_proc directly via ctx->disk_address.
    components::operators::operator_ptr create_plan_unregister_udf(const context_storage_t& context,
                                                                   const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl
