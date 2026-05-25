#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Lowers node_create_matview_t into operator_create_matview_t (composite
    // physical operator). Reads stamped fields from the matview node:
    //   - matview_oid + namespace_oid (allocated by dispatcher, stamped by planner)
    //   - inferred_columns + source_table_oid (stamped by enrich's
    //     derive_matview_output_schema)
    //   - catalog_writes (built by planner's rewrite_create_matview)
    //   - body_plan (child[0]) — recursively compiled via create_plan
    components::operators::operator_ptr
    create_plan_create_matview(const context_storage_t& context,
                               const components::compute::function_registry_t& function_registry,
                               const components::logical_plan::node_ptr& node,
                               const components::logical_plan::storage_parameters* params);

} // namespace services::planner::impl
