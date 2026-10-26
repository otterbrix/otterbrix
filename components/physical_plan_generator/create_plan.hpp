#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/planner/connector.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/context_storage.hpp>

#include <string_view>

namespace services::planner {

    // A successful result always holds an operator.
    using plan_result_t = core::result_wrapper_t<components::operators::operator_ptr>;

    // A generator's refusal: `what` says why this node has no physical plan.
    core::error_t plan_refusal(std::pmr::memory_resource* resource, std::string_view what);

    // A node names a table this plan has no resolved oid for.
    core::error_t unresolved_table_refusal(std::pmr::memory_resource* resource, const qualified_name_t& name);

    // What one factory of a table's external storage built for `relname`; a success without an operator is refused.
    plan_result_t storage_operator(std::pmr::memory_resource* resource, std::string_view relname, plan_result_t built);

    // The host's connector, for DDL on the connection's table or schema `name`; refused when the host gave none.
    core::result_wrapper_t<components::planner::connector_t*>
    connector_for(const context_storage_t& context, const components::planner::remote_name_t& name);
    // The connector's operator `backend` (or its refusal), then `catalog`, otterbrix's rows: steps the executor runs
    // in order.
    plan_result_t connector_then_catalog(const context_storage_t& context,
                                         plan_result_t backend,
                                         components::operators::operator_ptr catalog);
    // `table`'s parts as the connector sees them; they view `table`.
    components::planner::remote_name_t remote_name_of(const qualified_name_t& table);
    // The connector's view of a resolved table; it views `table`, its columns are filled into `columns`.
    components::planner::remote_table_t
    remote_table_of(const components::logical_plan::resolved_table_metadata_t& table,
                    std::pmr::vector<components::planner::remote_column_t>& columns);

    plan_result_t create_plan(const context_storage_t& context,
                              const components::compute::function_registry_t& function_registry,
                              const components::logical_plan::node_ptr& node,
                              components::logical_plan::limit_t limit,
                              const components::logical_plan::storage_parameters* params);

} // namespace services::planner
