#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/context_storage.hpp>

#include <string_view>

namespace services::planner {

    // A successful result always holds an operator.
    using plan_result_t = core::result_wrapper_t<components::operators::operator_ptr>;

    // A generator's refusal: `what` says why this node has no physical plan.
    core::error_t plan_refusal(std::pmr::memory_resource* resource, std::string_view what);

    // A node names a table this plan has no resolved oid for.
    core::error_t
    unresolved_table_refusal(std::pmr::memory_resource* resource, std::string_view dbname, std::string_view relname);

    plan_result_t create_plan(const context_storage_t& context,
                              const components::compute::function_registry_t& function_registry,
                              const components::logical_plan::node_ptr& node,
                              components::logical_plan::limit_t limit,
                              const components::logical_plan::storage_parameters* params);

} // namespace services::planner
