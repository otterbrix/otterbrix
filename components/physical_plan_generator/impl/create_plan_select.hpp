#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_select(const context_storage_t& context,
                       const components::logical_plan::node_ptr& node,
                       const components::logical_plan::storage_parameters* params = nullptr);

    // Build physical projection columns from a DML node's RETURNING expression
    // list (scalar get_field / arithmetic / constant / star_expand) into `columns`,
    // which stays empty when `returning` is empty (no RETURNING clause). Shared by
    // create_plan_insert / _update / _delete.
    //
    // Returns false on a defensive validation failure so the caller can return
    // nullptr -> executor surfaces the error (rule 9: no throw on the
    // operator-build path).
    [[nodiscard]] bool
    build_returning_columns(std::pmr::memory_resource* resource,
                            const std::pmr::vector<components::expressions::expression_ptr>& returning,
                            const components::logical_plan::storage_parameters* params,
                            std::pmr::vector<components::operators::select_column_t>& columns);

} // namespace services::planner::impl
