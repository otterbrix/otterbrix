#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Lower a node_register_udf_t into operator_register_udf_t. The operator
    // needs an executor fan-out callable to drive the per-executor
    // function_registry_ updates; pass it via an explicit parameter (the
    // planner has no static knowledge of the executor pool — only the
    // dispatcher does, which calls this helper from its register_udf entry
    // point).
    components::operators::operator_ptr
    create_plan_register_udf(const context_storage_t& context,
                              const components::logical_plan::node_ptr& node,
                              std::size_t executor_count,
                              components::operators::operator_register_udf_t::executor_register_fn_t fanout);

} // namespace services::planner::impl
