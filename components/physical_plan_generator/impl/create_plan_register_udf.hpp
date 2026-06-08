#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Lower a node_register_udf_t into operator_register_udf_t.
    //
    // The operator does NOT fan out to executors: the dispatcher (the only place
    // that owns the executor pool + scheduler and can honour needs_sched on a
    // send) performs the per-executor register_udf fan-out itself, co_awaits the
    // acks, drops any executor that errored, and passes the resulting per-executor
    // uids in here as plain data — no callable, no shared payload. The operator
    // validates those uids, mirrors the function into the default registry, and
    // persists pg_proc/pg_depend rows.
    //
    // The function payload is deep-copied out of the node (node owns the canonical
    // unique function_ptr) so the operator owns an independent instance.
    components::operators::operator_ptr
    create_plan_register_udf(const context_storage_t& context,
                             const components::logical_plan::node_ptr& node,
                             components::operators::operator_register_udf_t::executor_uids_t executor_uids);

} // namespace services::planner::impl
