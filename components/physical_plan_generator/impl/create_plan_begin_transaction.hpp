#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Lower a node_begin_transaction_t into operator_begin_transaction_t.
    // The operator carries no fields; its inputs (session, txn_manager) flow
    // through pipeline::context_t at execution time.
    components::operators::operator_ptr create_plan_begin_transaction(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl
