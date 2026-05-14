#include "create_plan_commit_transaction.hpp"

#include <components/logical_plan/node_commit_transaction.hpp>
#include <components/physical_plan/operators/operator_commit_transaction.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_commit_transaction(const context_storage_t& context,
                                    const components::logical_plan::node_ptr& node) {
        auto op = boost::intrusive_ptr(new components::operators::operator_commit_transaction_t(
            context.resource,
            context.log.clone()));
        // M4.J: propagate DDL-commit flag + WAL coordinates from the logical
        // node into the operator. RPC mode (is_ddl_commit=false) leaves the
        // operator in its legacy behavior; DDL mode adds the flush + WAL
        // commit_txn prefix.
        auto* n = static_cast<components::logical_plan::node_commit_transaction_t*>(node.get());
        if (n->is_ddl_commit()) {
            op->set_ddl_commit(n->txn_id(), n->database_oid());
        }
        return op;
    }

} // namespace services::planner::impl
