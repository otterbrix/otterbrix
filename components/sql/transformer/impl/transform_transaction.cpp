// SQL TransactionStmt → logical_plan lowering. BEGIN/COMMIT/ROLLBACK map to
// dedicated leaf nodes; the operator pipeline drives the txn_manager/disk side
// effects. SAVEPOINT / RELEASE / ROLLBACK TO / 2PC variants are unsupported and
// fall through to nullptr rather than throwing — transformer.cpp surfaces a
// runtime error for unknown plans, matching other partially-supported statements.

#include <components/logical_plan/node_transaction.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_transaction(TransactionStmt& node) {
        switch (node.kind) {
            case TRANS_STMT_BEGIN:
            case TRANS_STMT_START:
                return logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::begin);
            case TRANS_STMT_COMMIT:
                return logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::commit);
            case TRANS_STMT_ROLLBACK:
                return logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::abort);
            default:
                // SAVEPOINT, RELEASE, ROLLBACK TO, two-phase-commit forms — not supported.
                return nullptr;
        }
    }

} // namespace components::sql::transform
