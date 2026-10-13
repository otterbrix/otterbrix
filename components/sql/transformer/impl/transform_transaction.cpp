// SQL TransactionStmt → logical_plan lowering. BEGIN/COMMIT/ROLLBACK and
// SAVEPOINT/RELEASE/ROLLBACK TO map to dedicated leaf nodes; the operator
// pipeline drives the txn_manager/disk side effects. 2PC variants are
// unsupported and are refused here.

#include <components/logical_plan/node_transaction.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    std::string_view savepoint_name(TransactionStmt& node) {
        auto* def = pg_ptr_cast<DefElem>(linitial(node.options));
        return strVal(def->arg);
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_transaction(TransactionStmt& node) {
        switch (node.kind) {
            case TRANS_STMT_BEGIN:
            case TRANS_STMT_START:
                // TODO: proper support in engine
                if (node.options != nullptr && list_length(node.options) > 0) {
                    return core::error_t(core::error_code_t::unimplemented_yet,
                                         std::pmr::string{"transaction modes are not supported", resource_});
                }
                return logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::begin);
            case TRANS_STMT_COMMIT:
                return logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::commit);
            case TRANS_STMT_ROLLBACK:
                return logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::abort);
            case TRANS_STMT_SAVEPOINT:
                return logical_plan::make_node_transaction(resource_,
                                                           logical_plan::transaction_op::savepoint,
                                                           savepoint_name(node));
            case TRANS_STMT_RELEASE:
                return logical_plan::make_node_transaction(resource_,
                                                           logical_plan::transaction_op::release_savepoint,
                                                           savepoint_name(node));
            case TRANS_STMT_ROLLBACK_TO:
                return logical_plan::make_node_transaction(resource_,
                                                           logical_plan::transaction_op::rollback_to_savepoint,
                                                           savepoint_name(node));
            default:
                // Two-phase-commit forms — not supported.
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"unsupported transaction statement", resource_});
        }
    }

} // namespace components::sql::transform
