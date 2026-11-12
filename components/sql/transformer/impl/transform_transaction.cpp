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

    core::result_wrapper_t<logical_plan::transaction_access_t>
    transaction_access_of(List* modes, std::pmr::memory_resource* resource) {
        auto access = logical_plan::transaction_access_t::as_default;
        if (modes == nullptr) {
            return access;
        }
        for (const auto& cell : modes->lst) {
            auto* mode = pg_ptr_cast<DefElem>(cell.data);
            const std::string_view name = mode->defname;
            auto* value = pg_ptr_cast<A_Const>(mode->arg);
            if (name == "transaction_read_only") {
                access = intVal(&value->val) != 0 ? logical_plan::transaction_access_t::read_only
                                                  : logical_plan::transaction_access_t::read_write;
            } else if (name == "transaction_isolation") {
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"transaction isolation level " +
                                                          std::string{strVal(&value->val)} + " is not supported",
                                                      resource});
            } else if (name != "transaction_deferrable") {
                return core::error_t(
                    core::error_code_t::unimplemented_yet,
                    std::pmr::string{"transaction mode " + std::string{name} + " is not supported", resource});
            }
        }
        return access;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_transaction(TransactionStmt& node) {
        switch (node.kind) {
            case TRANS_STMT_BEGIN:
            case TRANS_STMT_START: {
                auto access = transaction_access_of(node.options, resource_);
                if (access.has_error()) {
                    return access.error();
                }
                auto begin = logical_plan::make_node_transaction(resource_, logical_plan::transaction_op::begin);
                begin->set_access(access.value());
                return logical_plan::node_ptr{begin};
            }
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
