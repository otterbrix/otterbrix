// SQL TransactionStmt → logical_plan lowering.
//
// Background: gram.y already parses BEGIN / COMMIT / ROLLBACK and produces a
// TransactionStmt AST node. Until task #94 there was no transformer entry,
// so SQL clients could not commit or roll back explicitly — only auto-commit
// behavior worked even though P4 already shipped node_commit_transaction_t /
// node_abort_transaction_t and their operator counterparts.
//
// Coverage decisions:
//   * COMMIT      → node_commit_transaction_t (existing P4 leaf)
//   * ROLLBACK    → node_abort_transaction_t  (existing P4 leaf)
//   * BEGIN/START → nullptr. otterbrix has no node_begin_transaction_t yet;
//                   manager_dispatcher_t::begin_transaction is reachable only
//                   via direct actor message. Returning a null logical plan
//                   here makes the SQL form a transformer-level no-op so
//                   parsers don't reject it. A follow-up can introduce a
//                   begin_transaction leaf and wire this case to it.
//   * SAVEPOINT / RELEASE / ROLLBACK TO / 2PC variants are unsupported and
//     fall through to the default no-op (nullptr) — the dispatch in
//     transformer.cpp ultimately surfaces a runtime error for unknown plans
//     so we don't need to throw here; quietly ignoring keeps parity with
//     other partially-supported statements (e.g. unsupported ALTER subtypes).

#include <components/logical_plan/node_abort_transaction.hpp>
#include <components/logical_plan/node_commit_transaction.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_transaction(TransactionStmt& node) {
        switch (node.kind) {
            case TRANS_STMT_BEGIN:
            case TRANS_STMT_START:
                // No-op until a node_begin_transaction_t exists. Dispatcher
                // currently begins transactions through the actor-message path
                // (manager_dispatcher_t::begin_transaction); SQL BEGIN does
                // not yet hook into that path.
                return nullptr;
            case TRANS_STMT_COMMIT:
                return logical_plan::node_ptr(new logical_plan::node_commit_transaction_t(resource_));
            case TRANS_STMT_ROLLBACK:
                return logical_plan::node_ptr(new logical_plan::node_abort_transaction_t(resource_));
            default:
                // SAVEPOINT, RELEASE, ROLLBACK TO, two-phase-commit forms — not supported.
                return nullptr;
        }
    }

} // namespace components::sql::transform
