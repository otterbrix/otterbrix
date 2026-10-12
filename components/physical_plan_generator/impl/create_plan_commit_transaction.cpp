#include "create_plan_commit_transaction.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/physical_plan/operators/operator_commit_transaction.hpp>

namespace services::planner::impl {

    components::operators::commit_checks_t commit_checks(std::pmr::memory_resource* resource,
                                                         const components::logical_plan::catalog_resolves_t* resolves,
                                                         core::error_t resolve_error) {
        components::operators::commit_checks_t checks;
        checks.refusal = std::move(resolve_error);
        if (resolves == nullptr || checks.refusal.contains_error() || !resolves->tables) {
            return checks;
        }
        const auto& tables = resolves->tables->entries();
        for (const auto& table : tables) {
            if (!table.table_md.has_value()) {
                checks.refusal = core::error_t{core::error_code_t::write_conflict,
                                               std::pmr::string{"commit refused: a table this transaction wrote "
                                                                "was dropped by a transaction that committed first",
                                                                resource}};
                return checks;
            }
        }
        if (!resolves->constraints) {
            return checks;
        }
        for (const auto& constraint : resolves->constraints->entries()) {
            components::operators::commit_table_checks_t table_checks;
            table_checks.table_oid = tables[constraint.target].table_md->table_oid;
            table_checks.fks = constraint.fks;
            if (constraint.direction == components::logical_plan::resolve_direction::outgoing) {
                table_checks.unique_constraints = constraint.unique_constraints;
                checks.outgoing.push_back(std::move(table_checks));
            } else {
                checks.referencing.push_back(std::move(table_checks));
            }
        }
        return checks;
    }

    components::operators::operator_ptr create_plan_commit_transaction(const context_storage_t& context,
                                                                       const components::logical_plan::node_ptr& node) {
        auto op = boost::intrusive_ptr(
            new components::operators::operator_commit_transaction_t(context.resource, context.log.clone()));
        // Propagate DDL-commit flag + WAL coordinates from the logical
        // node into the operator. DDL mode adds the flush + WAL commit_txn
        // prefix; RPC mode keeps the simpler commit.
        auto* n = static_cast<components::logical_plan::node_transaction_t*>(node.get());
        if (n->is_ddl_commit()) {
            op->set_ddl_commit(n->txn_id(), n->database_oid());
        }
        if (context.commit_input != nullptr) {
            op->set_input(std::move(context.commit_input->drain),
                          commit_checks(context.resource,
                                        context.catalog_resolves,
                                        std::move(context.commit_input->resolve_error)));
        }
        return op;
    }

} // namespace services::planner::impl
