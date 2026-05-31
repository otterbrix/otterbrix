#include "operator_begin_transaction.hpp"

#include <components/context/context.hpp>
#include <components/table/transaction_manager.hpp>

namespace components::operators {

    operator_begin_transaction_t::operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::begin_transaction) {}

    void operator_begin_transaction_t::on_execute_impl(pipeline::context_t* ctx) {
        // Idempotent begin: if the session already has an active transaction
        // (e.g. SQL clients that issued DML before BEGIN as part of a fragment
        // that opened an implicit txn), begin_transaction returns the existing
        // entry; mark_explicit then upgrades it. This matches Postgres semantics
        // where a stray BEGIN inside an open txn is a warning, not a restart.
        auto* tm = ctx->txn_manager;
        if (tm != nullptr) {
            auto& txn = tm->begin_transaction(ctx->session);
            txn.mark_explicit();
        }

        // Leaf: no rows out, no async work. mark_executed so the executor's
        // pipeline driver stops iterating and the cursor reports success.
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
