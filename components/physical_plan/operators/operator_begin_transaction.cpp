#include "operator_begin_transaction.hpp"

#include <components/context/context.hpp>
#include <components/table/transaction_manager.hpp>

namespace components::operators {

    operator_begin_transaction_t::operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::begin_transaction) {}

    void operator_begin_transaction_t::on_execute_impl(pipeline::context_t* ctx) {
        // Idempotent: if the session already has an active txn (DML opened an
        // implicit one before BEGIN), reuse it and just mark_explicit — a stray
        // BEGIN inside an open txn must not restart it (Postgres semantics).
        auto* tm = ctx->txn_manager;
        if (tm != nullptr) {
            auto& txn = tm->begin_transaction(ctx->session);
            txn.mark_explicit();
        }

        // Leaf: no rows out, no async work.
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
