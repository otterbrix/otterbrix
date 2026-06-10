#include "operator_begin_transaction.hpp"

#include <components/context/context.hpp>
#include <services/dispatcher/dispatcher.hpp>

namespace components::operators {

    operator_begin_transaction_t::operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::begin_transaction) {}

    void operator_begin_transaction_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // The only side effect is an async mailbox round-trip to the dispatcher.
        // Defer it to await_async_and_resume so it participates in the await chain.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_begin_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Ask the dispatcher (sole owner of transaction_manager_t) to mark the
        // session's txn explicit. The handler does an idempotent begin_transaction
        // THEN mark_explicit: if DML opened an implicit txn before BEGIN it is
        // reused, and a stray BEGIN inside an open txn must not restart it
        // (Postgres semantics).
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_m, mf] = actor_zeta::send(ctx->current_message_sender,
                                             &services::dispatcher::manager_dispatcher_t::txn_mark_explicit_msg,
                                             ctx->session);
            co_await std::move(mf);
        }

        // Leaf: no rows out.
        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
