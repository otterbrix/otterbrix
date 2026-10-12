#include "operator_abort_transaction.hpp"

#include <components/physical_plan/operators/transaction_teardown.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>

namespace components::operators {

    operator_abort_transaction_t::operator_abort_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::abort_transaction) {}

    actor_zeta::unique_future<void> operator_abort_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // One dispatcher round-trip: drain + abort() must return everything by value before the active map is
        // purged. Appends still need explicit revert below — their physical row slots persist on disk.
        components::table::txn_abort_drain_t drain;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::txn_abort_drain_msg,
                                            ctx->session,
                                            ctx->txn.transaction_id);
            drain = co_await std::move(drf);
        }
        co_await revert_transaction(resource(), ctx, log(), std::move(drain));

        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
