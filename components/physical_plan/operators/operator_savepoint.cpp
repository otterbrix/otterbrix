#include "operator_savepoint.hpp"

#include <components/context/context.hpp>
#include <services/dispatcher/dispatcher.hpp>

namespace components::operators {

    operator_savepoint_t::operator_savepoint_t(std::pmr::memory_resource* resource, log_t log, std::string_view name)
        : read_write_operator_t(resource, std::move(log), operator_type::savepoint)
        , name_(name) {}

    actor_zeta::unique_future<void> operator_savepoint_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_m, mf] = actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                        &services::dispatcher::manager_dispatcher_t::txn_savepoint_msg,
                                                        ctx->session,
                                                        ctx->txn.transaction_id,
                                                        name_);
            if (auto pushed = co_await std::move(mf); pushed.contains_error()) {
                set_error(std::move(pushed));
                co_return;
            }
        }

        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
