#include "operator_rollback_to_savepoint.hpp"

#include <components/physical_plan/operators/transaction_teardown.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_rollback_to_savepoint_t::operator_rollback_to_savepoint_t(std::pmr::memory_resource* resource,
                                                                       log_t log,
                                                                       std::string_view name)
        : read_write_operator_t(resource, std::move(log), operator_type::rollback_to_savepoint)
        , name_(name) {}

    actor_zeta::unique_future<void> operator_rollback_to_savepoint_t::await_async_and_resume(pipeline::context_t* ctx) {
        services::dispatcher::txn_rollback_to_drain_t rollback;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::txn_rollback_to_drain_msg,
                                            ctx->session,
                                            ctx->txn.transaction_id,
                                            name_);
            rollback = co_await std::move(drf);
        }
        if (rollback.error.contains_error()) {
            set_error(std::move(rollback.error));
            co_return;
        }

        auto marked = core::error_t::no_error();
        if (ctx->wal_address != actor_zeta::address_t::empty_address() && rollback.drain.txn.transaction_id != 0) {
            auto [_w, wf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                        &services::wal::manager_wal_replicate_t::rollback_to,
                                                        ctx->session,
                                                        rollback.drain.txn.transaction_id,
                                                        rollback.drain.from_sub_id,
                                                        components::catalog::well_known_oid::main_database);
            if (auto written = co_await std::move(wf); written.has_error()) {
                marked = written.error();
            }
        }
        co_await revert_transaction(resource(), ctx, log(), std::move(rollback.drain));
        if (marked.contains_error()) {
            set_error(std::move(marked));
            co_return;
        }

        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
