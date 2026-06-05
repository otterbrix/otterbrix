#include "operator_abort_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>

namespace components::operators {

    operator_abort_transaction_t::operator_abort_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::abort_transaction) {}

    void operator_abort_transaction_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_abort_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Snapshot txn_data + swap-appends and abort in one dispatcher
        // round-trip. The dispatcher (sole owner of transaction_manager_t)
        // finds the txn, drains the pg_catalog appends, discards the
        // delete-tables set and backfill markers (uncommitted tombstones with
        // delete_id == txn_id are invisible to every reader; abort() makes them
        // GC-eligible, and backfill targets ride in swap_appends), then calls
        // abort() — returning everything by value before the active map is
        // purged. The appends still need revert because their physical row slots
        // persist until storage_revert_appends.
        components::table::transaction_data txn_data{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends;
        // Null-sender guard: with no dispatcher to talk to there is no txn to
        // drain or abort — leave the locals empty.
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] = actor_zeta::send(ctx->current_message_sender,
                                               &services::dispatcher::manager_dispatcher_t::txn_abort_drain_msg,
                                               ctx->session);
            services::dispatcher::txn_abort_drain_t drain = co_await std::move(drf);
            txn_data = drain.txn;
            swap_appends = std::move(drain.swap_appends);
        }

        // revert any pg_catalog rows appended under this transaction.
        if (txn_data.transaction_id != 0 && !swap_appends.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t swap_ctx{ctx->session, txn_data, {}};
            auto [_r, rf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_revert_appends,
                                             swap_ctx,
                                             std::move(swap_appends));
            co_await std::move(rf);
        }

        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
