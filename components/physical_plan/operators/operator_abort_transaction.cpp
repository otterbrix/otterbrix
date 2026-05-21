#include "operator_abort_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_abort_transaction_t::operator_abort_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::abort_transaction) {}

    void operator_abort_transaction_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_abort_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Snapshot txn_data + swap-appends BEFORE abort() purges the
        // active map. delete_tables on abort: nothing to revert because
        // tombstones written under an uncommitted txn_id stay invisible to
        // readers, so they need no swap-side action.
        auto* tm = ctx->txn_manager;
        auto* txn_t = tm ? tm->find_transaction(ctx->session) : nullptr;

        components::table::transaction_data txn_data =
            txn_t ? txn_t->data() : components::table::transaction_data{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends;
        if (txn_t) {
            swap_appends = std::move(txn_t->pg_catalog_appends);
            // delete_tables: nothing to revert — uncommitted tombstones are invisible.
            txn_t->pg_catalog_delete_tables.clear();
        }

        // Step 2: abort the transaction in the txn_manager (synchronous).
        if (tm) {
            tm->abort(ctx->session);
        }

        // Step 3: revert any pg_catalog rows appended under this transaction
        // via the new batched API.
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
