#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_commit_transaction_t::operator_commit_transaction_t(std::pmr::memory_resource* resource,
                                                                  log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::commit_transaction) {}

    void operator_commit_transaction_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Synchronous txn_manager work + an async disk send. Defer the entire
        // body to await_async_and_resume so the disk send participates in the
        // pipeline's await chain.
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_commit_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Phase 5b: snapshot txn_data AND swap-info BEFORE commit() purges the
        // active map; then commit; then dispatch the new batched APIs against
        // the post-swap state.
        auto* tm = ctx->txn_manager;
        auto* txn_t = tm ? tm->find_transaction(ctx->session) : nullptr;

        components::table::transaction_data txn_data =
            txn_t ? txn_t->data() : components::table::transaction_data{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends;
        std::set<collection_full_name_t>                   swap_deletes;
        if (txn_t) {
            swap_appends = std::move(txn_t->pg_catalog_appends);
            swap_deletes = std::move(txn_t->pg_catalog_delete_tables);
        }

        // Step 2: commit the transaction in the txn_manager. Synchronous —
        // returns the commit_id used to flip MVCC on pg_catalog appends.
        commit_id_ = tm ? tm->commit(ctx->session) : 0;

        // Step 3: flip MVCC state on pg_catalog rows appended/deleted under this
        // explicit transaction. Phase 5b uses the new batched APIs that consume
        // the per-txn swap-info aggregated by the dispatcher.
        if (txn_data.transaction_id != 0 && commit_id_ > 0
            && ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t swap_ctx{ctx->session, txn_data, {}};
            if (!swap_appends.empty()) {
                auto [_a, af] = actor_zeta::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::storage_commit_appends,
                    swap_ctx,
                    commit_id_,
                    std::move(swap_appends));
                co_await std::move(af);
            }
            if (!swap_deletes.empty()) {
                auto [_d, df] = actor_zeta::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::storage_commit_deletes,
                    swap_ctx,
                    commit_id_,
                    std::move(swap_deletes));
                co_await std::move(df);
            }
        }

        // No row output — like operator_checkpoint_t, the manager-level driver
        // surfaces success via the operator's executed state and reads
        // commit_id() via the typed accessor.
        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
