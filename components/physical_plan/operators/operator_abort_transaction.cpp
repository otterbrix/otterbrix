#include "operator_abort_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/index/manager_index.hpp>

#include <set>
#include <vector>

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
        std::set<components::catalog::oid_t> base_append_tables;
        std::set<components::catalog::oid_t> base_delete_tables;
        // Null-sender guard: with no dispatcher to talk to there is no txn to
        // drain or abort — leave the locals empty.
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] = actor_zeta::send(ctx->current_message_sender,
                                               &services::dispatcher::manager_dispatcher_t::txn_abort_drain_msg,
                                               ctx->session);
            services::dispatcher::txn_abort_drain_t drain = co_await std::move(drf);
            txn_data = drain.txn;
            swap_appends = std::move(drain.swap_appends);
            base_append_tables = std::move(drain.base_append_tables);
            base_delete_tables = std::move(drain.base_delete_tables);
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

        // Index revert: drop this txn's PENDING in-memory index entries for every
        // base table it appended to AND clear the PENDING index DELETE markers for
        // every base table it deleted from — parity with executor.cpp's failed-DML
        // path, which an explicit SQL ROLLBACK must match (without it the aborted
        // txn's PENDING index entries/markers linger forever). revert_insert and
        // revert_delete are each keyed per (table_oid, txn_id) and revert ALL
        // uncommitted entries for that pair, so they are idempotent across
        // duplicate table oids; base_append_tables/base_delete_tables are already
        // unique sets. Fan out two-phase (send every revert first, then await
        // each). pg_catalog oids are deliberately excluded by the drain — they have
        // no index engines, so a revert there is a no-op by the engines_ lookup.
        //
        // F15: base_delete_tables is the DELETE-side mirror of base_append_tables;
        // the abort drain handler in dispatcher.cpp surfaces the unique base-delete
        // table oids precisely so this operator can revert_delete the index DELETE
        // markers an uncommitted DELETE staged (the markers sit outside the MVCC
        // visibility filter, so unlike the tombstones they need an explicit revert).
        if (txn_data.transaction_id != 0 && ctx->index_address != actor_zeta::address_t::empty_address() &&
            (!base_append_tables.empty() || !base_delete_tables.empty())) {
            std::pmr::vector<actor_zeta::unique_future<void>> revert_index_futures{resource()};
            revert_index_futures.reserve(base_append_tables.size() + base_delete_tables.size());
            for (auto oid : base_append_tables) {
                components::execution_context_t abort_ctx{ctx->session, txn_data, ctx->session_tz, oid};
                auto [_ri, rif] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::revert_insert,
                                                   abort_ctx,
                                                   oid);
                revert_index_futures.push_back(std::move(rif));
            }
            for (auto oid : base_delete_tables) {
                components::execution_context_t abort_ctx{ctx->session, txn_data, ctx->session_tz, oid};
                auto [_rd, rdf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::revert_delete,
                                                   abort_ctx,
                                                   oid);
                revert_index_futures.push_back(std::move(rdf));
            }
            for (auto& rif : revert_index_futures) {
                co_await std::move(rif);
            }
        }

        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
