#include "transaction_teardown.hpp"

#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <cassert>

namespace components::operators {

    components::table::txn_abort_drain_t undo_of_commit(const services::dispatcher::txn_commit_drain_t& drain) {
        components::table::txn_abort_drain_t undo;
        undo.txn = drain.txn;
        undo.swap_appends = drain.swap_appends;
        undo.base_appends = drain.base_appends;
        for (const auto& range : drain.base_appends) {
            undo.base_append_tables.insert(range.table_oid);
        }
        undo.base_delete_tables = drain.base_delete_tables;
        undo.pg_catalog_delete_tables = drain.swap_deletes;
        for (const auto& backfill : drain.swap_backfills) {
            if (const auto table_oid = backfill.column_stamped_table(); table_oid != components::catalog::INVALID_OID) {
                undo.column_stamped_tables.insert(table_oid);
            }
        }
        undo.dropped_storage_oids = drain.dropped_storage_oids;
        undo.created_storage_oids = drain.created_storage_oids;
        undo.created_indexes = drain.created_indexes;
        return undo;
    }

    actor_zeta::unique_future<void> revert_transaction(std::pmr::memory_resource* /*frame_resource*/,
                                                       pipeline::context_t* ctx,
                                                       log_t& log,
                                                       components::table::txn_abort_drain_t drain) {
        //assert(ctx->disk_address != actor_zeta::address_t::empty_address() && "revert_transaction: no disk");
        //assert(ctx->index_address != actor_zeta::address_t::empty_address() && "revert_transaction: no index");
        if (drain.txn.transaction_id == 0) {
            co_return;
        }

        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            // Index first: the created tables must be unregistered before the disk drops their storage.
            auto [_i, index_future] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                  &services::index::manager_index_t::abort_transaction,
                                                                  ctx->session,
                                                                  drain);
            co_await std::move(index_future);
        }

        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_d, disk_future] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                 &services::disk::manager_disk_t::abort_transaction,
                                                                 ctx->session,
                                                                 std::move(drain));
            if (const auto reverted = co_await std::move(disk_future); reverted.contains_error()) {
                error(log, "revert_transaction: append rollback did not complete: {}", reverted.what);
            }
        }
        co_return;
    }

} // namespace components::operators
