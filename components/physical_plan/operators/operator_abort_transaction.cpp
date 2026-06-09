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
        // Catalog tables a DROP in this txn stamped delete marks on; un-stamped
        // by storage_revert_deletes alongside base_delete_tables (see (1) below).
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;
        // Rollback teardown inputs: storages/indexes a DROP marked and a
        // CREATE brought into being in this (now aborting) txn. dropped_* must be
        // UN-marked (the table survives the rollback); created_* must be
        // physically removed (they were created mid-txn and never committed).
        std::vector<components::catalog::oid_t> dropped_storage_oids;
        std::vector<components::catalog::oid_t> created_storage_oids;
        std::vector<components::table::created_index_t> created_indexes;
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
            pg_catalog_delete_tables = std::move(drain.pg_catalog_delete_tables);
            dropped_storage_oids = std::move(drain.dropped_storage_oids);
            created_storage_oids = std::move(drain.created_storage_oids);
            created_indexes = std::move(drain.created_indexes);
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
        // base_delete_tables is the DELETE-side mirror of base_append_tables;
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

        // ===== Rollback teardown =====
        // Runs AFTER the pg_catalog-append + index-entry reverts above so the
        // physical state is consistent before we touch storage/index lifecycle.
        // Three independent un-do channels for the non-MVCC side effects DDL/DML
        // staged under this txn:
        //
        // (1) Heap delete-mark un-stamp. A DELETE inside this txn stamped delete
        //     markers (deleter == txn_id) on the base-table heap; a DROP inside
        //     this txn stamped them on the CATALOG heaps (pg_class, pg_attribute,
        //     pg_depend, ...). The MVCC tombstones are invisible to readers and
        //     abort() makes them GC-eligible, BUT the on-heap delete marks
        //     themselves persist and would block a future re-DELETE of the same
        //     rows (chunk_vector_info::delete_rows skips an already-marked slot —
        //     so a rolled-back DROP followed by a fresh DROP of the same table
        //     would never re-stamp the catalog row, leaving it forever readable).
        //     storage_revert_deletes un-stamps every mark whose deleter ==
        //     ctx.txn.transaction_id (the agent inner calls
        //     data_table_t::revert_all_deletes(txn_id)). Revert BOTH the base and
        //     the catalog tables: the union is the full set of heaps this txn
        //     deleted from. pg_catalog tables have no index engines, so they are
        //     correctly excluded from the index revert above but MUST be included
        //     here for the heap un-stamp.
        if (txn_data.transaction_id != 0 && (!base_delete_tables.empty() || !pg_catalog_delete_tables.empty()) &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            std::set<components::catalog::oid_t> revert_set{base_delete_tables.begin(), base_delete_tables.end()};
            revert_set.insert(pg_catalog_delete_tables.begin(), pg_catalog_delete_tables.end());
            std::vector<components::catalog::oid_t> revert_delete_tables{revert_set.begin(), revert_set.end()};
            components::execution_context_t rd_ctx{ctx->session, txn_data, ctx->session_tz};
            auto [_rd, rdf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::storage_revert_deletes,
                                               rd_ctx,
                                               std::move(revert_delete_tables));
            co_await std::move(rdf);
        }

        // (2) DROP rollback un-mark. operator_dynamic_cascade_delete only MARKED
        //     the dropped storages/indexes (tombstones keyed by txn_id) and left
        //     them physically intact precisely so this rollback can un-mark them
        //     and the table survives. storage_drop_aborted (manager_disk) and
        //     table_drop_aborted (manager_index) ERASE every tombstone whose
        //     dropped_at == txn_id — ONE send each, txn_id-keyed (NOT per-oid:
        //     the inner fan-out matches on txn_id across all agents). Gated on a
        //     non-empty drained set so a txn that ran no DROP pays nothing.
        if (txn_data.transaction_id != 0 && !dropped_storage_oids.empty()) {
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                auto [_sa, saf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_drop_aborted,
                                                   ctx->session,
                                                   txn_data.transaction_id);
                co_await std::move(saf);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_ta, taf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::table_drop_aborted,
                                                   ctx->session,
                                                   txn_data.transaction_id);
                co_await std::move(taf);
            }
        }

        // (3) CREATE rollback removal. A CREATE INDEX / CREATE TABLE inside this
        //     txn physically built the index engine / heap storage at plan time
        //     (the create operators send create_index / create_storage eagerly).
        //     Since the txn never committed, those artifacts must be PHYSICALLY
        //     removed (their catalog rows are reverted by the swap_appends revert
        //     above, but the on-disk storage + index engine are not catalog rows).
        //     created_indexes drop_index per (table_oid, name); created_storage
        //     oids unregister_collection (ALL) THEN one drop_storage_many (index
        //     before disk so no consumer references a freed collection — every
        //     unregister is awaited before the batched disk drop, like the COMMIT
        //     teardown). drop_index / drop_storage tolerate an unknown target, so
        //     a CREATE that never fully materialized is safe to tear down.
        if (txn_data.transaction_id != 0) {
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<actor_zeta::unique_future<void>> drop_index_futures{resource()};
                drop_index_futures.reserve(created_indexes.size());
                for (auto& idx : created_indexes) {
                    auto [_di, dif] = actor_zeta::send(ctx->index_address,
                                                       &services::index::manager_index_t::drop_index,
                                                       ctx->session,
                                                       idx.table_oid,
                                                       services::index::index_name_t(idx.name.c_str()));
                    drop_index_futures.push_back(std::move(dif));
                }
                for (auto& f : drop_index_futures) {
                    co_await std::move(f);
                }
            }
            // Batched CREATE-rollback removal: ALL unregister_collection
            // (manager_index) THEN ONE drop_storage_many (manager_disk), awaiting
            // every unregister before any disk drop so no index consumer references a
            // collection whose backing storage the disk actor is about to free
            // (index-before-disk, preserved GLOBALLY — strictly stronger than the
            // previous per-oid interleave). unregister_collection acts on the index
            // MANAGER's own maps, so the N sends pipeline onto one mailbox;
            // drop_storage_many partitions the oids per disk agent and fans out in
            // parallel. drop_storage tolerates an unknown target, so a CREATE that
            // never fully materialized is still safe to tear down.
            if (!created_storage_oids.empty()) {
                if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                    std::pmr::vector<actor_zeta::unique_future<void>> unregister_futures{resource()};
                    unregister_futures.reserve(created_storage_oids.size());
                    for (auto oid : created_storage_oids) {
                        auto [_u, uf] = actor_zeta::send(ctx->index_address,
                                                         &services::index::manager_index_t::unregister_collection,
                                                         ctx->session,
                                                         oid);
                        unregister_futures.push_back(std::move(uf));
                    }
                    // Await EVERY unregister before any disk drop (index-before-disk).
                    for (auto& uf : unregister_futures) {
                        co_await std::move(uf);
                    }
                }
                if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                    std::pmr::vector<components::catalog::oid_t> drop_oids{created_storage_oids.begin(),
                                                                          created_storage_oids.end(),
                                                                          resource()};
                    auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::drop_storage_many,
                                                     ctx->session,
                                                     std::move(drop_oids));
                    co_await std::move(df);
                }
            }
        }

        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
