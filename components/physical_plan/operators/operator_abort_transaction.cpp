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

    actor_zeta::unique_future<void> operator_abort_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // One dispatcher round-trip: drain + abort() must return everything by value before the active map is
        // purged. Appends still need explicit revert below — their physical row slots persist on disk.
        services::dispatcher::txn_abort_drain_t drain;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::txn_abort_drain_msg,
                                            ctx->session,
                                            ctx->txn.transaction_id);
            drain = co_await std::move(drf);
        }
        const auto& txn_data = drain.txn;

        if (txn_data.transaction_id != 0 && !drain.swap_appends.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t swap_ctx{ctx->session, txn_data, {}};
            auto [_r, rf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_revert_appends,
                                                        swap_ctx,
                                                        std::move(drain.swap_appends),
                                                        /*tail_only=*/false);
            // Already aborting, so there is nothing left to fail: a rollback that could not finish
            // is recorded rather than swallowed.
            if (const auto reverted = co_await std::move(rf); reverted.contains_error()) {
                error(log(),
                      "operator_abort_transaction: pg_catalog append rollback did not complete: {}",
                      reverted.what);
            }
        }

        // The user-table half. Without it the rows stay with stamps no commit ever rewrote
        // (row_version_manager: commit_append is the only writer of inserted[]), and since those stamps
        // live in the txn-id space they sit above any commit-id watermark forever — so
        // has_versions_above defers this table from every later checkpoint round and the WAL floor
        // freezes. tail_only because the removal truncates; a range with a concurrent session's rows
        // behind it is left in place and reported.
        if (txn_data.transaction_id != 0 && !drain.base_appends.empty() &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t base_ctx{ctx->session, txn_data, {}};
            auto [_b, bf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_revert_appends,
                                                        base_ctx,
                                                        std::move(drain.base_appends),
                                                        /*tail_only=*/true);
            if (const auto reverted = co_await std::move(bf); reverted.contains_error()) {
                error(log(),
                      "operator_abort_transaction: base-table append rollback did not complete: {}",
                      reverted.what);
            }
        }

        // Reverts this txn's PENDING index insert/delete entries — parity with executor.cpp's failed-DML path,
        // without which they'd linger forever. pg_catalog oids are excluded (no index engines); DELETE markers
        // need this explicit revert because they sit outside the MVCC visibility filter.
        if (txn_data.transaction_id != 0 && ctx->index_address != actor_zeta::address_t::empty_address() &&
            (!drain.base_append_tables.empty() || !drain.base_delete_tables.empty())) {
            std::pmr::vector<actor_zeta::unique_future<void>> revert_index_futures{resource()};
            revert_index_futures.reserve(drain.base_append_tables.size() + drain.base_delete_tables.size());
            for (auto oid : drain.base_append_tables) {
                components::execution_context_t abort_ctx{ctx->session,
                                                          txn_data,
                                                          ctx->execution_context.timezone_offset,
                                                          oid};
                auto [_ri, rif] = actor_zeta::otterbrix::send(ctx->index_address,
                                                              &services::index::manager_index_t::revert_insert,
                                                              abort_ctx,
                                                              oid);
                revert_index_futures.push_back(std::move(rif));
            }
            for (auto oid : drain.base_delete_tables) {
                components::execution_context_t abort_ctx{ctx->session,
                                                          txn_data,
                                                          ctx->execution_context.timezone_offset,
                                                          oid};
                auto [_rd, rdf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                              &services::index::manager_index_t::revert_delete,
                                                              abort_ctx,
                                                              oid);
                revert_index_futures.push_back(std::move(rdf));
            }
            for (auto& rif : revert_index_futures) {
                co_await std::move(rif);
            }
        }

        // Rollback teardown: on-heap delete marks (not just the MVCC tombstone) must be un-stamped, or a
        // future re-DELETE of the same rows silently no-ops (chunk_vector_info::delete_rows skips an
        // already-marked slot). Catalog tables are included here even though excluded from the index revert above.
        if (txn_data.transaction_id != 0 &&
            (!drain.base_delete_tables.empty() || !drain.pg_catalog_delete_tables.empty()) &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            std::set<components::catalog::oid_t> revert_set{drain.base_delete_tables.begin(),
                                                            drain.base_delete_tables.end()};
            revert_set.insert(drain.pg_catalog_delete_tables.begin(), drain.pg_catalog_delete_tables.end());
            std::vector<components::catalog::oid_t> revert_delete_tables{revert_set.begin(), revert_set.end()};
            components::execution_context_t rd_ctx{ctx->session, txn_data, ctx->execution_context.timezone_offset};
            auto [_rd, rdf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::storage_revert_deletes,
                                                          rd_ctx,
                                                          std::move(revert_delete_tables));
            co_await std::move(rdf);
        }

        // DROP was only MARKED (tombstones keyed by txn_id) and left physically intact, so this rollback can
        // un-mark it and the table survives. ONE send each, txn_id-keyed — not per-oid.
        if (txn_data.transaction_id != 0 && !drain.dropped_storage_oids.empty()) {
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                auto [_sa, saf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_drop_aborted,
                                                              ctx->session,
                                                              txn_data.transaction_id);
                co_await std::move(saf);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_ta, taf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                              &services::index::manager_index_t::table_drop_aborted,
                                                              ctx->session,
                                                              txn_data.transaction_id);
                co_await std::move(taf);
            }
        }

        // CREATE physically built the index engine / heap storage eagerly at plan time; since the txn never
        // committed, those artifacts (not catalog rows) must be physically removed too.
        if (txn_data.transaction_id != 0) {
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<actor_zeta::unique_future<void>> drop_index_futures{resource()};
                drop_index_futures.reserve(drain.created_indexes.size());
                for (auto& idx : drain.created_indexes) {
                    auto [_di, dif] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                  &services::index::manager_index_t::drop_index,
                                                                  ctx->session,
                                                                  idx.table_oid,
                                                                  idx.index_oid);
                    drop_index_futures.push_back(std::move(dif));
                }
                for (auto& f : drop_index_futures) {
                    co_await std::move(f);
                }
            }
            if (!drain.created_storage_oids.empty()) {
                if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                    std::pmr::vector<actor_zeta::unique_future<void>> unregister_futures{resource()};
                    unregister_futures.reserve(drain.created_storage_oids.size());
                    for (auto oid : drain.created_storage_oids) {
                        auto [_u, uf] =
                            actor_zeta::otterbrix::send(ctx->index_address,
                                                        &services::index::manager_index_t::unregister_collection,
                                                        ctx->session,
                                                        oid);
                        unregister_futures.push_back(std::move(uf));
                    }
                    for (auto& uf : unregister_futures) {
                        co_await std::move(uf);
                    }
                }
                if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                    std::pmr::vector<components::catalog::oid_t> drop_oids{drain.created_storage_oids.begin(),
                                                                           drain.created_storage_oids.end(),
                                                                           resource()};
                    auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
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
