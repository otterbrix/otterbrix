#include "operator_commit_transaction.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <algorithm>
#include <iterator>
#include <set>
#include <vector>

namespace components::operators {

    // Handles COMMIT only; ROLLBACK and statement-failure abort go through operator_abort_transaction_t.

    operator_commit_transaction_t::operator_commit_transaction_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::commit_transaction) {}

    actor_zeta::unique_future<void> operator_commit_transaction_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Must NOT call publish() here — that ProcArray barrier is deferred until after storage_publish_*/WAL.
        components::table::transaction_data txn_data{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends;
        std::set<components::catalog::oid_t> swap_deletes;
        std::vector<components::pg_attribute_commit_id_backfill_t> swap_backfills;
        std::vector<components::pg_catalog_append_range_t> base_appends;
        std::set<components::catalog::oid_t> base_delete_tables;
        std::vector<components::catalog::oid_t> dropped_storage_oids;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_dr, drf] =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::txn_commit_drain_msg,
                                            ctx->session);
            services::dispatcher::txn_commit_drain_t drain = co_await std::move(drf);
            txn_data = drain.txn;
            swap_appends = std::move(drain.swap_appends);
            swap_deletes = std::move(drain.swap_deletes);
            swap_backfills = std::move(drain.swap_backfills);
            base_appends = std::move(drain.base_appends);
            base_delete_tables = std::move(drain.base_delete_tables);
            dropped_storage_oids = std::move(drain.dropped_storage_oids);
            commit_id_ = drain.commit_id;
        }

        ctx->committed_id = commit_id_;

        // ORDERING INVARIANT: only the index insert-commit and the WAL marker below can still discard
        // commit_id; every later step only stamps/publishes an already-durable commit, so leaking commit_id_
        // past the WAL marker floors visible_to_all_locked() and stalls compact()/GC forever.
        std::pmr::set<components::catalog::oid_t> append_oid_set{resource_};
        for (const auto& r : base_appends) {
            append_oid_set.insert(r.table_oid);
        }
        std::pmr::vector<components::catalog::oid_t> base_append_oids{append_oid_set.begin(),
                                                                      append_oid_set.end(),
                                                                      resource_};
        std::pmr::vector<components::catalog::oid_t> base_delete_table_oids{base_delete_tables.begin(),
                                                                            base_delete_tables.end(),
                                                                            resource_};

        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0 && !base_append_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> append_oids{base_append_oids.begin(),
                                                                     base_append_oids.end(),
                                                                     resource_};
            auto [_ic, icf] = actor_zeta::otterbrix::send(
                ctx->index_address,
                &services::index::manager_index_t::commit_inserts,
                components::execution_context_t{ctx->session, txn_data, ctx->execution_context.timezone_offset},
                std::move(append_oids),
                commit_id_);
            core::error_t result = co_await std::move(icf);
            if (result.contains_error()) {
                if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
                    auto [_dx, dxf] =
                        actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                    &services::dispatcher::manager_dispatcher_t::txn_discard_msg,
                                                    commit_id_);
                    co_await std::move(dxf);
                }
                set_error(std::move(result));
                co_return;
            }
        }

        // The single durable commit point and the last step that can still fail.
        if (ctx->wal_address != actor_zeta::address_t::empty_address() && commit_id_ > 0 &&
            (is_ddl_commit_ ? txn_id_ != 0 : txn_data.transaction_id != 0)) {
            const std::uint64_t marker_txn_id = is_ddl_commit_ ? txn_id_ : txn_data.transaction_id;
            const components::catalog::oid_t marker_db_oid =
                is_ddl_commit_ ? database_oid_ : components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                        &services::wal::manager_wal_replicate_t::commit_txn,
                                                        ctx->session,
                                                        marker_txn_id,
                                                        services::wal::wal_sync_mode::FULL,
                                                        marker_db_oid,
                                                        commit_id_);
            // A refused fsync here is still a clean abort — commit_id is stamped nowhere yet.
            if (auto commit_result = co_await std::move(wf); commit_result.has_error()) {
                if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
                    auto [_dx, dxf] =
                        actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                    &services::dispatcher::manager_dispatcher_t::txn_discard_msg,
                                                    commit_id_);
                    co_await std::move(dxf);
                }
                set_error(commit_result.error());
                co_return;
            }
        }

        // A DROP tombstone is keyed by transaction_id, but GC compares against commit_id, so remap it now.
        if (!dropped_storage_oids.empty() && txn_data.transaction_id != 0 && commit_id_ > 0) {
            if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
                auto [_sd, sdf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_dropped_committed,
                                                ctx->session,
                                                txn_data.transaction_id,
                                                commit_id_);
                co_await std::move(sdf);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_td, tdf] =
                    actor_zeta::otterbrix::send(ctx->index_address,
                                                &services::index::manager_index_t::table_dropped_committed,
                                                ctx->session,
                                                txn_data.transaction_id,
                                                commit_id_);
                co_await std::move(tdf);
            }
        }

        // dropped_at markers carry the physical column release; split out before the move below empties swap_backfills.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> column_releases{resource_};
        for (const auto& b : swap_backfills) {
            if (b.kind == components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at &&
                !b.release_attname.empty() && b.release_table_oid != components::catalog::INVALID_OID) {
                column_releases.push_back(b);
            }
        }
        // RENAME markers are kept OUT of the batch below: renaming preserves added_at_commit_id.
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> column_renames{resource_};
        for (const auto& b : swap_backfills) {
            if (b.kind == components::pg_attribute_commit_id_backfill_t::kind_t::storage_rename &&
                !b.release_attname.empty() && !b.rename_to_attname.empty() &&
                b.release_table_oid != components::catalog::INVALID_OID) {
                column_renames.push_back(b);
            }
        }
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfill_markers{resource_};
        backfill_markers.reserve(swap_backfills.size());
        for (auto& b : swap_backfills) {
            if (b.kind != components::pg_attribute_commit_id_backfill_t::kind_t::storage_rename) {
                backfill_markers.push_back(std::move(b));
            }
        }
        swap_backfills.clear();
        if (!backfill_markers.empty() && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t backfill_ctx{ctx->session, txn_data, {}};
            const auto backfill_count = backfill_markers.size();
            auto [_b, bf] =
                actor_zeta::otterbrix::send(ctx->disk_address,
                                            &services::disk::manager_disk_t::update_pg_attribute_commit_id_fields,
                                            backfill_ctx,
                                            std::move(backfill_markers),
                                            commit_id_);
            // Must NOT set_error/co_return here: commit_id is already durable, refusing would strand it forever.
            if (auto backfill_result = co_await std::move(bf); backfill_result.contains_error()) {
                error(log_,
                      "operator_commit_transaction: the pg_attribute backfill of {} marker(s) for txn {} "
                      "commit_id {} reported refused stamp(s); each refused column keeps commit_id 0 and is "
                      "visible to older snapshots: {}",
                      backfill_count,
                      txn_data.transaction_id,
                      commit_id_,
                      backfill_result.what.c_str());
            } else {
                trace(log_,
                      "operator_commit_transaction: OPTION X drained {} pg_attribute backfill markers "
                      "for txn {} commit_id {} (patched in-place)",
                      backfill_count,
                      txn_data.transaction_id,
                      commit_id_);
            }
        }

        // Sits below the WAL marker: queues a deferred_delete_t stamped with commit_id, swept later.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && txn_data.transaction_id != 0 &&
            commit_id_ > 0 && !base_delete_table_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> delete_oids{base_delete_table_oids.begin(),
                                                                     base_delete_table_oids.end(),
                                                                     resource_};
            auto [_dc, dcf] = actor_zeta::otterbrix::send(
                ctx->index_address,
                &services::index::manager_index_t::commit_deletes,
                components::execution_context_t{ctx->session, txn_data, ctx->execution_context.timezone_offset},
                std::move(delete_oids),
                commit_id_);
            if (core::error_t result = co_await std::move(dcf); result.contains_error()) {
                error(log_,
                      "operator_commit_transaction: commit_deletes reported an error for txn {} commit_id {} — "
                      "the contract says it cannot ({}); the commit marker is already durable, so the publish "
                      "below proceeds and the deferred index erase is the thing that was lost",
                      txn_data.transaction_id,
                      commit_id_,
                      result.what);
            }
        }

        // Flips MVCC state — the only step allowed to stamp commit_id on live rows, since it runs after WAL sync.
        if (txn_data.transaction_id != 0 && commit_id_ > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t swap_ctx{ctx->session, txn_data, {}};
            std::vector<components::pg_catalog_append_range_t> all_appends = std::move(swap_appends);
            all_appends.insert(all_appends.end(),
                               std::make_move_iterator(base_appends.begin()),
                               std::make_move_iterator(base_appends.end()));
            if (!all_appends.empty()) {
                auto [_a, af] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_publish_commits,
                                                            swap_ctx,
                                                            commit_id_,
                                                            std::move(all_appends));
                co_await std::move(af);
            }
            std::set<components::catalog::oid_t> all_deletes = std::move(swap_deletes);
            all_deletes.insert(base_delete_tables.begin(), base_delete_tables.end());
            if (!all_deletes.empty()) {
                auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_publish_deletes,
                                                            swap_ctx,
                                                            commit_id_,
                                                            std::move(all_deletes));
                co_await std::move(df);
            }
        }

        // Must be the LAST step: everything above is already durable, so a crash before this can't lose a commit.
        uint64_t compact_watermark = 0;
        if (commit_id_ > 0 && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_p, pf] = actor_zeta::otterbrix::send(ctx->current_message_sender,
                                                        &services::dispatcher::manager_dispatcher_t::txn_publish_msg,
                                                        commit_id_);
            compact_watermark = co_await std::move(pf);
        }

        // Physical DROP happens only now, past the publish barrier; indexes are unregistered and awaited before
        // storage drops, since the two managers are separate mailboxes and FIFO alone doesn't order them.
        if (commit_id_ > 0 && !dropped_storage_oids.empty()) {
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                std::pmr::vector<actor_zeta::unique_future<void>> unregister_futures{resource_};
                unregister_futures.reserve(dropped_storage_oids.size());
                for (auto oid : dropped_storage_oids) {
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
                std::pmr::vector<components::catalog::oid_t> drop_oids{dropped_storage_oids.begin(),
                                                                       dropped_storage_oids.end(),
                                                                       resource_};
                auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::drop_storage_many,
                                                            ctx->session,
                                                            std::move(drop_oids));
                co_await std::move(df);
            }
        }

        // Mirrors the table DROP above: irreversible, so it waits for the pg_attribute tombstone to be durable.
        if (commit_id_ > 0 && !column_releases.empty() && ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (const auto& release : column_releases) {
                auto [_rc, rcf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::drop_storage_column,
                                                              ctx->session,
                                                              release.release_table_oid,
                                                              release.release_attname);
                auto released = co_await std::move(rcf);
                if (released.has_error()) {
                    set_error(released.error());
                    co_return;
                }
                trace(log_,
                      "operator_commit_transaction: released column '{}' of oid {} — {} (commit_id {})",
                      release.release_attname,
                      static_cast<unsigned>(release.release_table_oid),
                      released.value() ? "storage rebuilt without it" : "storage never carried it",
                      commit_id_);
            }
        }

        // Storage indexes columns BY NAME, so the copy must be renamed too or the next INSERT hits a stale name.
        if (commit_id_ > 0 && !column_renames.empty() && ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (const auto& rename : column_renames) {
                auto [_rn, rnf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::rename_storage_column,
                                                              ctx->session,
                                                              rename.release_table_oid,
                                                              rename.release_attname,
                                                              rename.rename_to_attname);
                auto renamed = co_await std::move(rnf);
                if (renamed.has_error()) {
                    set_error(renamed.error());
                    co_return;
                }
                trace(log_,
                      "operator_commit_transaction: renamed column '{}' -> '{}' of oid {} — {} (commit_id {})",
                      rename.release_attname,
                      rename.rename_to_attname,
                      static_cast<unsigned>(rename.release_table_oid),
                      renamed.value() ? "storage schema updated" : "storage never carried it",
                      commit_id_);
            }
        }

        // Gated on !base_delete_table_oids.empty(): an append-only commit produces zero dead rows to compact.
        if (ctx->disk_address != actor_zeta::address_t::empty_address() && commit_id_ > 0 &&
            !base_delete_table_oids.empty()) {
            std::pmr::vector<components::catalog::oid_t> compact_oids{resource_};
            compact_oids.reserve(base_append_oids.size() + base_delete_table_oids.size());
            std::set_union(base_append_oids.begin(),
                           base_append_oids.end(),
                           base_delete_table_oids.begin(),
                           base_delete_table_oids.end(),
                           std::back_inserter(compact_oids));
            // compact() shifts row positions; index engines hold POSITIONAL refs, so indexed tables are filtered out.
            std::pmr::vector<components::catalog::oid_t> safe_oids{resource_};
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_ti, tif] = actor_zeta::otterbrix::send(ctx->index_address,
                                                              &services::index::manager_index_t::tables_without_indexes,
                                                              ctx->session,
                                                              std::move(compact_oids));
                safe_oids = co_await std::move(tif);
            } else {
                safe_oids = std::move(compact_oids);
            }
            if (!safe_oids.empty()) {
                auto [_mc, mcf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::maybe_cleanup_many,
                                                components::execution_context_t{ctx->session,
                                                                                txn_data,
                                                                                ctx->execution_context.timezone_offset,
                                                                                components::catalog::INVALID_OID},
                                                std::move(safe_oids),
                                                compact_watermark);
                co_await std::move(mcf);
            }
        }

        // No row output: success surfaces via mark_executed(); commit_id already rode via ctx->committed_id.
        output_ = nullptr;
        mark_executed();
        co_return;
    }

} // namespace components::operators
