#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Catalog DDL routers: the WAL write, catalog scan, and storage mutation run on the owning
    // agent, so the manager never borrows the agent's slice across the actor boundary.

    // REFUSE rather than answer with an empty range: reporting a missing agent as "appended 0
    // rows" would let CREATE TABLE report success over a pg_class row it never wrote.
    manager_disk_t::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                          components::catalog::oid_t table_oid,
                                          components::vector::data_chunk_t row) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"append_pg_catalog_row: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"append_pg_catalog_row: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::append_pg_catalog_row_inner,
                                                              ctx,
                                                              table_oid,
                                                              std::move(row));
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<void> manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::int64_t oid_col_idx,
                                                                               components::catalog::oid_t target_oid) {
        // Blocking debt: no error channel, so a refused delete reaches no caller and risks a duplicate row.
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::delete_pg_catalog_rows_inner,
                                                                      ctx,
                                                                      table_oid,
                                                                      oid_col_idx,
                                                                      target_oid);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                auto deleted = co_await std::move(fut);
                if (deleted.has_error()) {
                    error(log_,
                          "manager_disk::delete_pg_catalog_rows: the scrub of catalog oid={} for target oid={} "
                          "was refused and this route cannot report it: {}",
                          static_cast<unsigned>(table_oid),
                          static_cast<unsigned>(target_oid),
                          deleted.error().what);
                }
            }
        }
        co_return;
    }

    // Serialized so WAL ordering matches N successive singular calls, stopping at the first refusal.
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
    manager_disk_t::delete_pg_catalog_rows_many(execution_context_t ctx,
                                                std::pmr::vector<pg_catalog_delete_spec_t> specs) {
        std::pmr::vector<std::uint64_t> deleted_per_spec(resource());
        // Zero specs is the one legitimate no-op: no storage that should have been there is missing.
        if (specs.empty()) {
            co_return std::move(deleted_per_spec);
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"delete_pg_catalog_rows_many: no disk agents", resource()}};
        }
        deleted_per_spec.reserve(specs.size());
        for (const auto& spec : specs) {
            const std::size_t idx = pool_idx_for_oid(spec.table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent == nullptr) {
                co_return core::error_t{
                    core::error_code_t::io_error,
                    std::pmr::string{"delete_pg_catalog_rows_many: owning disk agent is null", resource()}};
            }
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::delete_pg_catalog_rows_inner,
                                                                  ctx,
                                                                  spec.table_oid,
                                                                  spec.oid_col_idx,
                                                                  spec.target_oid);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            auto deleted = co_await std::move(fut);
            if (deleted.has_error()) {
                co_return deleted.convert_error<std::pmr::vector<std::uint64_t>>();
            }
            deleted_per_spec.push_back(deleted.value());
        }
        co_return std::move(deleted_per_spec);
    }

    manager_disk_t::unique_future<components::pg_attribute_backfill_result_t>
    manager_disk_t::update_pg_attribute_commit_id_fields(
        execution_context_t ctx,
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
        std::uint64_t commit_id) {
        // Serialized (co_await per item) so per-backfill WAL records are emitted in order.
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        components::pg_attribute_backfill_result_t empty_out;
        if (backfills.empty()) {
            co_return empty_out;
        }
        if (agents_.empty()) {
            empty_out.refusal = core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"update_pg_attribute_commit_id_fields: no disk agents; no commit_id stamp "
                                 "was applied",
                                 resource()}};
            co_return empty_out;
        }
        const std::size_t idx = pool_idx_for_oid(pg_attr_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            empty_out.refusal = core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"update_pg_attribute_commit_id_fields: the agent owning pg_attribute is null; "
                                 "no commit_id stamp was applied",
                                 resource()}};
            co_return empty_out;
        }
        // Every marker is attempted: a refusal must not strand the ranges the others already appended.
        core::error_t first_refusal = core::error_t::no_error();
        std::size_t refused_count = 0;
        std::vector<components::pg_catalog_append_range_t> appended;
        appended.reserve(backfills.size());
        for (const auto& b : backfills) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent->address(),
                                            &agent_disk_t::update_pg_attribute_commit_id_field_inner,
                                            ctx,
                                            b.attoid,
                                            b.kind,
                                            commit_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            auto stamped = co_await std::move(fut);
            if (stamped.has_error()) {
                ++refused_count;
                if (!first_refusal.contains_error()) {
                    first_refusal = core::error_on(resource(), stamped.error());
                }
                continue;
            }
            const auto stamped_range = stamped.value();
            if (stamped_range.count > 0) {
                appended.push_back(
                    components::pg_catalog_append_range_t{pg_attr_oid, stamped_range.start_row, stamped_range.count});
            }
        }

        components::pg_attribute_backfill_result_t out;
        out.appended = std::move(appended);
        if (refused_count > 0) {
            std::pmr::string what{"update_pg_attribute_commit_id_fields: ", resource()};
            what.append(std::to_string(refused_count).c_str());
            what.append(" of ");
            what.append(std::to_string(backfills.size()).c_str());
            what.append(" commit_id stamp(s) were refused (");
            what.append(std::to_string(backfills.size() - refused_count).c_str());
            what.append(" applied); first refusal: ");
            what.append(first_refusal.what.c_str());
            out.refusal = core::error_t{first_refusal.type, std::move(what)};
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::uint64_t>
    manager_disk_t::compact_relkind_g_storage(execution_context_t /*ctx*/,
                                              components::catalog::oid_t table_oid,
                                              std::set<std::string> live_attnames) {
        // The whole compaction runs intra-agent: no per-column manager<->agent round-trips.
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::compact_relkind_g_storage_inner,
                                                                      table_oid,
                                                                      std::move(live_attnames));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        co_return 0;
    }

    manager_disk_t::unique_future<core::error_t>
    manager_disk_t::add_storage_column(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       components::table::column_definition_t column) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::add_storage_column_inner,
                                                                      ctx,
                                                                      table_oid,
                                                                      std::move(column));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        std::pmr::string msg{"manager_disk::add_storage_column: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::error_t{core::error_code_t::actor_agent_missing, std::move(msg)};
    }

    manager_disk_t::unique_future<core::error_t>
    manager_disk_t::stamp_column_dropped(execution_context_t ctx,
                                         components::catalog::oid_t table_oid,
                                         components::catalog::oid_t attoid) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::stamp_column_dropped_inner,
                                                                      table_oid,
                                                                      attoid,
                                                                      ctx.txn);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        std::pmr::string msg{"manager_disk::stamp_column_dropped: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::error_t{core::error_code_t::actor_agent_missing, std::move(msg)};
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::publish_column_stamps(execution_context_t ctx,
                                          uint64_t commit_id,
                                          std::pmr::set<components::catalog::oid_t> tables) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0 || agents_.empty()) {
            co_return;
        }
        std::pmr::vector<std::pmr::vector<components::catalog::oid_t>> per_agent{resource()};
        per_agent.reserve(agents_.size());
        for (std::size_t i = 0; i < agents_.size(); ++i) {
            per_agent.emplace_back();
        }
        for (const auto& table_oid : tables) {
            per_agent[pool_idx_for_oid(table_oid, agents_.size())].push_back(table_oid);
        }
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(per_agent.size());
        for (std::size_t i = 0; i < per_agent.size(); ++i) {
            if (per_agent[i].empty() || agents_[i] == nullptr) {
                continue;
            }
            auto& agent = agents_[i];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::publish_column_stamps_inner,
                                                                  txn_id,
                                                                  commit_id,
                                                                  std::move(per_agent[i]));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            agent_futures.push_back(std::move(fut));
        }
        for (auto& fut : agent_futures) {
            co_await std::move(fut);
        }
        co_return;
    }

    // The storage's column name caches the catalog's, so a rename it never saw is repaired at the
    // next bootstrap rather than waiting for a restart, which would leave live appends stale.
    manager_disk_t::unique_future<core::result_wrapper_t<bool>>
    manager_disk_t::rename_storage_column(session_id_t /*session*/,
                                          components::catalog::oid_t table_oid,
                                          std::string old_attname,
                                          std::string new_attname) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::rename_storage_column_inner,
                                                                      table_oid,
                                                                      std::move(old_attname),
                                                                      std::move(new_attname));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        // "Done" here would leave storage on the OLD name, the divergence bootstrap reads as a DROP.
        std::pmr::string msg{"manager_disk::rename_storage_column: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::actor_agent_missing, std::move(msg)});
    }

} // namespace services::disk
