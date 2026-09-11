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

    manager_disk_t::unique_future<core::error_t> manager_disk_t::update_pg_attribute_commit_id_fields(
        execution_context_t ctx,
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
        std::uint64_t commit_id) {
        // Serialized (co_await per item) so per-backfill WAL records are emitted in order.
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        if (backfills.empty()) {
            co_return core::error_t::no_error();
        }
        if (agents_.empty()) {
            co_return core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"update_pg_attribute_commit_id_fields: no disk agents; no commit_id stamp "
                                 "was applied",
                                 resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(pg_attr_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"update_pg_attribute_commit_id_fields: the agent owning pg_attribute is null; "
                                 "no commit_id stamp was applied",
                                 resource()}};
        }
        // Every marker is attempted: this path is below the durable commit marker and can't be retried.
        core::error_t first_refusal = core::error_t::no_error();
        std::size_t refused_count = 0;
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
            if (stamped.contains_error()) {
                ++refused_count;
                if (!first_refusal.contains_error()) {
                    first_refusal = core::error_on(resource(), stamped);
                }
            }
        }

        // Can't take a second cross-actor await inside an append, so identity is parked here and
        // stamped at column creation; fire-and-forget relies on mailbox FIFO order to land it first.
        for (const auto& b : backfills) {
            if (b.kind != components::pg_attribute_commit_id_backfill_t::kind_t::added_at ||
                b.release_attname.empty() || b.release_table_oid == components::catalog::INVALID_OID ||
                b.attoid == components::catalog::INVALID_OID) {
                continue;
            }
            const std::size_t owner_idx = pool_idx_for_oid(b.release_table_oid, agents_.size());
            if (owner_idx >= agents_.size() || agents_[owner_idx] == nullptr) {
                continue;
            }
            auto& owner = agents_[owner_idx];
            auto [owner_sched, note_fut] = actor_zeta::otterbrix::send(owner->address(),
                                                                       &agent_disk_t::note_column_identity_inner,
                                                                       b.release_table_oid,
                                                                       b.release_attname,
                                                                       static_cast<std::uint32_t>(b.attoid),
                                                                       b.added_column_type);
            [[maybe_unused]] auto dropped_note_future = std::move(note_fut);
            if (owner_sched) {
                scheduler_disk_->enqueue(owner.get());
            }
        }
        // The identity notes are fire-and-forget, so the answer carries only what the stamps did.
        if (refused_count > 0) {
            std::pmr::string what{"update_pg_attribute_commit_id_fields: ", resource()};
            what.append(std::to_string(refused_count).c_str());
            what.append(" of ");
            what.append(std::to_string(backfills.size()).c_str());
            what.append(" commit_id stamp(s) were refused (");
            what.append(std::to_string(backfills.size() - refused_count).c_str());
            what.append(" applied); first refusal: ");
            what.append(first_refusal.what.c_str());
            co_return core::error_t{first_refusal.type, std::move(what)};
        }
        co_return core::error_t::no_error();
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

    // Not folded into compact_relkind_g_storage (SUBTRACTIVE: a caller's gap there drops a SURVIVING column).
    manager_disk_t::unique_future<core::result_wrapper_t<bool>>
    manager_disk_t::drop_storage_column(session_id_t /*session*/,
                                        components::catalog::oid_t table_oid,
                                        std::string attname) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::drop_storage_column_inner,
                                                                      table_oid,
                                                                      std::move(attname));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        // The caller's tombstone is already durable, so "done" here would hide the column forever.
        std::pmr::string msg{"manager_disk::drop_storage_column: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
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
        co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
    }

} // namespace services::disk
