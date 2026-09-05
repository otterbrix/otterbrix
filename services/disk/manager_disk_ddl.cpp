#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Catalog DDL routers. The crash-safe WAL write + catalog scan + storage mutation
    // now run on the owning agent (agent-0 / CATALOG) in append_pg_catalog_row_inner /
    // delete_pg_catalog_rows_inner / update_pg_attribute_commit_id_field_inner /
    // compact_relkind_g_storage_inner, so the manager no longer borrows the agent's
    // slice across the actor boundary. Every catalog OID routes to agents_[0] via
    // pool_idx_for_oid. Routers mirror storage_append: pool_idx_for_oid → otterbrix::send
    // → if needs_sched enqueue → co_return co_await.

    manager_disk_t::unique_future<components::pg_catalog_append_range_t>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                          components::catalog::oid_t table_oid,
                                          components::vector::data_chunk_t row) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
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
        }
        co_return components::pg_catalog_append_range_t{table_oid, 0, 0};
    }

    manager_disk_t::unique_future<void> manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::int64_t oid_col_idx,
                                                                               components::catalog::oid_t target_oid) {
        // Single route to the agent inner body (the same body delete_pg_catalog_rows_many
        // loops, so both paths emit identical WAL records).
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
                co_await std::move(fut);
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::delete_pg_catalog_rows_many(execution_context_t ctx,
                                                std::pmr::vector<pg_catalog_delete_spec_t> specs) {
        // Loop-route per spec; each spec emits the same WAL + storage records as one
        // singular delete_pg_catalog_rows call. Serialized (co_await per spec) so the
        // WAL ordering matches N successive singular calls.
        if (agents_.empty()) {
            co_return;
        }
        for (const auto& spec : specs) {
            const std::size_t idx = pool_idx_for_oid(spec.table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent == nullptr) {
                continue;
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
            co_await std::move(fut);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::update_pg_attribute_commit_id_fields(
        execution_context_t ctx,
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
        std::uint64_t commit_id) {
        // Loop-route per backfill with the shared commit_id; each emits its own
        // physical_update WAL record. pg_attribute always routes to agents_[0].
        // Serialized (co_await per item) so the per-backfill WAL records are emitted
        // in order.
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        if (agents_.empty()) {
            co_return;
        }
        const std::size_t idx = pool_idx_for_oid(pg_attr_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return;
        }
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
            co_await std::move(fut);
        }
        co_return;
    }

    manager_disk_t::unique_future<std::uint64_t>
    manager_disk_t::compact_relkind_g_storage(execution_context_t /*ctx*/,
                                              components::catalog::oid_t table_oid,
                                              std::set<std::string> live_attnames) {
        // Single route: the whole compaction (read mode + columns, compute to_drop,
        // drop each, count) runs intra-agent in compact_relkind_g_storage_inner —
        // no per-column manager↔agent round-trips.
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

    // B3c1 — ALTER TABLE DROP COLUMN's physical half. Thin router, the same shape as every
    // other DDL leg: pool_idx_for_oid -> otterbrix::send -> enqueue if needed -> await. The
    // work itself runs intra-agent (drop_storage_column_inner) on that agent's own slice; the
    // manager never borrows a storage entry across the actor boundary.
    //
    // Not folded into compact_relkind_g_storage: that leg is SUBTRACTIVE (live set in, the
    // complement dropped) and gated to IN_MEMORY because it rides VACUUM. This one is
    // additive and ungated on purpose — see the contract note in disk_contract.hpp.
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
        // No agent to route to at all. The caller is a COMMITTED ALTER whose tombstone is
        // already durable, so answering "done" here would be exactly the silent degradation
        // rule 6 forbids: the column would be hidden forever and its space never named again.
        std::pmr::string msg{"manager_disk::drop_storage_column: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
    }

} // namespace services::disk
