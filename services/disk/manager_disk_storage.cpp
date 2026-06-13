#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    uint64_t manager_disk_t::direct_append_sync(catalog::oid_t table_oid,
                                                components::vector::data_chunk_t& data,
                                                core::date::timezone_offset_t session_tz) {
        // Bootstrap / WAL-replay only (pre-scheduler-start). Replay records carry no
        // MVCC txn, so the append commits under transaction_data{0, 0}. The
        // storage_entry_sync borrow is safe in this single-threaded window.
        const components::table::transaction_data txn{0, 0};
        components::storage::storage_t* s = nullptr;
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            if (agents_[pool_idx] != nullptr) {
                if (const auto* agent_entry = agents_[pool_idx]->storage_entry_sync(table_oid);
                    agent_entry != nullptr && agent_entry->storage != nullptr) {
                    s = agent_entry->storage.get();
                }
            }
        }
        if (!s || data.size() == 0)
            return 0;

        auto local = rebuild_chunk(resource(), data);

        if (!s->has_schema() && local.column_count() > 0) {
            s->adopt_schema(local.types());
        }

        const auto& table_columns = s->columns();
        if (!table_columns.empty() && local.column_count() < table_columns.size()) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < local.column_count(); col++) {
                    if (local.data[col].type().has_alias() &&
                        local.data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(local.data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], local.size());
                    expanded_data.back().validity().set_all_invalid(local.size());
                }
            }
            local.data = std::move(expanded_data);
        }

        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < local.column_count(); i++) {
                auto src_type = local.data[i].type().type();
                auto tgt_type = table_columns[i].type().type();
                if (src_type != tgt_type && (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                    (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                    auto& src_vec = local.data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, local.size());
                    for (uint64_t row = 0; row < local.size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type, session_tz));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    local.data[i] = std::move(casted);
                }
            }
        }

        return s->append(local, txn);
    }

    void manager_disk_t::direct_delete_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count) {
        // Bootstrap / WAL-replay only; routes the physical delete to the owning agent
        // under transaction_data{0, 0} (replay carries no MVCC txn).
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            agents_[pool_idx]->direct_delete_sync(
                table_oid, row_ids, count, components::table::transaction_data{0, 0});
        }
    }

    void manager_disk_t::direct_update_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            components::vector::data_chunk_t& new_data) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            agents_[pool_idx]->direct_update_sync(table_oid, row_ids, new_data);
        }
    }

    // --- Storage management ---
    // Every site routes through agents_[pool_idx_for_oid(oid)] (storage_entry_sync
    // borrow or storage_*_inner mailbox handler). No manager-side storage_t* survives.

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage(session_id_t session, catalog::oid_t table_oid, catalog::oid_t /*database_oid*/) {
        trace(log_,
              "manager_disk_t::create_storage , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Pure router: the IN_MEMORY entry is built with the AGENT's own resource() on
        // the agent thread (create_storage_inner). Only the oid crosses the mailbox; no
        // entry is constructed on the manager thread.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::create_storage_inner, table_oid);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            // Await so the storage exists before the future resolves; the bool result
            // signals dup-key — drop it (the agent already logged the duplicate).
            const bool ok = co_await std::move(fut);
            if (!ok) {
                trace(log_,
                      "manager_disk_t::create_storage: agent[{}] already owned oid {}",
                      pool_idx,
                      static_cast<unsigned>(table_oid));
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_with_columns(session_id_t session,
                                                catalog::oid_t table_oid,
                                                catalog::oid_t /*database_oid*/,
                                                std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_with_columns , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Pure router: columns cross the mailbox by value (same as today's by-value
        // parameter); the entry is built on the agent thread via
        // create_storage_with_columns_inner. No entry on the manager thread.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::create_storage_with_columns_inner,
                                                                   table_oid,
                                                                   std::move(columns));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            const bool ok = co_await std::move(fut);
            if (!ok) {
                trace(log_,
                      "manager_disk_t::create_storage_with_columns: agent[{}] already owned oid {}",
                      pool_idx,
                      static_cast<unsigned>(table_oid));
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_disk(session_id_t session,
                                        catalog::oid_t table_oid,
                                        catalog::oid_t database_oid,
                                        std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_disk , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Pure router for runtime CREATE TABLE … DISK. The manager only derives the
        // path string; create_directories + SFBM construction (which holds the
        // exclusive posix WRITE_LOCK) both run on the agent thread via
        // create_storage_disk_inner. Only oid/columns(by value)/path cross the mailbox.
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                         std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            trace(log_,
                  "manager_disk_t::create_storage_disk: oid={} pool_idx={} path={}",
                  static_cast<unsigned>(table_oid),
                  pool_idx,
                  otbx_path.string());
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::create_storage_disk_inner,
                                                                   table_oid,
                                                                   std::move(columns),
                                                                   std::move(otbx_path));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            const bool ok = co_await std::move(fut);
            if (!ok) {
                trace(log_,
                      "manager_disk_t::create_storage_disk: agent[{}] already owns oid {}",
                      pool_idx,
                      static_cast<unsigned>(table_oid));
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::drop_storage_many(session_id_t /*session*/,
                                      std::pmr::vector<components::catalog::oid_t> table_oids) {
        // Partition oids per owning agent (pool_idx_for_oid), then fan out one
        // drop_storage_many_inner per agent in PARALLEL — a per-oid singular drop
        // would route one agent per oid with a co_await each, so N drops cost N
        // round-trips; here they cost one (at most num_agents parallel sends). Each
        // agent's inner loops the same idempotent erase, so an over-routed oid no-ops.
        // Same partition-by-agent shape as storage_publish_commits.
        if (agents_.empty()) {
            co_return;
        }
        std::pmr::vector<std::pmr::vector<components::catalog::oid_t>> per_agent{resource()};
        per_agent.reserve(agents_.size());
        for (std::size_t i = 0; i < agents_.size(); ++i) {
            per_agent.emplace_back();
        }
        for (auto oid : table_oids) {
            const std::size_t pool_idx = pool_idx_for_oid(oid, agents_.size());
            per_agent[pool_idx].push_back(oid);
        }
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(per_agent.size());
        for (std::size_t i = 0; i < per_agent.size(); ++i) {
            if (per_agent[i].empty()) {
                continue;
            }
            auto& agent = agents_[i];
            if (agent == nullptr) {
                continue;
            }
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::drop_storage_many_inner,
                                                                   std::move(per_agent[i]));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    // --- Storage queries ---

    manager_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_t::storage_types(session_id_t /*session*/, catalog::oid_t table_oid) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_types_inner,
                                                                   table_oid);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        co_return std::pmr::vector<components::types::complex_logical_type>(resource());
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_total_rows(session_id_t /*session*/,
                                                                               catalog::oid_t table_oid) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_total_rows_inner,
                                                                   table_oid);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        co_return 0;
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan(session_id_t session,
                                 catalog::oid_t table_oid,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int limit,
                                 components::table::transaction_data txn) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_scan,
                                                                   session,
                                                                   table_oid,
                                                                   std::move(filter),
                                                                   limit,
                                                                   txn);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        co_return nullptr;
    }

    manager_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan_batched(session_id_t /*session*/,
                                         catalog::oid_t table_oid,
                                         std::unique_ptr<components::table::table_filter_t> filter,
                                         int64_t limit,
                                         std::vector<size_t> projected_cols,
                                         components::table::transaction_data txn) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_scan_batched_inner,
                                                                   table_oid,
                                                                   std::move(filter),
                                                                   limit,
                                                                   projected_cols,
                                                                   txn);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        co_return std::pmr::vector<components::vector::data_chunk_t>{resource()};
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  catalog::oid_t table_oid,
                                  components::vector::vector_t row_ids,
                                  uint64_t count) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_fetch_inner,
                                                                   table_oid,
                                                                   row_ids,
                                                                   count);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        co_return nullptr;
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan_segment(session_id_t /*session*/,
                                         catalog::oid_t table_oid,
                                         int64_t start,
                                         uint64_t count) {
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_scan_segment_inner,
                                                                   table_oid,
                                                                   start,
                                                                   count);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        co_return nullptr;
    }

    manager_disk_t::unique_future<std::pair<uint64_t, uint64_t>>
    manager_disk_t::storage_append(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        // The full preprocessing pipeline (schema adoption/growth, column
        // expansion, NOT NULL, dedup, type promotion) and the canonical write live
        // in the agent twin, so every same-oid access is serialized by the agent's
        // mailbox — no borrowed-pointer access from the manager loop thread.
        if (!data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_append_inner,
                                                                       table_oid,
                                                                       std::move(data),
                                                                       ctx.txn,
                                                                       ctx.session_tz);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        co_return std::make_pair(uint64_t{0}, uint64_t{0});
    }

    manager_disk_t::unique_future<std::pair<int64_t, uint64_t>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   components::vector::vector_t row_ids,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        // Pure router to the agent twin — the agent's mailbox serializes
        // the canonical write with every other same-oid access.
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_update_inner,
                                                                       table_oid,
                                                                       std::move(row_ids),
                                                                       std::move(data),
                                                                       ctx.txn);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        co_return std::pair<int64_t, uint64_t>{0, 0};
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_delete_rows(execution_context_t ctx,
                                                                                catalog::oid_t table_oid,
                                                                                components::vector::vector_t row_ids,
                                                                                uint64_t count) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_delete_rows_inner,
                                                                       table_oid,
                                                                       std::move(row_ids),
                                                                       count,
                                                                       ctx.txn);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        co_return 0;
    }

    // MVCC commit/revert methods

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_publish_commits(execution_context_t /*ctx*/,
                                           uint64_t commit_id,
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
        // Fanout: ranges may mix catalog and user OIDs; the agent inner handler is
        // idempotent for not-owned OIDs, so over-routing is safe.
        if (!agents_.empty()) {
            // emplace_back() yields vector(alloc): libc++ uses-allocator construction
            // appends per_agent's allocator as a trailing arg to the inner vector's ctor.
            std::pmr::vector<std::pmr::vector<components::pg_catalog_append_range_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back();
            }
            for (const auto& r : ranges) {
                if (r.count == 0)
                    continue;
                const std::size_t pool_idx = pool_idx_for_oid(r.table_oid, agents_.size());
                per_agent[pool_idx].push_back(r);
            }
            std::pmr::vector<unique_future<void>> agent_futures{resource()};
            agent_futures.reserve(per_agent.size());
            for (std::size_t i = 0; i < per_agent.size(); ++i) {
                if (per_agent[i].empty())
                    continue;
                auto& agent = agents_[i];
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_publish_commits_inner,
                                                                       commit_id,
                                                                       std::move(per_agent[i]));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                agent_futures.emplace_back(std::move(fut));
            }
            for (auto& f : agent_futures) {
                co_await std::move(f);
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_publish_deletes(execution_context_t ctx,
                                                                               uint64_t commit_id,
                                                                               std::set<catalog::oid_t> tables) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0)
            co_return;

        // Same partition-by-agent fanout as storage_publish_commits.
        if (!agents_.empty()) {
            std::pmr::vector<std::pmr::vector<catalog::oid_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back();
            }
            for (const auto& tbl_oid : tables) {
                const std::size_t pool_idx = pool_idx_for_oid(tbl_oid, agents_.size());
                per_agent[pool_idx].push_back(tbl_oid);
            }
            std::pmr::vector<unique_future<void>> agent_futures{resource()};
            agent_futures.reserve(per_agent.size());
            for (std::size_t i = 0; i < per_agent.size(); ++i) {
                if (per_agent[i].empty())
                    continue;
                auto& agent = agents_[i];
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_publish_deletes_inner,
                                                                       txn_id,
                                                                       commit_id,
                                                                       std::move(per_agent[i]));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                agent_futures.emplace_back(std::move(fut));
            }
            for (auto& f : agent_futures) {
                co_await std::move(f);
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_revert_appends(execution_context_t /*ctx*/,
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
        // Batched abort, same partition-by-agent fanout as storage_publish_commits;
        // each agent's inner handler reverse-iterates to unwind in append-order opposite.
        if (!agents_.empty()) {
            std::pmr::vector<std::pmr::vector<components::pg_catalog_append_range_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back();
            }
            for (const auto& r : ranges) {
                if (r.count == 0)
                    continue;
                const std::size_t pool_idx = pool_idx_for_oid(r.table_oid, agents_.size());
                per_agent[pool_idx].push_back(r);
            }
            std::pmr::vector<unique_future<void>> agent_futures{resource()};
            agent_futures.reserve(per_agent.size());
            for (std::size_t i = 0; i < per_agent.size(); ++i) {
                if (per_agent[i].empty())
                    continue;
                auto& agent = agents_[i];
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_revert_appends_inner,
                                                                       std::move(per_agent[i]));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                agent_futures.emplace_back(std::move(fut));
            }
            for (auto& f : agent_futures) {
                co_await std::move(f);
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_revert_deletes(execution_context_t ctx, std::vector<catalog::oid_t> tables) {
        // Abort-path mirror of storage_publish_deletes: same partition-by-agent
        // fanout, but the agent inner un-stamps this txn's pending delete marks
        // back to NOT_DELETED_ID (revert_all_deletes) instead of stamping a commit_id.
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0)
            co_return;

        if (!agents_.empty()) {
            std::pmr::vector<std::pmr::vector<catalog::oid_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back();
            }
            for (const auto& tbl_oid : tables) {
                const std::size_t pool_idx = pool_idx_for_oid(tbl_oid, agents_.size());
                per_agent[pool_idx].push_back(tbl_oid);
            }
            std::pmr::vector<unique_future<void>> agent_futures{resource()};
            agent_futures.reserve(per_agent.size());
            for (std::size_t i = 0; i < per_agent.size(); ++i) {
                if (per_agent[i].empty())
                    continue;
                auto& agent = agents_[i];
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                       &agent_disk_t::storage_revert_deletes_inner,
                                                                       txn_id,
                                                                       std::move(per_agent[i]));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                agent_futures.emplace_back(std::move(fut));
            }
            for (auto& f : agent_futures) {
                co_await std::move(f);
            }
        }
        co_return;
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }

} //namespace services::disk
