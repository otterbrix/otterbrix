#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    uint64_t manager_disk_t::direct_append_sync(catalog::oid_t table_oid,
                                                components::vector::data_chunk_t& data,
                                                core::date::timezone_offset_t session_tz) {
        return direct_append_sync(table_oid, data, session_tz, components::table::transaction_data{0, 0});
    }

    uint64_t manager_disk_t::direct_append_sync(catalog::oid_t table_oid,
                                                components::vector::data_chunk_t& data,
                                                core::date::timezone_offset_t session_tz,
                                                const components::table::transaction_data& txn) {
        // Probe the routed agent via storage_entry_sync and apply the
        // append directly. storage_entry_sync returns a borrowed pointer
        // safe for the manager thread between mailbox handlers
        // (direct_append_sync runs pre-scheduler-start WAL replay/bootstrap
        // seeding, or through the manager mailbox at runtime).
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
        direct_delete_sync(table_oid, row_ids, count, components::table::transaction_data{0, 0});
    }

    void manager_disk_t::direct_delete_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count,
                                            const components::table::transaction_data& txn) {
        // Pure forward to the routed agent slice.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            agents_[pool_idx]->direct_delete_sync(table_oid, row_ids, count, txn);
        }
    }

    void manager_disk_t::direct_update_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            components::vector::data_chunk_t& new_data) {
        // Pure forward to the routed agent slice.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            agents_[pool_idx]->direct_update_sync(table_oid, row_ids, new_data);
        }
    }

    // --- Storage management ---
    //
    // Every storage read/write site probes the routed agent slice via
    // `agents_[pool_idx_for_oid(oid)]->storage_entry_sync(oid)` (or routes
    // through the agent's storage_*_inner mailbox handlers). No manager-side
    // storage_t* pointer survives.

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage(session_id_t session, catalog::oid_t table_oid, catalog::oid_t /*database_oid*/) {
        trace(log_,
              "manager_disk_t::create_storage , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Route CREATE through the agent slice. Safety contract:
        //   1. No agent mailbox handler MUTATES storages_ (find-only).
        //   2. The new OID is not yet visible to any router: this handler
        //      publishes it.
        //   3. The manager actor processes one message at a time.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto twin = std::make_unique<collection_storage_entry_t>(agent->resource());
            const bool ok = agent->bootstrap_inner_sync(table_oid, std::move(twin));
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
        // Route CREATE through the agent slice. Safety contract: see
        // create_storage above.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto twin = std::make_unique<collection_storage_entry_t>(agent->resource(), std::move(columns));
            const bool ok = agent->bootstrap_inner_sync(table_oid, std::move(twin));
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
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                         std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
        std::filesystem::create_directories(otbx_path.parent_path());
        // Runtime CREATE TABLE … DISK. SFBM is constructed on the routed
        // agent via bootstrap_create_disk_inner_sync; the manager never
        // opens .otbx (single_file_block_manager_t holds exclusive
        // WRITE_LOCK — posix advisory, per-process; only the agent path
        // may emplace).
        //
        // Safety contract for calling bootstrap_create_disk_inner_sync from
        // the manager mailbox handler at runtime mirrors create_storage:
        //   1. No agent mailbox handler mutates storages_ (find-only).
        //   2. The new OID is not yet visible to any router: this handler
        //      publishes it.
        //   3. The manager actor processes one message at a time.
        // The helper is noexcept and probes storages_ before constructing
        // the SFBM, so the dup-key path is observable-but-non-fatal.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            trace(log_,
                  "manager_disk_t::create_storage_disk: oid={} pool_idx={} path={}",
                  static_cast<unsigned>(table_oid),
                  pool_idx,
                  otbx_path.string());
            const bool ok = agent->bootstrap_create_disk_inner_sync(table_oid, std::move(columns), otbx_path);
            if (!ok) {
                trace(log_,
                      "manager_disk_t::create_storage_disk: agent[{}] already owns oid {} (path={})",
                      pool_idx,
                      static_cast<unsigned>(table_oid),
                      otbx_path.string());
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_storage(session_id_t session, catalog::oid_t table_oid) {
        trace(log_,
              "manager_disk_t::drop_storage , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Pure mailbox-router. The agent owns the canonical erase +
        // filesystem remove sequence in drop_storage_inner; it is
        // idempotent on a missing key (DROP IF EXISTS / WAL replay re-drop /
        // not-routed-to-this-agent all reduce to a logged no-op). co_await
        // preserves ordering w.r.t. operator_dynamic_cascade_delete's
        // subsequent sends.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::drop_storage_inner,
                                                                   table_oid);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_await std::move(fut);
        }
        co_return;
    }

    // --- Storage queries ---

    manager_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_t::storage_types(session_id_t /*session*/, catalog::oid_t table_oid) {
        // Pure router. Agent returns an empty pmr-vector for not-owned /
        // schema-less twins — propagate as-is.
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
        // Pure router. 0 from the agent means "not owned" OR "empty twin"
        // — both equivalent for this caller.
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
        // Pure router. Agent returns nullptr for not-owned OIDs.
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
        // Pure router. Agent returns an empty pmr-vector for not-owned /
        // empty twins.
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
        // Pure router. Agent returns nullptr for not-owned OIDs.
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
        // Pure router. Agent returns nullptr for not-owned OIDs.
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
        // Pure router. The full preprocessing pipeline (schema adoption /
        // growth, column expansion, NOT NULL, dedup, type promotion) and the
        // canonical write live in the agent twin (storage_append_inner) so
        // that every same-oid access is serialized by the agent's mailbox —
        // no borrowed-pointer access from the manager loop thread.
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
        // Pure router to the agent twin — the agent's mailbox serializes
        // the canonical write with every other same-oid access.
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

    manager_disk_t::unique_future<void> manager_disk_t::storage_publish_commit(execution_context_t /*ctx*/,
                                                                              catalog::oid_t table_oid,
                                                                              uint64_t commit_id,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        // Pure router — single-range payload to the plural agent twin
        // (storage_publish_commits_inner); the agent's mailbox serializes
        // the MVCC visibility flip with every other same-oid access.
        if (agents_.empty())
            co_return;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr)
            co_return;
        std::pmr::vector<components::pg_catalog_append_range_t> ranges{resource()};
        ranges.push_back(components::pg_catalog_append_range_t{table_oid, row_start, count});
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                               &agent_disk_t::storage_publish_commits_inner,
                                                               commit_id,
                                                               std::move(ranges));
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_await std::move(fut);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_revert_append(execution_context_t /*ctx*/,
                                                                              catalog::oid_t table_oid,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        // Pure forward to the routed agent slice.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::storage_revert_append_inner,
                                                                   table_oid,
                                                                   row_start,
                                                                   count);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_await std::move(fut);
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_publish_delete(execution_context_t ctx, catalog::oid_t table_oid, uint64_t commit_id) {
        // Pure router — single-oid payload to the plural agent twin
        // (storage_publish_deletes_inner); the agent's mailbox serializes
        // the MVCC delete commit with every other same-oid access. txn_id 0
        // (no real transaction) short-circuits, matching the twin's guard.
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0 || agents_.empty())
            co_return;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr)
            co_return;
        std::pmr::vector<components::catalog::oid_t> tables{resource()};
        tables.push_back(table_oid);
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                               &agent_disk_t::storage_publish_deletes_inner,
                                                               txn_id,
                                                               commit_id,
                                                               std::move(tables));
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_await std::move(fut);
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_publish_commits(execution_context_t /*ctx*/,
                                           uint64_t commit_id,
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
        // Pure router fanout. Per-agent payload is a PMR vector. ranges may
        // carry both catalog and user OIDs; agent inner handler is
        // idempotent for not-owned OIDs (over-routing is safe).
        if (!agents_.empty()) {
            // Per-agent slice partition. Each slot is an empty pmr-vector
            // whose allocator is propagated by uses-allocator construction
            // from the outer `per_agent`'s allocator (libc++ appends the
            // outer allocator as a trailing argument to `_Tp(...)`, so
            // `emplace_back()` with no arg yields `vector(alloc)`).
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

        // Pure router fanout — same partition-by-agent pattern as
        // storage_publish_commits.
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
        // Pure router fanout for batched abort. Same partition-by-agent
        // pattern as storage_publish_commits; each agent's inner handler
        // reverse-iterates its slice to preserve append-order opposite.
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

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }

} //namespace services::disk
