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
        // Step 8.12 (2026-05-31): manager-side `storages_` map is gone. The
        // routed agent slice owns every entry, so this body probes the
        // agent via storage_entry_sync and applies the append directly.
        // The pre-§8.12 manager-side fallback `get_storage(...)` has been
        // removed — `get_storage` returned nullptr unconditionally after
        // §8.11.
        //
        // Constraint #11 carve-out — storage_entry_sync returns a
        // borrowed pointer into the agent's slice that is safe for the
        // manager thread between mailbox handlers. direct_append_sync is
        // invoked sync (WAL replay + bootstrap seed paths run pre-
        // scheduler-start; runtime catalog seeding routes through the
        // manager mailbox), so the read is single-threaded vs. the agent
        // mailbox.
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
        // Step 8.12 (2026-05-31): pure forward to the routed agent slice.
        // The pre-§8.12 manager-side mirror against `get_storage(...)` has
        // been removed — `get_storage` returned nullptr unconditionally
        // after §8.11.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            agents_[pool_idx]->direct_delete_sync(table_oid, row_ids, count, txn);
        }
    }

    void manager_disk_t::direct_update_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            components::vector::data_chunk_t& new_data) {
        // Step 8.12 (2026-05-31): pure forward to the routed agent slice.
        // The pre-§8.12 manager-side mirror against `get_storage(...)` has
        // been removed — `get_storage` returned nullptr unconditionally
        // after §8.11.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            agents_[pool_idx]->direct_update_sync(table_oid, row_ids, new_data);
        }
    }

    // --- Storage management ---
    //
    // Step 8.12 (2026-05-31): `manager_disk_t::get_storage()` has been
    // DELETED along with `manager_disk_t::storages_`. Every storage read /
    // write site now probes the routed agent slice via
    // `agents_[pool_idx_for_oid(oid)]->storage_entry_sync(oid)` (or routes
    // through the agent's storage_*_inner mailbox handlers). No manager-
    // side storage_t* pointer survives.

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage(session_id_t session, catalog::oid_t table_oid, catalog::oid_t /*database_oid*/) {
        trace(log_,
              "manager_disk_t::create_storage , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. Route the
        // CREATE through the agent slice — agents_[pool_idx_for_oid] is the
        // canonical owner. Safety contract:
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. Route the
        // CREATE through the agent slice — agents_[pool_idx_for_oid] is the
        // canonical owner (catalog → agents_[0], user → agents_[1..N-1]).
        // Safety contract: see create_storage above.
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
        // Version B* Step 8.1.C runtime gap closure (2026-05-31).
        //
        // Pre-closure this handler emplaced the SFBM-owning entry into
        // manager.storages_ only. After §8.1.C migrated the bootstrap
        // create_storage_disk_sync path (manager_disk_io.cpp:427) to
        // agent-canonical ownership, the runtime mailbox handler became
        // the lone surviving manager-side SFBM emplace site for user
        // DISK tables — readers route through agents_[idx]->storage_
        // entry_sync (per §8.11.B) and would see nothing here, so
        // CREATE TABLE ... DISK at runtime produced an SFBM the routed
        // agent never observed → scans, appends, fetches returned empty
        // / 0 (the agent-first paths in this same file all fall back to
        // manager.storages_ on agent-miss, but post-§8.1.C readers were
        // migrated agent-first to remove that fallback for the DISK
        // case).
        //
        // Closure: mirror create_storage_disk_sync semantics. SFBM is
        // constructed on the routed agent via bootstrap_create_disk_
        // inner_sync; manager.storages_ skips the emplace. The
        // single_file_block_manager_t holds an exclusive WRITE_LOCK on
        // `.otbx` (posix advisory, per-process), so EXACTLY ONE of
        // (manager, agent) may emplace — the agent path is now
        // authoritative for ALL OIDs.
        //
        // Safety contract for calling bootstrap_create_disk_inner_sync
        // from the manager mailbox handler at runtime (post-scheduler-
        // start) mirrors the create_storage / create_storage_with_
        // columns precedent above:
        //   1. No agent mailbox handler mutates storages_ (all are
        //      find-only — verified at the §8.2 callsite annotation).
        //   2. The new OID is not yet visible to any router: this
        //      mailbox handler is the very call that publishes it.
        //   3. The manager actor processes one message at a time, so
        //      two manager-side create_storage_* calls cannot
        //      interleave their inserts into the same agent slice.
        // The bootstrap_create_disk_inner_sync helper is noexcept and
        // probes storages_ BEFORE constructing the SFBM, so the dup-key
        // path is observable-but-non-fatal (logged drop of the just-
        // built handle never happens because the manager emplace is
        // gone — the only collision would be a programmer-level double-
        // CREATE).
        //
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. agents_[pool_idx]
        // is the sole SFBM owner; no-agents test fixture (path-empty config) no
        // longer reaches this handler since manager_disk_t's constructor
        // unconditionally calls create_agent() when config_.path is set.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            trace(log_,
                  "[step-8.1.C-runtime-create-disk] oid={} pool_idx={} path={} "
                  "(agent canonical owner — manager.storages_ deleted)",
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
        // Version B* Step 8.4.B (DELETED 2026-05-31): the manager-side
        // storages_.find + erase + filesystem remove sequence has been moved
        // to agent_disk_t::drop_storage_inner. The agent's slice is now the
        // sole writer for runtime DROP TABLE — manager_disk_t::storages_
        // retains any entry that was emplaced under the dual-write era; the
        // residual orphan rows are cleared by §8.11/§8.12 when the manager
        // map is retired entirely. The mark_storage_dropped path reads
        // storages_ BEFORE this mailbox handler runs (operator_dynamic_
        // cascade_delete sends mark_storage_dropped first, drop_storage
        // second), so derivation of otbx_path + sidecars in mark_storage_
        // dropped continues to function on the manager-side map until
        // §8.4.C migrates that read to the agent.
        //
        // Routing matches the create-path gate: idx = pool_idx_for_oid(oid)
        // — catalog OIDs land on agents_[0], user OIDs on agents_[1..N-1].
        // drop_storage_inner is idempotent on a missing key (DROP IF
        // EXISTS / WAL replay re-drop / not-routed-to-this-agent OIDs all
        // reduce to a logged no-op).
        //
        // Constraint #11: mailbox-only forward (otterbrix::send). co_await
        // preserves drop_storage's mailbox-handler ordering w.r.t.
        // operator_dynamic_cascade_delete's subsequent sends.
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; the routed
        // agent slice is the sole source of truth. Agent returns an empty
        // pmr-vector for not-owned / schema-less twins — propagate as-is.
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; the routed
        // agent slice is the sole source of truth. 0 from the agent now means
        // "not owned" OR "empty twin" — both equivalent for this caller.
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; the routed
        // agent slice is the sole source of truth. Agent returns nullptr for
        // not-owned OIDs — propagate as-is.
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; the routed
        // agent slice is the sole source of truth. Agent returns an empty
        // pmr-vector for not-owned / empty twins — propagate as-is.
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
        // Step 8.12 (2026-05-31): manager-side `storages_` and
        // `get_storage()` deleted; the routed agent slice is the sole source
        // of truth. Agent returns nullptr for not-owned OIDs — propagate
        // as-is.
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
        // Step 8.12 (2026-05-31): manager-side `storages_` and
        // `get_storage()` deleted; the routed agent slice is the sole source
        // of truth. Agent returns nullptr for not-owned OIDs — propagate
        // as-is.
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
        auto& txn = ctx.txn;
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. Probe the
        // routed agent's slice via storage_entry_sync to get the canonical
        // storage_t (Constraint #11 carve-out — agent mailbox idle vs. this
        // sync probe inside the manager mailbox handler). Manager body
        // preprocessing (schema growth, NOT NULL, dedup, type promotion)
        // continues to run inline so the agent-side storage_append_inner
        // can stay a pure `entry->storage->append(*data, txn)` mirror.
        const collection_storage_entry_t* agent_entry = nullptr;
        components::storage::storage_t* s = nullptr;
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (agents_[idx] != nullptr) {
                agent_entry = agents_[idx]->storage_entry_sync(table_oid);
                if (agent_entry != nullptr && agent_entry->storage != nullptr) {
                    // const_cast — storage_entry_sync returns const for slice-
                    // ownership immutability, but storage_t::append / scan /
                    // adopt_schema are non-const operations on the entry's
                    // interior state. Same Constraint #11 carve-out as
                    // direct_append_sync (agent thread idle vs. this handler).
                    s = const_cast<components::storage::storage_t*>(agent_entry->storage.get());
                }
            }
        }
        if (!s || !data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }

        // Computing (relkind='g') tables — `is_computed` flag lives on the
        // collection_storage_entry_t (agent's owning copy). Read it via the
        // const borrow from storage_entry_sync above.
        const bool is_computed_table = (agent_entry != nullptr) ? agent_entry->is_computed : false;

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 1b. Dynamic schema growth for IN_MEMORY storages — mutates the
        // agent-owned collection_storage_entry_t directly via const_cast on
        // agent_entry (same Constraint #11 carve-out as the storage pointer
        // above; agent thread idle vs. this manager mailbox handler). See
        // pre-§8.11 history for the trigger-condition rationale (alias
        // mismatch at differing chunk/table width = schema growth; equal
        // width = positional rename, handled by column expansion below).
        if (s->has_schema() && data->column_count() > 0 &&
            agent_entry != nullptr &&
            (is_computed_table || data->column_count() != s->columns().size()) &&
            agent_entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
            std::vector<components::table::column_definition_t> new_columns;
            for (uint64_t col = 0; col < data->column_count(); col++) {
                if (!data->data[col].type().has_alias()) {
                    continue;
                }
                const auto alias = data->data[col].type().alias();
                const auto ctype = data->data[col].type().type();
                bool present = false;
                for (const auto& tc : s->columns()) {
                    if (tc.name() == alias && (!is_computed_table || tc.type().type() == ctype)) {
                        present = true;
                        break;
                    }
                }
                if (!present) {
                    auto ct = data->data[col].type();
                    ct.set_alias(alias);
                    new_columns.emplace_back(alias, ct);
                }
            }
            if (!new_columns.empty()) {
                auto* mutable_entry = const_cast<collection_storage_entry_t*>(agent_entry);
                for (auto& col : new_columns) {
                    mutable_entry->add_column(col, resource());
                }
                // add_column rebuilt the storage adapter; refresh our local
                // storage_t* to point at the new adapter.
                s = const_cast<components::storage::storage_t*>(mutable_entry->storage.get());
                if (!s) {
                    co_return std::make_pair(uint64_t{0}, uint64_t{0});
                }
            }
        }

        // 2. Column expansion
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() > 0) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            // Computing tables match by (name, type) so each type-variant lands in
            // its own physical column; unmatched variants get NULL. Positional
            // fallback is disabled there (it assumes one column per name).
            const bool positional_fallback = !is_computed_table && (data->column_count() == table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name() &&
                        (!is_computed_table || data->data[col].type().type() == table_columns[t].type().type())) {
                        expanded_data.push_back(std::move(data->data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found && positional_fallback && t < data->column_count()) {
                    expanded_data.push_back(std::move(data->data[t]));
                    found = true;
                }
                if (!found) {
                    if (table_columns[t].has_default_value()) {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        for (uint64_t row = 0; row < data->size(); row++) {
                            expanded_data.back().set_value(row, table_columns[t].default_value());
                        }
                    } else {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        expanded_data.back().validity().set_all_invalid(data->size());
                    }
                }
            }
            data->data = std::move(expanded_data);
        }

        // 2b. NOT NULL enforcement
        if (!table_columns.empty()) {
            for (size_t col = 0; col < table_columns.size() && col < data->column_count(); col++) {
                if (table_columns[col].is_not_null()) {
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!data->data[col].validity().row_is_valid(row)) {
                            trace(log_, "storage_append: NOT NULL violation on column '{}'", table_columns[col].name());
                            co_return std::make_pair(uint64_t{0}, uint64_t{0});
                        }
                    }
                }
            }
        }

        // 3. Dedup
        if (s->total_rows() > 0) {
            int64_t id_col = -1;
            for (uint64_t col = 0; col < data->column_count(); col++) {
                if (data->data[col].type().has_alias() && data->data[col].type().alias() == "_id") {
                    id_col = static_cast<int64_t>(col);
                    break;
                }
            }
            if (id_col >= 0) {
                auto existing = std::make_unique<components::vector::data_chunk_t>(resource(), s->types(), 0);
                s->scan(*existing, nullptr, -1);

                int64_t existing_id_col = -1;
                for (uint64_t col = 0; col < existing->column_count(); col++) {
                    if (existing->data[col].type().has_alias() && existing->data[col].type().alias() == "_id") {
                        existing_id_col = static_cast<int64_t>(col);
                        break;
                    }
                }

                if (existing_id_col >= 0 && existing->size() > 0) {
                    std::unordered_set<std::string> existing_ids;
                    for (uint64_t i = 0; i < existing->size(); i++) {
                        auto val = existing->data[static_cast<size_t>(existing_id_col)].value(i);
                        if (!val.is_null()) {
                            existing_ids.emplace(val.value<std::string_view>());
                        }
                    }

                    std::vector<uint64_t> keep_rows;
                    keep_rows.reserve(data->size());
                    for (uint64_t i = 0; i < data->size(); i++) {
                        auto val = data->data[static_cast<size_t>(id_col)].value(i);
                        if (val.is_null() ||
                            existing_ids.find(std::string(val.value<std::string_view>())) == existing_ids.end()) {
                            keep_rows.push_back(i);
                        }
                    }

                    if (keep_rows.empty()) {
                        co_return std::make_pair(uint64_t{0}, uint64_t{0});
                    }

                    if (keep_rows.size() < data->size()) {
                        auto filtered = std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                                           data->types(),
                                                                                           keep_rows.size());
                        for (uint64_t col = 0; col < data->column_count(); col++) {
                            for (uint64_t i = 0; i < keep_rows.size(); i++) {
                                auto val = data->data[col].value(keep_rows[i]);
                                filtered->data[col].set_value(i, val);
                            }
                        }
                        data = std::move(filtered);
                    }
                }
            }
        }

        // 4. Type promotion
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < data->column_count(); i++) {
                auto src_type = data->data[i].type();
                auto tgt_type = table_columns[i].type();
                if (src_type != tgt_type && src_type.is_convertable_to(tgt_type)) {
                    auto& src_vec = data->data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, data->size());
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type, ctx.session_tz));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        // 5. Append
        auto actual_count = data->size();
        uint64_t start_row;
        if (txn.transaction_id != 0) {
            start_row = s->append(*data, txn);
        } else {
            start_row = s->append(*data);
        }

        // Step 8.11 wrap (2026-05-31): the previous post-write router fanout
        // to agent_disk_t::storage_append_inner is GONE — `s` above already
        // points at the agent-owned storage_t (via storage_entry_sync), so
        // `s->append(*data, txn)` IS the canonical write against the agent
        // slice. Re-sending the chunk through storage_append_inner would
        // double-write the row range.
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_t::unique_future<std::pair<int64_t, uint64_t>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   components::vector::vector_t row_ids,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; route via
        // the agent's storage_entry_sync borrow. The agent fanout is gone —
        // the update IS the canonical write.
        components::storage::storage_t* s = nullptr;
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (agents_[idx] != nullptr) {
                if (const auto* entry = agents_[idx]->storage_entry_sync(table_oid);
                    entry != nullptr && entry->storage != nullptr) {
                    s = const_cast<components::storage::storage_t*>(entry->storage.get());
                }
            }
        }
        if (!s) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        co_return s->update(row_ids, *data, ctx.txn);
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_delete_rows(execution_context_t ctx,
                                                                                catalog::oid_t table_oid,
                                                                                components::vector::vector_t row_ids,
                                                                                uint64_t count) {
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; route via
        // the agent's storage_entry_sync borrow. The agent fanout is gone —
        // the delete IS the canonical write.
        components::storage::storage_t* s = nullptr;
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (agents_[idx] != nullptr) {
                if (const auto* entry = agents_[idx]->storage_entry_sync(table_oid);
                    entry != nullptr && entry->storage != nullptr) {
                    s = const_cast<components::storage::storage_t*>(entry->storage.get());
                }
            }
        }
        if (!s) {
            co_return 0;
        }
        uint64_t deleted = 0;
        if (ctx.txn.transaction_id != 0) {
            deleted = s->delete_rows(row_ids, count, ctx.txn.transaction_id);
        } else {
            deleted = s->delete_rows(row_ids, count);
        }
        co_return deleted;
    }

    // MVCC commit/revert methods

    manager_disk_t::unique_future<void> manager_disk_t::storage_publish_commit(execution_context_t /*ctx*/,
                                                                              catalog::oid_t table_oid,
                                                                              uint64_t commit_id,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; probe the
        // agent slice for the canonical storage_t and apply commit_append
        // directly. No prior agent fanout existed for this single-OID
        // mailbox handler.
        if (agents_.empty())
            co_return;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[idx] == nullptr)
            co_return;
        const auto* entry = agents_[idx]->storage_entry_sync(table_oid);
        if (entry == nullptr || entry->storage == nullptr)
            co_return;
        const_cast<components::storage::storage_t*>(entry->storage.get())
            ->commit_append(commit_id, row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_revert_append(execution_context_t /*ctx*/,
                                                                              catalog::oid_t table_oid,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        // Step 8.12 (2026-05-31): pure forward to the routed agent slice.
        // The pre-§8.12 manager-side `get_storage(...)->revert_append`
        // mirror has been removed — `get_storage` returned nullptr
        // unconditionally after §8.11.
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; probe the
        // agent slice for the canonical storage_t and apply
        // commit_all_deletes directly. No prior agent fanout existed.
        if (agents_.empty())
            co_return;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[idx] == nullptr)
            co_return;
        const auto* entry = agents_[idx]->storage_entry_sync(table_oid);
        if (entry == nullptr || entry->storage == nullptr)
            co_return;
        const_cast<components::storage::storage_t*>(entry->storage.get())
            ->commit_all_deletes(ctx.txn.transaction_id, commit_id);
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_publish_commits(execution_context_t /*ctx*/,
                                           uint64_t commit_id,
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
        // Step 8.12 (2026-05-31): pure router fanout. Per-agent payload is a
        // PMR vector (Constraint #11). Manager-side `get_storage(...)`
        // loop has been removed — `get_storage` returned nullptr
        // unconditionally after §8.11. ranges may carry both catalog and
        // user OIDs; agent inner handler is idempotent for not-owned OIDs,
        // so over-routing is safe.
        if (!agents_.empty()) {
            // Per-agent slice partition. Each slot is an empty pmr-vector
            // bound to the manager's resource (uses-allocator construction
            // when emplace_back is called on the outer pmr vector with an
            // explicit allocator-aware constructor argument).
            std::pmr::vector<std::pmr::vector<components::pg_catalog_append_range_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back(resource());
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

        // Step 8.12 (2026-05-31): pure router fanout. Same partition-by-
        // agent pattern as storage_publish_commits: user OIDs partition
        // into agents_[1..N-1] alongside catalog OIDs on agents_[0].
        // Manager-side `get_storage(...)` loop has been removed —
        // `get_storage` returned nullptr unconditionally after §8.11.
        // Constraint #11: PMR vector for the per-agent payload, mailbox-only
        // send, txn_id/commit_id passed by value.
        if (!agents_.empty()) {
            std::pmr::vector<std::pmr::vector<catalog::oid_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back(resource());
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
        // Step 8.12 (2026-05-31): pure router fanout for batched abort.
        // Same partition-by-agent pattern as storage_publish_commits: user
        // OIDs partition into agents_[1..N-1] alongside catalog OIDs on
        // agents_[0]. Manager-side reverse-iterate `get_storage(...)` loop
        // has been removed — `get_storage` returned nullptr unconditionally
        // after §8.11. Per-agent inner handler reverse-iterates its own
        // slice to preserve append-order opposite. Constraint #11: PMR
        // per-agent vector, mailbox-only send, ranges moved by value into
        // each fanout.
        if (!agents_.empty()) {
            std::pmr::vector<std::pmr::vector<components::pg_catalog_append_range_t>> per_agent{resource()};
            per_agent.reserve(agents_.size());
            for (std::size_t i = 0; i < agents_.size(); ++i) {
                per_agent.emplace_back(resource());
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
