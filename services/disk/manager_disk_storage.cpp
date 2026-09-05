#include "manager_disk_impl.hpp"

#include <cassert>

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    core::result_wrapper_t<uint64_t> manager_disk_t::direct_append_sync(catalog::oid_t table_oid,
                                                                        components::vector::data_chunk_t& data) {
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
        // THE TWO ZEROES THAT USED TO BE ONE. A chunk with no rows asks for nothing and is
        // the single legitimate no-op on this path — the same one direct_delete_sync keeps for
        // a record that names no row ids. NO STORAGE FOR THE OID is the opposite: a record of
        // COMMITTED rows that recovery has nowhere to put, which is what the other three
        // replay routers already refuse. Both answered 0, and so did a successful append of
        // the first row of a fresh table, so the sole caller could not have checked this even
        // if it had tried.
        if (data.size() == 0) {
            return uint64_t{0};
        }
        if (!s) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"direct_append_sync: the owning agent holds no storage for oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      "; the replayed rows have nowhere to land",
                                                  resource()});
        }

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

        // WAL-replay only (txn{0,0}), single-threaded. A write_conflict / out_of_memory here
        // is a hard recovery fault: no rows materialized, and the record is a committed change
        // recovery declined to restore. It used to come back as 0 — indistinguishable from the
        // first row of a fresh table — behind a warn line; it travels the wrapper now.
        auto append_r = s->append(local, txn);
        if (append_r.has_error()) {
            error(log_,
                  "manager_disk_t::direct_append_sync: replay append failed for oid={} : {}",
                  static_cast<unsigned>(table_oid),
                  append_r.error().what.c_str());
            return core::error_on(resource(), append_r.error());
        }
        return append_r.value();
    }

    // THE THREE REPLAY ROUTERS. Each names the owning agent with pool_idx_for_oid and
    // forwards; a manager with no agents, or an empty agent slot, is a journalled change with
    // nowhere to land and is reported rather than dropped. See agent_disk_t's declarations.
    core::error_t manager_disk_t::direct_delete_sync(catalog::oid_t table_oid,
                                                     const std::pmr::vector<int64_t>& row_ids,
                                                     uint64_t count) {
        // Bootstrap / WAL-replay only; routes the physical delete to the owning agent
        // under transaction_data{0, 0} (DIRECT_WRITE_TXN_ID — replay carries no MVCC txn).
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"direct_delete_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"direct_delete_sync: owning disk agent is null", resource()}};
        }
        return agents_[pool_idx]->direct_delete_sync(table_oid,
                                                     row_ids,
                                                     count,
                                                     components::table::transaction_data{0, 0});
    }

    core::error_t manager_disk_t::direct_update_sync(catalog::oid_t table_oid,
                                                     const std::pmr::vector<int64_t>& row_ids,
                                                     components::vector::data_chunk_t& new_data) {
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"direct_update_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"direct_update_sync: owning disk agent is null", resource()}};
        }
        return agents_[pool_idx]->direct_update_sync(table_oid, row_ids, new_data);
    }

    core::error_t manager_disk_t::direct_add_column_sync(catalog::oid_t table_oid,
                                                         const components::vector::data_chunk_t& schema_chunk) {
        // Bootstrap / WAL-replay only; routes the schema-growth record to the owning
        // agent so the new columns exist before the dependent PHYSICAL_INSERT replays.
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"direct_add_column_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"direct_add_column_sync: owning disk agent is null", resource()}};
        }
        return agents_[pool_idx]->direct_add_column_sync(table_oid, schema_chunk);
    }

    // --- Storage management ---
    // Every site routes through agents_[pool_idx_for_oid(oid)] (storage_entry_sync
    // borrow or storage_*_inner mailbox handler). No manager-side storage_t* survives.

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_disk(session_id_t session,
                                        catalog::oid_t table_oid,
                                        catalog::oid_t database_oid,
                                        std::vector<components::table::column_definition_t> columns,
                                        bool is_computed) {
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
                                                                  std::move(otbx_path),
                                                                  is_computed);
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

    // EVERY DATA ROUTER BELOW SHARES ONE RULE, and it is the rule the three replay routers at
    // the top of this file and storage_delete_rows already state: a manager with no agents, or
    // an empty agent slot, is a request with NOWHERE TO LAND. Each used to answer that with its
    // own natural empty value — an empty type list, 0 rows, an empty chunk vector, a
    // zero-length append range, a drained cursor, an empty fold — every one of which is also
    // the correct answer to a real question about a real table. That is not routing; it is a
    // refusal wearing the shape of a legitimately empty table.
    //   storage_close_cursor is the ONE exemption and stays a no-op: releasing a cursor has no
    // result to report and no failure mode, because an unreachable cursor is already the state
    // the call is asking for.
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
    manager_disk_t::storage_types(session_id_t /*session*/, catalog::oid_t table_oid) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_types: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_types: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] =
            actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::storage_types_inner, table_oid);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    manager_disk_t::storage_total_rows(session_id_t /*session*/, catalog::oid_t table_oid) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_total_rows: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_total_rows: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] =
            actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::storage_total_rows_inner, table_oid);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<void> manager_disk_t::storage_close_cursor(session_id_t session,
                                                                            catalog::oid_t table_oid,
                                                                            uint64_t cursor_id) {
        // Transparent router (A4). Fire-and-forget by shape: releasing a cursor has no result
        // to report and no failure mode — an unknown id is already the desired state.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::storage_close_cursor_inner,
                                                                  session,
                                                                  table_oid,
                                                                  cursor_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_await std::move(fut);
        }
        co_return;
    }

    manager_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    manager_disk_t::storage_fetch_next_batch(session_id_t session,
                                             catalog::oid_t table_oid,
                                             uint64_t cursor_id,
                                             std::unique_ptr<components::table::table_filter_t> filter,
                                             int64_t limit,
                                             std::vector<size_t> projected_cols,
                                             components::table::transaction_data txn) {
        // Transparent router: the agent reply carries the batch + minted/advanced cursor_id
        // (and any scan_error); forward the wrapper unchanged so the scan source operator
        // turns it into an error cursor on has_error() and keeps the cursor id otherwise. The
        // session is forwarded so the agent can mint a (session, counter) cursor id (R16).
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_fetch_next_batch: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"storage_fetch_next_batch: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_fetch_next_batch_inner,
                                                              session,
                                                              table_oid,
                                                              cursor_id,
                                                              std::move(filter),
                                                              limit,
                                                              projected_cols,
                                                              txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    manager_disk_t::storage_reduce(session_id_t session,
                                   catalog::oid_t table_oid,
                                   std::unique_ptr<components::table::table_filter_t> filter,
                                   std::vector<size_t> projected_cols,
                                   components::table::transaction_data txn,
                                   components::operators::pushed_aggregate_spec_t spec) {
        // Transparent router: pool_idx_for_oid -> the owning agent's storage_reduce_inner,
        // forwarding the one-reply reduced result (or its error) unchanged.
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_reduce: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_reduce: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_reduce_inner,
                                                              session,
                                                              table_oid,
                                                              std::move(filter),
                                                              projected_cols,
                                                              txn,
                                                              std::move(spec));
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  catalog::oid_t table_oid,
                                  components::vector::vector_t row_ids,
                                  uint64_t count,
                                  std::vector<size_t> projected_cols,
                                  components::table::transaction_data txn,
                                  components::table::fetch_visibility_t visibility,
                                  int64_t limit) {
        // Nothing asked, nothing fetched — an empty request has an empty answer and needs no
        // route, exactly as on storage_delete_rows.
        if (count == 0) {
            co_return std::pmr::vector<components::vector::data_chunk_t>(resource());
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_fetch: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_fetch: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_fetch_inner,
                                                              table_oid,
                                                              row_ids,
                                                              count,
                                                              std::move(projected_cols),
                                                              std::move(txn),
                                                              visibility,
                                                              limit);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
    manager_disk_t::storage_append(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::pmr::vector<components::vector::data_chunk_t> data) {
        // The full preprocessing pipeline (schema adoption/growth, column
        // expansion, NOT NULL, dedup, type promotion) and the canonical write live
        // in the agent twin, so every same-oid access is serialized by the agent's
        // mailbox — no borrowed-pointer access from the manager loop thread. The agent
        // owns the WAL-first write; the chunks append sequentially through the same mailbox,
        // so the per-chunk segments stay contiguous and coalesce into one [range_start, total)
        // range. The agent reply wraps a write_conflict / out_of_memory; the first error aborts
        // the batch (the wrapper is forwarded unchanged so operator_insert surfaces it).
        // An append with no rows in it needs no route: a zero-length range is the honest
        // answer and the loop below would send nothing anyway.
        bool has_rows = false;
        for (const auto& chunk : data) {
            if (chunk.size() != 0) {
                has_rows = true;
                break;
            }
        }
        if (!has_rows) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_append: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_append: owning disk agent is null", resource()}};
        }
        uint64_t range_start = 0;
        uint64_t total_count = 0;
        bool have_range = false;
        for (auto& chunk : data) {
            if (chunk.size() == 0) {
                continue;
            }
            auto one = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::storage_append_inner,
                                                                  ctx,
                                                                  table_oid,
                                                                  std::move(one));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            auto append_r = co_await std::move(fut);
            if (append_r.has_error()) {
                co_return std::move(append_r);
            }
            auto [start_row, actual_count] = append_r.value();
            if (actual_count == 0) {
                continue;
            }
            if (!have_range) {
                range_start = start_row;
                have_range = true;
            }
            total_count += actual_count;
        }
        co_return std::make_pair(range_start, total_count);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::pmr::vector<components::vector::vector_t> row_ids,
                                   std::pmr::vector<components::vector::data_chunk_t> data) {
        // Router to the agent twin — the agent's mailbox serializes the canonical write with
        // every other same-oid access. row_ids[i] pairs with data[i]; the per-chunk new-row
        // segments are contiguous and coalesce into one range. The agent reply wraps a
        // write_conflict / out_of_memory; the first error aborts the batch.
        // Same rule as storage_append: an update carrying no rows needs no route.
        bool has_rows = false;
        for (const auto& chunk : data) {
            if (chunk.size() != 0) {
                has_rows = true;
                break;
            }
        }
        if (!has_rows) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_update: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_update: owning disk agent is null", resource()}};
        }
        int64_t range_start = 0;
        uint64_t total_count = 0;
        bool have_range = false;
        for (std::size_t i = 0; i < data.size(); ++i) {
            if (data[i].size() == 0) {
                continue;
            }
            auto one = std::make_unique<components::vector::data_chunk_t>(std::move(data[i]));
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::storage_update_inner,
                                                                  table_oid,
                                                                  std::move(row_ids[i]),
                                                                  std::move(one),
                                                                  ctx.txn);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            auto update_r = co_await std::move(fut);
            if (update_r.has_error()) {
                co_return std::move(update_r);
            }
            auto [upd_start, upd_count] = update_r.value();
            if (!have_range) {
                range_start = upd_start;
                have_range = true;
            }
            total_count += upd_count;
        }
        co_return std::pair<int64_t, uint64_t>{range_start, total_count};
    }

    // Router to the agent twin. The reply wraps the count: a route that does not exist is
    // a delete that did not happen, and reporting it as 0 rows deleted is indistinguishable
    // from a healthy delete whose rows were already stamped — the reading that let an
    // ON DELETE CASCADE drop nothing and still report success. Same rule, same shape as
    // scan_by_keys' routing legs.
    manager_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    manager_disk_t::storage_delete_rows(execution_context_t ctx,
                                        catalog::oid_t table_oid,
                                        components::vector::vector_t row_ids,
                                        uint64_t count) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_delete_rows: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_delete_rows: owning disk agent is null", resource()}};
        }
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

    manager_disk_t::unique_future<void> manager_disk_t::storage_revert_deletes(execution_context_t ctx,
                                                                               std::vector<catalog::oid_t> tables) {
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

} //namespace services::disk
