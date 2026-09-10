#include "expand_chunk.hpp"
#include "manager_disk_impl.hpp"

#include <cassert>

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    core::result_wrapper_t<uint64_t> manager_disk_t::append_sync(catalog::oid_t table_oid,
                                                                 components::vector::data_chunk_t& data,
                                                                 components::table::transaction_data txn) {
        // storage_entry_sync's borrow is safe here: bootstrap and replay are single-threaded.
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
        // Empty chunk is the legit no-op; missing storage is a refusal, not 0 -- 0 means "first row".
        if (data.size() == 0) {
            return uint64_t{0};
        }
        if (!s) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"append_sync: the owning agent holds no storage for oid " +
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
            if (auto expanded = detail::expand_chunk_to_columns(resource(),
                                                                table_oid,
                                                                table_columns,
                                                                local,
                                                                /*is_computed=*/false);
                expanded.contains_error()) {
                return expanded;
            }
        }

        auto append_r = s->append(local, txn);
        if (append_r.has_error()) {
            error(log_,
                  "manager_disk_t::append_sync: append failed for oid={} : {}",
                  static_cast<unsigned>(table_oid),
                  append_r.error().what.c_str());
            return core::error_on(resource(), append_r.error());
        }
        return append_r.value();
    }

    core::error_t manager_disk_t::commit_append_sync(catalog::oid_t table_oid,
                                                     uint64_t commit_id,
                                                     int64_t row_start,
                                                     uint64_t count) {
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"commit_append_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"commit_append_sync: owning disk agent is null", resource()}};
        }
        const auto* agent_entry = agents_[pool_idx]->storage_entry_sync(table_oid);
        if (agent_entry == nullptr || agent_entry->storage == nullptr) {
            std::pmr::string what{"commit_append_sync: the owning agent holds no storage for oid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append("; the appended rows keep their pending stamps");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        agent_entry->storage->commit_append(commit_id, row_start, count);
        return core::error_t::no_error();
    }

    core::error_t manager_disk_t::delete_sync(catalog::oid_t table_oid,
                                              const std::pmr::vector<int64_t>& row_ids,
                                              uint64_t count,
                                              components::table::transaction_data txn) {
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"delete_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"delete_sync: owning disk agent is null", resource()}};
        }
        return agents_[pool_idx]->delete_sync(table_oid, row_ids, count, txn);
    }

    core::error_t manager_disk_t::commit_all_deletes_sync(catalog::oid_t table_oid,
                                                          uint64_t txn_id,
                                                          uint64_t commit_id) {
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"commit_all_deletes_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"commit_all_deletes_sync: owning disk agent is null", resource()}};
        }
        const auto* agent_entry = agents_[pool_idx]->storage_entry_sync(table_oid);
        if (agent_entry == nullptr || agent_entry->storage == nullptr) {
            std::pmr::string what{"commit_all_deletes_sync: the owning agent holds no storage for oid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append("; the delete marks keep their pending stamps");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        agent_entry->storage->commit_all_deletes(txn_id, commit_id);
        return core::error_t::no_error();
    }

    core::result_wrapper_t<components::storage::appended_range_t>
    manager_disk_t::update_sync(catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                components::vector::data_chunk_t& new_data,
                                components::table::transaction_data txn) {
        if (agents_.empty()) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"update_sync: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr) {
            return core::error_t{core::error_code_t::io_error,
                                 std::pmr::string{"update_sync: owning disk agent is null", resource()}};
        }
        return agents_[pool_idx]->update_sync(table_oid, row_ids, new_data, txn);
    }


    core::error_t manager_disk_t::direct_add_column_sync(catalog::oid_t table_oid,
                                                         const components::vector::data_chunk_t& schema_chunk) {
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

    // Every site routes through agents_[pool_idx_for_oid(oid)]; no manager-side storage_t* survives.

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
        // Partitions oids per owning agent and fans out in PARALLEL: costs at most num_agents round-trips, not N.
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

    manager_disk_t::unique_future<void> manager_disk_t::storage_close_cursor(session_id_t session,
                                                                            catalog::oid_t table_oid,
                                                                            uint64_t cursor_id) {
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

    manager_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    manager_disk_t::storage_open_scan_hold(session_id_t session, catalog::oid_t table_oid) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_open_scan_hold: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_open_scan_hold: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] =
            actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::storage_open_scan_hold_inner, session, table_oid);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    manager_disk_t::storage_compact_epoch(session_id_t session, catalog::oid_t table_oid) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_compact_epoch: no disk agents", resource()}};
        }
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[pool_idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_compact_epoch: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] =
            actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::storage_compact_epoch_inner, session, table_oid);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    manager_disk_t::storage_fetch_next_batch(session_id_t session,
                                             catalog::oid_t table_oid,
                                             uint64_t cursor_id,
                                             std::unique_ptr<components::table::table_filter_t> filter,
                                             int64_t limit,
                                             std::vector<size_t> projected_cols,
                                             components::table::transaction_data txn) {
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
                                  int64_t limit,
                                  uint64_t expected_compact_epoch) {
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
                                                              limit,
                                                              expected_compact_epoch);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<components::storage::appended_range_t>>
    manager_disk_t::storage_append(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::pmr::vector<components::vector::data_chunk_t> data) {
        bool has_rows = false;
        for (const auto& chunk : data) {
            if (chunk.size() != 0) {
                has_rows = true;
                break;
            }
        }
        if (!has_rows) {
            co_return components::storage::appended_range_t{};
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
        int64_t range_start = 0;
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
            auto appended = append_r.value();
            if (appended.count == 0) {
                continue;
            }
            if (!have_range) {
                range_start = appended.start_row;
                have_range = true;
            }
            total_count += appended.count;
        }
        co_return components::storage::appended_range_t{range_start, total_count};
    }

    manager_disk_t::unique_future<core::result_wrapper_t<components::storage::appended_range_t>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::pmr::vector<components::vector::vector_t> row_ids,
                                   std::pmr::vector<components::vector::data_chunk_t> data) {
        bool has_rows = false;
        for (const auto& chunk : data) {
            if (chunk.size() != 0) {
                has_rows = true;
                break;
            }
        }
        if (!has_rows) {
            co_return components::storage::appended_range_t{};
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
            auto upd = update_r.value();
            if (!have_range) {
                range_start = upd.start_row;
                have_range = true;
            }
            total_count += upd.count;
        }
        co_return components::storage::appended_range_t{range_start, total_count};
    }

    // The reply wraps the count: a route that doesn't exist is a delete that DID NOT HAPPEN, not 0 rows.
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

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_publish_commits(execution_context_t /*ctx*/,
                                            uint64_t commit_id,
                                            std::vector<components::pg_catalog_append_range_t> ranges) {
        // A partition miss means the OWNER has no storage (see report_publish_revert_miss in agent_disk.cpp).
        if (!agents_.empty()) {
            // emplace_back() with no args still yields vector(alloc) via libc++'s uses-allocator construction.
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

    manager_disk_t::unique_future<core::error_t>
    manager_disk_t::storage_revert_appends(execution_context_t /*ctx*/,
                                           std::vector<components::pg_catalog_append_range_t> ranges,
                                           bool tail_only) {
        auto first_error = core::error_t::no_error();
        // Each agent's inner handler reverse-iterates to unwind in the opposite of append order.
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
            std::pmr::vector<unique_future<core::error_t>> agent_futures{resource()};
            agent_futures.reserve(per_agent.size());
            for (std::size_t i = 0; i < per_agent.size(); ++i) {
                if (per_agent[i].empty())
                    continue;
                auto& agent = agents_[i];
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::storage_revert_appends_inner,
                                                                      std::move(per_agent[i]),
                                                                      tail_only);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                agent_futures.emplace_back(std::move(fut));
            }
            for (auto& f : agent_futures) {
                auto agent_error = co_await std::move(f);
                if (agent_error.contains_error() && !first_error.contains_error()) {
                    first_error = agent_error;
                }
            }
        }
        co_return first_error;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_revert_deletes(execution_context_t ctx,
                                                                               std::vector<catalog::oid_t> tables) {
        // Un-stamps this txn's pending deletes back to NOT_DELETED_ID (revert_all_deletes), not a commit_id.
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

}
