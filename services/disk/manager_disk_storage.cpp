#include "mailbox_payload.hpp"
#include "manager_disk_impl.hpp"

#include <cassert>
#include <components/vector/vector_builders.hpp>

namespace services::disk {

    static_assert(mailbox::owns_its_arguments<&manager_disk_t::storage_fetch_next_batch>);
    static_assert(mailbox::owns_its_arguments<&manager_disk_t::storage_offer>);
    static_assert(mailbox::owns_its_arguments<&manager_disk_t::storage_run_block>);
    static_assert(mailbox::owns_its_arguments<&agent_disk_t::storage_fetch_next_batch_inner>);
    static_assert(mailbox::owns_its_arguments<&agent_disk_t::storage_scan_inner>);
    static_assert(mailbox::owns_its_arguments<&agent_disk_t::storage_run_block_inner>);

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    namespace {
        core::error_t
        no_journalled_storage_error(std::pmr::memory_resource* resource, const char* who, catalog::oid_t table_oid) {
            std::pmr::string msg{who, resource};
            msg += std::pmr::string{": no storage on the owning agent for table oid ", resource};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource};
            msg += std::pmr::string{" — the journalled change was NOT applied", resource};
            return core::error_t{core::error_code_t::io_error, std::move(msg)};
        }
    } // namespace

    core::error_t apply_journalled_delete(std::pmr::memory_resource* resource,
                                          collection_storage_entry_t* entry,
                                          catalog::oid_t table_oid,
                                          const std::pmr::vector<int64_t>& row_ids,
                                          uint64_t count,
                                          const components::table::transaction_data& txn) {
        if (row_ids.empty() && count == 0) {
            return core::error_t::no_error();
        }
        if (static_cast<uint64_t>(row_ids.size()) != count) {
            std::pmr::string what{"apply_journalled_delete: the record counts ", resource};
            what.append(std::to_string(count).c_str());
            what.append(" row(s) but names ");
            what.append(std::to_string(row_ids.size()).c_str());
            what.append(" row id(s) for table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append(" — the journalled delete is NOT replayed");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        if (entry == nullptr || entry->storage == nullptr) {
            return no_journalled_storage_error(resource, "apply_journalled_delete", table_oid);
        }
        auto ids_batch = components::table::make_row_id_batch(resource, row_ids);
        // The storage refuses a row id that names no row group; the count-mismatch check below
        // stays for the case where it deleted fewer than journalled without refusing.
        VALUE_OR_RETURN(const uint64_t deleted, entry->storage->delete_rows(ids_batch, txn));
        if (deleted != count) {
            std::pmr::string what{"apply_journalled_delete: the storage deleted ", resource};
            what.append(std::to_string(deleted).c_str());
            what.append(" of ");
            what.append(std::to_string(count).c_str());
            what.append(" journalled row(s) for table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append(" — the rest are NOT replayed");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<components::storage::appended_range_t>
    apply_journalled_update(std::pmr::memory_resource* resource,
                            collection_storage_entry_t* entry,
                            catalog::oid_t table_oid,
                            const std::pmr::vector<int64_t>& row_ids,
                            const std::pmr::vector<catalog::oid_t>& attoids,
                            components::vector::data_chunk_t& new_data,
                            components::table::transaction_data txn) {
        if (row_ids.empty() && new_data.size() == 0) {
            return components::storage::appended_range_t{};
        }
        if (row_ids.size() != static_cast<std::size_t>(new_data.size())) {
            std::pmr::string what{"apply_journalled_update: the record carries ", resource};
            what.append(std::to_string(new_data.size()).c_str());
            what.append(" row(s) but names ");
            what.append(std::to_string(row_ids.size()).c_str());
            what.append(" row id(s) for table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append(" — the journalled update is NOT replayed");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        if (entry == nullptr || entry->storage == nullptr) {
            return no_journalled_storage_error(resource, "apply_journalled_update", table_oid);
        }
        const auto count = static_cast<uint64_t>(row_ids.size());
        components::vector::vector_t ids_vec(
            resource,
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, row_ids[i]);
        }
        components::vector::data_chunk_t local(resource, new_data.types(), new_data.size());
        new_data.copy(local, 0);

        if (!entry->storage->columns().empty() && local.column_count() > 0) {
            const auto& table = entry->table_storage.table();
            if (auto widened =
                    entry->is_computed ? table.widen_by_name(txn, local) : table.widen_by_attoid(attoids, local);
                widened.contains_error()) {
                return widened;
            }
        }
        return entry->storage->update(ids_vec, local, txn);
    }

    core::error_t apply_journalled_add_columns(std::pmr::memory_resource* resource,
                                               collection_storage_entry_t* entry,
                                               catalog::oid_t table_oid,
                                               const std::pmr::vector<catalog::oid_t>& attoids,
                                               const components::vector::data_chunk_t& schema_chunk,
                                               uint64_t first_position) {
        if (entry == nullptr || entry->storage == nullptr) {
            return no_journalled_storage_error(resource, "apply_journalled_add_columns", table_oid);
        }
        for (uint64_t col = 0; col < schema_chunk.column_count(); ++col) {
            uint64_t stored = entry->table_storage.table().column_count();
            if (!entry->is_computed) {
                const auto& columns = entry->table_storage.table().columns();
                bool present = std::any_of(columns.begin(), columns.end(), [&](const auto& column) {
                    return column.attoid() == attoids[col];
                });
                if (present) {
                    continue;
                }
            }
            uint64_t position = entry->is_computed ? first_position + col : stored;
            if (position < stored) {
                continue;
            }
            if (position > stored) {
                std::pmr::string what{"apply_journalled_add_columns: table oid ", resource};
                what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                what.append(" holds ");
                what.append(std::to_string(stored).c_str());
                what.append(" column(s), the journalled ADD COLUMN lands at position ");
                what.append(std::to_string(position).c_str());
                what.append(" — a column in between was never journalled; the ADD is NOT replayed");
                return core::error_t{core::error_code_t::data_corruption, std::move(what)};
            }
            const auto& ctype = schema_chunk.data[col].type();
            std::optional<components::vector::vector_t> default_value;
            if (schema_chunk.size() > 0 && !schema_chunk.data[col].is_null(0)) {
                default_value = components::vector::make_constant_copy(resource, schema_chunk.data[col], 0);
            }
            components::table::column_definition_t def(ctype.has_alias() ? std::string(ctype.alias()) : std::string{},
                                                       ctype,
                                                       std::move(default_value));
            def.set_attoid(attoids[col]);
            entry->add_column(def, resource);
            if (entry->storage == nullptr) {
                return no_journalled_storage_error(resource, "apply_journalled_add_columns", table_oid);
            }
            if (entry->table_storage.table().has_construction_error()) {
                return core::error_on(resource, entry->table_storage.table().construction_error());
            }
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<uint64_t> manager_disk_t::append_sync(const bootstrap_t& boot,
                                                                 catalog::oid_t table_oid,
                                                                 const std::pmr::vector<catalog::oid_t>& attoids,
                                                                 components::vector::data_chunk_t& data,
                                                                 components::table::transaction_data txn) {
        // The borrowed entry is safe here: bootstrap and replay are single-threaded.
        auto* entry = boot_entry(boot, table_oid);
        // Empty chunk is the legit no-op; missing storage is a refusal, not 0 -- 0 means "first row".
        if (data.size() == 0) {
            return uint64_t{0};
        }
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"append_sync: the owning agent holds no storage for oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      "; the replayed rows have nowhere to land",
                                                  resource()});
        }

        auto* s = entry->storage.get();
        auto local = rebuild_chunk(resource(), data);

        if (!s->has_schema() && local.column_count() > 0) {
            s->adopt_schema(local.types());
        }

        if (!s->columns().empty() && local.column_count() > 0) {
            const auto& table = entry->table_storage.table();
            if (auto widened =
                    entry->is_computed ? table.widen_by_name(txn, local) : table.widen_by_attoid(attoids, local);
                widened.contains_error()) {
                return widened;
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

    core::error_t manager_disk_t::commit_append_sync(const bootstrap_t& boot,
                                                     catalog::oid_t table_oid,
                                                     uint64_t commit_id,
                                                     int64_t row_start,
                                                     uint64_t count) {
        auto* agent_entry = boot_entry(boot, table_oid);
        if (agent_entry == nullptr || agent_entry->storage == nullptr) {
            std::pmr::string what{"commit_append_sync: the owning agent holds no storage for oid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append("; the appended rows keep their pending stamps");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        agent_entry->storage->commit_append(commit_id, row_start, count);
        return core::error_t::no_error();
    }

    core::error_t manager_disk_t::delete_sync(const bootstrap_t& boot,
                                              catalog::oid_t table_oid,
                                              const std::pmr::vector<int64_t>& row_ids,
                                              uint64_t count,
                                              components::table::transaction_data txn) {
        return apply_journalled_delete(resource(), boot_entry(boot, table_oid), table_oid, row_ids, count, txn);
    }

    core::error_t manager_disk_t::commit_all_deletes_sync(const bootstrap_t& boot,
                                                          catalog::oid_t table_oid,
                                                          uint64_t txn_id,
                                                          uint64_t commit_id) {
        auto* agent_entry = boot_entry(boot, table_oid);
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
    manager_disk_t::update_sync(const bootstrap_t& boot,
                                catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                const std::pmr::vector<catalog::oid_t>& attoids,
                                components::vector::data_chunk_t& new_data,
                                components::table::transaction_data txn) {
        return apply_journalled_update(resource(),
                                       boot_entry(boot, table_oid),
                                       table_oid,
                                       row_ids,
                                       attoids,
                                       new_data,
                                       txn);
    }

    core::error_t manager_disk_t::direct_add_column_sync(const bootstrap_t& boot,
                                                         catalog::oid_t table_oid,
                                                         const std::pmr::vector<catalog::oid_t>& attoids,
                                                         const components::vector::data_chunk_t& schema_chunk,
                                                         uint64_t first_position) {
        return apply_journalled_add_columns(resource(),
                                            boot_entry(boot, table_oid),
                                            table_oid,
                                            attoids,
                                            schema_chunk,
                                            first_position);
    }

    // Every site routes through agents_[pool_idx_for_oid(oid)]; no manager-side storage_t* survives.

    manager_disk_t::unique_future<core::result_wrapper_t<ddl_reply_t>>
    manager_disk_t::create_storage_disk(execution_context_t ctx,
                                        catalog::oid_t table_oid,
                                        catalog::oid_t database_oid,
                                        qualified_name_t name,
                                        std::vector<components::table::column_definition_t> columns,
                                        bool is_computed) {
        trace(log_,
              "manager_disk_t::create_storage_disk , session : {} , oid : {}",
              ctx.session.data(),
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
                                                                  is_computed,
                                                                  std::move(name.collection.t));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            const auto created = co_await std::move(fut);
            if (created.has_error()) {
                co_return created.error();
            }
            if (!created.value()) {
                trace(log_,
                      "manager_disk_t::create_storage_disk: agent[{}] already owns oid {}",
                      pool_idx,
                      static_cast<unsigned>(table_oid));
            }
        }
        co_return ddl_reply_t{true, host_rows_t{}};
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
        auto types = co_await std::move(fut);
        if (types.has_error()) {
            co_return types.error();
        }
        co_return std::move(types.value());
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

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_close_cursor(session_id_t session, catalog::oid_t table_oid, uint64_t cursor_id) {
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
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_open_scan_hold_inner,
                                                              session,
                                                              table_oid);
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
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_compact_epoch_inner,
                                                              session,
                                                              table_oid);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::error_t>
    manager_disk_t::storage_set_not_null(execution_context_t ctx, catalog::oid_t table_oid, catalog::oid_t attoid) {
        const std::size_t pool_idx = agents_.empty() ? 0 : pool_idx_for_oid(table_oid, agents_.size());
        if (agents_.empty() || agents_[pool_idx] == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_set_not_null: no disk agent owns the table", resource()}};
        }
        auto& agent = agents_[pool_idx];
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_set_not_null_inner,
                                                              ctx.txn,
                                                              table_oid,
                                                              attoid);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    manager_disk_t::storage_fetch_next_batch(session_id_t session,
                                             catalog::oid_t table_oid,
                                             uint64_t cursor_id,
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
                                                              limit,
                                                              projected_cols,
                                                              txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  catalog::oid_t table_oid,
                                  components::table::row_id_batch_t row_ids,
                                  std::vector<size_t> projected_cols,
                                  components::table::transaction_data txn,
                                  components::table::fetch_visibility_t visibility,
                                  int64_t limit,
                                  uint64_t expected_compact_epoch) {
        if (row_ids.empty()) {
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
                                                              std::move(row_ids),
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

    manager_disk_t::unique_future<core::result_wrapper_t<write_receipt_t>>
    manager_disk_t::storage_append(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::pmr::vector<catalog::oid_t> attoids,
                                   std::pmr::vector<components::vector::data_chunk_t> data,
                                   bool returning) {
        write_receipt_t receipt{0, 0, true, std::pmr::vector<components::vector::data_chunk_t>{resource()}};
        bool has_rows = false;
        for (const auto& chunk : data) {
            if (chunk.size() != 0) {
                has_rows = true;
                break;
            }
        }
        if (!has_rows) {
            co_return std::move(receipt);
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
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_append_inner,
                                                              ctx,
                                                              table_oid,
                                                              std::move(attoids),
                                                              std::move(data),
                                                              returning);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<update_receipt_t>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::pmr::vector<components::vector::vector_t> row_ids,
                                   std::pmr::vector<components::vector::data_chunk_t> data,
                                   bool returning) {
        bool has_rows = false;
        for (const auto& chunk : data) {
            if (chunk.size() != 0) {
                has_rows = true;
                break;
            }
        }
        update_receipt_t result{
            write_receipt_t{0, 0, true, std::pmr::vector<components::vector::data_chunk_t>{resource()}},
            std::pmr::vector<components::vector::data_chunk_t>{resource()},
            std::pmr::vector<std::uint32_t>{resource()}};
        if (!has_rows) {
            co_return std::move(result);
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
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_update_inner,
                                                              table_oid,
                                                              std::move(row_ids),
                                                              std::move(data),
                                                              ctx.txn,
                                                              returning);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    // The reply wraps the count: a route that doesn't exist is a delete that DID NOT HAPPEN, not 0 rows.
    manager_disk_t::unique_future<core::result_wrapper_t<write_receipt_t>>
    manager_disk_t::storage_delete_rows(execution_context_t ctx,
                                        catalog::oid_t table_oid,
                                        components::table::row_id_batch_t row_ids,
                                        bool returning) {
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
                                                              ctx.txn,
                                                              returning);
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
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
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
                                                                      std::move(per_agent[i]));
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
                                                                               uint64_t from_sub_id,
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
                                                                      from_sub_id,
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

} // namespace services::disk
