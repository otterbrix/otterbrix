#include "agent_disk.hpp"
#include "inline_scan.hpp"
#include "manager_disk.hpp"
#include <algorithm>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/physical_plan/operators/operator_hash_group.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/physical_plan_generator/impl/create_plan_group.hpp>
#include <components/table/pushed_filter.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/file/local_file_system.hpp>
#include <cstring>
#include <services/dispatcher/dispatcher.hpp>

namespace services::disk {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_table_checkpoints{0};
        std::atomic<uint64_t> g_publish_revert_misses{0};
        std::atomic<uint64_t> g_checkpoint_entries_deferred{0};
        std::atomic<uint64_t> g_checkpoint_entries_rewritten{0};
    } // namespace

    uint64_t table_checkpoints() noexcept { return g_table_checkpoints.load(std::memory_order_relaxed); }
    void reset_table_checkpoints() noexcept { g_table_checkpoints.store(0, std::memory_order_relaxed); }
    uint64_t publish_revert_misses() noexcept { return g_publish_revert_misses.load(std::memory_order_relaxed); }
    void reset_publish_revert_misses() noexcept { g_publish_revert_misses.store(0, std::memory_order_relaxed); }
    uint64_t checkpoint_entries_deferred() noexcept {
        return g_checkpoint_entries_deferred.load(std::memory_order_relaxed);
    }
    uint64_t checkpoint_entries_rewritten() noexcept {
        return g_checkpoint_entries_rewritten.load(std::memory_order_relaxed);
    }
    void reset_checkpoint_entry_tallies() noexcept {
        g_checkpoint_entries_deferred.store(0, std::memory_order_relaxed);
        g_checkpoint_entries_rewritten.store(0, std::memory_order_relaxed);
    }

    namespace {
        scan_advance_gate_t* g_scan_advance_gate = nullptr;
    } // namespace
    void dev_set_scan_advance_gate(scan_advance_gate_t* gate) { g_scan_advance_gate = gate; }
    scan_advance_gate_t* dev_scan_advance_gate() { return g_scan_advance_gate; }
#endif

    using namespace core::filesystem;

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_disk_block_rows{0};
    } // namespace
    uint64_t disk_block_rows() noexcept { return g_disk_block_rows.load(std::memory_order_relaxed); }
    void reset_disk_block_rows() noexcept { g_disk_block_rows.store(0, std::memory_order_relaxed); }

    namespace {
        std::atomic<uint64_t> g_catalog_key_scans{0};
    } // namespace
    uint64_t catalog_key_scans() noexcept { return g_catalog_key_scans.load(std::memory_order_relaxed); }
    void reset_catalog_key_scans() noexcept { g_catalog_key_scans.store(0, std::memory_order_relaxed); }
#endif

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource, const path_t& path_db, log_t& log)
        : agent_disk_t(resource, path_db, log, agent_role_t::CATALOG, 0) {}

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource,
                               const path_t& path_db,
                               log_t& log,
                               agent_role_t role,
                               std::size_t pool_idx)
        : actor_zeta::basic_actor<agent_disk_t>(resource)
        , log_(log.clone())
        , path_(path_db)
        , pool_idx_(pool_idx)
        , storages_(resource)
        , active_scans_(resource)
        , block_pages_(resource)
        , dropped_storages_(resource) {
        trace(log_,
              "agent_disk::create (role={}, pool_idx={})",
              role == agent_role_t::CATALOG ? "CATALOG" : "USER_POOL",
              pool_idx);
        // The engine factory creates the directory before it spawns the managers.
        assert([this] {
            std::error_code ec;
            return std::filesystem::is_directory(path_, ec);
        }());
    }

    agent_disk_t::~agent_disk_t() { trace(log_, "delete agent_disk_t"); }

    bool agent_disk_t::has_storage_sync(components::catalog::oid_t oid) const noexcept {
        return storages_.find(oid) != storages_.end();
    }

    const collection_storage_entry_t* agent_disk_t::storage_entry_sync(components::catalog::oid_t oid) const noexcept {
        auto it = storages_.find(oid);
        if (it == storages_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    bool agent_disk_t::bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                                 const std::filesystem::path& otbx_path,
                                                 wal::id_t sidecar_wal_id,
                                                 bool sidecar_readable,
                                                 std::vector<components::table::column_definition_t> catalog_columns,
                                                 bool is_computed) noexcept {
        // Probed before construction: open-then-close on a duplicate would release the live entry's WRITE_LOCK.
        if (storages_.find(oid) != storages_.end()) {
            trace(log_,
                  "agent_disk_t::bootstrap_disk_inner_sync: agent[{}] oid {} already in slice — drop "
                  "incoming load (path={})",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
            return false;
        }
        trace(log_,
              "agent_disk_t::bootstrap_disk_inner_sync: agent[{}] load oid={} path={} sidecar_wal_id={} readable={}",
              pool_idx_,
              static_cast<unsigned>(oid),
              otbx_path.string(),
              static_cast<uint64_t>(sidecar_wal_id),
              sidecar_readable);
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), otbx_path, catalog_columns, is_computed);
        if (entry->table_storage.construction_failed()) {
            warn(log_,
                 "agent_disk_t::bootstrap_disk_inner_sync: agent[{}] load oid={} path={} failed: {}",
                 pool_idx_,
                 static_cast<unsigned>(oid),
                 otbx_path.string(),
                 entry->table_storage.construction_error().what.c_str());
            return false;
        }
        if (!sidecar_readable) {
            // A default 0 would misread as "never checkpointed, replay everything", so carry "unknown".
            entry->table_storage.set_checkpoint_wal_id_unreadable();
        } else if (sidecar_wal_id > wal::id_t{0}) {
            entry->table_storage.set_checkpoint_wal_id(sidecar_wal_id);
        }
        entry->adopt_catalog_columns(catalog_columns, resource());
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    core::result_wrapper_t<bool>
    agent_disk_t::bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                                   std::vector<components::table::column_definition_t> columns,
                                                   const std::filesystem::path& otbx_path,
                                                   bool is_computed) noexcept {
        if (storages_.find(oid) != storages_.end()) {
            trace(log_,
                  "agent_disk_t::bootstrap_create_disk_inner_sync: agent[{}] oid {} already in slice — drop "
                  "incoming create (path={})",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
            return false;
        }
        trace(log_,
              "agent_disk_t::bootstrap_create_disk_inner_sync: agent[{}] create oid={} path={}",
              pool_idx_,
              static_cast<unsigned>(oid),
              otbx_path.string());
        auto entry =
            std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path, is_computed);
        if (entry->table_storage.construction_failed()) {
            warn(log_,
                 "agent_disk_t::bootstrap_create_disk_inner_sync: agent[{}] create oid={} path={} failed: {}",
                 pool_idx_,
                 static_cast<unsigned>(oid),
                 otbx_path.string(),
                 entry->table_storage.construction_error().what.c_str());
            return core::error_on(resource(), entry->table_storage.construction_error());
        }
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<bool>>
    agent_disk_t::create_storage_disk_inner(components::catalog::oid_t oid,
                                            std::vector<components::table::column_definition_t> columns,
                                            std::filesystem::path otbx_path,
                                            bool is_computed,
                                            std::string relname) {
        std::error_code ec;
        std::filesystem::create_directories(otbx_path.parent_path(), ec);
        if (ec) {
            warn(log_,
                 "agent_disk[{}]::create_storage_disk_inner: oid {} create_directories {} failed: {}",
                 pool_idx_,
                 static_cast<unsigned>(oid),
                 otbx_path.parent_path().string(),
                 ec.message());
            std::pmr::string what{"agent_disk::create_storage_disk_inner: oid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(oid)).c_str());
            what.append(": create_directories ");
            what.append(otbx_path.parent_path().string().c_str());
            what.append(" failed: ");
            what.append(ec.message().c_str());
            co_return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        auto created = bootstrap_create_disk_inner_sync(oid, std::move(columns), otbx_path, is_computed);
        if (!created.has_error() && created.value() && !relname.empty()) {
            storages_[oid]->table_storage.table().set_table_name(std::move(relname));
        }
        if (!created.has_error() && !created.value()) {
            trace(log_,
                  "agent_disk[{}]::create_storage_disk_inner: oid {} already owned (path={}) — duplicate",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
        }
        co_return created;
    }

    core::error_t agent_disk_t::no_replay_storage_error(const char* who, components::catalog::oid_t table_oid) {
        std::pmr::string msg{"agent_disk::", resource()};
        msg += std::pmr::string{who, resource()};
        msg += std::pmr::string{": no storage on the owning agent for table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        msg += std::pmr::string{" — the journalled change was NOT replayed", resource()};
        return core::error_t{core::error_code_t::io_error, std::move(msg)};
    }

    core::error_t agent_disk_t::delete_sync(components::catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count,
                                            const components::table::transaction_data& txn) {
        if (row_ids.empty() && count == 0) {
            return core::error_t::no_error();
        }
        if (static_cast<uint64_t>(row_ids.size()) != count) {
            std::pmr::string what{"agent_disk::delete_sync: the record counts ", resource()};
            what.append(std::to_string(count).c_str());
            what.append(" row(s) but names ");
            what.append(std::to_string(row_ids.size()).c_str());
            what.append(" row id(s) for table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append(" — the journalled delete is NOT replayed");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            return no_replay_storage_error("delete_sync", table_oid);
        }
        auto& entry = it->second;
        auto ids_batch = components::table::make_row_id_batch(resource(), row_ids);
        // The storage refuses a row id that names no row group; the count-mismatch check below
        // stays for the case where it deleted fewer than journalled without refusing.
        VALUE_OR_RETURN(const uint64_t deleted, entry->storage->delete_rows(ids_batch, txn));
        if (deleted != count) {
            std::pmr::string what{"agent_disk::delete_sync: the storage deleted ", resource()};
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
    agent_disk_t::update_sync(components::catalog::oid_t table_oid,
                              const std::pmr::vector<int64_t>& row_ids,
                              const std::pmr::vector<components::catalog::oid_t>& attoids,
                              components::vector::data_chunk_t& new_data,
                              components::table::transaction_data txn) {
        if (row_ids.empty() && new_data.size() == 0) {
            return components::storage::appended_range_t{};
        }
        if (row_ids.size() != static_cast<std::size_t>(new_data.size())) {
            std::pmr::string what{"agent_disk::update_sync: the record carries ", resource()};
            what.append(std::to_string(new_data.size()).c_str());
            what.append(" row(s) but names ");
            what.append(std::to_string(row_ids.size()).c_str());
            what.append(" row id(s) for table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append(" — the journalled update is NOT replayed");
            return core::error_t{core::error_code_t::io_error, std::move(what)};
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            return no_replay_storage_error("update_sync", table_oid);
        }
        auto& entry = it->second;
        const auto count = static_cast<uint64_t>(row_ids.size());
        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, row_ids[i]);
        }
        components::vector::data_chunk_t local(resource(), new_data.types(), new_data.size());
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

    core::error_t agent_disk_t::direct_add_column_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<components::catalog::oid_t>& attoids,
                                                       const components::vector::data_chunk_t& schema_chunk,
                                                       uint64_t first_position) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            return no_replay_storage_error("direct_add_column_sync", table_oid);
        }
        auto& entry = it->second;
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
                std::pmr::string what{"direct_add_column_sync: table oid ", resource()};
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
                default_value = components::vector::make_constant_copy(resource(), schema_chunk.data[col], 0);
            }
            components::table::column_definition_t def(ctype.has_alias() ? std::string(ctype.alias()) : std::string{},
                                                       ctype,
                                                       std::move(default_value));
            def.set_attoid(attoids[col]);
            entry->add_column(def, resource());
            if (entry->storage == nullptr) {
                return no_replay_storage_error("direct_add_column_sync", table_oid);
            }
            if (entry->table_storage.table().has_construction_error()) {
                return core::error_on(resource(), entry->table_storage.table().construction_error());
            }
        }
        return core::error_t::no_error();
    }

    actor_zeta::behavior_t agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_append_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_append_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_publish_commits_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_publish_commits_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_publish_deletes_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_publish_deletes_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_revert_deletes_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_revert_deletes_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_revert_appends_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_revert_appends_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_update_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_update_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_delete_rows_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_delete_rows_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_fetch_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_fetch_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_close_cursor_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_close_cursor_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_fetch_next_batch_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_fetch_next_batch_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::scan_by_keys_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::scan_by_keys_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::read_chunks_by_key_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::read_chunks_by_key_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::read_chunks_by_keys_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::read_chunks_by_keys_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_types_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_types_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_total_rows_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_total_rows_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::checkpoint_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::checkpoint_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::vacuum_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::vacuum_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::maybe_cleanup_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::maybe_cleanup_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::on_horizon_advanced_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::on_horizon_advanced_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_dropped_committed_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_dropped_committed_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_drop_aborted_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_drop_aborted_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::drop_storage_many_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::drop_storage_many_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_pg_catalog_row_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::append_pg_catalog_row_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::delete_pg_catalog_rows_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::delete_pg_catalog_rows_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::update_pg_attribute_commit_id_field_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::update_pg_attribute_commit_id_field_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::compact_relkind_g_storage_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::compact_relkind_g_storage_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::drop_storage_column_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::drop_storage_column_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::rename_storage_column_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::rename_storage_column_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::add_storage_column_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::add_storage_column_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::stamp_column_dropped_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::stamp_column_dropped_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::publish_column_stamps_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::publish_column_stamps_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::mark_storage_dropped_many_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::mark_storage_dropped_many_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::create_storage_disk_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::create_storage_disk_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_open_scan_hold_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_open_scan_hold_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_compact_epoch_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_compact_epoch_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::revert_column_stamps_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::revert_column_stamps_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_prepare_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_prepare_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_release_prepared_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_release_prepared_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_set_not_null_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_set_not_null_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_run_block_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_run_block_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_block_page_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_block_page_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_close_block_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_close_block_inner, msg);
                break;
            }
            default:
                break;
        }
    }

    agent_disk_t::unique_future<core::result_wrapper_t<write_receipt_t>>
    agent_disk_t::storage_append_inner(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::pmr::vector<components::catalog::oid_t> attoids,
                                       std::pmr::vector<components::vector::data_chunk_t> data,
                                       bool returning) {
        const auto txn = ctx.txn;
        write_receipt_t receipt{0, 0, true, std::pmr::vector<components::vector::data_chunk_t>{resource()}};
        data.erase(std::remove_if(data.begin(),
                                  data.end(),
                                  [](const components::vector::data_chunk_t& chunk) { return chunk.size() == 0; }),
                   data.end());
        if (data.empty()) {
            co_return std::move(receipt);
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_append: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            std::pmr::string what{"storage_append: table oid has an empty entry on its disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto* s = entry->storage.get();
        if (s == nullptr) {
            std::pmr::string what{"storage_append: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }

        // No same-oid append can interleave: cooperative_actor won't pop the next message mid-await.
        std::vector<components::table::column_definition_t> wal_added_columns;

        const bool is_computed_table = entry->is_computed;

        if (!s->has_schema() && data.front().column_count() > 0) {
            s->adopt_schema(data.front().types());
        }

        if (is_computed_table && s->has_schema()) {
            std::vector<components::table::column_definition_t> new_columns;
            for (const auto& chunk : data) {
                for (uint64_t col = 0; col < chunk.column_count(); col++) {
                    if (!chunk.data[col].type().has_alias()) {
                        continue;
                    }
                    const auto alias = chunk.data[col].type().alias();
                    const auto ctype = chunk.data[col].type().type();
                    bool present = false;
                    for (const auto& tc : s->columns()) {
                        if (tc.name() == alias && tc.type().type() == ctype) {
                            present = true;
                            break;
                        }
                    }
                    for (const auto& added : new_columns) {
                        if (added.name() == alias && added.type().type() == ctype) {
                            present = true;
                            break;
                        }
                    }
                    if (!present) {
                        auto ct = chunk.data[col].type();
                        ct.set_alias(alias);
                        new_columns.emplace_back(alias, ct);
                    }
                }
            }
            if (!new_columns.empty()) {
                for (auto& col : new_columns) {
                    entry->add_column(col, resource());
                    wal_added_columns.push_back(col);
                }
                s = entry->storage.get();
                if (!s) {
                    std::pmr::string what{"storage_append: the adapter rebuild after schema growth left no "
                                          "storage for table oid ",
                                          resource()};
                    what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                    co_return core::error_t{core::error_code_t::io_error, std::move(what)};
                }
            }
        }

        // Every chunk is checked and widened before any is journalled.
        uint64_t actual_count = 0;
        const auto& table_columns = s->columns();
        auto& table = entry->table_storage.table();
        if (!table_columns.empty() && !is_computed_table) {
            if (auto mismatch = table.check_payload_columns(txn, attoids); mismatch.contains_error()) {
                co_return mismatch;
            }
        }
        for (auto& chunk : data) {
            actual_count += chunk.size();
            if (!table_columns.empty() && chunk.column_count() > 0) {
                if (auto widened = is_computed_table ? table.widen_by_name(txn, chunk) : table.widen_insert(txn, chunk);
                    widened.contains_error()) {
                    co_return widened;
                }
                if (!is_computed_table) {
                    if (auto refused = table.refuse_null_values(txn, chunk); refused.contains_error()) {
                        co_return refused;
                    }
                }
            }
        }
        if (returning) {
            for (const auto& chunk : data) {
                if (!table_columns.empty() && chunk.column_count() > 0 && !is_computed_table) {
                    receipt.returned.push_back(table.visible_copy(txn, chunk));
                    continue;
                }
                components::vector::data_chunk_t copy(resource(), chunk.types(), chunk.size());
                chunk.copy(copy, 0);
                receipt.returned.push_back(std::move(copy));
            }
        }

        // No `_id` dedup here: uniqueness has exactly one implementation, operator_unique_constraint_t.

        // WAL-first: reserve start_row, journal, then materialize; mailbox-atomicity keeps it stable meanwhile.
        const uint64_t start_row = s->total_rows();

        if (txn.transaction_id != 0 && manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            const auto db_oid = (ctx.database_oid != components::catalog::INVALID_OID)
                                    ? ctx.database_oid
                                    : components::catalog::well_known_oid::main_database;

            // CREATE INDEX backfill uses start_row as the row-id base, so it must match the materialized start.
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.reserve(data.size());
            for (const auto& chunk : data) {
                components::vector::data_chunk_t wal_chunk(resource(), chunk.types(), chunk.size());
                chunk.copy(wal_chunk, 0);
                wal_chunks.emplace_back(std::move(wal_chunk));
            }

            // One journal write and one await: a second suspending cross-actor await in a handler is a lost wakeup.
            unique_future<core::result_wrapper_t<wal::id_t>> wal_future;
            if (wal_added_columns.empty()) {
                auto [_w, wf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                            &wal::manager_wal_replicate_t::write_physical_insert,
                                                            ctx.session,
                                                            table_oid,
                                                            entry->table_storage.table().column_attoids(),
                                                            std::move(wal_chunks),
                                                            start_row,
                                                            actual_count,
                                                            txn.transaction_id,
                                                            txn.sub_id,
                                                            db_oid);
                wal_future = std::move(wf);
            } else {
                std::pmr::vector<components::types::complex_logical_type> col_types(resource());
                col_types.reserve(wal_added_columns.size());
                for (const auto& col : wal_added_columns) {
                    auto t = col.type();
                    t.set_alias(col.name());
                    col_types.push_back(t);
                }
                auto schema_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), col_types, 0);
                schema_chunk->set_cardinality(0);
                auto [_g, gf] = actor_zeta::otterbrix::send(
                    manager_wal_addr_,
                    &wal::manager_wal_replicate_t::write_physical_grow,
                    ctx.session,
                    table_oid,
                    std::move(schema_chunk),
                    static_cast<std::uint64_t>(s->columns().size() - wal_added_columns.size()),
                    entry->table_storage.table().column_attoids(),
                    std::move(wal_chunks),
                    start_row,
                    actual_count,
                    txn.transaction_id,
                    txn.sub_id,
                    db_oid);
                wal_future = std::move(gf);
            }
            auto wal_result = co_await std::move(wal_future);
            if (wal_result.has_error()) {
                error(log_,
                      "agent_disk[{}]::storage_append_inner: the rows did not reach the journal for oid={}, they are "
                      "NOT appended: {}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      wal_result.error().what);
                co_return wal_result.convert_error<write_receipt_t>();
            }
            if (wal_result.value() == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::storage_append_inner: WAL returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }
        }

        // The chunks land one after another from start_row; one that fails takes the ones before it back.
        uint64_t materialized = 0;
        for (auto& chunk : data) {
            // Read before the append, which may take the chunk's rows.
            const uint64_t rows = chunk.size();
            auto append_r =
                s->append(chunk, txn.transaction_id != 0 ? txn : components::table::transaction_data::committed());
            core::error_t refused = core::error_t::no_error();
            if (append_r.has_error()) {
                refused = append_r.error();
            } else if (append_r.value() != start_row + materialized) {
                error(log_,
                      "agent_disk[{}]::storage_append_inner: oid={} reserved start_row {} for the journal but a "
                      "chunk materialized at {} — the message's rows are REVERTED and the append is refused rather "
                      "than answered with a base the journal does not name",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      start_row + materialized,
                      append_r.value());
                std::pmr::string what{"agent_disk::storage_append_inner: journalled start_row ", resource()};
                what.append(std::to_string(start_row + materialized).c_str());
                what.append(" but the rows materialized at ");
                what.append(std::to_string(append_r.value()).c_str());
                what.append(" for table oid ");
                what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                refused = core::error_t{core::error_code_t::data_corruption, std::move(what)};
                if (auto reverted = s->revert_append(static_cast<int64_t>(append_r.value()), rows);
                    reverted.contains_error()) {
                    error(log_,
                          "agent_disk[{}]::storage_append_inner: oid={} could NOT take back a misplaced chunk: {}",
                          pool_idx_,
                          static_cast<unsigned>(table_oid),
                          reverted.what);
                }
            }
            if (refused.contains_error()) {
                if (materialized != 0) {
                    if (auto reverted = s->revert_append(static_cast<int64_t>(start_row), materialized);
                        reverted.contains_error()) {
                        error(log_,
                              "agent_disk[{}]::storage_append_inner: oid={} could NOT take back the {} rows of the "
                              "refused message: {}",
                              pool_idx_,
                              static_cast<unsigned>(table_oid),
                              materialized,
                              reverted.what);
                    }
                }
                co_return refused;
            }
            materialized += rows;
        }
        receipt.start_row = static_cast<int64_t>(start_row);
        receipt.count = actual_count;
        co_return std::move(receipt);
    }

    namespace {
        void report_publish_revert_miss(log_t& log,
                                        std::size_t pool_idx,
                                        const char* leg,
                                        components::catalog::oid_t table_oid) {
            error(log,
                  "agent_disk[{}]::{}: this owning agent has NO storage for oid={} — the MVCC flip/unwind "
                  "for it DID NOT HAPPEN",
                  pool_idx,
                  leg,
                  static_cast<unsigned>(table_oid));
#ifdef DEV_MODE
            g_publish_revert_misses.fetch_add(1, std::memory_order_relaxed);
#endif
        }
    } // namespace

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_publish_commits_inner(uint64_t commit_id,
                                                std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
        for (const auto& r : ranges) {
            if (r.count == 0) {
                continue;
            }
            auto it = storages_.find(r.table_oid);
            if (it == storages_.end()) {
                report_publish_revert_miss(log_, pool_idx_, "storage_publish_commits_inner", r.table_oid);
                continue;
            }
            auto& entry = it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                report_publish_revert_miss(log_, pool_idx_, "storage_publish_commits_inner", r.table_oid);
                continue;
            }
            entry->storage->commit_append(commit_id, r.start_row, r.count);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_publish_deletes_inner(uint64_t txn_id,
                                                uint64_t commit_id,
                                                std::pmr::vector<components::catalog::oid_t> tables) {
        if (components::table::is_direct_write_txn(txn_id)) {
            co_return;
        }
        for (const auto& tbl_oid : tables) {
            auto it = storages_.find(tbl_oid);
            if (it == storages_.end()) {
                report_publish_revert_miss(log_, pool_idx_, "storage_publish_deletes_inner", tbl_oid);
                continue;
            }
            auto& entry = it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                report_publish_revert_miss(log_, pool_idx_, "storage_publish_deletes_inner", tbl_oid);
                continue;
            }
            entry->storage->commit_all_deletes(txn_id, commit_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_revert_deletes_inner(uint64_t txn_id,
                                               uint64_t from_sub_id,
                                               std::pmr::vector<components::catalog::oid_t> tables) {
        if (components::table::is_direct_write_txn(txn_id)) {
            co_return;
        }
        for (const auto& tbl_oid : tables) {
            auto it = storages_.find(tbl_oid);
            if (it == storages_.end()) {
                report_publish_revert_miss(log_, pool_idx_, "storage_revert_deletes_inner", tbl_oid);
                continue;
            }
            auto& entry = it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                report_publish_revert_miss(log_, pool_idx_, "storage_revert_deletes_inner", tbl_oid);
                continue;
            }
            entry->storage->revert_all_deletes(txn_id, from_sub_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<core::error_t>
    agent_disk_t::storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
        // Reports the FIRST refusal but keeps unwinding: a range that cannot be rolled back must not
        // strand the ranges after it.
        auto first_error = core::error_t::no_error();
        for (auto it = ranges.rbegin(); it != ranges.rend(); ++it) {
            if (it->count == 0) {
                continue;
            }
            auto slice_it = storages_.find(it->table_oid);
            if (slice_it == storages_.end()) {
                report_publish_revert_miss(log_, pool_idx_, "storage_revert_appends_inner", it->table_oid);
                continue;
            }
            auto& entry = slice_it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                report_publish_revert_miss(log_, pool_idx_, "storage_revert_appends_inner", it->table_oid);
                continue;
            }
            // Reverse iteration is what makes a multi-statement transaction work here: dropping its LAST
            // range moves the frontier back onto the one before it, so each in turn becomes the tail.
            if (auto reverted = entry->storage->revert_append(it->start_row, it->count);
                reverted.contains_error() && !first_error.contains_error()) {
                first_error = reverted;
            }
        }
        co_return first_error;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<update_receipt_t>>
    agent_disk_t::storage_update_inner(components::catalog::oid_t table_oid,
                                       std::pmr::vector<components::vector::vector_t> row_ids,
                                       std::pmr::vector<components::vector::data_chunk_t> data,
                                       components::table::transaction_data txn,
                                       bool returning) {
        update_receipt_t result{
            write_receipt_t{0, 0, true, std::pmr::vector<components::vector::data_chunk_t>{resource()}},
            std::pmr::vector<components::vector::data_chunk_t>{resource()},
            std::pmr::vector<std::uint32_t>{resource()}};
        bool has_rows = false;
        for (const auto& chunk : data) {
            has_rows = has_rows || chunk.size() != 0;
        }
        if (!has_rows) {
            co_return std::move(result);
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_update: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            std::pmr::string what{"storage_update: table oid has an empty entry on its disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        if (entry->storage == nullptr) {
            std::pmr::string what{"storage_update: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        // Every chunk is checked and widened before any row changes.
        auto& table = entry->table_storage.table();
        for (std::size_t i = 0; i < data.size(); ++i) {
            if (data[i].size() == 0) {
                continue;
            }
            if (auto widened = entry->is_computed ? table.widen_by_name(txn, data[i])
                                                  : table.widen_update(txn, row_ids[i], data[i]);
                widened.contains_error()) {
                co_return widened;
            }
            if (!entry->is_computed) {
                if (auto refused = table.refuse_null_values(txn, data[i]); refused.contains_error()) {
                    co_return refused;
                }
            }
        }
        // The new versions land one after another; a chunk that fails takes the versions before it back (the
        // delete marks of the old versions go with the transaction's revert of its deletes).
        bool have_range = false;
        for (std::size_t i = 0; i < data.size(); ++i) {
            if (data[i].size() == 0) {
                continue;
            }
            if (returning) {
                if (entry->is_computed) {
                    components::vector::data_chunk_t copy(resource(), data[i].types(), data[i].size());
                    data[i].copy(copy, 0);
                    result.receipt.returned.push_back(std::move(copy));
                } else {
                    result.receipt.returned.push_back(table.visible_copy(txn, data[i]));
                }
            }
            auto updated = entry->storage->update(row_ids[i], data[i], txn);
            if (updated.has_error()) {
                if (have_range && result.receipt.count != 0) {
                    if (auto reverted = entry->storage->revert_append(result.receipt.start_row, result.receipt.count);
                        reverted.contains_error()) {
                        error(log_,
                              "agent_disk[{}]::storage_update_inner: oid={} could NOT take back the {} new versions "
                              "of the refused message: {}",
                              pool_idx_,
                              static_cast<unsigned>(table_oid),
                              result.receipt.count,
                              reverted.what);
                    }
                }
                co_return updated.convert_error<update_receipt_t>();
            }
            if (!have_range) {
                result.receipt.start_row = updated.value().start_row;
                have_range = true;
            }
            result.receipt.count += updated.value().count;
            result.written.push_back(std::move(data[i]));
        }
        result.attoids = table.column_attoids();
        co_return std::move(result);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<write_receipt_t>>
    agent_disk_t::storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                            components::table::row_id_batch_t row_ids,
                                            components::table::transaction_data txn,
                                            bool returning) {
        write_receipt_t receipt{0, 0, true, std::pmr::vector<components::vector::data_chunk_t>{resource()}};
        if (row_ids.empty()) {
            co_return std::move(receipt);
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_delete_rows: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            std::pmr::string what{"storage_delete_rows: table oid has an empty entry on its disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        if (entry->storage == nullptr) {
            std::pmr::string what{"storage_delete_rows: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        if (returning) {
            // The rows as they were, read before the delete stamps them, in the order they were named.
            const auto types = entry->storage->read_types(nullptr, txn);
            const std::vector<size_t> every_column;
            for (auto& window : row_ids) {
                assert(window.count <= components::vector::DEFAULT_VECTOR_CAPACITY &&
                       "storage_delete_rows_inner: a window holds at most DEFAULT_VECTOR_CAPACITY ids");
                components::vector::data_chunk_t chunk(resource(), types, window.count);
                auto fetched = entry->storage->fetch(chunk,
                                                     window.ids,
                                                     window.count,
                                                     every_column,
                                                     txn,
                                                     components::table::fetch_visibility_t::SNAPSHOT);
                if (fetched.has_error()) {
                    co_return fetched.convert_error<write_receipt_t>();
                }
                // read_types / fetch without a projection read the visible columns already.
                if (chunk.size() != 0) {
                    receipt.returned.push_back(std::move(chunk));
                }
            }
        }
        auto deleted = entry->storage->delete_rows(row_ids, txn);
        if (deleted.has_error()) {
            co_return deleted.convert_error<write_receipt_t>();
        }
        receipt.count = deleted.value();
        co_return std::move(receipt);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::storage_fetch_inner(components::catalog::oid_t table_oid,
                                      components::table::row_id_batch_t row_ids,
                                      std::vector<size_t> projected_cols,
                                      components::table::transaction_data txn,
                                      components::table::fetch_visibility_t visibility,
                                      int64_t limit,
                                      uint64_t expected_compact_epoch) {
        std::pmr::vector<components::vector::data_chunk_t> out{resource()};
        if (row_ids.empty()) {
            co_return std::move(out);
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_fetch: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            std::pmr::string what{"storage_fetch: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        if (expected_compact_epoch != k_fetch_epoch_unchecked &&
            expected_compact_epoch != entry->table_storage.table().compact_epoch()) {
            std::pmr::string what{"storage_fetch: the index answer is stale — the table was compacted "
                                  "(row ids renumbered) after the index was built and before its rebuild "
                                  "finished; retry the statement",
                                  resource()};
            co_return core::error_t{core::error_code_t::write_conflict, std::move(what)};
        }
        auto types = entry->storage->read_types(projected_cols.empty() ? nullptr : &projected_cols, txn);
        const bool capped = limit >= 0;
        const uint64_t budget = capped ? static_cast<uint64_t>(limit) : 0;
        uint64_t produced = 0;
        // An id past the table's frontier is not an invisible row -- the gather skips it silently
        // (components/table/collection.cpp: an id outside every row group is dropped), and the caller
        // reads the short answer as "no such row". For an INDEX answer that is a lie: the index named
        // rows the table does not have, because a crash left it ahead of the table's last checkpoint.
        // Refuse rather than answer short; a shortfall from MVCC visibility or LIMIT stays legal
        // because it is bounded by ids that DO exist.
        if (expected_compact_epoch != k_fetch_epoch_unchecked) {
            const auto frontier = static_cast<int64_t>(entry->storage->total_rows());
            for (const auto& window : row_ids) {
                const auto* ids = window.ids.data<int64_t>();
                for (uint64_t i = 0; i < window.count; ++i) {
                    if (ids[i] >= 0 && ids[i] < frontier) {
                        continue;
                    }
                    std::pmr::string what{"storage_fetch: the index answered row id ", resource()};
                    what.append(std::to_string(ids[i]).c_str());
                    what.append(", which the table does not have (it holds ");
                    what.append(std::to_string(frontier).c_str());
                    what.append(" rows) -- the index is ahead of its table after a restart; rebuild the "
                                "index and retry the statement");
                    co_return core::error_t{core::error_code_t::index_corrupted, std::move(what)};
                }
            }
        }
        for (const auto& window : row_ids) {
            if (capped && produced >= budget) {
                break;
            }
            assert(window.count <= components::vector::DEFAULT_VECTOR_CAPACITY &&
                   "storage_fetch_inner: a window holds at most DEFAULT_VECTOR_CAPACITY ids");
            components::vector::data_chunk_t chunk(resource(), types, window.count);
            auto fetch_r = entry->storage->fetch(chunk, window.ids, window.count, projected_cols, txn, visibility);
            if (fetch_r.has_error()) {
                co_return fetch_r.convert_error<std::pmr::vector<components::vector::data_chunk_t>>();
            }
            assert(chunk.size() <= window.count &&
                   "storage_fetch_inner: a window produced more rows than it was asked for");
            if (capped && produced + chunk.size() > budget) {
                chunk.set_cardinality(budget - produced);
            }
            produced += chunk.size();
            if (chunk.size() != 0) {
                out.emplace_back(std::move(chunk));
            }
        }
        co_return std::move(out);
    }

    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::scan_local(components::catalog::oid_t table_oid,
                             components::table::table_filter_t* filter,
                             int64_t limit,
                             const std::vector<std::size_t>* projected_cols,
                             const components::table::transaction_data& txn) {
        std::pmr::vector<components::vector::data_chunk_t> batches{resource()};
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"scan: storage is not owned by this agent", resource()}};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"scan: storage is a record-only marker", resource()}};
        }
        auto scan_r = entry->storage->scan_batched(batches, filter, limit, projected_cols, txn);
        if (scan_r.has_error()) {
            return scan_r.convert_error<std::pmr::vector<components::vector::data_chunk_t>>();
        }
        return batches;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::storage_scan_inner(components::catalog::oid_t table_oid,
                                     int64_t limit,
                                     std::vector<size_t> projected_cols,
                                     components::table::transaction_data txn) {
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
        co_return scan_local(table_oid, nullptr, limit, projected_ptr, txn);
    }

    template<typename PerBatch>
    static core::error_t for_each_storage_batch(components::storage::storage_t& storage,
                                                components::storage::scan_position_t& scan_position,
                                                const components::table::table_filter_t* filter,
                                                const std::vector<std::size_t>* projected,
                                                const components::table::transaction_data& txn,
                                                std::pmr::memory_resource* resource,
                                                PerBatch&& fn) {
        auto read_types = storage.read_types(projected, txn);
        scan_position.next_row = 0;
        scan_position.max_row = static_cast<int64_t>(storage.total_rows());
        while (!scan_position.drained && scan_position.next_row < scan_position.max_row) {
            components::vector::data_chunk_t batch{resource, read_types, components::vector::DEFAULT_VECTOR_CAPACITY};
            auto fetch_r = storage.fetch_next_batch(batch, scan_position, filter, projected, txn);
            if (fetch_r.has_error()) {
                return fetch_r.error();
            }
            if (batch.size() == 0) {
                break;
            }
            if (auto err = fn(batch); err.contains_error()) {
                return err;
            }
        }
        return core::error_t::no_error();
    }

    namespace {
        fetch_batch_t drained_page(std::pmr::memory_resource* resource, uint64_t cursor_id) {
            auto empty = std::make_unique<components::vector::data_chunk_t>(
                resource,
                std::pmr::vector<components::types::complex_logical_type>{resource},
                components::vector::DEFAULT_VECTOR_CAPACITY);
            empty->set_cardinality(0);
            return fetch_batch_t{std::move(empty), cursor_id};
        }

        core::error_t no_storage_of(std::pmr::memory_resource* resource, components::catalog::oid_t table_oid) {
            std::pmr::string what{"storage_run_block: no materialized storage to read: ", resource};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }

        // The WHERE selects no row whatever the table holds: col OP NULL is never true, nor col OP ANY|ALL (NULL).
        bool selects_nothing(const components::expressions::compare_expression_t& where,
                             const components::vector::storage_parameters& parameters) {
            namespace ce = components::expressions;
            if (where.type() == ce::compare_type::all_false || where.type() == ce::compare_type::all_unknown) {
                return true;
            }
            return !where.is_union() && where.type() != ce::compare_type::is_null &&
                   where.type() != ce::compare_type::is_not_null && ce::is_parameter(where.right()) &&
                   parameters.is_null(ce::as_parameter(where.right()));
        }
    } // namespace

    void agent_disk_t::open_scan_(uint64_t cursor_id,
                                  components::catalog::oid_t table_oid,
                                  collection_storage_entry_t& entry,
                                  std::unique_ptr<components::table::table_filter_t> filter,
                                  int64_t limit,
                                  std::vector<size_t> projected_cols,
                                  components::table::transaction_data txn) {
        active_scan_t scan{};
        scan.table_oid = table_oid;
        scan.pos.next_row = 0;
        scan.pos.max_row = static_cast<int64_t>(entry.storage->total_rows());
        scan.filter = std::move(filter);
        scan.projected_cols = std::move(projected_cols);
        scan.txn = txn;
        scan.matched_limit = limit;
        const auto open_types = entry.storage->types();
        scan.open_types.assign(open_types.begin(), open_types.end());
        for (const auto& c : entry.table_storage.table().columns()) {
            scan.open_columns.push_back(active_scan_t::open_column_t{c.attoid(), c.name()});
        }
        active_scans_.try_emplace(cursor_id, std::move(scan));
    }

    uint64_t agent_disk_t::mint_cursor_() {
        constexpr uint64_t counter_bits = (uint64_t{1} << 56) - 1;
        while (true) {
            const uint64_t id = (static_cast<uint64_t>(pool_idx_) << 56) | (next_cursor_++ & counter_bits);
            if (id != 0 && active_scans_.find(id) == active_scans_.end() &&
                block_pages_.find(id) == block_pages_.end()) {
                return id;
            }
        }
    }

    // Only the resume position is stored across ADVANCE calls; no pin survives.
    agent_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    agent_disk_t::storage_fetch_next_batch_inner(session_id_t /*session*/,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t cursor_id,
                                                 int64_t limit,
                                                 std::vector<size_t> projected_cols,
                                                 components::table::transaction_data txn) {
        if (cursor_id == 0) {
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                std::pmr::string what{"storage_fetch_next_batch: no materialized storage to open a scan on: ",
                                      resource()};
                what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
            }
            cursor_id = mint_cursor_();
            open_scan_(cursor_id, table_oid, *it->second, nullptr, limit, std::move(projected_cols), txn);
        }
        co_return advance_scan_(cursor_id);
    }

    core::result_wrapper_t<fetch_batch_t> agent_disk_t::advance_scan_(uint64_t cursor_id) {
        auto collect_identity = [](collection_storage_entry_t& entry, std::vector<active_scan_t::open_column_t>& out) {
            const auto& physical = entry.table_storage.table().columns();
            out.clear();
            out.reserve(physical.size());
            for (const auto& c : physical) {
                out.push_back(active_scan_t::open_column_t{c.attoid(), c.name()});
            }
        };

        auto cit = active_scans_.find(cursor_id);
        if (cit == active_scans_.end()) {
            return drained_page(resource(), cursor_id);
        }
        auto& scan = cit->second;
        const auto table_oid = scan.table_oid;

        if (scan.pos.drained ||
            (scan.matched_limit >= 0 && scan.matched_emitted >= static_cast<uint64_t>(scan.matched_limit))) {
            active_scans_.erase(cit);
            return drained_page(resource(), cursor_id);
        }

        auto storage_it = storages_.find(table_oid);
        if (storage_it == storages_.end() || storage_it->second == nullptr || storage_it->second->storage == nullptr) {
            active_scans_.erase(cit);
            std::pmr::string what{"storage_fetch_next_batch: the table was dropped under an open cursor: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            return core::error_t{core::error_code_t::table_not_exists, std::move(what)};
        }
        auto* storage = storage_it->second->storage.get();

        auto all_types = storage->types();
        const bool schema_unchanged = [&] {
            const auto& physical = storage_it->second->table_storage.table().columns();
            if (physical.size() != scan.open_columns.size() || all_types.size() != scan.open_types.size()) {
                return false;
            }
            for (size_t i = 0; i < scan.open_columns.size(); ++i) {
                const auto& live = physical[i];
                if (live.attoid() != scan.open_columns[i].attoid || live.name() != scan.open_columns[i].name ||
                    !(all_types[i] == scan.open_types[i])) {
                    return false;
                }
            }
            return true;
        }();

        std::vector<size_t> remapped_projected;
        if (!schema_unchanged) {
            std::vector<active_scan_t::open_column_t> current_columns;
            collect_identity(*storage_it->second, current_columns);
            auto refuse = [&](const char* what) {
                active_scans_.erase(cit);
                std::pmr::string msg{"storage_fetch_next_batch: schema changed under open cursor on table oid ",
                                     resource()};
                msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
                msg += std::pmr::string{": ", resource()};
                msg += std::pmr::string{what, resource()};
                return core::error_t{core::error_code_t::internal_error, std::move(msg)};
            };

            // attoid survives RENAME; falls back to (name, type) when either side lacks one. `gone` = no match.
            const size_t gone = current_columns.size();
            std::vector<size_t> open_to_current(scan.open_columns.size(), gone);
            for (size_t o = 0; o < scan.open_columns.size(); ++o) {
                const auto& oc = scan.open_columns[o];
                size_t found = gone;
                bool ambiguous = false;
                for (size_t c = 0; c < current_columns.size(); ++c) {
                    const auto& cc = current_columns[c];
                    const bool match = (oc.attoid != 0 && cc.attoid != 0)
                                           ? oc.attoid == cc.attoid
                                           : (oc.name == cc.name && all_types[c] == scan.open_types[o]);
                    if (!match) {
                        continue;
                    }
                    if (found != gone) {
                        ambiguous = true;
                        break;
                    }
                    found = c;
                }
                if (ambiguous) {
                    return refuse("a column's identity is ambiguous after the schema change");
                }
                if (found != gone && !(all_types[found] == scan.open_types[o])) {
                    found = gone;
                }
                open_to_current[o] = found;
            }

            if (scan.filter != nullptr) {
                for (size_t o = 0; o < open_to_current.size(); ++o) {
                    if (open_to_current[o] != o) {
                        return refuse("the cursor's filter is bound to column positions the change moved");
                    }
                }
            }

            remapped_projected.reserve(scan.projected_cols.empty() ? scan.open_columns.size()
                                                                   : scan.projected_cols.size());
            auto need_slot = [&](size_t slot) -> bool {
                if (slot >= open_to_current.size() || open_to_current[slot] == gone) {
                    return false;
                }
                remapped_projected.push_back(open_to_current[slot]);
                return true;
            };
            if (scan.projected_cols.empty()) {
                for (size_t slot = 0; slot < scan.open_columns.size(); ++slot) {
                    if (!need_slot(slot)) {
                        return refuse("a column this cursor serves was dropped from the storage");
                    }
                }
            } else {
                for (size_t slot : scan.projected_cols) {
                    if (!need_slot(slot)) {
                        return refuse("a column this cursor serves was dropped from the storage");
                    }
                }
            }
        }

        const std::vector<size_t>* projected_ptr =
            schema_unchanged ? (scan.projected_cols.empty() ? nullptr : &scan.projected_cols) : &remapped_projected;
        auto batch = std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                        storage->read_types(projected_ptr, scan.txn),
                                                                        components::vector::DEFAULT_VECTOR_CAPACITY);
        auto fetch_r = storage->fetch_next_batch(*batch, scan.pos, scan.filter.get(), projected_ptr, scan.txn);
        if (fetch_r.has_error()) {
            active_scans_.erase(cit);
            return fetch_r.convert_error<fetch_batch_t>();
        }

        if (scan.matched_limit >= 0) {
            const uint64_t budget = static_cast<uint64_t>(scan.matched_limit) - scan.matched_emitted;
            if (batch->size() >= budget) {
                batch->set_cardinality(budget);
                scan.pos.drained = true;
            }
        }
        scan.matched_emitted += batch->size();

        if (batch->size() == 0) {
            active_scans_.erase(cit);
            return drained_page(resource(), cursor_id);
        }
        return fetch_batch_t{std::move(batch), cursor_id};
    }

    agent_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    agent_disk_t::storage_run_block_inner(execution_context_t ctx,
                                          components::logical_plan::detached_block_t block,
                                          components::vector::storage_parameters parameters) {
        namespace lp = components::logical_plan;
        namespace ce = components::expressions;
        const auto& root = *block.root;
        const auto table_oid = root.table_oid();
        const auto* table = block.table(table_oid);
        auto it = storages_.find(table_oid);
        if (table == nullptr || it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            co_return no_storage_of(resource(), table_oid);
        }
        auto& entry = *it->second;
        // The columns the block reads, as otterbrix's scan reads them: a computed table whole, its columns that rows
        // carry.
        std::vector<size_t> projected_cols;
        if (entry.is_computed) {
            for (const auto& column : table->columns) {
                if (column.chunk_position >= 0) {
                    projected_cols.push_back(static_cast<size_t>(column.chunk_position));
                }
            }
        } else {
            projected_cols = static_cast<const lp::node_aggregate_t&>(root).projected_cols();
        }
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;

        const lp::node_match_t* where = nullptr;
        const lp::node_group_t* group = nullptr;
        for (const auto& child : root.children()) {
            if (child->type() == lp::node_type::match_t) {
                where = static_cast<const lp::node_match_t*>(child.get());
            } else if (child->type() == lp::node_type::group_t) {
                group = static_cast<const lp::node_group_t*>(child.get());
            }
        }
        bool nothing = false;
        std::unique_ptr<components::table::table_filter_t> filter;
        if (where != nullptr) {
            const auto& condition = static_cast<const ce::compare_expression_t&>(*where->expressions().front());
            nothing = selects_nothing(condition, parameters);
            if (!nothing && condition.type() != ce::compare_type::all_true) {
                auto built = components::table::build_table_filter(resource(),
                                                                   where->expressions().front(),
                                                                   parameters,
                                                                   components::graph_execution_context{ctx.session_tz},
                                                                   entry.storage->read_types(projected_ptr, ctx.txn));
                if (built.has_error()) {
                    co_return built.error();
                }
                filter = std::move(built.value());
            }
        }

        const uint64_t cursor_id = mint_cursor_();
        if (group == nullptr) {
            if (nothing || where == nullptr) {
                co_return drained_page(resource(), cursor_id);
            }
            open_scan_(cursor_id,
                       table_oid,
                       entry,
                       std::move(filter),
                       where->read_cap().to_disk(),
                       std::move(projected_cols),
                       ctx.txn);
            co_return next_block_page_(cursor_id);
        }

        // The GROUP BY otterbrix would run, over the rows the WHERE keeps.
        auto grouping = services::planner::impl::make_hash_group(resource(), log_.clone(), *group);
        const lp::node_t& typed = group->has_output_types() ? static_cast<const lp::node_t&>(*group) : root;
        if (typed.has_output_types()) {
            grouping->set_output_types(typed.output_types());
        }
        components::logical_plan::storage_parameters no_parameters{resource()};
        components::pipeline::context_t pipeline{ctx.session,
                                                 address(),
                                                 actor_zeta::address_t::empty_address(),
                                                 nullptr,
                                                 no_parameters,
                                                 components::pipeline::no_mailbox(),
                                                 components::pipeline::no_mailbox(),
                                                 components::pipeline::no_mailbox()};
        pipeline.txn = ctx.txn;
        if (!nothing) {
            components::storage::scan_position_t position{};
            components::operators::chunks_vector_t sink{resource()};
            if (auto err = for_each_storage_batch(*entry.storage,
                                                  position,
                                                  filter.get(),
                                                  projected_ptr,
                                                  ctx.txn,
                                                  resource(),
                                                  [&](components::vector::data_chunk_t& batch) {
                                                      return grouping->push(&pipeline, std::move(batch), sink);
                                                  });
                err.contains_error()) {
                co_return err;
            }
        }
        components::operators::chunks_vector_t rows{resource()};
        if (auto err = grouping->finalize(&pipeline, rows); err.contains_error()) {
            co_return err;
        }
        auto& kept = block_pages_
                         .try_emplace(cursor_id,
                                      block_pages_t{std::pmr::vector<components::vector::data_chunk_t>{resource()}, 0})
                         .first->second;
        for (auto& chunk : rows) {
            if (chunk.size() != 0) {
                kept.pages.push_back(std::move(chunk));
            }
        }
        co_return next_block_page_(cursor_id);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    agent_disk_t::storage_block_page_inner(execution_context_t /*ctx*/, uint64_t cursor_id) {
        co_return next_block_page_(cursor_id);
    }

    core::result_wrapper_t<fetch_batch_t> agent_disk_t::next_block_page_(uint64_t cursor_id) {
        core::result_wrapper_t<fetch_batch_t> page = drained_page(resource(), cursor_id);
        if (auto found = block_pages_.find(cursor_id); found != block_pages_.end()) {
            if (found->second.next == found->second.pages.size()) {
                block_pages_.erase(found);
            } else {
                page = fetch_batch_t{std::make_unique<components::vector::data_chunk_t>(
                                         std::move(found->second.pages[found->second.next++])),
                                     cursor_id};
            }
        } else {
            page = advance_scan_(cursor_id);
        }
#ifdef DEV_MODE
        if (!page.has_error() && page.value().batch) {
            g_disk_block_rows.fetch_add(page.value().batch->size(), std::memory_order_relaxed);
        }
#endif
        return page;
    }

    agent_disk_t::unique_future<void> agent_disk_t::storage_close_block_inner(uint64_t cursor_id) {
        block_pages_.erase(cursor_id);
        active_scans_.erase(cursor_id);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::storage_close_cursor_inner(session_id_t /*session*/,
                                                                               components::catalog::oid_t table_oid,
                                                                               uint64_t cursor_id) {
        trace(log_,
              "agent_disk[{}]::storage_close_cursor_inner: oid={} cursor={}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              cursor_id);
        active_scans_.erase(cursor_id);
        co_return;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_open_scan_hold_inner(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"storage_open_scan_hold: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        active_scan_t hold{};
        hold.table_oid = table_oid;
        const uint64_t hold_id = mint_cursor_();
        active_scans_.try_emplace(hold_id, std::move(hold));
        trace(log_,
              "agent_disk[{}]::storage_open_scan_hold_inner: oid={} hold={}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              hold_id);
        co_return hold_id;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_compact_epoch_inner(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"storage_compact_epoch: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        co_return it->second->table_storage.table().compact_epoch();
    }

    // result[i] = row_ids matching key-tuple i; one streamed pass costs O(table_rows + nkeys), not
    // O(nkeys * table_rows).
    core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    fk_hash_semijoin(std::pmr::memory_resource* resource,
                     components::storage::storage_t& storage,
                     const std::pmr::vector<std::uint64_t>& key_col_indices,
                     components::vector::data_chunk_t& keys,
                     components::table::transaction_data txn) {
        const std::uint64_t nkeys = keys.size();
        std::pmr::vector<std::pmr::vector<std::int64_t>> result{resource};
        result.reserve(nkeys);
        for (std::uint64_t i = 0; i < nkeys; ++i) {
            result.emplace_back();
        }
        if (nkeys == 0) {
            return result;
        }
        // Unevaluable must refuse, not answer all-miss — empty buckets affirm "no children" to CASCADE/RESTRICT.
        if (key_col_indices.empty()) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"fk semi-join: no key columns given", resource}};
        }
        if (keys.column_count() != key_col_indices.size()) {
            return core::error_t{
                core::error_code_t::internal_error,
                std::pmr::string{"fk semi-join: key chunk arity does not match key columns", resource}};
        }

        const auto visible_types = storage.types(txn);

        std::pmr::vector<components::types::complex_logical_type> stored_key_types{resource};
        stored_key_types.reserve(key_col_indices.size());
        for (auto ci : key_col_indices) {
            stored_key_types.push_back(visible_types[ci]);
        }
        components::vector::data_chunk_t norm_keys(resource, stored_key_types, nkeys);
        norm_keys.set_cardinality(nkeys);
        // A key cell outside the stored domain is a miss, not a refusal — a vector-wide cast would abort the
        // in-domain keys batched with it, and magnitude-only range checks miss cases like DOUBLE 1.5 -> 1.
        std::pmr::vector<std::uint8_t> domain_miss(nkeys, std::uint8_t{0}, resource);
        for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
            auto& src = keys.data[j];
            if (src.get_vector_type() != components::vector::vector_type::FLAT) {
                src.flatten(nkeys);
            }
            if (src.type() == stored_key_types[j]) {
                components::vector::vector_ops::copy(src, norm_keys.data[j], nkeys, 0, 0);
                continue;
            }
            {
                components::vector::vector_t null_probe(resource, src.type(), 1);
                null_probe.set_null(0, true);
                auto probe = components::vector::vector_ops::cast_vector(resource, null_probe, stored_key_types[j], 1);
                if (probe.has_error()) {
                    return probe.error();
                }
            }
            components::vector::vector_t one_src(resource, src.type(), 1);
            for (std::uint64_t i = 0; i < nkeys; ++i) {
                if (src.is_null(i)) {
                    norm_keys.data[j].set_null(i, true);
                    continue;
                }
                components::vector::vector_ops::copy(src, one_src, i + 1, i, 0);
                auto fwd = components::vector::vector_ops::cast_vector(resource, one_src, stored_key_types[j], 1);
                if (fwd.has_error()) {
                    domain_miss[i] = 1;
                    norm_keys.data[j].set_null(i, true);
                    continue;
                }
                // A fraction like 1.5->1 forward-casts without error, so only the round trip catches it.
                auto back = components::vector::vector_ops::cast_vector(resource, fwd.value(), src.type(), 1);
                if (back.has_error() || !components::vector::cells_equal(back.value(), 0, one_src, 0)) {
                    domain_miss[i] = 1;
                    norm_keys.data[j].set_null(i, true);
                    continue;
                }
                components::vector::vector_ops::copy(fwd.value(), norm_keys.data[j], 1, 0, i);
            }
        }

        components::vector::vector_t key_hash_vec(resource, components::types::logical_type::UBIGINT, nkeys);
        std::vector<std::uint64_t> norm_col_ids(key_col_indices.size());
        for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
            norm_col_ids[j] = j;
        }
        norm_keys.hash(norm_col_ids, key_hash_vec);
        const auto* key_hashes = key_hash_vec.data<std::uint64_t>();
        std::pmr::unordered_map<std::uint64_t, std::pmr::vector<std::uint64_t>> key_index{resource};
        for (std::uint64_t i = 0; i < nkeys; ++i) {
            if (domain_miss[i] != 0) {
                continue;
            }
            bool any_null = false;
            for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
                if (keys.data[j].is_null(i)) {
                    any_null = true;
                    break;
                }
            }
            if (any_null) {
                continue;
            }
            key_index[key_hashes[i]].push_back(i);
        }

        std::vector<std::size_t> projected_cols(key_col_indices.begin(), key_col_indices.end());
        components::storage::scan_position_t pos{};
        auto scan_error = for_each_storage_batch(
            storage,
            pos,
            /*filter=*/nullptr,
            &projected_cols,
            txn,
            resource,
            [&](components::vector::data_chunk_t& batch) -> core::error_t {
                const uint64_t rows = batch.size();
                for (auto& col : batch.data) {
                    if (col.get_vector_type() != components::vector::vector_type::FLAT) {
                        col.flatten(rows);
                    }
                }
                components::vector::vector_t row_hash_vec(resource, components::types::logical_type::UBIGINT, rows);
                batch.hash(row_hash_vec);
                const auto* row_hashes = row_hash_vec.data<std::uint64_t>();
                const auto* row_ids = batch.row_ids.data<std::int64_t>();
                for (uint64_t r = 0; r < rows; ++r) {
                    bool any_null = false;
                    for (const auto& col : batch.data) {
                        if (col.is_null(r)) {
                            any_null = true;
                            break;
                        }
                    }
                    if (any_null) {
                        continue;
                    }
                    auto it_h = key_index.find(row_hashes[r]);
                    if (it_h == key_index.end()) {
                        continue;
                    }
                    for (std::uint64_t cand : it_h->second) {
                        bool match = true;
                        for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
                            if (!components::vector::cells_equal(norm_keys.data[j], cand, batch.data[j], r)) {
                                match = false;
                                break;
                            }
                        }
                        if (match) {
                            result[cand].push_back(row_ids[r]);
                        }
                    }
                }
                return core::error_t::no_error();
            });
        if (scan_error.contains_error()) {
            return scan_error;
        }
        return result;
    }

    // Must error, not return empty, or a misrouted/corrupt read looks like "Database does not exist".
    // Positions count only the columns `txn` sees: the scan reads its ids in that numbering
    static core::error_t resolve_key_col_indices(const collection_storage_entry_t* entry,
                                                 const std::pmr::vector<std::string>& key_col_names,
                                                 const components::table::transaction_data& txn,
                                                 std::pmr::vector<std::uint64_t>& out_indices,
                                                 std::pmr::memory_resource* resource) {
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"keyed read: storage is not owned by this agent", resource}};
        }
        if (key_col_names.empty()) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"keyed read: no key columns given", resource}};
        }
        const auto& cols = entry->storage->columns();
        out_indices.reserve(key_col_names.size());
        for (const auto& kname : key_col_names) {
            std::size_t col_idx = cols.size();
            std::size_t visible = 0;
            for (const auto& column : cols) {
                if (!column.visible_to(txn)) {
                    continue;
                }
                if (column.name() == kname) {
                    col_idx = visible;
                    break;
                }
                ++visible;
            }
            if (col_idx == cols.size()) {
                std::pmr::string what{"keyed read: table has no column ", resource};
                what.append(kname.c_str());
                return core::error_t{core::error_code_t::internal_error, std::move(what)};
            }
            out_indices.push_back(static_cast<std::uint64_t>(col_idx));
        }
        return core::error_t::no_error();
    }

    static core::error_t validate_key_col_indices(const collection_storage_entry_t* entry,
                                                  const std::pmr::vector<std::uint64_t>& key_col_indices,
                                                  std::pmr::memory_resource* resource) {
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"keyed read: storage is not owned by this agent", resource}};
        }
        if (key_col_indices.empty()) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"keyed read: no key columns given", resource}};
        }
        const auto ncols = entry->storage->columns().size();
        for (const auto idx : key_col_indices) {
            if (idx >= ncols) {
                return core::error_t{core::error_code_t::internal_error,
                                     std::pmr::string{"keyed read: key column index out of range", resource}};
            }
        }
        return core::error_t::no_error();
    }

    // A projected read appends the key columns it lacks after the answer columns; key_positions index
    // the scan's output, not the table.
    struct key_scan_layout_t {
        std::vector<std::size_t> scan_projection;
        std::vector<std::size_t> key_positions;
    };

    static key_scan_layout_t key_scan_layout(const std::pmr::vector<std::uint64_t>& projected_cols,
                                             const std::pmr::vector<std::uint64_t>& key_col_indices) {
        key_scan_layout_t layout{{projected_cols.begin(), projected_cols.end()}, {}};
        layout.key_positions.reserve(key_col_indices.size());
        for (const auto key_col : key_col_indices) {
            if (projected_cols.empty()) {
                layout.key_positions.push_back(static_cast<std::size_t>(key_col));
                continue;
            }
            auto found = std::find(layout.scan_projection.begin(), layout.scan_projection.end(), key_col);
            if (found == layout.scan_projection.end()) {
                layout.scan_projection.push_back(static_cast<std::size_t>(key_col));
                found = std::prev(layout.scan_projection.end());
            }
            layout.key_positions.push_back(static_cast<std::size_t>(found - layout.scan_projection.begin()));
        }
        return layout;
    }

    // Drops the key columns key_scan_layout appended past a projected answer.
    static void trim_to_projection(std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                   const std::pmr::vector<std::uint64_t>& projected_cols) {
        if (projected_cols.empty()) {
            return;
        }
        const auto width = static_cast<std::ptrdiff_t>(projected_cols.size());
        for (auto& chunk : chunks) {
            chunk.data.erase(chunk.data.begin() + width, chunk.data.end());
        }
    }

    static components::expressions::compare_expression_ptr make_key_eq(std::pmr::memory_resource* resource,
                                                                       const std::vector<std::size_t>& key_positions,
                                                                       std::size_t col,
                                                                       core::parameter_id_t param_id) {
        components::expressions::key_t column{resource};
        column.set_path(
            std::pmr::vector<size_t>{{key_positions[col]}, std::pmr::polymorphic_allocator<size_t>{resource}});
        return components::expressions::make_compare_expression(resource,
                                                                components::expressions::compare_type::eq,
                                                                column,
                                                                param_id);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
    agent_disk_t::scan_by_keys_inner(components::catalog::oid_t table_oid,
                                     std::pmr::vector<std::string> key_col_names,
                                     components::vector::data_chunk_t keys,
                                     components::table::transaction_data txn) {
        std::pmr::vector<std::pmr::vector<std::int64_t>> result{resource()};
        result.reserve(keys.size());

        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        if (auto resolved = resolve_key_col_indices(entry, key_col_names, txn, key_col_indices, resource());
            resolved.contains_error()) {
            co_return resolved;
        }

        // fk_hash_semijoin is a free function so tests can drive it via a counting storage; sole caller (R6).
        co_return fk_hash_semijoin(resource(), *entry->storage, key_col_indices, keys, txn);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                           std::pmr::vector<std::uint64_t> key_col_indices,
                                           components::vector::data_chunk_t keys,
                                           std::pmr::vector<std::uint64_t> projected_cols,
                                           components::table::transaction_data txn) {
        auto matched_r = read_by_keys_local(table_oid, key_col_indices, std::span{&keys, 1}, projected_cols, txn);
        if (matched_r.has_error()) {
            co_return matched_r.error();
        }
        auto matched = std::move(matched_r.value());
        trim_to_projection(matched, projected_cols);
        co_return std::move(matched);
    }

    agent_disk_t::unique_future<
        core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
    agent_disk_t::read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                            std::pmr::vector<std::uint64_t> key_col_indices,
                                            std::pmr::vector<components::vector::data_chunk_t> keys,
                                            std::pmr::vector<std::uint64_t> projected_cols,
                                            components::table::transaction_data txn) {
        auto matched_r = read_by_keys_local(table_oid, key_col_indices, keys, projected_cols, txn);
        if (matched_r.has_error()) {
            co_return matched_r.error();
        }
        auto matched = std::move(matched_r.value());

        std::uint64_t nkeys = 0;
        for (const auto& key_chunk : keys) {
            nkeys += key_chunk.size();
        }
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> result{resource()};
        result.resize(nkeys);
        if (nkeys == 1) {
            trim_to_projection(matched, projected_cols);
            result[0] = std::move(matched);
            co_return std::move(result);
        }

        const auto layout = key_scan_layout(projected_cols, key_col_indices);
        const auto read_types = storages_.find(table_oid)->second->storage->read_types(
            layout.scan_projection.empty() ? nullptr : &layout.scan_projection,
            txn);
        namespace expr = components::expressions;
        const std::size_t narity = key_col_indices.size();
        std::pmr::vector<std::unique_ptr<components::table::table_filter_t>> key_filters{resource()};
        key_filters.reserve(nkeys);
        for (const auto& key_chunk : keys) {
            for (std::uint64_t row = 0; row < key_chunk.size(); ++row) {
                components::vector::storage_parameters key_parameters{resource()};
                auto key_predicate = expr::make_compare_union_expression(resource(), expr::compare_type::union_and);
                for (std::size_t ki = 0; ki < narity; ++ki) {
                    const auto param_id =
                        key_parameters.add_cell(key_chunk.data[ki], row, components::vector::parameter_kind::literal);
                    key_predicate->append_child(make_key_eq(resource(), layout.key_positions, ki, param_id));
                }
                auto key_built =
                    expr::build_condition_graph(resource(), key_parameters, key_predicate.get(), read_types);
                if (key_built.has_error()) {
                    co_return key_built.error();
                }
                key_filters.emplace_back(std::make_unique<components::table::table_filter_t>(
                    std::move(key_parameters),
                    components::graph_execution_context{},
                    std::move(key_built.value()),
                    components::expressions::condition_kind::computed));
            }
        }

        const std::size_t answer_width = projected_cols.empty() ? read_types.size() : projected_cols.size();
        std::pmr::vector<components::types::complex_logical_type> answer_types(
            read_types.begin(),
            read_types.begin() + static_cast<std::ptrdiff_t>(answer_width),
            resource());
        for (auto& chunk : matched) {
            const auto rows = chunk.size();
            if (rows == 0) {
                continue;
            }
            for (std::uint64_t i = 0; i < nkeys; ++i) {
                auto decided = expr::run_graph(key_filters[i]->graph.get(),
                                               key_filters[i]->parameters,
                                               chunk,
                                               key_filters[i]->context);
                if (decided.has_error()) {
                    co_return decided.error();
                }
                const auto& decisions = decided.value().data.front();
                components::vector::indexing_vector_t selected(resource(), rows);
                std::uint64_t count = 0;
                for (std::uint64_t r = 0; r < rows; ++r) {
                    if (!decisions.is_null(r) && decisions.get_value<bool>(r)) {
                        selected.set_index(count, r);
                        ++count;
                    }
                }
                if (count == 0) {
                    continue;
                }
                components::vector::data_chunk_t out{resource(), answer_types, count};
                for (std::size_t c = 0; c < answer_width; ++c) {
                    components::vector::vector_ops::copy(chunk.data[c], out.data[c], selected, count, 0, 0);
                }
                components::vector::vector_ops::copy(chunk.row_ids, out.row_ids, selected, count, 0, 0);
                out.set_cardinality(count);
                result[i].push_back(std::move(out));
            }
        }
        co_return std::move(result);
    }

    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::read_by_keys_local(components::catalog::oid_t table_oid,
                                     const std::pmr::vector<std::uint64_t>& key_col_indices,
                                     std::span<const components::vector::data_chunk_t> keys,
                                     const std::pmr::vector<std::uint64_t>& projected_cols,
                                     const components::table::transaction_data& txn) {
        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        if (auto valid = validate_key_col_indices(entry, key_col_indices, resource()); valid.contains_error()) {
            return valid;
        }

        std::uint64_t nkeys = 0;
        for (const auto& key_chunk : keys) {
            if (key_chunk.column_count() != key_col_indices.size()) {
                return core::error_t{
                    core::error_code_t::internal_error,
                    std::pmr::string{"keyed read: key chunk arity does not match key columns", resource()}};
            }
            nkeys += key_chunk.size();
        }
        if (nkeys == 0) {
            return std::pmr::vector<components::vector::data_chunk_t>{resource()};
        }
        // All key tuples combined into 1 expr
        namespace expr = components::expressions;
        std::size_t narity = key_col_indices.size();

        if (nkeys * narity > std::numeric_limits<std::uint32_t>::max()) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"keyed read: key batch too large to bind", resource()}};
        }

        const auto layout = key_scan_layout(projected_cols, key_col_indices);
        const auto read_types =
            entry->storage->read_types(layout.scan_projection.empty() ? nullptr : &layout.scan_projection, txn);

        components::vector::storage_parameters all_parameters{resource()};
        auto all_predicate = expr::make_compare_union_expression(resource(),
                                                                 nkeys == 1 ? expr::compare_type::union_and
                                                                            : expr::compare_type::union_or);
        for (const auto& key_chunk : keys) {
            for (std::uint64_t row = 0; row < key_chunk.size(); ++row) {
                auto tuple_predicate =
                    nkeys == 1 ? nullptr
                               : expr::make_compare_union_expression(resource(), expr::compare_type::union_and);
                for (std::size_t ki = 0; ki < narity; ++ki) {
                    const auto param_id =
                        all_parameters.add_cell(key_chunk.data[ki], row, components::vector::parameter_kind::literal);
                    auto eq = make_key_eq(resource(), layout.key_positions, ki, param_id);
                    if (tuple_predicate) {
                        tuple_predicate->append_child(std::move(eq));
                    } else {
                        all_predicate->append_child(std::move(eq));
                    }
                }
                if (tuple_predicate) {
                    all_predicate->append_child(std::move(tuple_predicate));
                }
            }
        }

        auto all_built = expr::build_condition_graph(resource(), all_parameters, all_predicate.get(), read_types);
        if (all_built.has_error()) {
            return all_built.error();
        }
        components::table::table_filter_t scan_filter(std::move(all_parameters),
                                                      components::graph_execution_context{},
                                                      std::move(all_built.value()),
                                                      expr::condition_kind::computed);

#ifdef DEV_MODE
        g_catalog_key_scans.fetch_add(1, std::memory_order_relaxed);
#endif
        return scan_local(table_oid,
                          &scan_filter,
                          int64_t{-1},
                          layout.scan_projection.empty() ? nullptr : &layout.scan_projection,
                          txn);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
    agent_disk_t::storage_types_inner(components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_types: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            std::pmr::string what{"storage_types: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        co_return entry->storage->types();
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_total_rows_inner(components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_total_rows: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            std::pmr::string what{"storage_total_rows: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        co_return entry->storage->total_rows();
    }

    // Staged (write+fsync) before write_header commits — a crash mid-write risks a zero-length sidecar.
    namespace {
        std::filesystem::path checkpoint_sidecar_path(const std::filesystem::path& otbx_path) {
            auto p = otbx_path;
            p += ".wal_id";
            return p;
        }

        std::filesystem::path checkpoint_sidecar_staging_path(const std::filesystem::path& otbx_path) {
            auto p = checkpoint_sidecar_path(otbx_path);
            p += ".tmp";
            return p;
        }

        [[nodiscard]] core::error_t stage_checkpoint_sidecar(std::pmr::memory_resource* resource,
                                                             const std::filesystem::path& otbx_path,
                                                             wal::id_t wal_id) {
            const auto sidecar_path = checkpoint_sidecar_path(otbx_path);
            const auto tmp_path = checkpoint_sidecar_staging_path(otbx_path);

            core::filesystem::local_file_system_t fs;
            auto refuse = [&](std::string reason) {
                std::error_code rm_ec;
                std::filesystem::remove(tmp_path, rm_ec);
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"stage_checkpoint_sidecar: " + sidecar_path.string() +
                                                          " was NOT updated: " + std::move(reason),
                                                      resource});
            };

            std::error_code stale_ec;
            std::filesystem::remove(tmp_path, stale_ec);
            auto tmp = core::filesystem::open_file(fs,
                                                   tmp_path,
                                                   core::filesystem::file_flags::WRITE |
                                                       core::filesystem::file_flags::FILE_CREATE_NEW);
            if (tmp == nullptr) {
                return refuse("could not open the staging file " + tmp_path.string());
            }
            auto v = static_cast<uint64_t>(wal_id);
            const auto written = tmp->write(&v, sizeof(v));
            if (!written.complete || written.bytes_written != sizeof(v)) {
                tmp.reset();
                return refuse("the staging write landed " + std::to_string(written.bytes_written) + " of " +
                              std::to_string(sizeof(v)) + " bytes");
            }
            if (!tmp->sync()) {
                tmp.reset();
                return refuse("the staging file could not be fsynced");
            }
            tmp.reset();
            return core::error_t::no_error();
        }

        // The remove error is reported, not swallowed — a stale tmp would make
        // stage_checkpoint_sidecar defer this entry forever.
        void
        discard_staged_checkpoint_sidecar(log_t& log, std::size_t pool_idx, const std::filesystem::path& otbx_path) {
            const auto staging = checkpoint_sidecar_staging_path(otbx_path);
            std::error_code ec;
            std::filesystem::remove(staging, ec);
            if (ec) {
                warn(log,
                     "agent_disk[{}]: the staged checkpoint sidecar {} could not be removed ({}) — the next "
                     "round's staging will fail on the same obstacle and keep deferring this entry",
                     pool_idx,
                     staging.string(),
                     ec.message());
            }
        }

        [[nodiscard]] core::error_t publish_checkpoint_sidecar(std::pmr::memory_resource* resource,
                                                               const std::filesystem::path& otbx_path) {
            const auto sidecar_path = checkpoint_sidecar_path(otbx_path);
            const auto tmp_path = checkpoint_sidecar_staging_path(otbx_path);

            core::filesystem::local_file_system_t fs;
            if (!core::filesystem::move_files(fs, tmp_path, sidecar_path)) {
                std::error_code rm_ec;
                std::filesystem::remove(tmp_path, rm_ec);
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"publish_checkpoint_sidecar: " + sidecar_path.string() +
                                                          " was NOT updated: the rename over the live sidecar was "
                                                          "refused",
                                                      resource});
            }
            return core::error_t::no_error();
        }

        // Kept separate from publish: a refused rename splits the durable floor, while a refused directory
        // fsync only risks surfacing the previous id on crash — a milder failure.
        [[nodiscard]] core::error_t sync_checkpoint_sidecar_directory(std::pmr::memory_resource* resource,
                                                                      const std::filesystem::path& otbx_path) {
            const auto sidecar_path = checkpoint_sidecar_path(otbx_path);
            core::filesystem::local_file_system_t fs;
            auto dir = core::filesystem::open_file(fs, sidecar_path.parent_path(), core::filesystem::file_flags::READ);
            if (dir == nullptr || !core::filesystem::file_sync(fs, *dir)) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"sync_checkpoint_sidecar_directory: " + sidecar_path.string() +
                                                          " was written and renamed, but its directory could not be "
                                                          "fsynced -- a crash may still surface the previous id",
                                                      resource});
            }
            return core::error_t::no_error();
        }
    } // namespace

    agent_disk_t::unique_future<checkpoint_result_t>
    agent_disk_t::checkpoint_inner(session_id_t /*session*/, wal::id_t current_wal_id, uint64_t compact_watermark) {
        trace(log_, "agent_disk[{}]::checkpoint_inner: {} entries in local slice", pool_idx_, storages_.size());
        // Step order is load-bearing: sidecar rename/fsync must run only after the checkpoint's header commit.
#ifdef DEV_MODE
        g_table_checkpoints.fetch_add(1, std::memory_order_relaxed);
#endif
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        uint64_t deferred = 0;
        uint64_t rewritten = 0;
        uint64_t advanced = 0;
        for (auto& [tbl_oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            if (entry->otbx_path.empty()) {
                continue;
            }

            if (entry->table_storage.storage_degraded()) {
                warn(log_,
                     "agent_disk[{}]::checkpoint_inner oid={} block storage is degraded (a write/fsync did "
                     "not reach the device, or the free list is corrupt) — deferring this entry and NOT "
                     "compacting it; the file must be rebuilt",
                     pool_idx_,
                     static_cast<unsigned>(tbl_oid));
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                ++deferred;
                continue;
            }

            if (has_active_scan_for_oid(tbl_oid)) {
                trace(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} has an active scan cursor — deferring the whole entry "
                      "this round (no compact, no checkpoint); its WAL floor is unchanged",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid));
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                ++deferred;
                continue;
            }

            // compact() always fully rebuilds regardless of change: measured 205.7 ms for an empty round vs
            // 124.4 ms for one that wrote (100 tables).
            const bool unchanged = !entry->table_storage.needs_checkpoint();
            bool skip_compact_this_round = false;
            if (!unchanged) {
                skip_compact_this_round = entry->table_storage.last_checkpoint_failed();
                if (skip_compact_this_round) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner oid={} previous checkpoint failed — retrying WITHOUT "
                         "compaction; the rebuild resumes once a checkpoint commits",
                         pool_idx_,
                         static_cast<unsigned>(tbl_oid));
                }

                if (entry->table_storage.has_versions_above(compact_watermark)) {
                    trace(log_,
                          "agent_disk[{}]::checkpoint_inner oid={} has version stamps above watermark {} — "
                          "skipping this round",
                          pool_idx_,
                          static_cast<unsigned>(tbl_oid),
                          compact_watermark);
                    min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                    ++deferred;
                    continue;
                }
            }

            if (auto staged = stage_checkpoint_sidecar(resource(), entry->otbx_path, current_wal_id);
                staged.contains_error()) {
                warn(log_,
                     "agent_disk[{}]::checkpoint_inner oid={} could not stage its checkpoint sidecar — deferring "
                     "this entry with NOTHING committed: {}",
                     pool_idx_,
                     static_cast<unsigned>(tbl_oid),
                     staged.what.c_str());
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                ++deferred;
                continue;
            }

            if (unchanged) {
                trace(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} is unchanged since its durable root — advancing "
                      "its wal id without rewriting it",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid));
                entry->table_storage.advance_wal_id_without_rewrite(current_wal_id);
                ++advanced;
            } else {
                if (!skip_compact_this_round && !entry->table_storage.table().compact(compact_watermark)) {
                    trace(log_,
                          "agent_disk[{}]::checkpoint_inner oid={} could not be rebuilt (the scan or the "
                          "rebuild append refused) — skipping this round",
                          pool_idx_,
                          static_cast<unsigned>(tbl_oid));
                    discard_staged_checkpoint_sidecar(log_, pool_idx_, entry->otbx_path);
                    min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                    ++deferred;
                    continue;
                }

                trace(log_,
                      "agent_disk[{}]::checkpoint_inner checkpointing oid={}",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid));

                auto cp_r = entry->table_storage.checkpoint(current_wal_id);
                if (cp_r.has_error()) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner oid={} checkpoint failed (rules 2/9) — deferring this "
                         "round",
                         pool_idx_,
                         static_cast<unsigned>(tbl_oid));
                    discard_staged_checkpoint_sidecar(log_, pool_idx_, entry->otbx_path);
                    min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                    ++deferred;
                    continue;
                }
                ++rewritten;
            }
            const auto& otbx_path = entry->otbx_path;

            if (auto rename_err = publish_checkpoint_sidecar(resource(), otbx_path); rename_err.contains_error()) {
                error(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} checkpoint sidecar: {}",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid),
                      rename_err.what.c_str());
            } else if (auto dir_err = sync_checkpoint_sidecar_directory(resource(), otbx_path);
                       dir_err.contains_error()) {
                warn(log_,
                     "agent_disk[{}]::checkpoint_inner oid={} checkpoint sidecar: {}",
                     pool_idx_,
                     static_cast<unsigned>(tbl_oid),
                     dir_err.what.c_str());
            }

            min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
        }
#ifdef DEV_MODE
        g_checkpoint_entries_deferred.fetch_add(deferred, std::memory_order_relaxed);
        g_checkpoint_entries_rewritten.fetch_add(rewritten, std::memory_order_relaxed);
#endif
        co_return checkpoint_result_t{min_prev_id, deferred, rewritten, advanced};
    }

    agent_disk_t::unique_future<void> agent_disk_t::vacuum_inner(session_id_t /*session*/,
                                                                 uint64_t lowest_active_start_time) {
        trace(log_, "agent_disk[{}]::vacuum_inner: {} entries in local slice", pool_idx_, storages_.size());
        for (auto& slot : storages_) {
            auto& entry = slot.second;
            if (entry == nullptr) {
                continue;
            }
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
        }
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::maybe_cleanup_inner(components::catalog::oid_t table_oid,
                                                                        uint64_t /*compact_watermark*/) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::maybe_cleanup_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::maybe_cleanup_inner: oid {} has null entry — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }

        // An uncommitted compact() only spends space: +2.9 MB per VACUUM on an unchanged 12k-row table.
        // Deferred to checkpoint_inner, not lost: 13053 DISK rounds performed 12962 compacts. Disabling
        // compact entirely leaves test_s3_cleanup_scaling with 700000 rows and only 149988 live.
        trace(log_,
              "agent_disk[{}]::maybe_cleanup_inner: oid={} — compaction belongs to the checkpoint round that "
              "can commit the release",
              pool_idx_,
              static_cast<unsigned>(table_oid));
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::on_horizon_advanced_inner(uint64_t new_horizon) {
        trace(log_,
              "agent_disk[{}]::on_horizon_advanced_inner: horizon={}, {} dropped entries in local slice",
              pool_idx_,
              new_horizon,
              dropped_storages_.size());
        std::pmr::vector<dropped_storage_entry_t> kept{resource()};
        kept.reserve(dropped_storages_.size());
        for (auto& entry : dropped_storages_) {
            if (entry.dropped_at_commit_id < new_horizon) {
                std::error_code ec;
                std::filesystem::remove(entry.path, ec);
                if (ec) {
                    trace(log_,
                          "agent_disk[{}]::on_horizon_advanced_inner , remove failed for {} : {}",
                          pool_idx_,
                          entry.path.string(),
                          ec.message());
                }
                for (const auto& sidecar : entry.sidecar_paths) {
                    std::error_code sec;
                    std::filesystem::remove(sidecar, sec);
                    if (sec) {
                        trace(log_,
                              "agent_disk[{}]::on_horizon_advanced_inner , remove sidecar failed for {} : {}",
                              pool_idx_,
                              sidecar.string(),
                              sec.message());
                    }
                }
                std::error_code dec;
                std::filesystem::remove(entry.path.parent_path(), dec);
                if (dec) {
                    trace(log_,
                          "agent_disk[{}]::on_horizon_advanced_inner , remove oid directory failed for {} : {}",
                          pool_idx_,
                          entry.path.parent_path().string(),
                          dec.message());
                }
            } else {
                kept.push_back(std::move(entry));
            }
        }
        dropped_storages_ = std::move(kept);

        if (dropped_storages_.empty() && manager_dispatcher_addr_ != actor_zeta::address_t::empty_address()) {
            constexpr uint8_t DISK_KIND = 1;
            [[maybe_unused]] auto _ =
                actor_zeta::otterbrix::send(manager_dispatcher_addr_,
                                            &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                            DISK_KIND);
        }
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::storage_dropped_committed_inner(uint64_t txn_id,
                                                                                    uint64_t commit_id) {
        for (auto& entry : dropped_storages_) {
            if (entry.dropped_at_commit_id == txn_id) {
                entry.dropped_at_commit_id = commit_id;
                trace(log_,
                      "agent_disk[{}]::storage_dropped_committed_inner: remapped oid {} from txn_id {} to commit_id {}",
                      pool_idx_,
                      static_cast<unsigned>(entry.oid),
                      txn_id,
                      commit_id);
            }
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_drop_aborted_inner(uint64_t txn_id, std::pmr::vector<components::catalog::oid_t> oids) {
        for (auto it = dropped_storages_.begin(); it != dropped_storages_.end();) {
            if (it->dropped_at_commit_id == txn_id && std::find(oids.begin(), oids.end(), it->oid) != oids.end()) {
                trace(log_,
                      "agent_disk[{}]::storage_drop_aborted_inner: un-marked DROP for oid {} (txn_id {})",
                      pool_idx_,
                      static_cast<unsigned>(it->oid),
                      txn_id);
                it = dropped_storages_.erase(it);
            } else {
                ++it;
            }
        }
        co_return;
    }

    // Bootstrap-only; after scheduler.start the address is read-only.
    void agent_disk_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        manager_dispatcher_addr_ = std::move(address);
    }

    void agent_disk_t::set_manager_wal_sync(actor_zeta::address_t address) { manager_wal_addr_ = std::move(address); }

    void agent_disk_t::register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                           uint64_t dropped_at_commit_id,
                                                           std::filesystem::path path,
                                                           std::pmr::vector<std::filesystem::path> sidecar_paths) {
        dropped_storages_.push_back(
            dropped_storage_entry_t{oid, dropped_at_commit_id, std::move(path), std::move(sidecar_paths)});
    }

    void agent_disk_t::drop_storage_one_local(components::catalog::oid_t oid) {
        // otbx_path must be read before the erase, while the unique_ptr is still alive.
        std::filesystem::path otbx_path;
        if (auto it = storages_.find(oid); it != storages_.end()) {
            if (it->second != nullptr) {
                otbx_path = it->second->otbx_path;
            }
        }
        const auto erased = storages_.erase(oid);
        if (erased == 0) {
            trace(log_,
                  "agent_disk[{}]::drop_storage_one_local: oid {} not in local slice (no-op)",
                  pool_idx_,
                  static_cast<unsigned>(oid));
        } else {
            trace(log_,
                  "agent_disk[{}]::drop_storage_one_local: erased oid {} from local slice",
                  pool_idx_,
                  static_cast<unsigned>(oid));
        }
        if (!otbx_path.empty()) {
            // A surviving .otbx would let a restart synthesise a phantom storage colliding with the recycled oid.
            std::error_code ec;
            std::filesystem::remove(otbx_path, ec);
            std::filesystem::remove(checkpoint_sidecar_path(otbx_path), ec);
            std::filesystem::remove(checkpoint_sidecar_staging_path(otbx_path), ec);
            std::filesystem::remove(otbx_path.parent_path(), ec);
        }
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::drop_storage_many_inner(std::pmr::vector<components::catalog::oid_t> oids) {
        for (auto oid : oids) {
            drop_storage_one_local(oid);
        }
        co_return;
    }

    namespace {
        struct catalog_name_key_t {
            uint64_t name_col;
            int64_t ns_col;
            const char* name_alias;
            const char* ns_alias;
            core::error_code_t code;
            const char* kind;
            // A row with this flag set is a tombstone and holds no name; -1 when the catalog has none
            int64_t tombstone_col;
            // The scope column is text (pg_database.datconnection), not an oid.
            bool text_scope{false};
            // The name column is an oid (pg_cast.castsource), not text.
            bool oid_name{false};
            // A text column that tells apart rows of one name and scope (pg_proc.proargmatchers: one row per argument
            // list); -1 when the key has none.
            int64_t match_col{-1};
            const char* match_alias{nullptr};
        };

        const catalog_name_key_t* catalog_name_key_for(components::catalog::oid_t table_oid) {
            namespace cat = components::catalog;
            static constexpr catalog_name_key_t pg_class_key{cat::pg_class_col::relname,
                                                             static_cast<int64_t>(cat::pg_class_col::relnamespace),
                                                             "relname",
                                                             "relnamespace",
                                                             core::error_code_t::table_already_exists,
                                                             "relation",
                                                             int64_t{-1}};
            static constexpr catalog_name_key_t pg_namespace_key{
                cat::pg_namespace_col::nspname,
                static_cast<int64_t>(cat::pg_namespace_col::nspdatabase),
                "nspname",
                "nspdatabase",
                core::error_code_t::schema_already_exists,
                "schema",
                int64_t{-1}};
            // A database name is unique per connection: a local database and a connection's may share it.
            static constexpr catalog_name_key_t pg_database_key{
                cat::pg_database_col::datname,
                static_cast<int64_t>(cat::pg_database_col::datconnection),
                "datname",
                "datconnection",
                core::error_code_t::database_already_exists,
                "database",
                int64_t{-1},
                true};
            static constexpr catalog_name_key_t pg_type_key{cat::pg_type_col::typname,
                                                            static_cast<int64_t>(cat::pg_type_col::typnamespace),
                                                            "typname",
                                                            "typnamespace",
                                                            core::error_code_t::duplicate_object,
                                                            "type",
                                                            int64_t{-1}};
            static constexpr catalog_name_key_t pg_attribute_key{
                cat::pg_attribute_col::attname,
                static_cast<int64_t>(cat::pg_attribute_col::attrelid),
                "attname",
                "attrelid",
                core::error_code_t::duplicate_column,
                "column",
                static_cast<int64_t>(cat::pg_attribute_col::attisdropped)};
            // PostgreSQL 18 pg_proc_proname_args_nsp_index and pg_cast_source_target_index.
            static constexpr catalog_name_key_t pg_proc_key{cat::pg_proc_col::proname,
                                                            static_cast<int64_t>(cat::pg_proc_col::pronamespace),
                                                            "proname",
                                                            "pronamespace",
                                                            core::error_code_t::duplicate_function,
                                                            "function",
                                                            int64_t{-1},
                                                            false,
                                                            false,
                                                            static_cast<int64_t>(cat::pg_proc_col::proargmatchers),
                                                            "proargmatchers"};
            static constexpr catalog_name_key_t pg_cast_key{cat::pg_cast_col::castsource,
                                                            static_cast<int64_t>(cat::pg_cast_col::casttarget),
                                                            "castsource",
                                                            "casttarget",
                                                            core::error_code_t::duplicate_object,
                                                            "cast",
                                                            int64_t{-1},
                                                            false,
                                                            true};
            switch (table_oid) {
                case cat::well_known_oid::pg_class_table:
                    return &pg_class_key;
                case cat::well_known_oid::pg_namespace_table:
                    return &pg_namespace_key;
                case cat::well_known_oid::pg_database_table:
                    return &pg_database_key;
                case cat::well_known_oid::pg_type_table:
                    return &pg_type_key;
                case cat::well_known_oid::pg_attribute_table:
                    return &pg_attribute_key;
                case cat::well_known_oid::pg_proc_table:
                    return &pg_proc_key;
                case cat::well_known_oid::pg_cast_table:
                    return &pg_cast_key;
                default:
                    return nullptr;
            }
        }

        bool name_freed_by_delete(uint64_t delete_stamp, uint64_t writer_txn_id) {
            if (delete_stamp == components::table::NOT_DELETED_ID) {
                return false;
            }
            if (delete_stamp < components::table::TRANSACTION_ID_START) {
                return true;
            }
            return writer_txn_id != 0 && delete_stamp == writer_txn_id;
        }

        core::error_t catalog_name_conflict(std::pmr::memory_resource* resource,
                                            collection_storage_entry_t& entry,
                                            components::catalog::oid_t table_oid,
                                            const catalog_name_key_t& key,
                                            const components::vector::data_chunk_t& row,
                                            const components::table::transaction_data& txn) {
            const auto* def = components::catalog::find_system_table(table_oid);
            if (def == nullptr) {
                return core::error_t::no_error();
            }
            int64_t in_name_col = -1;
            int64_t in_ns_col = -1;
            int64_t in_tombstone_col = -1;
            int64_t in_match_col = -1;
            if (row.column_count() == def->columns.size()) {
                in_name_col = static_cast<int64_t>(key.name_col);
                in_ns_col = key.ns_col;
                in_tombstone_col = key.tombstone_col;
                in_match_col = key.match_col;
            } else {
                for (uint64_t c = 0; c < row.column_count(); c++) {
                    if (!row.data[c].type().has_alias()) {
                        continue;
                    }
                    const auto& alias = row.data[c].type().alias();
                    if (alias == key.name_alias) {
                        in_name_col = static_cast<int64_t>(c);
                    } else if (key.ns_alias != nullptr && alias == key.ns_alias) {
                        in_ns_col = static_cast<int64_t>(c);
                    } else if (key.match_alias != nullptr && alias == key.match_alias) {
                        in_match_col = static_cast<int64_t>(c);
                    }
                }
            }
            if (in_name_col < 0) {
                return core::error_t::no_error();
            }

            auto& table = entry.table_storage.table();
            const uint64_t total = entry.storage->total_rows();
            // The catalog read holds the name, then the namespace and the tombstone when the key has them.
            std::vector<size_t> projected{static_cast<size_t>(key.name_col)};
            uint64_t held_name_col = 0;
            int64_t held_ns_col = -1;
            int64_t held_tombstone_col = -1;
            int64_t held_match_col = -1;
            if (key.ns_col >= 0) {
                held_ns_col = static_cast<int64_t>(projected.size());
                projected.push_back(static_cast<size_t>(key.ns_col));
            }
            if (key.tombstone_col >= 0) {
                held_tombstone_col = static_cast<int64_t>(projected.size());
                projected.push_back(static_cast<size_t>(key.tombstone_col));
            }
            if (key.match_col >= 0) {
                held_match_col = static_cast<int64_t>(projected.size());
                projected.push_back(static_cast<size_t>(key.match_col));
            }
            auto types = entry.storage->read_types(&projected, txn);
            auto is_tombstone = [](const components::vector::data_chunk_t& rows, int64_t column, uint64_t index) {
                return column >= 0 && !rows.is_null(static_cast<uint64_t>(column), index) &&
                       rows.get_value<bool>(static_cast<uint64_t>(column), index);
            };

            struct incoming_name_t {
                std::string_view name;
                std::uint32_t name_oid;
                bool has_ns;
                std::uint32_t ns;
                std::string_view ns_text;
                std::string_view match;
            };
            const auto text_name = [&key](const components::vector::data_chunk_t& rows, uint64_t column, uint64_t i) {
                return key.oid_name ? std::string_view{} : rows.get_value<std::string_view>(column, i);
            };
            const auto oid_name = [&key](const components::vector::data_chunk_t& rows, uint64_t column, uint64_t i) {
                return key.oid_name ? rows.get_value<std::uint32_t>(column, i) : std::uint32_t{0};
            };
            const auto match_of = [](const components::vector::data_chunk_t& rows, int64_t column, uint64_t i) {
                return column < 0 || rows.is_null(static_cast<uint64_t>(column), i)
                           ? std::string_view{}
                           : rows.get_value<std::string_view>(static_cast<uint64_t>(column), i);
            };
            const auto oid_scope = [&key](const components::vector::data_chunk_t& rows, uint64_t column, uint64_t i) {
                return key.text_scope ? std::uint32_t{0} : rows.get_value<std::uint32_t>(column, i);
            };
            const auto text_scope = [&key](const components::vector::data_chunk_t& rows, uint64_t column, uint64_t i) {
                return key.text_scope ? rows.get_value<std::string_view>(column, i) : std::string_view{};
            };
            std::pmr::vector<incoming_name_t> incoming(resource);
            for (uint64_t in_r = 0; in_r < row.size(); in_r++) {
                if (row.is_null(static_cast<uint64_t>(in_name_col), in_r) ||
                    is_tombstone(row, in_tombstone_col, in_r)) {
                    continue;
                }
                const bool has_ns =
                    key.ns_col >= 0 && in_ns_col >= 0 && !row.is_null(static_cast<uint64_t>(in_ns_col), in_r);
                const auto ns_col = static_cast<uint64_t>(in_ns_col);
                const auto name_col = static_cast<uint64_t>(in_name_col);
                incoming.push_back(incoming_name_t{text_name(row, name_col, in_r),
                                                   oid_name(row, name_col, in_r),
                                                   has_ns,
                                                   has_ns ? oid_scope(row, ns_col, in_r) : 0,
                                                   has_ns ? text_scope(row, ns_col, in_r) : std::string_view{},
                                                   match_of(row, in_match_col, in_r)});
            }
            if (incoming.empty()) {
                return core::error_t::no_error();
            }

            // One pass over the catalog for every name this append brings
            for (uint64_t offset = 0; offset < total; offset += components::vector::DEFAULT_VECTOR_CAPACITY) {
                const uint64_t n = std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, total - offset);
                components::vector::vector_t window_ids(resource, components::types::logical_type::BIGINT, n);
                auto* ids = window_ids.data<int64_t>();
                for (uint64_t i = 0; i < n; i++) {
                    ids[i] = static_cast<int64_t>(offset + i);
                }
                components::vector::data_chunk_t chunk(resource, types, n);
                auto fetch_r = entry.storage->fetch(chunk,
                                                    window_ids,
                                                    n,
                                                    projected,
                                                    txn,
                                                    components::table::fetch_visibility_t::RAW);
                if (fetch_r.has_error()) {
                    return fetch_r.error();
                }
                const auto* got_ids = chunk.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    if (chunk.is_null(held_name_col, i) || is_tombstone(chunk, held_tombstone_col, i)) {
                        continue;
                    }
                    const auto held_name = text_name(chunk, held_name_col, i);
                    const auto held_name_oid = oid_name(chunk, held_name_col, i);
                    const auto held_match = match_of(chunk, held_match_col, i);
                    bool held_has_ns = held_ns_col >= 0 && !chunk.is_null(static_cast<uint64_t>(held_ns_col), i);
                    const auto ns_col = static_cast<uint64_t>(held_ns_col);
                    const std::uint32_t held_ns = held_has_ns ? oid_scope(chunk, ns_col, i) : 0;
                    const std::string_view held_ns_text =
                        held_has_ns ? text_scope(chunk, ns_col, i) : std::string_view{};
                    for (const auto& wanted : incoming) {
                        if (held_name != wanted.name || held_name_oid != wanted.name_oid ||
                            held_match != wanted.match) {
                            continue;
                        }
                        if (wanted.has_ns && held_has_ns && (held_ns != wanted.ns || held_ns_text != wanted.ns_text)) {
                            continue;
                        }
                        const uint64_t stamp = table.row_group()->delete_stamp(got_ids[i]);
                        if (name_freed_by_delete(stamp, txn.transaction_id)) {
                            continue;
                        }
                        std::pmr::string msg{key.kind, resource};
                        msg += " '";
                        if (key.oid_name) {
                            msg += std::to_string(wanted.name_oid) + "' -> '" + std::to_string(wanted.ns);
                        } else {
                            msg.append(wanted.name.data(), wanted.name.size());
                        }
                        msg += "' already exists: the catalog holds a live row under this name "
                               "(committed, or pending in another transaction)";
                        return core::error_t{key.code, std::move(msg)};
                    }
                }
            }
            return core::error_t::no_error();
        }
    } // namespace

    agent_disk_t::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
    agent_disk_t::append_pg_catalog_row_inner(execution_context_t ctx,
                                              components::catalog::oid_t table_oid,
                                              components::vector::data_chunk_t row) {
        if (row.size() != 0) {
            if (const auto* key = catalog_name_key_for(table_oid)) {
                auto it_gate = storages_.find(table_oid);
                if (it_gate != storages_.end() && it_gate->second != nullptr && it_gate->second->storage != nullptr) {
                    auto conflict = catalog_name_conflict(resource(), *it_gate->second, table_oid, *key, row, ctx.txn);
                    if (conflict.contains_error()) {
                        error(log_,
                              "agent_disk[{}]::append_pg_catalog_row_inner: name-uniqueness gate refused the "
                              "append for oid={}: {}",
                              pool_idx_,
                              static_cast<unsigned>(table_oid),
                              conflict.what);
                        co_return conflict;
                    }
                }
            }
        }

        // The cast runs before the journal write: a refused cast must leave no PHYSICAL_INSERT behind, and
        // replay must apply the cast row, never the raw one.
        if (auto it_cast = storages_.find(table_oid);
            row.size() != 0 && it_cast != storages_.end() && it_cast->second != nullptr &&
            it_cast->second->storage != nullptr && it_cast->second->storage->has_schema()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            const auto& table_columns = it_cast->second->storage->columns();
            const bool by_position = row.column_count() >= table_columns.size();
            for (size_t i = 0; i < row.column_count(); i++) {
                const components::types::complex_logical_type* column_type = nullptr;
                if (by_position) {
                    if (i < table_columns.size()) {
                        column_type = &table_columns[i].type();
                    }
                } else if (row.data[i].type().has_alias()) {
                    for (const auto& col_def : table_columns) {
                        if (col_def.name() == row.data[i].type().alias()) {
                            column_type = &col_def.type();
                            break;
                        }
                    }
                }
                if (column_type == nullptr) {
                    continue;
                }
                auto src_type = row.data[i].type().type();
                auto tgt_type = column_type->type();
                if (src_type != tgt_type && (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                    (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                    auto& src_vec = row.data[i];
                    auto target_type = *column_type;
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    if (src_vec.get_vector_type() != components::vector::vector_type::FLAT) {
                        src_vec.flatten(row.size());
                    }
                    auto casted =
                        components::vector::vector_ops::cast_vector(resource(), src_vec, target_type, row.size());
                    if (casted.has_error()) {
                        std::pmr::string what{"append_pg_catalog_row: a catalog cell does not fit its column: ",
                                              resource()};
                        what.append(casted.error().what.c_str());
                        co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
                    }
                    row.data[i] = std::move(casted.value());
                }
            }
        }

        // PostgreSQL keeps catalog rows as full heap tuples: widen before the journal, so the record and the
        // storage get the same table-ordered chunk and the attoid list matches it by construction.
        if (auto it_widen = storages_.find(table_oid);
            row.size() != 0 && it_widen != storages_.end() && it_widen->second != nullptr &&
            it_widen->second->storage != nullptr && it_widen->second->storage->has_schema()) {
            const auto& table_columns = it_widen->second->storage->columns();
            if (row.column_count() < table_columns.size()) {
                std::vector<components::vector::vector_t> expanded_data;
                expanded_data.reserve(table_columns.size());
                for (const auto& col_def : table_columns) {
                    bool found = false;
                    for (uint64_t col = 0; col < row.column_count(); col++) {
                        if (row.data[col].type().has_alias() && row.data[col].type().alias() == col_def.name()) {
                            expanded_data.push_back(std::move(row.data[col]));
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        expanded_data.emplace_back(resource(), col_def.type(), row.size());
                        expanded_data.back().validity().set_all_invalid(row.size());
                    }
                }
                row.data = std::move(expanded_data);
            }
        }

        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            components::vector::data_chunk_t wal_chunk(resource(), row.types(), row.size());
            wal_chunk.set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                components::vector::vector_ops::copy(row.data[col], wal_chunk.data[col], row.size(), 0, 0);
            }
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] =
                actor_zeta::otterbrix::send(manager_wal_addr_,
                                            &wal::manager_wal_replicate_t::write_physical_insert,
                                            ctx.session,
                                            table_oid,
                                            components::catalog::system_column_attoids(resource(), table_oid),
                                            std::move(wal_chunks),
                                            std::uint64_t{0},
                                            static_cast<std::uint64_t>(row.size()),
                                            ctx.txn.transaction_id,
                                            ctx.txn.sub_id,
                                            db_oid);
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                error(log_,
                      "agent_disk[{}]::append_pg_catalog_row_inner: the catalog row's WAL record did not reach "
                      "the journal for oid={}, the row is NOT appended: {}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      wal_result.error().what);
                co_return wal_result.convert_error<components::pg_catalog_append_range_t>();
            }
            if (wal_result.value() == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::append_pg_catalog_row_inner: WAL write returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }
        }

        const auto count = static_cast<std::uint64_t>(row.size());
        uint64_t start_row = 0;

        auto it = storages_.find(table_oid);
        if (it != storages_.end() && it->second != nullptr && it->second->storage != nullptr && row.size() != 0) {
            auto* s = it->second->storage.get();

            auto types = row.types();
            const uint64_t n = row.size();
            components::vector::data_chunk_t local(resource(), types, n > 0 ? n : 1);
            local.set_cardinality(0);
            row.copy(local, 0);

            if (!s->has_schema() && local.column_count() > 0) {
                s->adopt_schema(local.types());
            }

            auto append_r = s->append(local, ctx.txn);
            if (append_r.has_error()) {
                co_return append_r.convert_error<components::pg_catalog_append_range_t>();
            }
            start_row = append_r.value();
        } else if (row.size() == 0) {
            trace(log_,
                  "agent_disk[{}]::append_pg_catalog_row_inner: empty row for oid={} — nothing to append",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
        } else {
            std::pmr::string msg{"agent_disk::append_pg_catalog_row: no storage on the owning agent for catalog "
                                 "oid ",
                                 resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            msg += std::pmr::string{" — the row was not written", resource()};
            co_return core::error_t{core::error_code_t::io_error, std::move(msg)};
        }

        if (components::table::is_direct_write_txn(ctx.txn.transaction_id) || count == 0) {
            co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), 0};
        }
        co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), count};
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::uint64_t>>
    agent_disk_t::delete_pg_catalog_rows_inner(execution_context_t ctx,
                                               components::catalog::oid_t table_oid,
                                               std::int64_t oid_col_idx,
                                               components::catalog::oid_t target_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string msg{"agent_disk::delete_pg_catalog_rows: no storage on the owning agent for catalog "
                                 "oid ",
                                 resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            msg += std::pmr::string{" — nothing was deleted", resource()};
            co_return core::error_t{core::error_code_t::io_error, std::move(msg)};
        }
        auto& entry = it->second;

        // The scan must carry ctx.txn — {0,0} can't see this txn's own uncommitted catalog row.
        // Regression: test_catalog_delete_refusal.cpp.
        core::pmr::otterbrix_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        detail::inline_scan(entry->table_storage.table(),
                            {oid_col_idx},
                            &scan_resource,
                            ctx.txn,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                if (chunk.is_null(0, i))
                                    return true;
                                auto v = chunk.get_value<std::uint32_t>(0, i);
                                if (static_cast<components::catalog::oid_t>(v) == target_oid) {
                                    row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                                }
                                return true;
                            });
        if (row_ids.empty()) {
            co_return std::uint64_t{0};
        }
        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<std::int64_t> wal_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                        &wal::manager_wal_replicate_t::write_physical_delete,
                                                        ctx.session,
                                                        table_oid,
                                                        std::move(wal_ids),
                                                        static_cast<std::uint64_t>(row_ids.size()),
                                                        ctx.txn.transaction_id,
                                                        ctx.txn.sub_id,
                                                        components::catalog::well_known_oid::main_database);
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                error(log_,
                      "agent_disk[{}]::delete_pg_catalog_rows_inner: the PHYSICAL_DELETE did not reach the "
                      "journal for oid={}, the rows are NOT deleted: {}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      wal_result.error().what);
                co_return wal_result.error();
            } else if (wal_result.value() == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::delete_pg_catalog_rows_inner: WAL write returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }
        }
        if (auto del_err = delete_sync(table_oid, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
            del_err.contains_error()) {
            error(log_,
                  "agent_disk[{}]::delete_pg_catalog_rows_inner: the slice it had just scanned refused the "
                  "delete: {}",
                  pool_idx_,
                  del_err.what);
            co_return std::move(del_err);
        }
        co_return static_cast<std::uint64_t>(row_ids.size());
    }

    agent_disk_t::unique_future<core::result_wrapper_t<components::storage::appended_range_t>>
    agent_disk_t::update_pg_attribute_commit_id_field_inner(execution_context_t ctx,
                                                            components::catalog::oid_t attoid,
                                                            components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                                            std::uint64_t commit_id) {
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        auto it = storages_.find(pg_attr_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"update_pg_attribute_commit_id_field: this agent holds no pg_attribute; "
                                  "the commit_id stamp for attoid ",
                                  resource()};
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append(" was not applied");
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& entry = it->second;

        auto& tbl = entry->table_storage.table();
        const std::size_t col_count = tbl.column_count();
        std::vector<std::int64_t> all_col_indices;
        all_col_indices.reserve(col_count);
        for (std::size_t i = 0; i < col_count; ++i) {
            all_col_indices.push_back(static_cast<std::int64_t>(i));
        }

        // Patch the target column: 10 = added_at_commit_id, 11 = dropped_at_commit_id.
        const std::size_t patch_col_idx =
            (kind == components::pg_attribute_commit_id_backfill_t::kind_t::added_at) ? 10u : 11u;
        if (patch_col_idx >= col_count) {
            std::pmr::string what{"update_pg_attribute_commit_id_field: pg_attribute row for attoid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append(" has no commit_id column at index ");
            what.append(std::to_string(patch_col_idx).c_str());
            what.append(" (row width ");
            what.append(std::to_string(col_count).c_str());
            what.append(")");
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }

        const auto& table_columns = entry->table_storage.table().columns();
        std::pmr::vector<components::types::complex_logical_type> chunk_types(resource());
        chunk_types.reserve(table_columns.size());
        for (const auto& col_def : table_columns) {
            auto t = col_def.type();
            t.set_alias(col_def.name());
            chunk_types.push_back(std::move(t));
        }
        components::vector::data_chunk_t patch(resource(), chunk_types, 1);
        patch.set_cardinality(1);

        core::pmr::otterbrix_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());

        detail::inline_scan(
            tbl,
            all_col_indices,
            &scan_resource,
            ctx.txn,
            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                if (chunk.is_null(0, i))
                    return true;
                if (static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i)) != attoid)
                    return true;
                row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                for (std::size_t c = 0; c < col_count; ++c) {
                    if (c == patch_col_idx) {
                        continue;
                    }
                    components::vector::vector_ops::copy(chunk.data[c],
                                                         patch.data[c],
                                                         components::vector::indexing_vector_t(resource()),
                                                         chunk.size(),
                                                         i,
                                                         0,
                                                         1);
                }
                return false;
            });
        if (row_ids.empty()) {
            std::pmr::string what{"update_pg_attribute_commit_id_field: no pg_attribute row visible to this "
                                  "transaction carries attoid ",
                                  resource()};
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append("; its commit_id stamp was not applied");
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        patch.data[patch_col_idx].set_value(0, static_cast<std::int64_t>(commit_id));

        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            components::vector::data_chunk_t wal_chunk(resource(), chunk_types, 1);
            wal_chunk.set_cardinality(1);
            for (std::size_t c = 0; c < col_count; ++c) {
                components::vector::vector_ops::copy(patch.data[c], wal_chunk.data[c], 1, 0, 0);
            }
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            std::pmr::vector<std::int64_t> wal_row_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] =
                actor_zeta::otterbrix::send(manager_wal_addr_,
                                            &wal::manager_wal_replicate_t::write_physical_update,
                                            ctx.session,
                                            pg_attr_oid,
                                            std::move(wal_row_ids),
                                            components::catalog::system_column_attoids(resource(), pg_attr_oid),
                                            std::move(wal_chunks),
                                            static_cast<std::uint64_t>(row_ids.size()),
                                            ctx.txn.transaction_id,
                                            ctx.txn.sub_id,
                                            components::catalog::well_known_oid::main_database);
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                error(log_,
                      "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: the PHYSICAL_UPDATE did not "
                      "reach the journal for attoid={}, the stamp is NOT applied: {}",
                      pool_idx_,
                      static_cast<unsigned>(attoid),
                      wal_result.error().what);
                co_return core::error_on(resource(), wal_result.error());
            }
            if (wal_result.value() == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: WAL write returned zero id "
                      "for attoid={} (journal disabled — the patch proceeds without a record)",
                      pool_idx_,
                      static_cast<unsigned>(attoid));
            }
        }

        auto stamped = update_sync(pg_attr_oid,
                                   row_ids,
                                   components::catalog::system_column_attoids(resource(), pg_attr_oid),
                                   patch,
                                   ctx.txn);
        if (stamped.has_error()) {
            error(log_,
                  "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: the slice it had just read "
                  "refused the update: {}",
                  pool_idx_,
                  stamped.error().what);
            co_return stamped.error();
        }
        co_return stamped.value();
    }

    // SUBTRACTIVE: drops every column not in live_attnames — a gap in the caller's derivation
    // drops a surviving column.
    agent_disk_t::unique_future<core::result_wrapper_t<std::uint64_t>>
    agent_disk_t::compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                  std::set<std::string> live_attnames) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            co_return core::result_wrapper_t<std::uint64_t>(std::uint64_t{0});
        }
        auto& entry = it->second;

        std::vector<std::string> to_drop;
        {
            const auto& cols = entry->table_storage.table().columns();
            to_drop.reserve(cols.size());
            for (const auto& c : cols) {
                if (live_attnames.find(c.name()) == live_attnames.end()) {
                    to_drop.push_back(c.name());
                }
            }
        }

        std::uint64_t dropped = 0;
        for (const auto& attname : to_drop) {
            auto dropped_r = entry->drop_column(attname, resource());
            if (dropped_r.has_error()) {
                // The storage is untouched by a refused drop, but the columns already dropped stay dropped;
                // the statement must hear about the refusal rather than count them.
                co_return core::result_wrapper_t<std::uint64_t>(dropped_r.error());
            }
            if (dropped_r.value()) {
                ++dropped;
            } else {
                trace(log_,
                      "agent_disk[{}]::compact_relkind_g_storage_inner: oid {} column '{}' not found",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      attname);
            }
        }
        co_return core::result_wrapper_t<std::uint64_t>(dropped);
    }

    // Runs only after the WAL commit marker + ProcArray barrier — the rebuild is irreversible.
    agent_disk_t::unique_future<core::result_wrapper_t<bool>>
    agent_disk_t::drop_storage_column_inner(components::catalog::oid_t table_oid, std::string attname) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string msg{"agent_disk::drop_storage_column: no materialized storage for table oid ", resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::internal_error, std::move(msg)});
        }
        auto dropped = it->second->drop_column(attname, resource());
        if (dropped.has_error()) {
            co_return dropped;
        }
        trace(log_,
              "agent_disk[{}]::drop_storage_column_inner: oid={} column='{}' {}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              attname,
              dropped.value() ? "dropped" : "absent from the storage schema — nothing physical to release");
        co_return dropped;
    }

    // Bootstrap reconciliation reads a storage-only name as a DROP: a RENAME that stopped at
    // pg_attribute would delete a surviving column's data on the next start.
    agent_disk_t::unique_future<core::result_wrapper_t<bool>>
    agent_disk_t::rename_storage_column_inner(components::catalog::oid_t table_oid,
                                              std::string old_attname,
                                              std::string new_attname) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string msg{"agent_disk::rename_storage_column: no materialized storage for table oid ",
                                 resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::internal_error, std::move(msg)});
        }
        auto renamed = it->second->rename_column(old_attname, new_attname);
        if (renamed.has_error()) {
            co_return renamed;
        }
        trace(log_,
              "agent_disk[{}]::rename_storage_column_inner: oid={} '{}' -> '{}' {}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              old_attname,
              new_attname,
              renamed.value() ? "renamed" : "absent from the storage schema — nothing to rename");
        co_return renamed;
    }

    void agent_disk_t::mark_storage_dropped_one_local(components::catalog::oid_t table_oid,
                                                      uint64_t dropped_at_commit_id) {
        trace(log_,
              "agent_disk[{}]::mark_storage_dropped_one_local: oid {} commit_id {}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              dropped_at_commit_id);
        std::filesystem::path otbx_path;
        std::pmr::vector<std::filesystem::path> sidecars{resource()};
        if (auto it = storages_.find(table_oid); it != storages_.end() && it->second != nullptr) {
            otbx_path = it->second->otbx_path;
            if (!otbx_path.empty()) {
                sidecars.push_back(checkpoint_sidecar_path(otbx_path));
                sidecars.push_back(checkpoint_sidecar_staging_path(otbx_path));
            }
        }
        register_dropped_storage_inner_sync(table_oid, dropped_at_commit_id, std::move(otbx_path), std::move(sidecars));
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                                  uint64_t dropped_at_commit_id) {
        for (auto table_oid : table_oids) {
            mark_storage_dropped_one_local(table_oid, dropped_at_commit_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<core::error_t>
    agent_disk_t::add_storage_column_inner(execution_context_t ctx,
                                           components::catalog::oid_t table_oid,
                                           components::table::column_definition_t column) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"add_storage_column: no materialized storage for table oid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }

        // Journal BEFORE the column exists, so replay re-adds it ahead of anything written into it. The
        // record carries this transaction's id, so a rollback simply never commits it and replay drops it.
        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<components::types::complex_logical_type> col_types(resource());
            auto type = column.type();
            type.set_alias(column.name());
            col_types.push_back(std::move(type));
            auto schema_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), col_types, 1);
            if (column.has_default_value() && !column.default_value().is_null(0)) {
                components::vector::vector_ops::copy(column.default_value(), schema_chunk->data[0], 1, 0, 0);
            } else {
                schema_chunk->data[0].validity().set_invalid(0);
            }
            schema_chunk->set_cardinality(1);
            auto position = static_cast<std::uint64_t>(it->second->table_storage.table().column_count());
            auto [_w, wf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                        &wal::manager_wal_replicate_t::write_physical_add_column,
                                                        ctx.session,
                                                        table_oid,
                                                        wal::column_attoids_t{{column.attoid()}, resource()},
                                                        std::move(schema_chunk),
                                                        position,
                                                        ctx.txn.transaction_id,
                                                        ctx.txn.sub_id,
                                                        components::catalog::well_known_oid::main_database);
            auto journalled = co_await std::move(wf);
            if (journalled.has_error()) {
                co_return core::error_on(resource(), journalled.error());
            }
        }

        auto& entry = *it->second;
        entry.add_column(column, resource());
        auto& table = entry.table_storage.table();
        if (table.has_construction_error()) {
            co_return core::error_on(resource(), table.construction_error());
        }
        // ADD appends, so the new column is the last one; nothing before it moves.
        const auto position = table.column_count() - 1;
        table.stamp_column_added(position, ctx.txn.transaction_id, ctx.txn.sub_id);
        trace(log_,
              "agent_disk[{}]::add_storage_column_inner: oid={} column='{}' position={} txn={}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              column.name(),
              position,
              ctx.txn.transaction_id);
        co_return core::error_t::no_error();
    }

    agent_disk_t::unique_future<core::error_t>
    agent_disk_t::stamp_column_dropped_inner(components::catalog::oid_t table_oid,
                                             components::catalog::oid_t attoid,
                                             components::table::transaction_data txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"stamp_column_dropped: no materialized storage for table oid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& table = it->second->table_storage.table();
        const auto position = table.find_visible_column(txn, static_cast<std::uint32_t>(attoid));
        if (position == components::table::storage::INVALID_INDEX) {
            std::pmr::string what{"stamp_column_dropped: no storage column with attoid ", resource()};
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append(" in table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        // The transaction's own id: its later statements stop seeing the column, every other snapshot
        // keeps it until this transaction commits and publish_column_stamps rewrites the stamp.
        table.stamp_column_dropped(position, txn.transaction_id, txn.sub_id);
        trace(log_,
              "agent_disk[{}]::stamp_column_dropped_inner: oid={} attoid={} position={} txn={}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              static_cast<unsigned>(attoid),
              position,
              txn.transaction_id);
        co_return core::error_t::no_error();
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::publish_column_stamps_inner(uint64_t txn_id,
                                              uint64_t commit_id,
                                              std::pmr::vector<components::catalog::oid_t> tables) {
        for (const auto& table_oid : tables) {
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                report_publish_revert_miss(log_, pool_idx_, "publish_column_stamps_inner", table_oid);
                continue;
            }
            const auto published = it->second->table_storage.table().publish_column_stamps(txn_id, commit_id);
            trace(log_,
                  "agent_disk[{}]::publish_column_stamps_inner: oid={} published={} stamp(s) at commit_id={}",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  published,
                  commit_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::revert_column_stamps_inner(uint64_t txn_id,
                                             uint64_t from_sub_id,
                                             std::pmr::vector<components::catalog::oid_t> tables) {
        for (const auto& table_oid : tables) {
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                report_publish_revert_miss(log_, pool_idx_, "revert_column_stamps_inner", table_oid);
                continue;
            }
            auto reverted = it->second->table_storage.table().revert_column_stamps(txn_id, from_sub_id);
            trace(log_,
                  "agent_disk[{}]::revert_column_stamps_inner: oid={} reverted={} stamp(s) of txn={}",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  reverted,
                  txn_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<core::error_t>
    agent_disk_t::storage_prepare_inner(components::table::transaction_data txn,
                                        std::pmr::vector<components::catalog::oid_t> tables) {
        for (const auto& table_oid : tables) {
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                continue;
            }
            if (auto refused = it->second->table_storage.table().prepare(txn); refused.contains_error()) {
                co_return core::error_on(resource(), refused);
            }
        }
        co_return core::error_t::no_error();
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_release_prepared_inner(uint64_t txn_id, std::pmr::vector<components::catalog::oid_t> tables) {
        for (const auto& table_oid : tables) {
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                continue;
            }
            it->second->table_storage.table().release_prepared(txn_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<core::error_t>
    agent_disk_t::storage_set_not_null_inner(components::table::transaction_data txn,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t attoid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"storage_set_not_null: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        auto& table = it->second->table_storage.table();
        const auto& columns = table.columns();
        for (uint64_t position = 0; position < columns.size(); ++position) {
            if (columns[position].attoid() == attoid && columns[position].visible_to(txn)) {
                co_return core::error_on(resource(), table.stamp_column_not_null(txn, position));
            }
        }
        std::pmr::string what{"storage_set_not_null: the table has no column of attoid ", resource()};
        what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
        co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
    }

} //namespace services::disk
