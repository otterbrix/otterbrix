#include "agent_disk.hpp"
#include "inline_scan.hpp"
#include "manager_disk.hpp"
#include <algorithm>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/physical_plan/operators/operator_hash_group.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/file/local_file_system.hpp>
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

    // DEV_MODE counter: rows shipped across the mailbox by the last pushdown reduce reply, not raw scanned rows.
#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_pushdown_reply_rows{0};
    } // namespace
    uint64_t pushdown_reply_rows() noexcept { return g_pushdown_reply_rows.load(std::memory_order_relaxed); }
    void reset_pushdown_reply_rows() noexcept { g_pushdown_reply_rows.store(0, std::memory_order_relaxed); }

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
        , dropped_storages_(resource) {
        trace(log_,
              "agent_disk::create (role={}, pool_idx={})",
              role == agent_role_t::CATALOG ? "CATALOG" : "USER_POOL",
              pool_idx);
        create_directories(path_);
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

    bool agent_disk_t::bootstrap_disk_inner_sync(
        components::catalog::oid_t oid,
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
        entry->adopt_catalog_columns(catalog_columns);
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    bool agent_disk_t::bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
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
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path, is_computed);
        if (entry->table_storage.construction_failed()) {
            warn(log_,
                 "agent_disk_t::bootstrap_create_disk_inner_sync: agent[{}] create oid={} path={} failed: {}",
                 pool_idx_,
                 static_cast<unsigned>(oid),
                 otbx_path.string(),
                 entry->table_storage.construction_error().what.c_str());
            return false;
        }
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    agent_disk_t::unique_future<bool>
    agent_disk_t::create_storage_disk_inner(components::catalog::oid_t oid,
                                            std::vector<components::table::column_definition_t> columns,
                                            std::filesystem::path otbx_path,
                                            bool is_computed) {
        std::error_code ec;
        std::filesystem::create_directories(otbx_path.parent_path(), ec);
        if (ec) {
            warn(log_,
                 "agent_disk[{}]::create_storage_disk_inner: create_directories {} failed: {}",
                 pool_idx_,
                 otbx_path.parent_path().string(),
                 ec.message());
        }
        const bool ok = bootstrap_create_disk_inner_sync(oid, std::move(columns), otbx_path, is_computed);
        if (!ok) {
            trace(log_,
                  "agent_disk[{}]::create_storage_disk_inner: oid {} already owned (path={}) — duplicate",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
        }
        co_return ok;
    }

    core::error_t agent_disk_t::no_replay_storage_error(const char* who, components::catalog::oid_t table_oid) {
        std::pmr::string msg{"agent_disk::", resource()};
        msg += std::pmr::string{who, resource()};
        msg += std::pmr::string{": no storage on the owning agent for table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        msg += std::pmr::string{" — the journalled change was NOT replayed", resource()};
        return core::error_t{core::error_code_t::io_error, std::move(msg)};
    }

    core::error_t agent_disk_t::direct_delete_sync(components::catalog::oid_t table_oid,
                                                   const std::pmr::vector<int64_t>& row_ids,
                                                   uint64_t count,
                                                   const components::table::transaction_data& txn) {
        if (row_ids.empty() && count == 0) {
            return core::error_t::no_error();
        }
        if (static_cast<uint64_t>(row_ids.size()) != count) {
            std::pmr::string what{"agent_disk::direct_delete_sync: the record counts ", resource()};
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
            return no_replay_storage_error("direct_delete_sync", table_oid);
        }
        auto& entry = it->second;
        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, row_ids[i]);
        }
        const uint64_t deleted = entry->storage->delete_rows(ids_vec, count, txn.transaction_id);
        if (deleted != count) {
            std::pmr::string what{"agent_disk::direct_delete_sync: the storage deleted ", resource()};
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

    core::error_t agent_disk_t::direct_update_sync(components::catalog::oid_t table_oid,
                                                   const std::pmr::vector<int64_t>& row_ids,
                                                   components::vector::data_chunk_t& new_data) {
        if (row_ids.empty() && new_data.size() == 0) {
            return core::error_t::no_error();
        }
        if (row_ids.size() != static_cast<std::size_t>(new_data.size())) {
            std::pmr::string what{"agent_disk::direct_update_sync: the record carries ", resource()};
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
            return no_replay_storage_error("direct_update_sync", table_oid);
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
        return entry->storage->update(ids_vec, local);
    }

    core::error_t agent_disk_t::direct_add_column_sync(components::catalog::oid_t table_oid,
                                                       const components::vector::data_chunk_t& schema_chunk) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            return no_replay_storage_error("direct_add_column_sync", table_oid);
        }
        auto& entry = it->second;
        auto* s = entry->storage.get();
        const bool is_computed_table = entry->is_computed;
        for (uint64_t col = 0; col < schema_chunk.column_count(); ++col) {
            const auto ctype = schema_chunk.data[col].type();
            if (!ctype.has_alias()) {
                continue;
            }
            const auto name = std::string(ctype.alias());
            bool present = false;
            for (const auto& tc : s->columns()) {
                if (tc.name() == name && (!is_computed_table || tc.type().type() == ctype.type())) {
                    present = true;
                    break;
                }
            }
            if (present) {
                continue;
            }
            components::table::column_definition_t def(name, ctype);
            // Must read the parked DEFAULT before take_column_identity — that call consumes it.
            if (const auto* published = entry->find_unmaterialized(name); published != nullptr) {
                def.set_default_value(published->default_value_opt());
            }
            def.set_attoid(entry->take_column_identity(name));
            entry->add_column(def, resource());
            s = entry->storage.get();
            if (s == nullptr) {
                return no_replay_storage_error("direct_add_column_sync", table_oid);
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
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_reduce_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_reduce_inner, msg);
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
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::note_column_identity_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::note_column_identity_inner, msg);
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
            default:
                break;
        }
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
    agent_disk_t::storage_append_inner(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::vector::data_chunk_t> data) {
        const auto txn = ctx.txn;
        if (!data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_append: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            std::pmr::string what{"storage_append: table oid has an empty entry on its disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto* s = entry->storage.get();
        if (s == nullptr) {
            std::pmr::string what{"storage_append: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }

        // No same-oid append can interleave: cooperative_actor won't pop the next message mid-await.
        std::vector<components::table::column_definition_t> wal_added_columns;

        const bool is_computed_table = entry->is_computed;

        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        if (s->has_schema() && data->column_count() > 0 &&
            (is_computed_table || data->column_count() != s->columns().size())) {
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
                for (auto& col : new_columns) {
                    if (const auto* published = entry->find_unmaterialized(col.name()); published != nullptr) {
                        col.set_default_value(published->default_value_opt());
                    }
                    col.set_attoid(entry->take_column_identity(col.name()));
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

        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() > 0) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name() &&
                        (!is_computed_table || data->data[col].type().type() == table_columns[t].type().type())) {
                        const auto& incoming_type = data->data[col].type();
                        const auto& stored_type = table_columns[t].type();
                        if (incoming_type != stored_type) {
                            const auto spell = [](const components::types::complex_logical_type& t_) {
                                auto spec = components::catalog::encode_type_spec(t_);
                                return spec.empty() ? std::to_string(static_cast<int>(t_.type())) : spec;
                            };
                            std::pmr::string what{"storage_append: column '", resource()};
                            what.append(table_columns[t].name().c_str());
                            what.append("' of table oid ");
                            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                            what.append(" stores type ");
                            what.append(spell(stored_type).c_str());
                            what.append(", the incoming chunk carries ");
                            what.append(spell(incoming_type).c_str());
                            what.append("; nothing was appended");
                            co_return core::error_t{core::error_code_t::schema_error, std::move(what)};
                        }
                        expanded_data.push_back(std::move(data->data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], data->size());
                    expanded_data.back().validity().set_all_invalid(data->size());
                }
            }
            data->data = std::move(expanded_data);
        }

        if (!table_columns.empty()) {
            for (size_t col = 0; col < table_columns.size() && col < data->column_count(); col++) {
                if (table_columns[col].is_not_null()) {
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!data->data[col].validity().row_is_valid(row)) {
                            std::pmr::string what{"storage_append: NOT NULL violation on column '", resource()};
                            what.append(table_columns[col].name().c_str());
                            what.append("' of table oid ");
                            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                            what.append("; nothing was appended");
                            co_return core::error_t{core::error_code_t::invalid_constraint, std::move(what)};
                        }
                    }
                }
            }
        }

        // No `_id` dedup here: uniqueness has exactly one implementation, operator_unique_constraint_t.

        // WAL-first: reserve start_row, journal, then materialize; mailbox-atomicity keeps it stable meanwhile.
        const auto actual_count = data->size();
        const uint64_t start_row = s->total_rows();

        if (txn.transaction_id != 0 && manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            const auto db_oid = (ctx.database_oid != components::catalog::INVALID_OID)
                                    ? ctx.database_oid
                                    : components::catalog::well_known_oid::main_database;

        // Sent before PHYSICAL_INSERT so replay re-adds the column first, but not awaited here — a second
        // suspending cross-actor await in one handler is a lost-wakeup.
            unique_future<core::result_wrapper_t<wal::id_t>> add_column_future;
            if (!wal_added_columns.empty()) {
                std::pmr::vector<components::types::complex_logical_type> col_types(resource());
                col_types.reserve(wal_added_columns.size());
                for (const auto& col : wal_added_columns) {
                    auto t = col.type();
                    t.set_alias(col.name());
                    col_types.push_back(t);
                }
                auto schema_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), col_types, 0);
                schema_chunk->set_cardinality(0);
                auto [_sc, scf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                              &wal::manager_wal_replicate_t::write_physical_add_column,
                                                              ctx.session,
                                                              table_oid,
                                                              std::move(schema_chunk),
                                                              static_cast<std::uint64_t>(wal_added_columns.size()),
                                                              txn.transaction_id,
                                                              db_oid);
                add_column_future = std::move(scf);
            }

            // CREATE INDEX backfill uses start_row as the row-id base, so it must match the materialized start.
            components::vector::data_chunk_t wal_chunk(resource(), data->types(), data->size());
            data->copy(wal_chunk, 0);
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            auto [_w, wf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                        &wal::manager_wal_replicate_t::write_physical_insert,
                                                        ctx.session,
                                                        table_oid,
                                                        std::move(wal_chunks),
                                                        start_row,
                                                        actual_count,
                                                        txn.transaction_id,
                                                        db_oid);
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                error(log_,
                      "agent_disk[{}]::storage_append_inner: the PHYSICAL_INSERT did not reach the journal for "
                      "oid={}, the rows are NOT appended: {}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      wal_result.error().what);
                co_return wal_result.convert_error<std::pair<uint64_t, uint64_t>>();
            }
            if (wal_result.value() == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::storage_append_inner: physical_insert WAL returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }

            if (add_column_future.valid()) {
                auto add_column_result = co_await std::move(add_column_future);
                if (add_column_result.has_error()) {
                    error(log_,
                          "agent_disk[{}]::storage_append_inner: the PHYSICAL_ADD_COLUMN did not reach the "
                          "journal for oid={}, the rows are NOT appended: {}",
                          pool_idx_,
                          static_cast<unsigned>(table_oid),
                          add_column_result.error().what);
                    co_return add_column_result.convert_error<std::pair<uint64_t, uint64_t>>();
                }
                if (add_column_result.value() == wal::id_t{}) {
                    trace(log_,
                          "agent_disk[{}]::storage_append_inner: physical_add_column WAL returned zero id for "
                          "oid={}",
                          pool_idx_,
                          static_cast<unsigned>(table_oid));
                }
            }
        }

        auto append_r =
            s->append(*data, txn.transaction_id != 0 ? txn : components::table::transaction_data{0, 0});
        if (append_r.has_error()) {
            trace(log_,
                  "agent_disk[{}]::storage_append_inner: materialize failed for oid={} — surfacing error",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return append_r.convert_error<std::pair<uint64_t, uint64_t>>();
        }
        const uint64_t materialized_start = append_r.value();
        // Checked, not asserted (NDEBUG deletes asserts) — nothing else reverts rows once operator_insert returns.
        if (materialized_start != start_row) {
            error(log_,
                  "agent_disk[{}]::storage_append_inner: oid={} reserved start_row {} for the journal but the "
                  "rows materialized at {} — the rows are REVERTED and the append is refused rather than "
                  "answered with a base the journal does not name",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  start_row,
                  materialized_start);
            s->revert_append(static_cast<int64_t>(materialized_start), actual_count);
            std::pmr::string what{"agent_disk::storage_append_inner: journalled start_row ", resource()};
            what.append(std::to_string(start_row).c_str());
            what.append(" but the rows materialized at ");
            what.append(std::to_string(materialized_start).c_str());
            what.append(" for table oid ");
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            what.append("; the rows were reverted and nothing was appended");
            co_return core::error_t{core::error_code_t::data_corruption, std::move(what)};
        }
        co_return std::make_pair(materialized_start, actual_count);
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
    agent_disk_t::storage_revert_deletes_inner(uint64_t txn_id, std::pmr::vector<components::catalog::oid_t> tables) {
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
            entry->storage->revert_all_deletes(txn_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
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
            entry->storage->revert_append(it->start_row, it->count);
        }
        co_return;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
    agent_disk_t::storage_update_inner(components::catalog::oid_t table_oid,
                                       components::vector::vector_t row_ids,
                                       std::unique_ptr<components::vector::data_chunk_t> data,
                                       components::table::transaction_data txn) {
        if (!data || data->size() == 0) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_update: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            std::pmr::string what{"storage_update: table oid has an empty entry on its disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        if (entry->storage == nullptr) {
            std::pmr::string what{"storage_update: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        co_return entry->storage->update(row_ids, *data, txn);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                            components::vector::vector_t row_ids,
                                            uint64_t count,
                                            components::table::transaction_data txn) {
        if (count == 0) {
            co_return std::uint64_t{0};
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_delete_rows: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            std::pmr::string what{"storage_delete_rows: table oid has an empty entry on its disk agent: ",
                                  resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        if (entry->storage == nullptr) {
            std::pmr::string what{"storage_delete_rows: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        if (txn.transaction_id != 0) {
            co_return entry->storage->delete_rows(row_ids, count, txn.transaction_id);
        }
        co_return entry->storage->delete_rows(row_ids, count);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::storage_fetch_inner(components::catalog::oid_t table_oid,
                                      components::vector::vector_t row_ids,
                                      uint64_t count,
                                      std::vector<size_t> projected_cols,
                                      components::table::transaction_data txn,
                                      components::table::fetch_visibility_t visibility,
                                      int64_t limit,
                                      uint64_t expected_compact_epoch) {
        std::pmr::vector<components::vector::data_chunk_t> out{resource()};
        if (count == 0) {
            co_return std::move(out);
        }
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_fetch: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            std::pmr::string what{"storage_fetch: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        if (expected_compact_epoch != k_fetch_epoch_unchecked &&
            expected_compact_epoch != entry->table_storage.table().compact_epoch()) {
            std::pmr::string what{"storage_fetch: the index answer is stale — the table was compacted "
                                  "(row ids renumbered) after the index was built and before its rebuild "
                                  "finished; retry the statement",
                                  resource()};
            co_return core::error_t{core::error_code_t::stale_index, std::move(what)};
        }
        auto types = entry->storage->types();
        const bool capped = limit >= 0;
        const uint64_t budget = capped ? static_cast<uint64_t>(limit) : 0;
        uint64_t produced = 0;
        const auto* ids = row_ids.data<int64_t>();
        for (uint64_t offset = 0; offset < count; offset += components::vector::DEFAULT_VECTOR_CAPACITY) {
            if (capped && produced >= budget) {
                break;
            }
            const uint64_t n = std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, count - offset);
            components::vector::vector_t window_ids(resource(), components::types::logical_type::BIGINT, n);
            std::memcpy(window_ids.data(), ids + offset, n * sizeof(int64_t));
            components::vector::data_chunk_t chunk(resource(), types, n);
            auto fetch_r = entry->storage->fetch(chunk, window_ids, n, projected_cols, txn, visibility);
            if (fetch_r.has_error()) {
                co_return fetch_r.convert_error<std::pmr::vector<components::vector::data_chunk_t>>();
            }
            assert(chunk.size() <= n && "storage_fetch_inner: a window produced more rows than it was asked for");
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
            return core::error_t{core::error_code_t::missing_table,
                                 std::pmr::string{"scan: storage is not owned by this agent", resource()}};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t{core::error_code_t::missing_table,
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
                                     std::unique_ptr<components::table::table_filter_t> filter,
                                     int64_t limit,
                                     std::vector<size_t> projected_cols,
                                     components::table::transaction_data txn) {
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
        co_return scan_local(table_oid, filter.get(), limit, projected_ptr, txn);
    }

    template<typename PerBatch>
    static core::error_t for_each_storage_batch(components::storage::storage_t& storage,
                                                components::storage::scan_position_t& scan_position,
                                                const components::table::table_filter_t* filter,
                                                const std::vector<std::size_t>* projected,
                                                const components::table::transaction_data& txn,
                                                std::pmr::memory_resource* resource,
                                                PerBatch&& fn) {
        auto all_types = storage.types();
        scan_position.next_row = 0;
        scan_position.max_row = static_cast<int64_t>(storage.total_rows());
        while (!scan_position.drained && scan_position.next_row < scan_position.max_row) {
            components::vector::data_chunk_t batch =
                projected ? components::vector::data_chunk_t{resource,
                                                             all_types,
                                                             *projected,
                                                             components::vector::DEFAULT_VECTOR_CAPACITY}
                          : components::vector::data_chunk_t{resource,
                                                             all_types,
                                                             components::vector::DEFAULT_VECTOR_CAPACITY};
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

    static core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    reduce_pushed_aggregate(std::pmr::memory_resource* resource,
                            log_t log,
                            components::storage::storage_t* storage,
                            session_id_t session,
                            actor_zeta::address_t self_address,
                            const components::table::table_filter_t* filter,
                            const std::vector<std::size_t>& projected_cols,
                            const components::table::transaction_data& txn,
                            const components::operators::pushed_aggregate_spec_t& spec) {
        namespace ops = components::operators;
        std::pmr::vector<components::vector::data_chunk_t> out{resource};

        components::compute::function_registry_t reg{resource};
        components::compute::register_default_functions(reg);

        // No HAVING / DISTINCT / computed columns — the optimizer never stamps those.
        ops::operator_hash_group_t group{resource, log.clone()};
        for (const auto& gk : spec.group_keys) {
            ops::group_key_t key{resource};
            key.name.assign(gk.name.begin(), gk.name.end());
            key.type = ops::group_key_t::kind::column;
            key.full_path.assign(gk.path.begin(), gk.path.end());
            group.add_key(std::move(key));
        }
        for (const auto& agg : spec.aggregates) {
            group.add_value(agg.alias, agg.result_type);
        }
        for (const auto& output : spec.outputs) {
            group.add_output(output);
        }
        group.set_input_types(spec.input_types);
        group.set_output_types(spec.output_types);

        components::logical_plan::storage_parameters params{resource};
        components::pipeline::context_t ctx{session,
                                            self_address,
                                            actor_zeta::address_t::empty_address(),
                                            &reg,
                                            params,
                                            components::pipeline::no_mailbox(),
                                            components::pipeline::no_mailbox(),
                                            components::pipeline::no_mailbox()};
        ctx.txn = txn;

        if (storage != nullptr) {
            const std::vector<std::size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
            components::storage::scan_position_t pos{};
            ops::chunks_vector_t sink{resource};
            if (auto err = for_each_storage_batch(
                    *storage,
                    pos,
                    filter,
                    projected_ptr,
                    txn,
                    resource,
                    [&](components::vector::data_chunk_t& batch) { return group.push(&ctx, std::move(batch), sink); });
                err.contains_error()) {
                return err;
            }
        }

        if (auto err = group.finalize(&ctx, out); err.contains_error()) {
            return err;
        }
        return out;
    }

    // Only the resume position is stored across ADVANCE calls; no pin survives.
    agent_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    agent_disk_t::storage_fetch_next_batch_inner(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t cursor_id,
                                                 std::unique_ptr<components::table::table_filter_t> filter,
                                                 int64_t limit,
                                                 std::vector<size_t> projected_cols,
                                                 components::table::transaction_data txn) {
        auto make_drained = [this](uint64_t reply_cursor_id) -> fetch_batch_t {
            auto empty = std::make_unique<components::vector::data_chunk_t>(
                resource(),
                std::pmr::vector<components::types::complex_logical_type>{resource()},
                components::vector::DEFAULT_VECTOR_CAPACITY);
            empty->set_cardinality(0);
            return fetch_batch_t{std::move(empty), reply_cursor_id};
        };

        auto collect_identity = [](collection_storage_entry_t& entry,
                                   std::vector<active_scan_t::open_column_t>& out) {
            const auto& physical = entry.table_storage.table().columns();
            out.clear();
            out.reserve(physical.size() + entry.unmaterialized_columns.size());
            for (const auto& c : physical) {
                out.push_back(active_scan_t::open_column_t{c.attoid(), c.name()});
            }
            for (const auto& c : entry.unmaterialized_columns) {
                out.push_back(active_scan_t::open_column_t{c.attoid(), c.name()});
            }
        };

        if (cursor_id == 0) {
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                std::pmr::string what{"storage_fetch_next_batch: no materialized storage to open a scan on: ",
                                      resource()};
                what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
            }
            active_scan_t scan{};
            scan.table_oid = table_oid;
            scan.pos.next_row = 0;
            scan.pos.max_row = static_cast<int64_t>(it->second->storage->total_rows());
            scan.filter = std::move(filter);
            scan.projected_cols = std::move(projected_cols);
            scan.txn = txn;
            scan.matched_limit = limit;
            {
                const auto open_types = it->second->storage->types();
                scan.open_types.assign(open_types.begin(), open_types.end());
                collect_identity(*it->second, scan.open_columns);
            }
            const uint64_t counter = next_scan_cursor_id_++;
            const uint64_t minted = (session.data() << 20) ^ counter;
            cursor_id = (minted == 0 || active_scans_.find(minted) != active_scans_.end()) ? counter : minted;
            active_scans_.try_emplace(cursor_id, std::move(scan));
        }

        auto cit = active_scans_.find(cursor_id);
        if (cit == active_scans_.end()) {
            co_return make_drained(cursor_id);
        }
        auto& scan = cit->second;

        if (scan.pos.drained ||
            (scan.matched_limit >= 0 && scan.matched_emitted >= static_cast<uint64_t>(scan.matched_limit))) {
            active_scans_.erase(cit);
            co_return make_drained(cursor_id);
        }

        auto storage_it = storages_.find(table_oid);
        if (storage_it == storages_.end() || storage_it->second == nullptr || storage_it->second->storage == nullptr) {
            active_scans_.erase(cit);
            std::pmr::string what{"storage_fetch_next_batch: the table was dropped under an open cursor: ",
                                  resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::table_dropped, std::move(what)};
        }
        auto* storage = storage_it->second->storage.get();

        auto all_types = storage->types();
        const bool schema_unchanged = [&] {
            const auto& physical = storage_it->second->table_storage.table().columns();
            const auto& unmaterialized = storage_it->second->unmaterialized_columns;
            if (physical.size() + unmaterialized.size() != scan.open_columns.size() ||
                all_types.size() != scan.open_types.size()) {
                return false;
            }
            for (size_t i = 0; i < scan.open_columns.size(); ++i) {
                const auto& live = i < physical.size() ? physical[i] : unmaterialized[i - physical.size()];
                if (live.attoid() != scan.open_columns[i].attoid || live.name() != scan.open_columns[i].name ||
                    !(all_types[i] == scan.open_types[i])) {
                    return false;
                }
            }
            return true;
        }();

        std::vector<size_t> remapped_projected;
        std::vector<std::pair<size_t, size_t>> reslot;
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
                return core::error_t{core::error_code_t::schema_error, std::move(msg)};
            };

            // Identity is attoid when both sides carry one (survives RENAME), else (name, type); `gone` = no home.
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
                    co_return refuse("a column's identity is ambiguous after the schema change");
                }
                if (found != gone && !(all_types[found] == scan.open_types[o])) {
                    found = gone;
                }
                open_to_current[o] = found;
            }

            if (scan.filter != nullptr) {
                for (size_t o = 0; o < open_to_current.size(); ++o) {
                    if (open_to_current[o] != o) {
                        co_return refuse("the cursor's filter is bound to column positions the change moved");
                    }
                }
            }

            reslot.reserve(scan.projected_cols.empty() ? scan.open_columns.size() : scan.projected_cols.size());
            remapped_projected.reserve(reslot.capacity());
            auto need_slot = [&](size_t slot) -> bool {
                if (slot >= open_to_current.size() || open_to_current[slot] == gone) {
                    return false;
                }
                remapped_projected.push_back(open_to_current[slot]);
                reslot.emplace_back(slot, open_to_current[slot]);
                return true;
            };
            if (scan.projected_cols.empty()) {
                for (size_t slot = 0; slot < scan.open_columns.size(); ++slot) {
                    if (!need_slot(slot)) {
                        co_return refuse("a column this cursor serves was dropped from the storage");
                    }
                }
            } else {
                for (size_t slot : scan.projected_cols) {
                    if (!need_slot(slot)) {
                        co_return refuse("a column this cursor serves was dropped from the storage");
                    }
                }
            }
        }

        const std::vector<size_t>* projected_ptr =
            schema_unchanged ? (scan.projected_cols.empty() ? nullptr : &scan.projected_cols) : &remapped_projected;
        auto batch =
            projected_ptr
                ? std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                     all_types,
                                                                     *projected_ptr,
                                                                     components::vector::DEFAULT_VECTOR_CAPACITY)
                : std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                     all_types,
                                                                     components::vector::DEFAULT_VECTOR_CAPACITY);
        auto fetch_r = storage->fetch_next_batch(*batch, scan.pos, scan.filter.get(), projected_ptr, scan.txn);
        if (fetch_r.has_error()) {
            active_scans_.erase(cit);
            co_return fetch_r.convert_error<fetch_batch_t>();
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
            co_return make_drained(cursor_id);
        }

        if (!schema_unchanged) {
            std::pmr::vector<components::types::complex_logical_type> open_types{resource()};
            open_types.assign(scan.open_types.begin(), scan.open_types.end());
            auto reshaped =
                scan.projected_cols.empty()
                    ? std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                         open_types,
                                                                         components::vector::DEFAULT_VECTOR_CAPACITY)
                    : std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                         open_types,
                                                                         scan.projected_cols,
                                                                         components::vector::DEFAULT_VECTOR_CAPACITY);
            for (const auto& [open_slot, current_ordinal] : reslot) {
                reshaped->data[open_slot].reference(batch->data[current_ordinal]);
            }
            reshaped->set_cardinality(batch->size());
            batch = std::move(reshaped);
        }
        co_return fetch_batch_t{std::move(batch), cursor_id};
    }

    agent_disk_t::unique_future<void> agent_disk_t::storage_close_cursor_inner(session_id_t /*session*/,
                                                                               components::catalog::oid_t table_oid,
                                                                               uint64_t cursor_id) {
        trace(log_, "agent_disk[{}]::storage_close_cursor_inner: oid={} cursor={}", pool_idx_,
              static_cast<unsigned>(table_oid), cursor_id);
        active_scans_.erase(cursor_id);
        co_return;
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_open_scan_hold_inner(session_id_t session, components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"storage_open_scan_hold: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        active_scan_t hold{};
        hold.table_oid = table_oid;
        const uint64_t counter = next_scan_cursor_id_++;
        const uint64_t minted = (session.data() << 20) ^ counter;
        const uint64_t hold_id =
            (minted == 0 || active_scans_.find(minted) != active_scans_.end()) ? counter : minted;
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
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        co_return it->second->table_storage.table().compact_epoch();
    }

    // Valid only while one agent owns the whole table — no cross-agent partial merge.
    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::storage_reduce_inner(session_id_t session,
                                       components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::table::table_filter_t> filter,
                                       std::vector<size_t> projected_cols,
                                       components::table::transaction_data txn,
                                       components::operators::pushed_aggregate_spec_t spec) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string what{"storage_reduce: no materialized storage to reduce over: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto reduced_r = reduce_pushed_aggregate(resource(),
                                                 log_.clone(),
                                                 it->second->storage.get(),
                                                 session,
                                                 address(),
                                                 filter.get(),
                                                 projected_cols,
                                                 txn,
                                                 spec);
        if (reduced_r.has_error()) {
            co_return reduced_r;
        }
#ifdef DEV_MODE
        {
            uint64_t reply_rows = 0;
            for (const auto& c : reduced_r.value()) {
                reply_rows += c.size();
            }
            g_pushdown_reply_rows.fetch_add(reply_rows, std::memory_order_relaxed);
        }
#endif
        co_return reduced_r;
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
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"fk semi-join: no key columns given", resource}};
        }
        if (keys.column_count() != key_col_indices.size()) {
            return core::error_t{
                core::error_code_t::invalid_parameter,
                std::pmr::string{"fk semi-join: key chunk arity does not match key columns", resource}};
        }

        const auto& cols = storage.columns();

        std::pmr::vector<components::types::complex_logical_type> stored_key_types{resource};
        stored_key_types.reserve(key_col_indices.size());
        for (auto ci : key_col_indices) {
            stored_key_types.push_back(cols[ci].type());
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
                auto probe = components::vector::vector_ops::cast_vector(resource, null_probe,
                                                                          stored_key_types[j], 1);
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
        std::vector<std::uint64_t> scan_col_ids(key_col_indices.begin(), key_col_indices.end());
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
                for (auto ci : key_col_indices) {
                    auto& col = batch.data[ci];
                    if (col.get_vector_type() != components::vector::vector_type::FLAT) {
                        col.flatten(rows);
                    }
                }
                components::vector::vector_t row_hash_vec(resource, components::types::logical_type::UBIGINT, rows);
                batch.hash(scan_col_ids, row_hash_vec);
                const auto* row_hashes = row_hash_vec.data<std::uint64_t>();
                const auto* row_ids = batch.row_ids.data<std::int64_t>();
                for (uint64_t r = 0; r < rows; ++r) {
                    bool any_null = false;
                    for (auto ci : key_col_indices) {
                        if (batch.data[ci].is_null(r)) {
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
                            if (!components::vector::cells_equal(norm_keys.data[j],
                                                                 cand,
                                                                 batch.data[key_col_indices[j]],
                                                                 r)) {
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

    // A failure here must not collapse into an empty result, or a misrouted/corrupt read surfaces as
    // "Database does not exist".
    static core::error_t resolve_key_col_indices(const collection_storage_entry_t* entry,
                                                 const std::pmr::vector<std::string>& key_col_names,
                                                 std::pmr::vector<std::uint64_t>& out_indices,
                                                 std::pmr::memory_resource* resource) {
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t{core::error_code_t::missing_table,
                                 std::pmr::string{"keyed read: storage is not owned by this agent", resource}};
        }
        if (key_col_names.empty()) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"keyed read: no key columns given", resource}};
        }
        const auto& cols = entry->storage->columns();
        out_indices.reserve(key_col_names.size());
        for (const auto& kname : key_col_names) {
            std::size_t col_idx = cols.size();
            for (std::size_t ci = 0; ci < cols.size(); ++ci) {
                if (cols[ci].name() == kname) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == cols.size()) {
                std::pmr::string what{"keyed read: table has no column ", resource};
                what.append(kname.c_str());
                return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
            }
            out_indices.push_back(static_cast<std::uint64_t>(col_idx));
        }
        return core::error_t::no_error();
    }

    static core::error_t validate_key_col_indices(const collection_storage_entry_t* entry,
                                                  const std::pmr::vector<std::uint64_t>& key_col_indices,
                                                  std::pmr::memory_resource* resource) {
        if (entry == nullptr || entry->storage == nullptr) {
            return core::error_t{core::error_code_t::missing_table,
                                 std::pmr::string{"keyed read: storage is not owned by this agent", resource}};
        }
        if (key_col_indices.empty()) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"keyed read: no key columns given", resource}};
        }
        const auto ncols = entry->storage->columns().size();
        for (const auto idx : key_col_indices) {
            if (idx >= ncols) {
                return core::error_t{core::error_code_t::invalid_parameter,
                                     std::pmr::string{"keyed read: key column index out of range", resource}};
            }
        }
        return core::error_t::no_error();
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
        if (auto resolved = resolve_key_col_indices(entry, key_col_names, key_col_indices, resource());
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
        // `keys` already IS the 1-row batch read_chunks_by_keys_inner expects; this co_await is a local call.
        std::pmr::vector<components::vector::data_chunk_t> empty{resource()};
        auto r = co_await read_chunks_by_keys_inner(table_oid,
                                                    std::move(key_col_indices),
                                                    std::move(keys),
                                                    std::move(projected_cols),
                                                    txn);
        if (r.has_error()) {
            co_return r.error();
        }
        co_return r.value().empty() ? std::move(empty) : std::move(r.value()[0]);
    }

    agent_disk_t::unique_future<
        core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
    agent_disk_t::read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                            std::pmr::vector<std::uint64_t> key_col_indices,
                                            components::vector::data_chunk_t keys,
                                            std::pmr::vector<std::uint64_t> projected_cols,
                                            components::table::transaction_data txn) {
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> result{resource()};
        result.reserve(keys.size());

        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        if (auto valid = validate_key_col_indices(entry, key_col_indices, resource()); valid.contains_error()) {
            co_return valid;
        }

        const std::uint64_t nkeys = keys.size();
        if (keys.column_count() != key_col_indices.size()) {
            co_return core::error_t{
                core::error_code_t::invalid_parameter,
                std::pmr::string{"keyed read: key chunk arity does not match key columns", resource()}};
        }
    // The key tuple becomes a pushed-WHERE graph (OR'd across keys), so N keys cost ONE scan.
        namespace expr = components::expressions;
        const std::size_t narity = key_col_indices.size();

        if (nkeys * narity > std::numeric_limits<std::uint16_t>::max()) {
            co_return core::error_t{core::error_code_t::invalid_parameter,
                                    std::pmr::string{"keyed read: key batch too large to bind", resource()}};
        }

        auto make_eq_child = [&](std::size_t col, std::uint16_t param_id) {
            expr::key_t column{resource()};
            column.set_path(
                std::pmr::vector<size_t>{{key_col_indices[col]}, std::pmr::polymorphic_allocator<size_t>{resource()}});
            return expr::make_compare_expression(resource(),
                                                 expr::compare_type::eq,
                                                 column,
                                                 core::parameter_id_t{param_id});
        };

        std::pmr::vector<std::unique_ptr<components::table::table_filter_t>> key_filters{resource()};
        key_filters.reserve(nkeys);
        components::types::parameter_map_t all_parameters{resource()};
        auto all_predicate = expr::make_compare_union_expression(resource(),
                                                                 nkeys == 1 ? expr::compare_type::union_and
                                                                            : expr::compare_type::union_or);

        for (std::uint64_t i = 0; i < nkeys; ++i) {
            components::types::parameter_map_t key_parameters{resource()};
            auto key_predicate = expr::make_compare_union_expression(resource(), expr::compare_type::union_and);
            auto tuple_predicate =
                nkeys == 1 ? nullptr : expr::make_compare_union_expression(resource(), expr::compare_type::union_and);
            for (std::size_t ki = 0; ki < narity; ++ki) {
                const auto cell = keys.value(ki, i);
                key_parameters.emplace(core::parameter_id_t{static_cast<std::uint16_t>(ki)}, cell);
                const auto global_id = static_cast<std::uint16_t>(i * narity + ki);
                all_parameters.emplace(core::parameter_id_t{global_id}, cell);
                key_predicate->append_child(make_eq_child(ki, static_cast<std::uint16_t>(ki)));
                if (tuple_predicate) {
                    tuple_predicate->append_child(make_eq_child(ki, global_id));
                } else {
                    all_predicate->append_child(make_eq_child(ki, global_id));
                }
            }
            if (tuple_predicate) {
                all_predicate->append_child(std::move(tuple_predicate));
            }
            auto key_built =
                expr::build_condition_graph(resource(), key_parameters, key_predicate.get(), entry->storage->types());
            if (key_built.has_error()) {
                co_return key_built.error();
            }
            key_filters.emplace_back(
                std::make_unique<components::table::table_filter_t>(std::move(key_parameters),
                                                                    components::graph_execution_context{},
                                                                    std::move(key_built.value()),
                                                                    expr::condition_kind::computed));
            result.emplace_back();
        }

        std::unique_ptr<components::table::table_filter_t> scan_filter;
        if (nkeys == 1) {
            scan_filter = std::move(key_filters.front());
        } else {
            auto all_built =
                expr::build_condition_graph(resource(), all_parameters, all_predicate.get(), entry->storage->types());
            if (all_built.has_error()) {
                co_return all_built.error();
            }
            scan_filter = std::make_unique<components::table::table_filter_t>(std::move(all_parameters),
                                                                              components::graph_execution_context{},
                                                                              std::move(all_built.value()),
                                                                              expr::condition_kind::computed);
        }

#ifdef DEV_MODE
        g_catalog_key_scans.fetch_add(1, std::memory_order_relaxed);
#endif
        std::vector<std::size_t> scan_projection(projected_cols.begin(), projected_cols.end());
        for (const auto key_col : key_col_indices) {
            if (!scan_projection.empty() &&
                std::find(scan_projection.begin(), scan_projection.end(), key_col) == scan_projection.end()) {
                scan_projection.push_back(static_cast<std::size_t>(key_col));
            }
        }
        auto scan_r = scan_local(table_oid,
                                 scan_filter.get(),
                                 int64_t{-1},
                                 scan_projection.empty() ? nullptr : &scan_projection,
                                 txn);
        if (scan_r.has_error()) {
            co_return scan_r.error();
        }
        auto matched = std::move(scan_r.value());

        if (nkeys == 1) {
            result[0] = std::move(matched);
            co_return std::move(result);
        }

        for (auto& chunk : matched) {
            const auto rows = chunk.size();
            if (rows == 0) {
                continue;
            }
            const auto chunk_types = chunk.types();
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
                components::vector::data_chunk_t out =
                    scan_projection.empty()
                        ? components::vector::data_chunk_t{resource(), chunk_types, count}
                        : components::vector::data_chunk_t{resource(), chunk_types, scan_projection, count};
                for (std::size_t c = 0; c < chunk.column_count(); ++c) {
                    if (out.data[c].data() == nullptr && out.data[c].auxiliary() == nullptr) {
                        continue;
                    }
                    components::vector::vector_ops::copy(chunk.data[c], out.data[c], selected, count, 0, 0);
                }
                components::vector::vector_ops::copy(chunk.row_ids, out.row_ids, selected, count, 0, 0);
                out.set_cardinality(count);
                result[i].push_back(std::move(out));
            }
        }
        co_return std::move(result);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
    agent_disk_t::storage_types_inner(components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_types: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            std::pmr::string what{"storage_types: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        co_return entry->storage->types();
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_total_rows_inner(components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            std::pmr::string what{"storage_total_rows: table oid is not owned by this disk agent: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            std::pmr::string what{"storage_total_rows: table oid has no materialized storage: ", resource()};
            what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        co_return entry->storage->total_rows();
    }

        // Staging (write+fsync) runs before write_header commits, since a crash mid-write risks a
        // zero-length sidecar.
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
        void discard_staged_checkpoint_sidecar(log_t& log,
                                               std::size_t pool_idx,
                                               const std::filesystem::path& otbx_path) {
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

        // Kept separate from publish: a refused rename SPLITS the durable floor, while a refused directory
        // fsync only risks surfacing the previous id on crash — a much milder failure.
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
        // Order matters: free refusals exit before the device is touched, then stage, compact, checkpoint's
        // header commit — only the sidecar rename/fsync run after that point.
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

            // compact() always fully rebuilds — measured 205.7ms for an empty round over 100 tables
            // vs 124.4ms rewritten.
            const bool unchanged = !entry->table_storage.needs_checkpoint();
            bool skip_compact_this_round = false;
            if (!unchanged) {
                skip_compact_this_round =
                    entry->table_storage.last_checkpoint_failed() && !entry->table_storage.has_pending_update_overlay();
                if (skip_compact_this_round) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner oid={} previous checkpoint failed — retrying WITHOUT "
                         "compaction; the rebuild resumes once a checkpoint commits",
                         pool_idx_,
                         static_cast<unsigned>(tbl_oid));
                } else if (entry->table_storage.last_checkpoint_failed()) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner oid={} previous checkpoint failed, but the table "
                         "carries a committed-update overlay — rebuilding anyway, because only the rebuild "
                         "folds it into the segments a checkpoint can write",
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

            // An uncommitted compact() only spends space — measured +2.9 MB per VACUUM on an
            // unchanged 12k-row table. Deferred to checkpoint_inner, not lost — measured 13053
            // DISK rounds performed 12962 compacts; test_s3_cleanup_scaling keeps 700000 rows with
            // only 149988 live when compact is disabled.
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

    agent_disk_t::unique_future<void> agent_disk_t::storage_drop_aborted_inner(uint64_t txn_id) {
        for (auto it = dropped_storages_.begin(); it != dropped_storages_.end();) {
            if (it->dropped_at_commit_id == txn_id) {
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
        };

        const catalog_name_key_t* catalog_name_key_for(components::catalog::oid_t table_oid) {
            namespace cat = components::catalog;
            static constexpr catalog_name_key_t pg_class_key{cat::pg_class_col::relname,
                                                             static_cast<int64_t>(cat::pg_class_col::relnamespace),
                                                             "relname",
                                                             "relnamespace",
                                                             core::error_code_t::table_already_exists,
                                                             "relation"};
            static constexpr catalog_name_key_t pg_namespace_key{cat::pg_namespace_col::nspname,
                                                                 int64_t{-1},
                                                                 "nspname",
                                                                 nullptr,
                                                                 core::error_code_t::database_already_exists,
                                                                 "database"};
            static constexpr catalog_name_key_t pg_type_key{cat::pg_type_col::typname,
                                                            static_cast<int64_t>(cat::pg_type_col::typnamespace),
                                                            "typname",
                                                            "typnamespace",
                                                            core::error_code_t::type_already_exists,
                                                            "type"};
            switch (table_oid) {
                case cat::well_known_oid::pg_class_table:
                    return &pg_class_key;
                case cat::well_known_oid::pg_namespace_table:
                    return &pg_namespace_key;
                case cat::well_known_oid::pg_type_table:
                    return &pg_type_key;
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
            if (row.column_count() == def->columns.size()) {
                in_name_col = static_cast<int64_t>(key.name_col);
                in_ns_col = key.ns_col;
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
                    }
                }
            }
            if (in_name_col < 0) {
                return core::error_t::no_error();
            }

            auto& table = entry.table_storage.table();
            const uint64_t total = entry.storage->total_rows();
            const auto types = entry.storage->types();
            std::vector<size_t> projected{static_cast<size_t>(key.name_col)};
            if (key.ns_col >= 0) {
                projected.push_back(static_cast<size_t>(key.ns_col));
            }

            for (uint64_t in_r = 0; in_r < row.size(); in_r++) {
                if (row.is_null(static_cast<uint64_t>(in_name_col), in_r)) {
                    continue;
                }
                const auto in_name = row.get_value<std::string_view>(static_cast<uint64_t>(in_name_col), in_r);
                const bool has_ns = key.ns_col >= 0 && in_ns_col >= 0 &&
                                    !row.is_null(static_cast<uint64_t>(in_ns_col), in_r);
                const std::uint32_t in_ns =
                    has_ns ? row.get_value<std::uint32_t>(static_cast<uint64_t>(in_ns_col), in_r) : 0;

                for (uint64_t offset = 0; offset < total; offset += components::vector::DEFAULT_VECTOR_CAPACITY) {
                    const uint64_t n =
                        std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, total - offset);
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
                        if (chunk.is_null(key.name_col, i)) {
                            continue;
                        }
                        if (chunk.get_value<std::string_view>(key.name_col, i) != in_name) {
                            continue;
                        }
                        if (has_ns && !chunk.is_null(static_cast<uint64_t>(key.ns_col), i) &&
                            chunk.get_value<std::uint32_t>(static_cast<uint64_t>(key.ns_col), i) != in_ns) {
                            continue;
                        }
                        const uint64_t stamp = table.row_group()->delete_stamp(got_ids[i]);
                        if (name_freed_by_delete(stamp, txn.transaction_id)) {
                            continue;
                        }
                        std::pmr::string msg{key.kind, resource};
                        msg += " '";
                        msg.append(in_name.data(), in_name.size());
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

        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            components::vector::data_chunk_t wal_chunk(resource(), row.types(), row.size());
            wal_chunk.set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk.data[col].set_value(r, row.data[col].value(r));
                }
            }
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                        &wal::manager_wal_replicate_t::write_physical_insert,
                                                        ctx.session,
                                                        table_oid,
                                                        std::move(wal_chunks),
                                                        std::uint64_t{0},
                                                        static_cast<std::uint64_t>(row.size()),
                                                        ctx.txn.transaction_id,
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
                        for (uint64_t r = 0; r < local.size(); r++) {
                            if (src_vec.validity().row_is_valid(r)) {
                                auto casted_val = src_vec.value(r).cast_as(target_type, ctx.session_tz);
                                if (casted_val.has_error()) {
                                    co_return casted_val
                                        .convert_error<components::pg_catalog_append_range_t>();
                                }
                                casted.set_value(r, casted_val.value());
                            } else {
                                casted.validity().set_invalid(r);
                            }
                        }
                        local.data[i] = std::move(casted);
                    }
                }
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
                            [&, oid_col_idx](components::vector::data_chunk_t& chunk, uint64_t i) {
                                if (chunk.is_null(static_cast<uint64_t>(oid_col_idx), i))
                                    return true;
                                auto v = chunk.get_value<std::uint32_t>(static_cast<uint64_t>(oid_col_idx), i);
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
        if (auto del_err = direct_delete_sync(table_oid, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
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

    agent_disk_t::unique_future<core::error_t>
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
            co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
        }
        auto& entry = it->second;

        auto& tbl = entry->table_storage.table();
        const std::size_t col_count = tbl.column_count();
        std::vector<std::int64_t> all_col_indices;
        all_col_indices.reserve(col_count);
        for (std::size_t i = 0; i < col_count; ++i) {
            all_col_indices.push_back(static_cast<std::int64_t>(i));
        }

        core::pmr::otterbrix_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        std::pmr::vector<components::types::logical_value_t> row_values(resource());
        row_values.reserve(col_count);

        detail::inline_scan(tbl,
                            all_col_indices,
                            &scan_resource,
                            ctx.txn,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                if (chunk.is_null(0, i))
                                    return true;
                                if (static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i)) !=
                                    attoid)
                                    return true;
                                row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                                for (std::size_t c = 0; c < col_count; ++c) {
                                    row_values.push_back(chunk.value(static_cast<uint64_t>(c), i));
                                }
                                return false;
                            });
        if (row_ids.empty()) {
            std::pmr::string what{"update_pg_attribute_commit_id_field: no pg_attribute row visible to this "
                                  "transaction carries attoid ",
                                  resource()};
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append("; its commit_id stamp was not applied");
            co_return core::error_t{core::error_code_t::do_not_exists, std::move(what)};
        }
        if (row_values.size() != col_count) {
            std::pmr::string what{"update_pg_attribute_commit_id_field: the scan returned ", resource()};
            what.append(std::to_string(row_values.size()).c_str());
            what.append(" value(s) for a pg_attribute row of ");
            what.append(std::to_string(col_count).c_str());
            what.append(" column(s) (attoid ");
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append("); refusing to write a partially-initialised patch");
            co_return core::error_t{core::error_code_t::schema_error, std::move(what)};
        }

        // Patch the target column: 10 = added_at_commit_id, 11 = dropped_at_commit_id.
        const std::size_t patch_col_idx =
            (kind == components::pg_attribute_commit_id_backfill_t::kind_t::added_at) ? 10u : 11u;
        if (patch_col_idx >= row_values.size()) {
            std::pmr::string what{"update_pg_attribute_commit_id_field: pg_attribute row for attoid ",
                                  resource()};
            what.append(std::to_string(static_cast<unsigned>(attoid)).c_str());
            what.append(" has no commit_id column at index ");
            what.append(std::to_string(patch_col_idx).c_str());
            what.append(" (row width ");
            what.append(std::to_string(row_values.size()).c_str());
            what.append(")");
            co_return core::error_t{core::error_code_t::schema_error, std::move(what)};
        }
        row_values[patch_col_idx] =
            components::types::logical_value_t(resource(), static_cast<std::int64_t>(commit_id));

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
        for (std::size_t c = 0; c < table_columns.size() && c < row_values.size(); ++c) {
            if (row_values[c].is_null()) {
                patch.data[c].validity().set_invalid(0);
            } else {
                patch.data[c].set_value(0, row_values[c]);
            }
        }

        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            components::vector::data_chunk_t wal_chunk(resource(), chunk_types, 1);
            wal_chunk.set_cardinality(1);
            for (std::size_t c = 0; c < table_columns.size() && c < row_values.size(); ++c) {
                if (row_values[c].is_null()) {
                    wal_chunk.data[c].validity().set_invalid(0);
                } else {
                    wal_chunk.data[c].set_value(0, row_values[c]);
                }
            }
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            std::pmr::vector<std::int64_t> wal_row_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::otterbrix::send(manager_wal_addr_,
                                                        &wal::manager_wal_replicate_t::write_physical_update,
                                                        ctx.session,
                                                        pg_attr_oid,
                                                        std::move(wal_row_ids),
                                                        std::move(wal_chunks),
                                                        static_cast<std::uint64_t>(row_ids.size()),
                                                        ctx.txn.transaction_id,
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

        if (auto upd_err = direct_update_sync(pg_attr_oid, row_ids, patch); upd_err.contains_error()) {
            error(log_,
                  "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: the slice it had just read "
                  "refused the update: {}",
                  pool_idx_,
                  upd_err.what);
            co_return std::move(upd_err);
        }
        co_return core::error_t::no_error();
    }

        // SUBTRACTIVE: drops every column not in live_attnames — a gap in the caller's derivation
        // drops a SURVIVING column.
    agent_disk_t::unique_future<std::uint64_t>
    agent_disk_t::compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                  std::set<std::string> live_attnames) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            co_return 0;
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
            if (entry->drop_column(attname, resource())) {
                ++dropped;
            } else {
                trace(log_,
                      "agent_disk[{}]::compact_relkind_g_storage_inner: oid {} column '{}' not found",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      attname);
            }
        }
        co_return dropped;
    }

    // Runs only after the WAL commit marker + ProcArray barrier — the rebuild is irreversible.
    agent_disk_t::unique_future<core::result_wrapper_t<bool>>
    agent_disk_t::drop_storage_column_inner(components::catalog::oid_t table_oid, std::string attname) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            std::pmr::string msg{"agent_disk::drop_storage_column: no materialized storage for table oid ",
                                 resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
        }
        const bool dropped = it->second->drop_column(attname, resource());
        trace(log_,
              "agent_disk[{}]::drop_storage_column_inner: oid={} column='{}' {}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              attname,
              dropped ? "dropped" : "absent from the storage schema — nothing physical to release");
        co_return core::result_wrapper_t<bool>(dropped);
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
            co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
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

    agent_disk_t::unique_future<void>
    agent_disk_t::note_column_identity_inner(components::catalog::oid_t table_oid,
                                             std::string attname,
                                             std::uint32_t attoid,
                                             components::pg_attribute_commit_id_backfill_t::added_column_type_t type) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr) {
            trace(log_,
                  "agent_disk[{}]::note_column_identity_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        // Decoded on this agent's resource(): logical_value_t's copy ctor carries the source resource pointer,
        // so a caller-local arena would dangle. Loud, not fatal — every later load re-derives the value.
        std::optional<components::types::logical_value_t> default_value;
        if (!type.default_spec.empty()) {
            auto ec =
                components::catalog::decode_default_spec(resource(), type.type, type.default_spec, default_value);
            if (ec.contains_error()) {
                error(log_,
                      "agent_disk[{}]::note_column_identity_inner: oid {} column '{}' attdefspec is unreadable "
                      "({}); the column is published WITHOUT its DEFAULT and reads NULL until the next load",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      attname,
                      ec.what.c_str());
                default_value.reset();
            }
        }
        it->second->note_column_identity(std::move(attname), attoid, type.type, default_value);
        co_return;
    }

} //namespace services::disk
