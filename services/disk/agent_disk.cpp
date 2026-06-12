#include "agent_disk.hpp"
#include "manager_disk.hpp"
#include "inline_scan.hpp" // services::disk::detail::inline_scan (catalog DDL on the agent)
#include <services/dispatcher/dispatcher.hpp>
#include <fstream>
#include <unordered_set>

namespace services::disk {

    using namespace core::filesystem;

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource,
                               manager_disk_t* manager,
                               const path_t& path_db,
                               log_t& log)
        : agent_disk_t(resource, manager, path_db, log, agent_role_t::CATALOG, 0) {}

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource,
                               manager_disk_t* /*manager*/,
                               const path_t& path_db,
                               log_t& log,
                               agent_role_t role,
                               std::size_t pool_idx)
        : actor_zeta::basic_actor<agent_disk_t>(resource)
        , log_(log.clone())
        , path_(path_db)
        , fs_(core::filesystem::local_file_system_t())
        , file_wal_id_(nullptr)
        , role_(role)
        , pool_idx_(pool_idx)
        , storages_(resource)
        , dropped_storages_(resource) {
        trace(log_,
              "agent_disk::create (role={}, pool_idx={})",
              role == agent_role_t::CATALOG ? "CATALOG" : "USER_POOL",
              pool_idx);
        create_directories(path_);
        file_wal_id_ = open_file(fs_,
                                 path_ / "WAL_ID",
                                 file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                 file_lock_type::NO_LOCK);
    }

    agent_disk_t::~agent_disk_t() { trace(log_, "delete agent_disk_t"); }

    bool agent_disk_t::has_storage_sync(components::catalog::oid_t oid) const noexcept {
        return storages_.find(oid) != storages_.end();
    }

    // Borrowed pointer — see header. nullptr when the OID isn't owned.
    const collection_storage_entry_t*
    agent_disk_t::storage_entry_sync(components::catalog::oid_t oid) const noexcept {
        auto it = storages_.find(oid);
        if (it == storages_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    bool agent_disk_t::bootstrap_inner_sync(components::catalog::oid_t oid,
                                            std::unique_ptr<collection_storage_entry_t> entry) noexcept {
        if (entry == nullptr) {
            return false;
        }
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    bool agent_disk_t::bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                                  const std::filesystem::path& otbx_path,
                                                  wal::id_t sidecar_wal_id) noexcept {
        // Probe BEFORE constructing the SFBM: on a duplicate key we must not even
        // open the .otbx, because open-then-close would release the live entry's
        // WRITE_LOCK (per-process posix lock).
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
              "agent_disk_t::bootstrap_disk_inner_sync: agent[{}] load oid={} path={} sidecar_wal_id={}",
              pool_idx_,
              static_cast<unsigned>(oid),
              otbx_path.string(),
              static_cast<uint64_t>(sidecar_wal_id));
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), otbx_path);
        if (sidecar_wal_id > wal::id_t{0}) {
            entry->table_storage.set_checkpoint_wal_id(sidecar_wal_id);
        }
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    bool agent_disk_t::bootstrap_create_disk_inner_sync(
        components::catalog::oid_t oid,
        std::vector<components::table::column_definition_t> columns,
        const std::filesystem::path& otbx_path) noexcept {
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
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path);
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    // Runtime CREATE mailbox handlers (see header). The entry is built on the AGENT's
    // OWN resource() here on the agent thread, then emplaced via the existing
    // bootstrap_*_inner_sync helpers (now called intra-actor). Each returns false on
    // duplicate key, mirroring the helpers' contract.
    agent_disk_t::unique_future<bool> agent_disk_t::create_storage_inner(components::catalog::oid_t oid) {
        auto entry = std::make_unique<collection_storage_entry_t>(resource());
        const bool ok = bootstrap_inner_sync(oid, std::move(entry));
        if (!ok) {
            trace(log_,
                  "agent_disk[{}]::create_storage_inner: oid {} already owned — duplicate",
                  pool_idx_,
                  static_cast<unsigned>(oid));
        }
        co_return ok;
    }

    agent_disk_t::unique_future<bool>
    agent_disk_t::create_storage_with_columns_inner(components::catalog::oid_t oid,
                                                    std::vector<components::table::column_definition_t> columns) {
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), std::move(columns));
        const bool ok = bootstrap_inner_sync(oid, std::move(entry));
        if (!ok) {
            trace(log_,
                  "agent_disk[{}]::create_storage_with_columns_inner: oid {} already owned — duplicate",
                  pool_idx_,
                  static_cast<unsigned>(oid));
        }
        co_return ok;
    }

    agent_disk_t::unique_future<bool>
    agent_disk_t::create_storage_disk_inner(components::catalog::oid_t oid,
                                            std::vector<components::table::column_definition_t> columns,
                                            std::filesystem::path otbx_path) {
        // create_directories runs on the AGENT thread (manager builds nothing). The SFBM
        // is then constructed by bootstrap_create_disk_inner_sync, which holds the
        // exclusive posix WRITE_LOCK on the .otbx — agent-only, so no construction race.
        std::error_code ec;
        std::filesystem::create_directories(otbx_path.parent_path(), ec);
        if (ec) {
            warn(log_,
                 "agent_disk[{}]::create_storage_disk_inner: create_directories {} failed: {}",
                 pool_idx_,
                 otbx_path.parent_path().string(),
                 ec.message());
        }
        const bool ok = bootstrap_create_disk_inner_sync(oid, std::move(columns), otbx_path);
        if (!ok) {
            trace(log_,
                  "agent_disk[{}]::create_storage_disk_inner: oid {} already owned (path={}) — duplicate",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
        }
        co_return ok;
    }

    // WAL-replay direct_* helpers (see header). Mutation logic is intentionally
    // minimal: schema-adoption / column-expansion / type-promotion run upstream in
    // the mailbox body, and replay records arrive pre-aligned with the table schema,
    // so a direct delete/update against the entry's storage adapter is correct.
    void agent_disk_t::direct_delete_sync(components::catalog::oid_t table_oid,
                                          const std::pmr::vector<int64_t>& row_ids,
                                          uint64_t count,
                                          const components::table::transaction_data& txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::direct_delete_sync: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::direct_delete_sync: oid {} has null entry (unreachable post-§8.1.B/C) — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        if (row_ids.empty() || entry->storage == nullptr) {
            return;
        }
        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count && i < row_ids.size(); i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        entry->storage->delete_rows(ids_vec, count, txn.transaction_id);
    }

    void agent_disk_t::direct_update_sync(components::catalog::oid_t table_oid,
                                          const std::pmr::vector<int64_t>& row_ids,
                                          components::vector::data_chunk_t& new_data) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::direct_update_sync: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::direct_update_sync: oid {} has null entry (unreachable post-§8.1.B/C) — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        if (row_ids.empty() || entry->storage == nullptr) {
            return;
        }
        const auto count = static_cast<uint64_t>(row_ids.size());
        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        // new_data is on the WAL-replay resource; the storage is on this agent's.
        // update() slices zero-copy refs into the chunk, so deep-copy onto resource()
        // first — else validity_mask_t::operator= asserts resource_ == other.resource_
        // on Debug builds. See docs/wal-recovery-pmr-mismatch.md.
        components::vector::data_chunk_t local(resource(), new_data.types(), new_data.size());
        new_data.copy(local, 0);
        entry->storage->update(ids_vec, local);
    }

    actor_zeta::behavior_t agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::fix_wal_id>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::fix_wal_id, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan, msg);
                break;
            }
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
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan_batched_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan_batched_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan_segment_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan_segment_inner, msg);
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
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::mark_storage_dropped_many_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::mark_storage_dropped_many_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::create_storage_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::create_storage_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::create_storage_with_columns_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::create_storage_with_columns_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::create_storage_disk_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::create_storage_disk_inner, msg);
                break;
            }
            default:
                break;
        }
    }

    agent_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    agent_disk_t::storage_scan(session_id_t /*session*/,
                               components::catalog::oid_t table_oid,
                               std::unique_ptr<components::table::table_filter_t> filter,
                               int limit,
                               components::table::transaction_data txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_scan: oid {} not owned by this agent — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            // Defensive: null entries unreachable in current control flow.
            trace(log_,
                  "agent_disk[{}]::storage_scan: oid {} has null entry/storage — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto types = entry->storage->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        entry->storage->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    // Mutation fanout targets. The manager router pre-validates, but the agent
    // re-checks (not-owned / null no-op) because it owns its slice independently.

    agent_disk_t::unique_future<std::pair<uint64_t, uint64_t>>
    agent_disk_t::storage_append_inner(components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::vector::data_chunk_t> data,
                                       components::table::transaction_data txn,
                                       core::date::timezone_offset_t session_tz) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_append_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_append_inner: oid {} has null entry — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }
        auto* s = entry->storage.get();
        if (!s || !data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }

        // Full preprocessing pipeline (stages 1-5 below) runs on the owning agent so
        // its reads and the final write are mailbox-serialized with every same-oid access.
        const bool is_computed_table = entry->is_computed;

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 1b. Dynamic schema growth for IN_MEMORY storages. Trigger: alias
        // mismatch at differing chunk/table width = schema growth; equal
        // width = positional rename, handled by column expansion below.
        if (s->has_schema() && data->column_count() > 0 &&
            (is_computed_table || data->column_count() != s->columns().size()) &&
            entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
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
                    entry->add_column(col, resource());
                }
                // add_column rebuilt the storage adapter; refresh our local
                // storage_t* to point at the new adapter.
                s = entry->storage.get();
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
                            trace(log_,
                                  "agent_disk[{}]::storage_append_inner: NOT NULL violation on column '{}'",
                                  pool_idx_,
                                  table_columns[col].name());
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
                            casted.set_value(row, src_vec.value(row).cast_as(target_type, session_tz));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        // 5. Append — the canonical write.
        auto actual_count = data->size();
        uint64_t start_row;
        if (txn.transaction_id != 0) {
            start_row = s->append(*data, txn);
        } else {
            start_row = s->append(*data);
        }
        co_return std::make_pair(start_row, actual_count);
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_publish_commits_inner(uint64_t commit_id,
                                                std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
        // MVCC visibility flip. Ranges not in this agent's slice are skipped — the
        // owning agent gets its own slice from the manager's partitioning send.
        for (const auto& r : ranges) {
            if (r.count == 0) {
                continue;
            }
            auto it = storages_.find(r.table_oid);
            if (it == storages_.end()) {
                continue;
            }
            auto& entry = it->second;
            if (entry == nullptr || entry->storage == nullptr) {
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
        // txn_id==0 means no real transaction (legacy fast path) — short-circuit.
        if (txn_id == 0) {
            co_return;
        }
        for (const auto& tbl_oid : tables) {
            auto it = storages_.find(tbl_oid);
            if (it == storages_.end()) {
                continue;
            }
            auto& entry = it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                continue;
            }
            entry->storage->commit_all_deletes(txn_id, commit_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_revert_deletes_inner(uint64_t txn_id,
                                               std::pmr::vector<components::catalog::oid_t> tables) {
        // Abort-path twin of storage_publish_deletes_inner: un-stamp this txn's
        // pending delete marks back to NOT_DELETED_ID instead of committing them.
        // txn_id==0 means no real transaction (legacy fast path) — short-circuit.
        if (txn_id == 0) {
            co_return;
        }
        for (const auto& tbl_oid : tables) {
            auto it = storages_.find(tbl_oid);
            if (it == storages_.end()) {
                continue;
            }
            auto& entry = it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                continue;
            }
            entry->storage->revert_all_deletes(txn_id);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
        // Reverse-iterate so nested ranges unwind in append-order opposite.
        for (auto it = ranges.rbegin(); it != ranges.rend(); ++it) {
            if (it->count == 0) {
                continue;
            }
            auto slice_it = storages_.find(it->table_oid);
            if (slice_it == storages_.end()) {
                continue;
            }
            auto& entry = slice_it->second;
            if (entry == nullptr || entry->storage == nullptr) {
                continue;
            }
            entry->storage->revert_append(it->start_row, it->count);
        }
        co_return;
    }

    agent_disk_t::unique_future<std::pair<int64_t, uint64_t>>
    agent_disk_t::storage_update_inner(components::catalog::oid_t table_oid,
                                       components::vector::vector_t row_ids,
                                       std::unique_ptr<components::vector::data_chunk_t> data,
                                       components::table::transaction_data txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_update_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_update_inner: oid {} has null entry — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        if (!data || entry->storage == nullptr) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        // No preprocessing here: the manager body already aligned `data` with the
        // canonical schema (the twin shares column defs via bootstrap_inner_sync).
        co_return entry->storage->update(row_ids, *data, txn);
    }

    agent_disk_t::unique_future<uint64_t>
    agent_disk_t::storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                            components::vector::vector_t row_ids,
                                            uint64_t count,
                                            components::table::transaction_data txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_delete_rows_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return 0;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_delete_rows_inner: oid {} has null entry — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return 0;
        }
        if (entry->storage == nullptr || count == 0) {
            co_return 0;
        }
        if (txn.transaction_id != 0) {
            co_return entry->storage->delete_rows(row_ids, count, txn.transaction_id);
        }
        co_return entry->storage->delete_rows(row_ids, count);
    }

    agent_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    agent_disk_t::storage_fetch_inner(components::catalog::oid_t table_oid,
                                      components::vector::vector_t row_ids,
                                      uint64_t count) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_fetch_inner: oid {} not owned by this agent — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_fetch_inner: oid {} has null entry — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto types = entry->storage->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types, count);
        entry->storage->fetch(*result, row_ids, count);
        std::memcpy(result->row_ids.data(), row_ids.data(), count * sizeof(int64_t));
        co_return std::move(result);
    }

    std::pmr::vector<components::vector::data_chunk_t>
    agent_disk_t::scan_batched_local(components::catalog::oid_t table_oid,
                                     components::table::table_filter_t* filter,
                                     int64_t limit,
                                     const std::vector<std::size_t>* projected_cols,
                                     const components::table::transaction_data& txn) {
        std::pmr::vector<components::vector::data_chunk_t> batches{resource()};
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::scan_batched_local: oid {} not owned by this agent",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return batches;
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::scan_batched_local: oid {} is a DISK record-only marker",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return batches;
        }
        entry->storage->scan_batched(batches, filter, limit, projected_cols, txn);
        return batches;
    }

    // Thin mailbox wrapper over scan_batched_local (D6: same-actor callers use the local
    // helper directly; this exists for the manager→agent mailbox route).
    agent_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::storage_scan_batched_inner(components::catalog::oid_t table_oid,
                                             std::unique_ptr<components::table::table_filter_t> filter,
                                             int64_t limit,
                                             std::vector<size_t> projected_cols,
                                             components::table::transaction_data txn) {
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
        co_return scan_batched_local(table_oid, filter.get(), limit, projected_ptr, txn);
    }

    agent_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    agent_disk_t::storage_scan_segment_inner(components::catalog::oid_t table_oid,
                                             int64_t start,
                                             uint64_t count) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_scan_segment_inner: oid {} not owned by this agent — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_scan_segment_inner: oid {} is a DISK record-only marker — "
                  "fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto types = entry->storage->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        entry->storage->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) {
            chunk.copy(*result, 0);
        });
        co_return std::move(result);
    }

    agent_disk_t::unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    agent_disk_t::scan_by_keys_inner(components::catalog::oid_t table_oid,
                                     std::pmr::vector<std::string> key_col_names,
                                     components::vector::data_chunk_t keys,
                                     components::table::transaction_data txn) {
        // result[i] = row_ids matching keys[i]; one (possibly empty) entry per key,
        // preserving input order. Name→index resolution runs once for the whole
        // batch, then each key gets an eq-AND filtered scan.
        std::pmr::vector<std::pmr::vector<std::int64_t>> result{resource()};
        result.reserve(keys.size());

        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr ||
            key_col_names.empty()) {
            // Not owned / record-only marker / no key columns: one empty row per key.
            for (std::size_t i = 0; i < keys.size(); ++i) {
                result.emplace_back();
            }
            co_return std::move(result);
        }
        auto& entry = it->second;

        // Resolve key column NAMES to storage indices once (same column set for
        // every key). Any unknown column degrades the whole batch to empty rows.
        const auto& cols = entry->storage->columns();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        key_col_indices.reserve(key_col_names.size());
        for (const auto& kname : key_col_names) {
            std::size_t col_idx = cols.size();
            for (std::size_t ci = 0; ci < cols.size(); ++ci) {
                if (cols[ci].name() == kname) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == cols.size()) {
                for (std::size_t i = 0; i < keys.size(); ++i) {
                    result.emplace_back();
                }
                co_return std::move(result);
            }
            key_col_indices.push_back(static_cast<std::uint64_t>(col_idx));
        }

        // Columnar keys: column j == key_col_names[j], row i == key-tuple i. Arity is
        // uniform across the chunk, so a mismatch (chunk column count != resolved key
        // columns) voids the whole batch with one empty row per key. Each filter
        // constant is the single materialization of a key cell (keys.value(ki, i)): the
        // filter API requires a logical_value_t, so this is the irreducible floor — no
        // row-major keys cross the mailbox.
        const std::uint64_t nkeys = keys.size();
        if (keys.column_count() != key_col_indices.size()) {
            for (std::uint64_t i = 0; i < nkeys; ++i) {
                result.emplace_back();
            }
            co_return std::move(result);
        }
        for (std::uint64_t i = 0; i < nkeys; ++i) {
            std::pmr::vector<std::int64_t> row_ids{resource()};
            auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
            for (std::size_t ki = 0; ki < key_col_indices.size(); ++ki) {
                std::pmr::vector<std::uint64_t> idx_vec{resource()};
                idx_vec.push_back(key_col_indices[ki]);
                filter->child_filters.push_back(
                    std::make_unique<components::table::constant_filter_t>(components::expressions::compare_type::eq,
                                                                          keys.value(ki, i),
                                                                          std::move(idx_vec)));
            }
            std::pmr::vector<components::vector::data_chunk_t> batches{resource()};
            entry->storage->scan_batched(batches, filter.get(), int64_t{-1}, nullptr, txn);
            for (auto& chunk : batches) {
                for (uint64_t r = 0; r < chunk.size(); ++r) {
                    row_ids.push_back(chunk.row_ids.data<std::int64_t>()[r]);
                }
            }
            result.emplace_back(std::move(row_ids));
        }
        co_return std::move(result);
    }

    agent_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                           std::pmr::vector<std::string> key_col_names,
                                           components::vector::data_chunk_t keys,
                                           components::table::transaction_data txn) {
        // Single key-tuple (keys has exactly one row): resolve the key column NAMES to
        // storage indices, build an eq-AND filter and scan its own slice directly via
        // scan_batched_local (D6: no self-send). Empty result on any degenerate input.
        std::pmr::vector<components::vector::data_chunk_t> empty{resource()};
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr ||
            key_col_names.empty() || keys.size() == 0) {
            co_return std::move(empty);
        }
        auto& entry = it->second;

        // Resolve each key column NAME to a storage column index; an unknown column voids
        // the whole call (empty result).
        const auto& cols = entry->storage->columns();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        key_col_indices.reserve(key_col_names.size());
        for (const auto& kname : key_col_names) {
            std::size_t col_idx = cols.size();
            for (std::size_t ci = 0; ci < cols.size(); ++ci) {
                if (cols[ci].name() == kname) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == cols.size()) {
                co_return std::move(empty);
            }
            key_col_indices.push_back(static_cast<std::uint64_t>(col_idx));
        }

        // Each filter constant is keys.value(ki, 0) — a logical_value_t materialized only
        // here for the filter API (the irreducible floor, same as scan_by_keys_inner). No
        // row-major key crosses the mailbox; the carrier is the columnar `keys` chunk.
        auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
        for (std::size_t ki = 0; ki < key_col_indices.size(); ++ki) {
            std::pmr::vector<std::uint64_t> idx_vec{resource()};
            idx_vec.push_back(key_col_indices[ki]);
            filter->child_filters.push_back(
                std::make_unique<components::table::constant_filter_t>(components::expressions::compare_type::eq,
                                                                      keys.value(ki, 0),
                                                                      std::move(idx_vec)));
        }

        // All columns (projected = nullptr), no row limit (-1) — same as the prior
        // manager-side storage_scan_batched_inner call (it passed an empty projected list).
        co_return scan_batched_local(table_oid, filter.get(), int64_t{-1}, nullptr, txn);
    }

    agent_disk_t::unique_future<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                            std::pmr::vector<std::string> key_col_names,
                                            components::vector::data_chunk_t keys,
                                            components::table::transaction_data txn) {
        // result[i] = matched chunks for key-tuple i; one (possibly empty) entry per key,
        // preserving input order. Name→index resolution runs once for the whole batch, then
        // each key gets an eq-AND filtered scan via scan_batched_local (D6: no self-send).
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> result{resource()};
        result.reserve(keys.size());

        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr ||
            key_col_names.empty()) {
            // Not owned / record-only marker / no key columns: one empty entry per key.
            for (std::size_t i = 0; i < keys.size(); ++i) {
                result.emplace_back();
            }
            co_return std::move(result);
        }
        auto& entry = it->second;

        // Resolve key column NAMES to storage indices once (same column set for every key).
        // Any unknown column degrades the whole batch to empty entries.
        const auto& cols = entry->storage->columns();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        key_col_indices.reserve(key_col_names.size());
        for (const auto& kname : key_col_names) {
            std::size_t col_idx = cols.size();
            for (std::size_t ci = 0; ci < cols.size(); ++ci) {
                if (cols[ci].name() == kname) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == cols.size()) {
                for (std::size_t i = 0; i < keys.size(); ++i) {
                    result.emplace_back();
                }
                co_return std::move(result);
            }
            key_col_indices.push_back(static_cast<std::uint64_t>(col_idx));
        }

        // Columnar keys: column j == key_col_names[j], row i == key-tuple i. Arity is uniform
        // across the chunk, so a mismatch (chunk column count != resolved key columns) voids the
        // whole batch with one empty entry per key. Each filter constant is the single
        // materialization of a key cell (keys.value(ki, i)): the filter API requires a
        // logical_value_t, so this is the irreducible floor — no row-major keys cross the mailbox.
        const std::uint64_t nkeys = keys.size();
        if (keys.column_count() != key_col_indices.size()) {
            for (std::uint64_t i = 0; i < nkeys; ++i) {
                result.emplace_back();
            }
            co_return std::move(result);
        }
        for (std::uint64_t i = 0; i < nkeys; ++i) {
            auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
            for (std::size_t ki = 0; ki < key_col_indices.size(); ++ki) {
                std::pmr::vector<std::uint64_t> idx_vec{resource()};
                idx_vec.push_back(key_col_indices[ki]);
                filter->child_filters.push_back(
                    std::make_unique<components::table::constant_filter_t>(components::expressions::compare_type::eq,
                                                                          keys.value(ki, i),
                                                                          std::move(idx_vec)));
            }
            // All columns (projected = nullptr), no row limit (-1) — same as read_chunks_by_key_inner.
            result.emplace_back(scan_batched_local(table_oid, filter.get(), int64_t{-1}, nullptr, txn));
        }
        co_return std::move(result);
    }

    agent_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    agent_disk_t::storage_types_inner(components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_types_inner: oid {} not owned by this agent — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::pmr::vector<components::types::complex_logical_type>{resource()};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_types_inner: oid {} is a DISK record-only marker — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::pmr::vector<components::types::complex_logical_type>{resource()};
        }
        co_return entry->storage->types();
    }

    agent_disk_t::unique_future<uint64_t>
    agent_disk_t::storage_total_rows_inner(components::catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_total_rows_inner: oid {} not owned by this agent — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return uint64_t{0};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_total_rows_inner: oid {} is a DISK record-only marker — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return uint64_t{0};
        }
        co_return entry->storage->total_rows();
    }

    agent_disk_t::unique_future<void> agent_disk_t::fix_wal_id(wal::id_t wal_id) {
        trace(log_, "agent_disk::fix_wal_id : {}", wal_id);
        auto id = std::to_string(wal_id);
        file_wal_id_->write(id.data(), id.size(), 0);
        file_wal_id_->truncate(static_cast<int64_t>(id.size()));
        co_return;
    }

    agent_disk_t::unique_future<checkpoint_result_t> agent_disk_t::checkpoint_inner(session_id_t /*session*/,
                                                                                     wal::id_t current_wal_id,
                                                                                     uint64_t compact_watermark) {
        trace(log_, "agent_disk[{}]::checkpoint_inner: {} entries in local slice", pool_idx_, storages_.size());
        // Per DISK entry, crash-safe checkpoint sequence (order matters):
        //   compact (MVCC-gated), backup .otbx → .prev, checkpoint(wal_id), persist
        //   the .wal_id sidecar via tmp+rename, then delete the .prev backup on
        //   success. Tally min(prev_checkpoint_wal_id_) for the manager's
        //   cross-agent std::min. IN_MEMORY twins and null entries are skipped
        //   for checkpointing, but an IN_MEMORY twin flips has_in_memory so
        //   checkpoint_all can gate WAL-floor sealing without a separate sync
        //   slice read (folded from has_in_memory_inner_sync).
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        bool has_in_memory = false;
        for (auto& [tbl_oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            if (entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
                has_in_memory = true;
            }
            if (entry->table_storage.mode() != storage_mode_t::DISK) {
                continue;
            }
            if (entry->otbx_path.empty()) {
                continue;
            }

            // MVCC gate FIRST: compact() refuses the rebuild when any version
            // stamp is above the watermark (an active snapshot or an in-flight
            // commit still needs the history, or a positional commit_append is
            // pending). Persisting a non-compacted table would resurrect dead /
            // uncommitted rows on recovery (.otbx has no version metadata), so
            // the entry's checkpoint is deferred to a later round; the WAL keeps
            // its replay records because the old sidecar/prev ids stay in the min.
            if (!entry->table_storage.table().compact(compact_watermark)) {
                trace(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} has version stamps above watermark {} — "
                      "skipping this round",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid),
                      compact_watermark);
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                continue;
            }

            trace(log_,
                  "agent_disk[{}]::checkpoint_inner checkpointing oid={}",
                  pool_idx_,
                  static_cast<unsigned>(tbl_oid));

            const auto& otbx_path = entry->otbx_path;
            auto prev_path = otbx_path;
            prev_path += ".prev";

            // Backup current checkpoint before overwriting.
            std::error_code copy_error;
            if (std::filesystem::exists(otbx_path)) {
                std::filesystem::copy_file(otbx_path,
                                           prev_path,
                                           std::filesystem::copy_options::overwrite_existing,
                                           copy_error);
                if (copy_error) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner copy {} -> {} failed: {}",
                         pool_idx_,
                         otbx_path.string(),
                         prev_path.string(),
                         copy_error.message());
                }
            }

            entry->table_storage.checkpoint(current_wal_id);

            // Persist sidecar wal_id atomically (tmp + rename).
            {
                auto sidecar_path = otbx_path;
                sidecar_path += ".wal_id";
                auto tmp_path = sidecar_path;
                tmp_path += ".tmp";
                std::ofstream sidecar(tmp_path, std::ios::binary | std::ios::trunc);
                if (sidecar.is_open()) {
                    auto v = static_cast<uint64_t>(current_wal_id);
                    sidecar.write(reinterpret_cast<const char*>(&v), sizeof(v));
                    sidecar.close();
                    std::error_code rename_error;
                    std::filesystem::rename(tmp_path, sidecar_path, rename_error);
                    if (rename_error) {
                        warn(log_,
                             "agent_disk[{}]::checkpoint_inner sidecar rename failed: {}",
                             pool_idx_,
                             rename_error.message());
                    }
                }
            }

            // Delete backup only after a successful checkpoint.
            if (std::filesystem::exists(prev_path)) {
                std::error_code remove_error;
                std::filesystem::remove(prev_path, remove_error);
            }

            min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
        }
        co_return checkpoint_result_t{min_prev_id, has_in_memory};
    }

    agent_disk_t::unique_future<void> agent_disk_t::vacuum_inner(session_id_t /*session*/,
                                                                 uint64_t lowest_active_start_time,
                                                                 uint64_t compact_watermark) {
        trace(log_, "agent_disk[{}]::vacuum_inner: {} entries in local slice", pool_idx_, storages_.size());
        for (auto& [oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
            // MVCC-gated: a no-op when any version stamp is above the watermark
            // (concurrent snapshot / in-flight commit still needs the history).
            table.compact(compact_watermark);
        }
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::maybe_cleanup_inner(components::catalog::oid_t table_oid,
                                                                        uint64_t compact_watermark) {
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

        auto& table = entry->table_storage.table();
        auto rg = table.row_group();
        auto total = rg->total_rows();
        if (total == 0) {
            co_return;
        }

        auto committed = rg->committed_row_count();
        auto deleted = total - committed;

        static constexpr double gc_threshold = 0.3;
        if (static_cast<double>(deleted) / static_cast<double>(total) > gc_threshold) {
            trace(log_,
                  "agent_disk[{}]::maybe_cleanup_inner: oid={}, deleted {}/{}, running compact (watermark {})",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  deleted,
                  total,
                  compact_watermark);
            // compact() refuses the rebuild when any version stamp is above the
            // watermark (concurrent snapshot / in-flight commit still needs the
            // history); reclaim is merely deferred to a later commit. The agent
            // mailbox serializing the row_groups_ swap covers the data-race
            // side; the watermark covers version visibility.
            // Compact alone (no preceding cleanup_versions): scan_committed
            // depends on intact version metadata to filter tombstones;
            // cleanup_versions would strip it before compact rebuilds the
            // row_group.
            table.compact(compact_watermark);
        }

        co_return;
    }

    // GC pass over dropped_storages_ (see header). The kept-vector rebuild avoids
    // iterator-invalidation on partial erase; every filesystem::remove uses the
    // std::error_code overload — exceptions FORBIDDEN.
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
            } else {
                kept.push_back(std::move(entry));
            }
        }
        dropped_storages_ = std::move(kept);

        // Once the slice drains, ack on_subscriber_empty so the dispatcher clears
        // disk_has_dropped_ and stops broadcasting. Gated on != empty_address() so
        // test fixtures without a dispatcher pass cleanly.
        if (dropped_storages_.empty()
            && manager_dispatcher_addr_ != actor_zeta::address_t::empty_address()) {
            // DISK_KIND matches the dispatcher's subscriber-kind enum.
            constexpr uint8_t DISK_KIND = 1;
            [[maybe_unused]] auto _ = actor_zeta::send(manager_dispatcher_addr_,
                                                       &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                                       DISK_KIND);
        }
        co_return;
    }

    // DROP-GC value-space remap (see header). Rewrites every dropped_storages_ entry
    // whose dropped_at_commit_id still equals the txn_id placeholder into commit-id
    // space, so the on_horizon_advanced sweep (which compares against a commit-id
    // horizon) can eventually reclaim it.
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

    // DROP-rollback un-mark (see header). The abort mirror of
    // storage_dropped_committed_inner: instead of remapping a GC entry's value into
    // commit-id space, it ERASES every dropped_storages_ entry still carrying the
    // txn_id placeholder, un-marking the DROP so the still-live .otbx survives.
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

    // See header. Bootstrap-only; after scheduler.start the address is read-only.
    void agent_disk_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        manager_dispatcher_addr_ = std::move(address);
    }

    // See header. Bootstrap-only; after scheduler.start the address is read-only.
    void agent_disk_t::set_manager_wal_sync(actor_zeta::address_t address) {
        manager_wal_addr_ = std::move(address);
    }

    // GC-slice push-back (see header). Called pre-scheduler-start by base_spaces
    // catalog rebuild and at runtime by mark_storage_dropped_many_inner.
    void agent_disk_t::register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                            uint64_t dropped_at_commit_id,
                                                            std::filesystem::path path,
                                                            std::pmr::vector<std::filesystem::path> sidecar_paths) {
        dropped_storages_.push_back(dropped_storage_entry_t{oid,
                                                             dropped_at_commit_id,
                                                             std::move(path),
                                                             std::move(sidecar_paths)});
    }

    // Canonical erase + .otbx removal (see header). Idempotent on a missing key;
    // the erase drops the unique_ptr, closing the file_handle_t once. The mailbox
    // serializes this against the only other slice writers (bootstrap pre-start;
    // runtime storage_*_inner handlers only read).
    // Canonical single-oid erase + .otbx removal. Used by drop_storage_many_inner,
    // which loops it over its oid slice. Synchronous (no co_await) — the caller runs
    // on the agent thread.
    void agent_disk_t::drop_storage_one_local(components::catalog::oid_t oid) {
        // Read otbx_path BEFORE the erase, while the unique_ptr is still live. Empty
        // path (IN_MEMORY twins) skips the remove block. Remove sequence: .otbx +
        // .wal_id + .prev sidecars + per-oid directory, all via std::error_code
        // overloads — exceptions FORBIDDEN.
        std::filesystem::path otbx_path;
        if (auto it = storages_.find(oid); it != storages_.end()) {
            if (it->second != nullptr) {
                otbx_path = it->second->otbx_path;
            }
        }
        const auto erased = storages_.erase(oid);
        if (erased == 0) {
            // Trace, not warn: drop_storage_many over-routes idempotently, so this
            // path is hit for a truly-missing / not-owned OID — benign (idempotent
            // DROP).
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
            // Remove .otbx + sidecars + per-oid directory now. A surviving .otbx
            // would let a restart synthesise a phantom storage on WAL replay and let
            // a re-CREATE TABLE collide with the recycled oid. (The on_horizon GC
            // sweep is only the secondary net; this is the primary cleanup.)
            std::error_code ec;
            std::filesystem::remove(otbx_path, ec);
            auto sidecar = otbx_path;
            sidecar += ".wal_id";
            std::filesystem::remove(sidecar, ec);
            auto prev = otbx_path;
            prev += ".prev";
            std::filesystem::remove(prev, ec);
            std::filesystem::remove(otbx_path.parent_path(), ec);
        }
    }

    // Batched DROP: one message per agent carries that agent's whole oid slice
    // (manager partitioned by pool_idx_for_oid). Loops the canonical singular erase;
    // each oid is idempotent on a missing key, so an over-routed oid is a no-op.
    agent_disk_t::unique_future<void>
    agent_disk_t::drop_storage_many_inner(std::pmr::vector<components::catalog::oid_t> oids) {
        for (auto oid : oids) {
            drop_storage_one_local(oid);
        }
        co_return;
    }

    // ---------------------------------------------------------------------------
    // Catalog DDL handlers (Track A). These moved off the manager loop: the catalog
    // scan + mutation now run on this (CATALOG / agent-0) thread against the agent's
    // OWN slice, so the manager no longer borrows the agent's storage_entry across
    // the actor boundary. WAL goes through manager_wal_addr_ (plain actor_zeta::send +
    // co_await; the WAL manager self-schedules, so NO scheduler_disk_->enqueue here).
    // ---------------------------------------------------------------------------

    // Crash-safe pg_catalog row append: WAL physical_insert is written first so a
    // crash before the storage update can be replayed on restart, then storage is
    // updated on this agent's own slice. The preprocessing body applies only schema
    // adoption + alias-keyed column expansion + numeric/string cast rather than the
    // heavier storage_append_inner pipeline — storage_append_inner adds NOT NULL
    // rejection, _id dedup, default-value / positional fallback, and broader
    // is_convertable_to casting, none of which the catalog-append path applied, so
    // this lighter path keeps WAL-time semantics faithful.
    agent_disk_t::unique_future<components::pg_catalog_append_range_t>
    agent_disk_t::append_pg_catalog_row_inner(execution_context_t ctx,
                                              components::catalog::oid_t table_oid,
                                              components::vector::data_chunk_t row) {
        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            auto wal_chunk =
                std::make_unique<components::vector::data_chunk_t>(resource(), row.types(), row.size());
            wal_chunk->set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk->data[col].set_value(r, row.data[col].value(r));
                }
            }
            // pg_catalog writes route to main_database (ctx.database_oid is always
            // INVALID_OID for catalog writes).
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(manager_wal_addr_,
                                             &wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_chunk),
                                             std::uint64_t{0},
                                             static_cast<std::uint64_t>(row.size()),
                                             ctx.txn.transaction_id,
                                             db_oid);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::append_pg_catalog_row_inner: WAL write returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }
        }

        const auto count = static_cast<std::uint64_t>(row.size());
        uint64_t start_row = 0;

        // Append on this agent's own slice.
        auto it = storages_.find(table_oid);
        if (it != storages_.end() && it->second != nullptr && it->second->storage != nullptr &&
            row.size() != 0) {
            auto* s = it->second->storage.get();

            // rebuild onto this agent's resource (row arrives on the mailbox resource).
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
                    if (src_type != tgt_type &&
                        (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                        (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                        auto& src_vec = local.data[i];
                        auto target_type = table_columns[i].type();
                        if (src_vec.type().has_alias()) {
                            target_type.set_alias(src_vec.type().alias());
                        }
                        components::vector::vector_t casted(resource(), target_type, local.size());
                        for (uint64_t r = 0; r < local.size(); r++) {
                            if (src_vec.validity().row_is_valid(r)) {
                                casted.set_value(r, src_vec.value(r).cast_as(target_type, ctx.session_tz));
                            } else {
                                casted.validity().set_invalid(r);
                            }
                        }
                        local.data[i] = std::move(casted);
                    }
                }
            }

            start_row = s->append(local, ctx.txn);
        } else {
            trace(log_,
                  "agent_disk[{}]::append_pg_catalog_row_inner: oid {} not owned/empty — no storage append",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
        }

        if (ctx.txn.transaction_id == 0 || count == 0) {
            co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), 0};
        }
        co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), count};
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::delete_pg_catalog_rows_inner(execution_context_t ctx,
                                               components::catalog::oid_t table_oid,
                                               std::int64_t oid_col_idx,
                                               components::catalog::oid_t target_oid) {
        // Read the slice directly. Bind entry NON-const so inline_scan binds the
        // non-const data_table_t& overload (no const_cast).
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            co_return;
        }
        auto& entry = it->second;

        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        detail::inline_scan(entry->table_storage.table(),
                            {oid_col_idx},
                            &scan_resource,
                            [&, oid_col_idx](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto v = chunk.value(static_cast<uint64_t>(oid_col_idx), i);
                                if (v.is_null())
                                    return true;
                                if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) ==
                                    target_oid) {
                                    row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                                }
                                return true;
                            });
        if (row_ids.empty()) {
            co_return;
        }
        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<std::int64_t> wal_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_addr_,
                                             &wal::manager_wal_replicate_t::write_physical_delete,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_ids),
                                             static_cast<std::uint64_t>(row_ids.size()),
                                             ctx.txn.transaction_id,
                                             components::catalog::well_known_oid::main_database);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::delete_pg_catalog_rows_inner: WAL write returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }
        }
        direct_delete_sync(table_oid, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
        co_return;
    }

    // Implementation pitfall (preserved from the manager body): data_table_t::update()
    // rewrites EVERY column in the target row (it builds column_ids = [0..count)
    // unconditionally), so a "patch one column" chunk would NULL out the others. We
    // read the full row, mutate the target field in the read-back chunk, and write the
    // whole chunk back.
    agent_disk_t::unique_future<void> agent_disk_t::update_pg_attribute_commit_id_field_inner(
        execution_context_t ctx,
        components::catalog::oid_t attoid,
        components::pg_attribute_commit_id_backfill_t::kind_t kind,
        std::uint64_t commit_id) {
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        auto it = storages_.find(pg_attr_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            co_return;
        }
        auto& entry = it->second;

        // Scan all columns for the attoid row, capturing row_id + a snapshot of
        // every column value. attoid is never reused, so at most one row matches.
        auto& tbl = entry->table_storage.table();
        const std::size_t col_count = tbl.column_count();
        std::vector<std::int64_t> all_col_indices;
        all_col_indices.reserve(col_count);
        for (std::size_t i = 0; i < col_count; ++i) {
            all_col_indices.push_back(static_cast<std::int64_t>(i));
        }

        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        std::pmr::vector<components::types::logical_value_t> row_values(resource());
        row_values.reserve(col_count);

        detail::inline_scan(tbl,
                            all_col_indices,
                            &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto v0 = chunk.value(0, i);
                                if (v0.is_null())
                                    return true;
                                if (static_cast<components::catalog::oid_t>(v0.value<std::uint32_t>()) !=
                                    attoid)
                                    return true;
                                row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                                for (std::size_t c = 0; c < col_count; ++c) {
                                    row_values.push_back(chunk.value(static_cast<uint64_t>(c), i));
                                }
                                return false; // single-row identity — short-circuit
                            });
        if (row_ids.empty()) {
            trace(log_,
                  "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: attoid={} not found (skipping)",
                  pool_idx_,
                  static_cast<unsigned>(attoid));
            co_return;
        }

        // Patch the target column: 10 = added_at_commit_id, 11 = dropped_at_commit_id.
        const std::size_t patch_col_idx =
            (kind == components::pg_attribute_commit_id_backfill_t::kind_t::added_at) ? 10u : 11u;
        if (patch_col_idx >= row_values.size()) {
            trace(log_,
                  "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: patch_col_idx={} out of range "
                  "(col_count={})",
                  pool_idx_,
                  patch_col_idx,
                  col_count);
            co_return;
        }
        row_values[patch_col_idx] =
            components::types::logical_value_t(resource(), static_cast<std::int64_t>(commit_id));

        // Build a full-width update chunk: every column keeps its scanned value, only
        // patch_col_idx gets the new commit_id. Aliases mirror the table's column names
        // so direct_update_sync's name-match routing lands each vector on the correct
        // storage column.
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

        // WAL physical_update: the chunk mirrors the patch chunk full-width so replay's
        // direct_update_sync takes the same alias-matching path.
        if (manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            auto wal_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), chunk_types, 1);
            wal_chunk->set_cardinality(1);
            for (std::size_t c = 0; c < table_columns.size() && c < row_values.size(); ++c) {
                if (row_values[c].is_null()) {
                    wal_chunk->data[c].validity().set_invalid(0);
                } else {
                    wal_chunk->data[c].set_value(0, row_values[c]);
                }
            }
            std::pmr::vector<std::int64_t> wal_row_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_addr_,
                                             &wal::manager_wal_replicate_t::write_physical_update,
                                             ctx.session,
                                             pg_attr_oid,
                                             std::move(wal_row_ids),
                                             std::move(wal_chunk),
                                             static_cast<std::uint64_t>(row_ids.size()),
                                             ctx.txn.transaction_id,
                                             components::catalog::well_known_oid::main_database);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: WAL write returned zero id "
                      "for attoid={}",
                      pool_idx_,
                      static_cast<unsigned>(attoid));
            }
        }

        direct_update_sync(pg_attr_oid, row_ids, patch);
        co_return;
    }

    // Whole-op intra-agent compaction: read own slice (mode + columns), compute the
    // columns NOT in live_attnames, drop each via entry->drop_column on its own slice,
    // and return the dropped count. This eliminates the per-column manager↔agent
    // round-trips the former manager body did.
    agent_disk_t::unique_future<std::uint64_t>
    agent_disk_t::compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                  std::set<std::string> live_attnames) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            co_return 0;
        }
        auto& entry = it->second;
        if (entry->table_storage.mode() != storage_mode_t::IN_MEMORY) {
            trace(log_,
                  "agent_disk[{}]::compact_relkind_g_storage_inner: skip DISK-backed oid={} (out of scope)",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return 0;
        }

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
                      "agent_disk[{}]::compact_relkind_g_storage_inner: oid {} column '{}' not "
                      "found / DISK no-op",
                      pool_idx_,
                      static_cast<unsigned>(table_oid),
                      attname);
            }
        }
        co_return dropped;
    }

    // Runtime DROP path, canonical per-oid mark: read otbx_path + derive .wal_id/.prev
    // sidecars from the own slice, then record the GC entry via
    // register_dropped_storage_inner_sync. Replaces the manager-side storage_entry borrow
    // at mark_storage_dropped_many. Synchronous (no co_await) — the caller runs on the
    // agent thread.
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
                auto wal_id_sidecar = otbx_path;
                wal_id_sidecar += ".wal_id";
                sidecars.push_back(std::move(wal_id_sidecar));
                auto prev_sidecar = otbx_path;
                prev_sidecar += ".prev";
                sidecars.push_back(std::move(prev_sidecar));
            }
        }
        // IN_MEMORY storages leave otbx_path/sidecars empty, but we still record a GC
        // entry so disk_has_dropped_ bookkeeping is uniform (sweep no-ops on empty path).
        register_dropped_storage_inner_sync(table_oid,
                                            dropped_at_commit_id,
                                            std::move(otbx_path),
                                            std::move(sidecars));
    }

    // Batched DROP-mark: one message per agent carries that agent's whole oid slice
    // (manager partitioned by pool_idx_for_oid) plus the shared dropped_at_commit_id.
    // Loops the canonical per-oid mark; an over-routed / not-owned oid records an empty
    // GC entry (no-op sweep), matching the IN_MEMORY case.
    agent_disk_t::unique_future<void>
    agent_disk_t::mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                                  uint64_t dropped_at_commit_id) {
        for (auto table_oid : table_oids) {
            mark_storage_dropped_one_local(table_oid, dropped_at_commit_id);
        }
        co_return;
    }

} //namespace services::disk
