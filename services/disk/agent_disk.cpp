#include "agent_disk.hpp"
#include "inline_scan.hpp" // services::disk::detail::inline_scan (catalog DDL on the agent)
#include "manager_disk.hpp"
#include <components/logical_plan/node_group.hpp>              // node_group_t::set_pushdown (re-lowering guard)
#include <components/physical_plan/operators/aggregate/operator_func.hpp> // aggregate::operator_func_t (reduce rebuild)
#include <components/physical_plan/operators/operator_group.hpp>          // operator_group_t + group_key_t (aggregate-pushdown reduce)
#include <components/physical_plan/operators/scan/transfer_scan.hpp> // source-swap leaf accessors
#include <components/physical_plan_generator/create_plan.hpp>  // create_plan + function_registry + context_storage_t
#include <components/vector/cell_equal.hpp> // components::vector::cells_equal (typed FK hash-verify)
#include <components/vector/vector_operations.hpp>
#include <algorithm> // std::min
#include <fstream>
#include <services/dispatcher/dispatcher.hpp>
#include <unordered_set>

namespace services::disk {

    using namespace core::filesystem;

    // Test-observable counter of ROWS shipped in the last aggregate-pushdown reduce reply
    // (see agent_disk.hpp). Bumped by the sum of data_chunk_t::size() over the reduced
    // chunks right before they cross the mailbox; tests reset it, run one aggregate,
    // then assert the count is TINY vs. the scanned input. DEV_MODE-only.
#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_pushdown_reply_rows{0};
    } // namespace
    uint64_t pushdown_reply_rows() noexcept {
        return g_pushdown_reply_rows.load(std::memory_order_relaxed);
    }
    void reset_pushdown_reply_rows() noexcept {
        g_pushdown_reply_rows.store(0, std::memory_order_relaxed);
    }
#endif

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
        , active_scans_(resource)
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
    const collection_storage_entry_t* agent_disk_t::storage_entry_sync(components::catalog::oid_t oid) const noexcept {
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
        // The DISK load ctor records io_error/data_corruption instead of throwing (this helper is noexcept
        // and reachable on the agent thread). Drop a failed-construction entry so we never emplace a
        // half-loaded storage; the manager-side probe drives .prev corrupt-recovery before this.
        if (entry->table_storage.construction_failed()) {
            warn(log_,
                 "agent_disk_t::bootstrap_disk_inner_sync: agent[{}] load oid={} path={} failed: {}",
                 pool_idx_,
                 static_cast<unsigned>(oid),
                 otbx_path.string(),
                 entry->table_storage.construction_error().what.c_str());
            return false;
        }
        if (sidecar_wal_id > wal::id_t{0}) {
            entry->table_storage.set_checkpoint_wal_id(sidecar_wal_id);
        }
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    bool agent_disk_t::bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
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
        // The DISK create ctor records io_error instead of throwing (this helper is noexcept and runs on
        // the agent thread via create_storage_disk_inner). Drop a failed-construction entry rather than
        // emplacing a storage with a null table_/block_manager_.
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
            ids_vec.set_value(i, row_ids[i]);
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
            ids_vec.set_value(i, row_ids[i]);
        }
        // new_data is on the WAL-replay resource; the storage is on this agent's.
        // update() slices zero-copy refs into the chunk, so deep-copy onto resource()
        // first — else validity_mask_t::operator= asserts resource_ == other.resource_
        // on Debug builds. See docs/wal-recovery-pmr-mismatch.md.
        components::vector::data_chunk_t local(resource(), new_data.types(), new_data.size());
        new_data.copy(local, 0);
        entry->storage->update(ids_vec, local);
    }

    void agent_disk_t::direct_add_column_sync(components::catalog::oid_t table_oid,
                                              const components::vector::data_chunk_t& schema_chunk) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr) {
            trace(log_,
                  "agent_disk[{}]::direct_add_column_sync: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        auto& entry = it->second;
        if (entry->storage == nullptr) {
            return;
        }
        auto* s = entry->storage.get();
        // For each schema column, add it unless a same-named column already exists
        // (idempotent replay). The column type carries its alias = the column name.
        for (uint64_t col = 0; col < schema_chunk.column_count(); ++col) {
            const auto ctype = schema_chunk.data[col].type();
            if (!ctype.has_alias()) {
                continue;
            }
            const auto name = std::string(ctype.alias());
            bool present = false;
            for (const auto& tc : s->columns()) {
                if (tc.name() == name) {
                    present = true;
                    break;
                }
            }
            if (present) {
                continue;
            }
            components::table::column_definition_t def(name, ctype);
            entry->add_column(def, resource());
            // add_column rebuilt the adapter; refresh the local pointer.
            s = entry->storage.get();
            if (s == nullptr) {
                return;
            }
        }
    }

    actor_zeta::behavior_t agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::fix_wal_id>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::fix_wal_id, msg);
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
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan_inner, msg);
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

    // Mutation fanout targets. The manager router pre-validates, but the agent
    // re-checks (not-owned / null no-op) because it owns its slice independently.

    agent_disk_t::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
    agent_disk_t::storage_append_inner(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::vector::data_chunk_t> data) {
        const auto txn = ctx.txn;
        const auto session_tz = ctx.session_tz;
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

        // WAL-FIRST append. The whole body (preprocess -> WAL co_await -> materialize)
        // runs as ONE agent mailbox handler. The agent is a cooperative_actor: it
        // processes exactly one handler coroutine at a time, atomically across every
        // internal co_await (it does NOT pop the next mailbox message while this one is
        // suspended on the WAL future — see cooperative_actor::resume_impl). So no other
        // same-oid append can interleave between the start_row read below and the
        // materializing s->append: the start_row computed pre-append is still valid at
        // append time.
        //
        // Columns added by the dynamic-schema-growth stage (1b) are recorded here so a
        // PHYSICAL_ADD_COLUMN WAL record is emitted BEFORE the PHYSICAL_INSERT, keeping
        // schema-then-rows order on replay.
        std::vector<components::table::column_definition_t> wal_added_columns;

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
                    // Record for the PHYSICAL_ADD_COLUMN WAL record written below.
                    wal_added_columns.push_back(col);
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
                    auto& existing_id_vec = existing->data[static_cast<size_t>(existing_id_col)];
                    for (uint64_t i = 0; i < existing->size(); i++) {
                        if (!existing_id_vec.is_null(i)) {
                            existing_ids.emplace(existing_id_vec.get_value<std::string_view>(i));
                        }
                    }

                    std::vector<uint64_t> keep_rows;
                    keep_rows.reserve(data->size());
                    auto& id_vec = data->data[static_cast<size_t>(id_col)];
                    for (uint64_t i = 0; i < data->size(); i++) {
                        if (id_vec.is_null(i) || existing_ids.find(std::string(
                                                     id_vec.get_value<std::string_view>(i))) == existing_ids.end()) {
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
                    const bool array_target = target_type.type() == components::types::logical_type::ARRAY;
                    // A whole-column NULL literal arrives as an NA-typed source vector, which
                    // carries no values (and no meaningful validity mask) — every row is null.
                    const bool src_is_null_type = src_vec.type().type() == components::types::logical_type::NA;
                    components::vector::vector_t casted(resource(), target_type, data->size());
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!src_is_null_type && src_vec.validity().row_is_valid(row)) {
                            // A fixed ARRAY column reconciles a length mismatch against the
                            // column DEFAULT (truncate / pad-with-default); other columns use
                            // the plain value cast.
                            auto reconciled = array_target
                                                  ? components::table::reconcile_to_fixed_array(resource(),
                                                                                                src_vec.value(row),
                                                                                                table_columns[i],
                                                                                                session_tz)
                                                  : src_vec.value(row).cast_as(target_type, session_tz);
                            // reconcile_to_fixed_array yields a NULL value only when a NOT NULL
                            // fixed ARRAY column receives a too-short value with no default to
                            // pad from. operator_check_constraint already rejects this with a
                            // clean error before the append, so this is a defensive backstop.
                            if (array_target && reconciled.is_null()) {
                                trace(log_,
                                      "agent_disk[{}]::storage_append_inner: NOT NULL fixed ARRAY column '{}' "
                                      "cannot be padded from a too-short value",
                                      pool_idx_,
                                      table_columns[i].name());
                                co_return std::make_pair(uint64_t{0}, uint64_t{0});
                            }
                            casted.set_value(row, reconciled);
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        // 5. WAL-first: allocate the start_row WITHOUT materializing, write WAL,
        //    then materialize. total_rows() is the next append position (the standard
        //    append computes the same value). No other same-oid handler runs between
        //    this read and the s->append below (mailbox-atomic handler), so the value
        //    is stable.
        const auto actual_count = data->size();
        const uint64_t start_row = s->total_rows();

        // 5a. WAL records (WAL-first), only for a real transaction. Replay filters
        //     uncommitted txns, so a txn_id==0 (legacy / replay) append writes no WAL.
        if (txn.transaction_id != 0 && manager_wal_addr_ != actor_zeta::address_t::empty_address()) {
            const auto db_oid = (ctx.database_oid != components::catalog::INVALID_OID)
                                    ? ctx.database_oid
                                    : components::catalog::well_known_oid::main_database;

            // 5a-i. Schema-growth record BEFORE the rows that depend on it. The
            //       payload is a 0-row chunk whose columns ARE the new columns
            //       (alias-tagged types); replay rebuilds the column defs and
            //       re-applies add_column ahead of the PHYSICAL_INSERT.
            //
            //       Issued fire-and-forget (the future is intentionally dropped): we
            //       MUST NOT co_await it here. This handler already co_awaits the
            //       PHYSICAL_INSERT future below; a SECOND sequential cross-actor
            //       co_await on the same agent coroutine triggers the cooperative_actor
            //       lost-wakeup (the await re-suspends after the first resume, the
            //       producer's flag-based readiness never unblocks the parked mailbox,
            //       and resume_impl returns early on the blocked-check before reaching
            //       the awaited-continuation drain — see docs/actor-zeta-lost-wakeup.md,
            //       "the coroutine re-suspended after resume on the next co_await").
            //       That hung the engine on the first schema-growth INSERT.
            //
            //       Durability + ordering are preserved without the await: both records
            //       target the SAME single WAL worker, whose mailbox is FIFO, and the
            //       manager allocates wal_id synchronously in send order, so the
            //       ADD_COLUMN record (lower wal_id) is durably written ahead of its
            //       dependent PHYSICAL_INSERT (higher wal_id). When the INSERT future
            //       below resolves, the worker has necessarily already processed the
            //       earlier ADD_COLUMN message. Replay applies records in ascending
            //       wal_id order, so the column re-add precedes the row replay.
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
                [[maybe_unused]] auto _sc = actor_zeta::send(manager_wal_addr_,
                                                             &wal::manager_wal_replicate_t::write_physical_add_column,
                                                             ctx.session,
                                                             table_oid,
                                                             std::move(schema_chunk),
                                                             static_cast<std::uint64_t>(wal_added_columns.size()),
                                                             txn.transaction_id,
                                                             db_oid);
            }

            // 5a-ii. PHYSICAL_INSERT carrying the FINAL preprocessed chunk + the
            //        reserved start_row + count. Replay re-appends sequentially (it
            //        ignores physical_row_start for placement) but CREATE INDEX
            //        backfill-from-WAL uses start_row as the row-id base, so it must
            //        equal the materialized start_row — which it does (computed above
            //        and materialized below in the same atomic handler). This is the
            //        ONE co_await of this handler (see 5a-i): awaiting it also confirms
            //        the FIFO-earlier ADD_COLUMN record was durably written.
            components::vector::data_chunk_t wal_chunk(resource(), data->types(), data->size());
            data->copy(wal_chunk, 0);
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            auto [_w, wf] = actor_zeta::send(manager_wal_addr_,
                                             &wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_chunks),
                                             start_row,
                                             actual_count,
                                             txn.transaction_id,
                                             db_oid);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::storage_append_inner: physical_insert WAL returned zero id for oid={}",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
            }
        }

        // 5b. Materialize — the canonical write. Lands at total_rows() == start_row.
        //     The txn path can surface a write_conflict (concurrent DDL re-rooted the
        //     table) or out_of_memory (row-group/segment alloc) as a value; this is a plain
        //     synchronous local call (no co_await), so reading the wrapper adds NO second
        //     cross-actor await — the single co_await above (PHYSICAL_INSERT) stays this
        //     handler's only one (a second sequential cross-actor await would risk a
        //     lost-wakeup hang). The WAL record was already written; on a materialize failure
        //     the txn aborts and storage_revert_appends unwinds it.
        uint64_t materialized_start;
        if (txn.transaction_id != 0) {
            auto append_r = s->append(*data, txn);
            if (append_r.has_error()) {
                trace(log_,
                      "agent_disk[{}]::storage_append_inner: materialize failed for oid={} — surfacing error",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
                co_return append_r.convert_error<std::pair<uint64_t, uint64_t>>();
            }
            materialized_start = append_r.value();
        } else {
            materialized_start = s->append(*data);
        }
        assert(materialized_start == start_row &&
               "WAL-first append: materialized start_row diverged from the reserved start_row");
        co_return std::make_pair(materialized_start, actual_count);
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
    agent_disk_t::storage_revert_deletes_inner(uint64_t txn_id, std::pmr::vector<components::catalog::oid_t> tables) {
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

    agent_disk_t::unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
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
        // canonical schema (the twin shares column defs via bootstrap_inner_sync). The
        // wrapper carries any write_conflict / out_of_memory as a value.
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

    agent_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::storage_fetch_inner(components::catalog::oid_t table_oid,
                                      components::vector::vector_t row_ids,
                                      uint64_t count) {
        std::pmr::vector<components::vector::data_chunk_t> out{resource()};
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_fetch_inner: oid {} not owned by this agent — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::move(out);
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_fetch_inner: oid {} has null entry — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::move(out);
        }
        auto types = entry->storage->types();
        // Fetch in ≤DEFAULT_VECTOR_CAPACITY windows so each produced chunk is born within
        // the capacity bound — no oversized chunk is ever materialized.
        const auto* ids = row_ids.data<int64_t>();
        for (uint64_t offset = 0; offset < count; offset += components::vector::DEFAULT_VECTOR_CAPACITY) {
            const uint64_t n = std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, count - offset);
            components::vector::vector_t window_ids(resource(), components::types::logical_type::BIGINT, n);
            std::memcpy(window_ids.data(), ids + offset, n * sizeof(int64_t));
            components::vector::data_chunk_t chunk(resource(), types, n);
            entry->storage->fetch(chunk, window_ids, n);
            std::memcpy(chunk.row_ids.data(), ids + offset, n * sizeof(int64_t));
            out.emplace_back(std::move(chunk));
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
            trace(log_,
                  "agent_disk[{}]::scan_local: oid {} not owned by this agent",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return batches;
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::scan_local: oid {} is a DISK record-only marker",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return batches;
        }
        // The adapter surfaces any buffer-pool OOM / data_corruption the table-layer scan left
        // in state.table_state.scan_error; propagate it up the wrapper.
        auto scan_r = entry->storage->scan_batched(batches, filter, limit, projected_cols, txn);
        if (scan_r.has_error()) {
            return scan_r.convert_error<std::pmr::vector<components::vector::data_chunk_t>>();
        }
        return batches;
    }

    // Thin mailbox wrapper over scan_local (D6: same-actor callers use the local
    // helper directly; this exists for the manager→agent mailbox route). The reply carries the
    // scan_error; the manager funnel / operators read has_error() before .value().
    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::storage_scan_inner(components::catalog::oid_t table_oid,
                                     std::unique_ptr<components::table::table_filter_t> filter,
                                     int64_t limit,
                                     std::vector<size_t> projected_cols,
                                     components::table::transaction_data txn) {
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
        co_return scan_local(table_oid, filter.get(), limit, projected_ptr, txn);
    }

    // Shared bounded streaming-scan skeleton for the send-free agent-local reducers
    // (aggregate-pushdown REDUCE + fk_hash_semijoin). Owns the position + fetch loop:
    // re-seek from row 0, read ONE batch at a time (applying `filter` + `projected`), and
    // invoke `fn(batch)` per NON-empty batch until the slice drains. `fn` is a TEMPLATE
    // callable (R14 — NOT std::function) returning core::error_t, so a per-batch failure
    // (e.g. group.push) stops the loop and propagates. A fetch_next_batch error stops the
    // loop and returns that error; a clean drain (empty batch) returns no_error(). Peak
    // memory is one batch — nothing pinned survives the round-trip. NOT [[nodiscard]]:
    // fk_hash_semijoin intentionally discards the error to return a PARTIAL result.
    template <typename PerBatch>
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

    // Agent-side aggregate-pushdown REDUCE. Builds the operator_group DIRECTLY
    // from the POD (no create_plan / node tree) and drives it over one owned `storage` slice
    // ENTIRELY LOCALLY (send-free) — a bounded storage_t::fetch_next_batch loop applies the shipped
    // WHERE `filter` + `projected_cols`, folding each batch into the group; finalize() then
    // materializes the FINAL aggregated rows (one per group). Peak memory is one batch + the
    // bounded group table — the same streaming discipline as fk_hash_semijoin. Empty slice: a
    // scalar aggregate still emits its single (NULL/COUNT-0) row via operator_group's empty-
    // input finalize; a GROUP BY emits nothing. `txn` is the caller's real snapshot so the
    // reduce sees read-your-own-writes (never a zero txn{0,0}). A fetch / push / finalize failure
    // surfaces as an error_t on the result wrapper — DISTINCT from a legitimately-empty result —
    // never thrown across the mailbox (R2), so a scalar-aggregate error is not mistaken for a
    // drained (no-row) reply.
    // `storage` may be NULL: a not-owned / record-only slice reduces over an EMPTY
    // input — the batch loop is skipped and operator_group's empty-input finalize
    // still emits a scalar aggregate's mandatory single row (typed via output_types).
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

        // (1) Agent-local function registry (the agent has none of the executor's state). The
        //     optimizer refused any UDF (is_udf_uid), so every builtin func_uid resolves here.
        components::compute::function_registry_t reg{resource};
        components::compute::register_default_functions(reg);

        // (2) Rebuild the operator_group from the POD: plain-column keys + builtin
        //     SUM/COUNT/MIN/MAX/AVG (COUNT(*) == empty arg path). No HAVING / DISTINCT / computed
        //     columns (the optimizer never stamps those), so internal_aggregate_count==0. HAVING is
        //     no longer a group concern — it is a separate node_having_t / operator_match above.
        ops::operator_group_t group{resource, log.clone(), 0};
        for (const auto& gk : spec.group_keys) {
            ops::group_key_t key{resource};
            key.name.assign(gk.name.begin(), gk.name.end());
            key.type = ops::group_key_t::kind::column;
            key.full_path.assign(gk.path.begin(), gk.path.end());
            group.add_key(std::move(key));
        }
        for (const auto& agg : spec.aggregates) {
            std::pmr::vector<components::expressions::param_storage> args{resource};
            if (!agg.arg_col_path.empty()) {
                components::expressions::key_t k{resource};
                std::pmr::vector<size_t> p{resource};
                p.assign(agg.arg_col_path.begin(), agg.arg_col_path.end());
                k.set_path(std::move(p));
                args.emplace_back(std::move(k));
            } // else COUNT(*): empty args (operator_group treats it as count-star)
            group.add_value(agg.alias,
                            boost::intrusive_ptr(new ops::aggregate::operator_func_t(resource,
                                                                                     log.clone(),
                                                                                     reg.get_function(agg.func_uid),
                                                                                     std::move(args),
                                                                                     agg.distinct)));
        }
        // MANDATORY: forward the plan-resolved FINAL output types so an empty-slice scalar
        // result stays typed (SUM(int)->INTEGER NULL) instead of the 0-byte NA sentinel (gcc -O3).
        group.set_output_types(spec.output_types);

        // (3) Pipeline context for group.push/finalize. Build IN PLACE (its move-ctor DROPS
        //     txn/function_registry — NEVER move it). No parameters/session_tz are needed: the
        //     WHERE is already baked into `filter`, and builtin SUM/COUNT/... read neither.
        components::logical_plan::storage_parameters params{resource};
        components::pipeline::context_t ctx{session,
                                            self_address,
                                            actor_zeta::address_t::empty_address(),
                                            &reg,
                                            params};
        ctx.txn = txn;

        // (4) Send-free streaming drive: re-seek + read ONE batch from `storage` (applying the
        //     WHERE filter + projection), fold it into the group, repeat. fetch_next_batch walks
        //     past fully-filtered vectors internally, so an empty batch means end-of-scan. A
        //     NULL storage (not-owned / record-only slice) is an empty input: skip straight to
        //     the finalize, which still emits the scalar empty-slice row.
        if (storage != nullptr) {
            const std::vector<std::size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
            components::storage::scan_position_t pos{};
            ops::chunks_vector_t sink{resource}; // group.push is a sink — appends nothing here
            if (auto err = for_each_storage_batch(*storage,
                                                  pos,
                                                  filter,
                                                  projected_ptr,
                                                  txn,
                                                  resource,
                                                  [&](components::vector::data_chunk_t& batch) {
                                                      return group.push(&ctx, std::move(batch), sink);
                                                  });
                err.contains_error()) {
                return err;
            }
        }

        // (5) Finalize into the reply chunks (single owner finalizes — identity passthrough).
        if (auto err = group.finalize(&ctx, out); err.contains_error()) {
            return err;
        }
        return out;
    }

    // Streaming fetch-next scan source (STEP 3 / phase B). POSITION-ONLY index-resume: the cursor
    // in active_scans_ stores ONLY the absolute resume position + the scan params; every fetch
    // re-seeks a TRANSIENT scan state from that position (storage_t::fetch_next_batch), reads ONE
    // batch, advances the stored position, and lets the pins destruct — so peak scan memory is one
    // batch and ZERO pins survive this round-trip. cursor_id==0 OPENs (minting a (session,counter)
    // id, capping the matched-row head at offset+limit); non-zero ADVANCEs the same cursor. The
    // cursor is GC'd (erased) the moment it drains or hits the matched-row limit.
    agent_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    agent_disk_t::storage_fetch_next_batch_inner(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t cursor_id,
                                                 std::unique_ptr<components::table::table_filter_t> filter,
                                                 int64_t limit,
                                                 std::vector<size_t> projected_cols,
                                                 components::table::transaction_data txn) {
        // Drained sentinel: an EMPTY chunk (cardinality 0). The executor breaks on size 0 and never
        // pushes it, so the schema is irrelevant (the source operator carries its own projected
        // empty-guard).
        auto make_drained = [this](uint64_t reply_cursor_id) -> fetch_batch_t {
            auto empty = std::make_unique<components::vector::data_chunk_t>(
                resource(),
                std::pmr::vector<components::types::complex_logical_type>{resource()},
                components::vector::DEFAULT_VECTOR_CAPACITY);
            empty->set_cardinality(0);
            return fetch_batch_t{std::move(empty), reply_cursor_id};
        };

        if (cursor_id == 0) {
            // OPEN: resolve the owned slice entry and snapshot the source-row bound. A not-owned /
            // record-only oid replies a drained sentinel (no cursor minted), mirroring the
            // whole-vector path's empty reply. (A pushed-aggregate REDUCE rides the dedicated
            // storage_reduce path — this handler is a raw scan only.)
            auto it = storages_.find(table_oid);
            if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
                trace(log_,
                      "agent_disk[{}]::storage_fetch_next_batch_inner: oid {} not owned / record-only — drained",
                      pool_idx_,
                      static_cast<unsigned>(table_oid));
                co_return make_drained(0);
            }
            active_scan_t scan{};
            scan.table_oid = table_oid;
            // Raw scan cursor: position-only, re-seeks per fetch. `limit` is the (offset+limit)
            // head cap the source pushed down; with a filter it is a POST-filter matched-row
            // cap (the legacy scan_batched applied it post-hoc), so it bounds matched rows
            // handed out, never source rows scanned.
            scan.pos.next_row = 0;
            scan.pos.max_row = static_cast<int64_t>(it->second->storage->total_rows());
            scan.filter = std::move(filter);
            scan.projected_cols = std::move(projected_cols);
            scan.txn = txn;
            scan.matched_limit = limit;
            // Mint cursor id = (session, agent counter) per R16: the session disambiguates across
            // queries, the agent-local counter across concurrent cursors of one session. Fall back
            // to the bare counter on the (vanishingly unlikely) reserved-0 / collision.
            const uint64_t counter = next_scan_cursor_id_++;
            const uint64_t minted = (session.data() << 20) ^ counter;
            cursor_id = (minted == 0 || active_scans_.find(minted) != active_scans_.end()) ? counter : minted;
            active_scans_.try_emplace(cursor_id, std::move(scan));
            // fall through to ADVANCE and hand out the first batch
        }

        auto cit = active_scans_.find(cursor_id);
        if (cit == active_scans_.end()) {
            // Unknown / already-drained cursor.
            co_return make_drained(cursor_id);
        }
        auto& scan = cit->second;

        // Position exhausted or matched-row limit already met: GC and reply drained.
        if (scan.pos.drained ||
            (scan.matched_limit >= 0 && scan.matched_emitted >= static_cast<uint64_t>(scan.matched_limit))) {
            active_scans_.erase(cit);
            co_return make_drained(cursor_id);
        }

        // Re-resolve the storage (a concurrent DROP between fetches drains the cursor).
        auto storage_it = storages_.find(table_oid);
        if (storage_it == storages_.end() || storage_it->second == nullptr || storage_it->second->storage == nullptr) {
            active_scans_.erase(cit);
            co_return make_drained(cursor_id);
        }
        auto* storage = storage_it->second->storage.get();

        // Construct the projected output chunk, then re-seek + read ONE batch from the stored
        // position. fetch_next_batch builds a transient scan state, pins only one batch's segments,
        // and releases them before returning — nothing pinned survives this handler.
        auto all_types = storage->types();
        const std::vector<size_t>* projected_ptr = scan.projected_cols.empty() ? nullptr : &scan.projected_cols;
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

        // Enforce the post-filter matched-row limit across batches: trim the boundary batch to the
        // remaining budget and stop advancing once it is spent.
        if (scan.matched_limit >= 0) {
            const uint64_t budget = static_cast<uint64_t>(scan.matched_limit) - scan.matched_emitted;
            if (batch->size() >= budget) {
                batch->set_cardinality(budget);
                scan.pos.drained = true;
            }
        }
        scan.matched_emitted += batch->size();

        if (batch->size() == 0) {
            // No rows this round (drained, or a boundary batch trimmed to 0): GC and reply drained.
            active_scans_.erase(cit);
            co_return make_drained(cursor_id);
        }
        co_return fetch_batch_t{std::move(batch), cursor_id};
    }

    // AGGREGATE-PUSHDOWN REDUCE — the DEDICATED protocol leg. Runs the whole
    // GROUP BY over this agent's OWN slice (send-free, synchronous: reduce_pushed_aggregate
    // rebuilds the operator_group from the POD and applies the shipped WHERE
    // `filter` + `projected_cols`), and replies ALL final aggregated rows in ONE reply —
    // the result is bounded by #groups, so no cursor is minted and the raw-scan protocol
    // stays a pure scan. A not-owned / record-only oid reduces over the EMPTY input: the
    // group's empty-input finalize still emits a scalar aggregate's single (COUNT=0 /
    // NULL) row, typed via spec.output_types. SINGLE-OWNER INVARIANT: these are FINAL
    // rows (not partials) — valid only while ONE agent owns the whole table; sharded
    // slices would need partial states + a real coordinator merge (operator_group_merge
    // is the socket for that).
    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::storage_reduce_inner(session_id_t session,
                                       components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::table::table_filter_t> filter,
                                       std::vector<size_t> projected_cols,
                                       components::table::transaction_data txn,
                                       components::operators::pushed_aggregate_spec_t spec) {
        auto it = storages_.find(table_oid);
        const bool no_storage =
            (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr);
        auto reduced_r = reduce_pushed_aggregate(resource(),
                                                 log_.clone(),
                                                 no_storage ? nullptr : it->second->storage.get(),
                                                 session,
                                                 address(),
                                                 filter.get(),
                                                 projected_cols,
                                                 txn,
                                                 spec);
        // A real reduce error (fetch / push / finalize) rides the wrapper as an error_t —
        // DISTINCT from a legitimately-empty GROUP BY result — so a scalar-aggregate
        // failure surfaces instead of masquerading as an empty reply. No throw across the
        // mailbox (R2).
        if (reduced_r.has_error()) {
            co_return reduced_r;
        }
#ifdef DEV_MODE
        // Record EXACTLY the rows that will cross the agent->coordinator mailbox: the sum
        // over the reduced chunks (1 for a scalar aggregate, one per group for a GROUP BY)
        // — never the raw scanned rows. See pushdown_reply_rows().
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

    agent_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::storage_scan_segment_inner(components::catalog::oid_t table_oid, int64_t start, uint64_t count) {
        std::pmr::vector<components::vector::data_chunk_t> out{resource()};
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_scan_segment_inner: oid {} not owned by this agent — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::move(out);
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_scan_segment_inner: oid {} is a DISK record-only marker — "
                  "fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::move(out);
        }
        auto types = entry->storage->types();
        // scan_segment yields ≤DEFAULT_VECTOR_CAPACITY chunks; copy each into its own
        // owning chunk (the callback chunk is transient) and collect them — no merge into
        // an oversized chunk.
        entry->storage->scan_segment(start, count, [&](components::vector::data_chunk_t& chunk) {
            const auto chunk_rows = chunk.size();
            if (chunk_rows == 0) {
                return;
            }
            components::vector::data_chunk_t one(resource(), types, chunk_rows);
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                auto& src = chunk.data[col];
                if (src.get_vector_type() != components::vector::vector_type::FLAT) {
                    src.flatten(chunk_rows);
                }
                components::vector::vector_ops::copy(src, one.data[col], chunk_rows, 0, 0);
            }
            components::vector::vector_ops::copy(chunk.row_ids, one.row_ids, chunk_rows, 0, 0);
            one.set_cardinality(chunk_rows);
            out.emplace_back(std::move(one));
        });
        co_return std::move(out);
    }

    // STREAMING SINGLE-PASS HASH SEMI-JOIN. Given the input key-tuple set
    // (`keys`, column j == key_col_indices[j]-th stored column, row i == key-tuple i) and one
    // owned `storage`, return result[i] = the row_ids of every table row whose key columns
    // equal key-tuple i (one bucket per input key, input order; empty when nothing matches).
    // Builds ONE typed hash of the key set, then STREAMS the table exactly once
    // (storage.fetch_next_batch, projected to the key columns), probing each streamed row
    // against the hash and bucketing its row_id into every matching key — one scan pass per
    // call (i.e. per <=1024-key input chunk the FK operators send), NOT one per key, so
    // O(table_rows + nkeys) instead of O(nkeys * table_rows). No logical_value_t round-trip
    // (R1): typed data_chunk_t::hash + the NULL-aware typed verify
    // components::vector::cells_equal, shared with GROUP BY / HASH JOIN / UNIQUE.
    std::pmr::vector<std::pmr::vector<std::int64_t>>
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
        // Arity guard: a mismatch (chunk column count != resolved key columns) or an empty key
        // column set voids the whole batch with one empty bucket per key.
        if (nkeys == 0 || key_col_indices.empty() || keys.column_count() != key_col_indices.size()) {
            return result;
        }

        const auto& cols = storage.columns();

        // PHYSICAL-TYPE NORMALIZATION. FK equality must coerce cross-type keys (INT-width,
        // INT<->FLOAT — constant_filter_t::compare<T>, column_state.hpp), but a raw typed hash
        // does NOT coerce — so cast each input key column to its STORED key column's physical
        // type before hashing; both sides then hash identically. (Temporal cross-UNIT FKs —
        // DATE(days) <-> TIMESTAMP(µs) — are out of scope for this raw cast; an FK column
        // normally shares the referenced column's exact type.)
        std::pmr::vector<components::types::complex_logical_type> stored_key_types{resource};
        stored_key_types.reserve(key_col_indices.size());
        for (auto ci : key_col_indices) {
            stored_key_types.push_back(cols[ci].type());
        }
        components::vector::data_chunk_t norm_keys(resource, stored_key_types, nkeys);
        norm_keys.set_cardinality(nkeys);
        for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
            auto& src = keys.data[j];
            if (src.get_vector_type() != components::vector::vector_type::FLAT) {
                src.flatten(nkeys);
            }
            if (src.type().to_physical_type() == stored_key_types[j].to_physical_type()) {
                components::vector::vector_ops::copy(src, norm_keys.data[j], nkeys, 0, 0);
            } else {
                norm_keys.data[j] = components::vector::vector_ops::cast_vector(resource, src, stored_key_types[j], nkeys);
            }
        }

        // Typed hash index: tuple-hash -> input key indices. Skip any input tuple with a NULL
        // key cell (a NULL foreign key references nothing — matches the callers' MATCH null-
        // skip), so it never matches a scanned row. Nullness is read from the ORIGINAL keys
        // chunk (cast_vector does not carry validity).
        components::vector::vector_t key_hash_vec(resource, components::types::logical_type::UBIGINT, nkeys);
        std::vector<std::uint64_t> norm_col_ids(key_col_indices.size());
        for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
            norm_col_ids[j] = j;
        }
        norm_keys.hash(norm_col_ids, key_hash_vec);
        const auto* key_hashes = key_hash_vec.data<std::uint64_t>();
        std::pmr::unordered_map<std::uint64_t, std::pmr::vector<std::uint64_t>> key_index{resource};
        for (std::uint64_t i = 0; i < nkeys; ++i) {
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

        // STREAM the table ONCE, projecting to the key columns in KEY ORDER so both sides' hash
        // col-ids align. fetch_next_batch re-seeks a TRANSIENT scan from pos each call, so peak
        // memory is one batch and nothing pinned survives — the same streaming source the agent
        // drives for the fetch-next scan.
        std::vector<std::size_t> projected_cols(key_col_indices.begin(), key_col_indices.end());
        std::vector<std::uint64_t> scan_col_ids(key_col_indices.begin(), key_col_indices.end());
        components::storage::scan_position_t pos{};
        // The scan error is intentionally NOT propagated (return discarded): a fetch failure
        // stops the scan and leaves the remaining buckets empty — a PARTIAL result callers
        // already tolerate.
        for_each_storage_batch(
            storage, pos, /*filter=*/nullptr, &projected_cols, txn, resource,
            [&](components::vector::data_chunk_t& batch) -> core::error_t {
                const uint64_t rows = batch.size();
                // Flatten the projected key columns so the typed verify can read raw cells.
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
                    // Skip a scanned row with ANY NULL key cell (a NULL never satisfies FK equality).
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
                    // Verify every hash-colliding input key against this row (typed, no coercion —
                    // both sides are already in the stored physical type) and bucket the row_id into
                    // each matching key (duplicate input keys each collect the row).
                    for (std::uint64_t cand : it_h->second) {
                        bool match = true;
                        for (std::size_t j = 0; j < key_col_indices.size(); ++j) {
                            if (!components::vector::cells_equal(norm_keys.data[j], cand, batch.data[key_col_indices[j]], r)) {
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
        return result;
    }

    // Resolve the key-column NAMES to storage column indices against an owned slice entry.
    // Returns true (and fills out_indices in name order) on success; false when the entry is not
    // owned / a record-only marker / has no key columns / names a column the table lacks — the
    // caller then degrades the whole keyed batch to one empty entry per key. Shared by
    // scan_by_keys_inner and read_chunks_by_keys_inner.
    static bool resolve_key_col_indices(const collection_storage_entry_t* entry,
                                        const std::pmr::vector<std::string>& key_col_names,
                                        std::pmr::vector<std::uint64_t>& out_indices) {
        if (entry == nullptr || entry->storage == nullptr || key_col_names.empty()) {
            return false;
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
                return false;
            }
            out_indices.push_back(static_cast<std::uint64_t>(col_idx));
        }
        return true;
    }

    agent_disk_t::unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    agent_disk_t::scan_by_keys_inner(components::catalog::oid_t table_oid,
                                     std::pmr::vector<std::string> key_col_names,
                                     components::vector::data_chunk_t keys,
                                     components::table::transaction_data txn) {
        // result[i] = row_ids matching keys[i]; one (possibly empty) entry per key,
        // preserving input order. Name→index resolution runs once for the whole batch,
        // then the whole key set is answered by ONE streamed hash semi-join pass
        // (fk_hash_semijoin), not one filtered scan per key.
        std::pmr::vector<std::pmr::vector<std::int64_t>> result{resource()};
        result.reserve(keys.size());

        // Resolve the key column NAMES to storage indices once (same column set for every key).
        // A not-owned / record-only / empty-names / unknown-column entry degrades the whole batch
        // to one empty row per key.
        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        if (!resolve_key_col_indices(entry, key_col_names, key_col_indices)) {
            for (std::size_t i = 0; i < keys.size(); ++i) {
                result.emplace_back();
            }
            co_return std::move(result);
        }

        // Delegate the actual match to the streaming SINGLE-PASS hash semi-join: ONE streamed
        // pass over the table per call, regardless of key count. The helper is a free function
        // so it can be driven directly by a counting storage in unit tests; scan_by_keys_inner
        // is the sole production caller (single path, R6). The `result` reserved above is only
        // consumed by the early-return (not-owned / unknown-column) branches; here the helper
        // owns the reply.
        co_return fk_hash_semijoin(resource(), *entry->storage, key_col_indices, keys, txn);
    }

    agent_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                           std::pmr::vector<std::string> key_col_names,
                                           components::vector::data_chunk_t keys,
                                           components::table::transaction_data txn) {
        // Single key-tuple: `keys` already carries exactly one row, so it IS the 1-row
        // batch view the plural read_chunks_by_keys_inner expects. Delegate to it inline
        // on this same agent thread (its resolve/filter/scan_local body never crosses the
        // mailbox) and unwrap the single entry.
        std::pmr::vector<components::vector::data_chunk_t> empty{resource()};
        auto r = co_await read_chunks_by_keys_inner(table_oid, std::move(key_col_names), std::move(keys), txn);
        co_return r.empty() ? std::move(empty) : std::move(r[0]);
    }

    agent_disk_t::unique_future<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                            std::pmr::vector<std::string> key_col_names,
                                            components::vector::data_chunk_t keys,
                                            components::table::transaction_data txn) {
        // result[i] = matched chunks for key-tuple i; one (possibly empty) entry per key,
        // preserving input order. Name→index resolution runs once for the whole batch, then
        // each key gets an eq-AND filtered scan via scan_local (D6: no self-send).
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> result{resource()};
        result.reserve(keys.size());

        // Resolve the key column NAMES to storage indices once (same column set for every key).
        // A not-owned / record-only / empty-names / unknown-column entry degrades the whole batch
        // to one empty entry per key.
        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        if (!resolve_key_col_indices(entry, key_col_names, key_col_indices)) {
            for (std::size_t i = 0; i < keys.size(); ++i) {
                result.emplace_back();
            }
            co_return std::move(result);
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
            // Catalog-read path: a scan_error degrades this key's entry to empty, matching the
            // not-owned/record-only fallback (callers handle empty entries).
            auto scan_r = scan_local(table_oid, filter.get(), int64_t{-1}, nullptr, txn);
            if (scan_r.has_error()) {
                result.emplace_back();
            } else {
                result.emplace_back(std::move(scan_r.value()));
            }
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

    agent_disk_t::unique_future<uint64_t> agent_disk_t::storage_total_rows_inner(components::catalog::oid_t table_oid) {
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

    agent_disk_t::unique_future<checkpoint_result_t>
    agent_disk_t::checkpoint_inner(session_id_t /*session*/, wal::id_t current_wal_id, uint64_t compact_watermark) {
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

            // Cursor gate: skip compact while a streaming fetch-next cursor is open on this oid —
            // its stored absolute position indexes the un-swapped collection, so the atomic
            // row_groups_ swap would shift rows out from under it (R17). The entry is still
            // checkpointed below WITHOUT the rebuild; the WAL keeps its replay records, so a later
            // checkpoint round compacts it once the cursor drains.
            if (has_active_scan_for_oid(tbl_oid)) {
                trace(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} has an active scan cursor — skipping compact this round",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid));
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
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

            // checkpoint(wal_id) returns out_of_memory on a column flush pin failure; it
            // aborts BEFORE the header swap and leaves the wal_id fields unchanged. On error,
            // defer this entry to a later round (same as the MVCC-gate skip above): restore
            // the .prev backup over any partial write, do NOT persist the sidecar or delete
            // the backup, and feed the unchanged prev_checkpoint_wal_id into the min() so the
            // WAL keeps this table's replay records.
            auto cp_r = entry->table_storage.checkpoint(current_wal_id);
            if (cp_r.has_error()) {
                warn(log_,
                     "agent_disk[{}]::checkpoint_inner oid={} checkpoint failed (rules 2/9) — deferring this round",
                     pool_idx_,
                     static_cast<unsigned>(tbl_oid));
                if (std::filesystem::exists(prev_path)) {
                    std::error_code restore_error;
                    std::filesystem::copy_file(prev_path,
                                               otbx_path,
                                               std::filesystem::copy_options::overwrite_existing,
                                               restore_error);
                    std::error_code remove_error;
                    std::filesystem::remove(prev_path, remove_error);
                }
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                continue;
            }

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
            // Cursor gate: skip compact while a streaming fetch-next cursor is open on this oid (its
            // stored absolute position indexes the un-swapped collection; the atomic swap would
            // shift rows out from under it — R17). Reclaim is deferred to a later vacuum round.
            if (has_active_scan_for_oid(oid)) {
                continue;
            }
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

        // Cursor gate: skip compact while a streaming fetch-next cursor is open on this oid (its
        // stored absolute position indexes the un-swapped collection; the atomic swap would shift
        // rows out from under it — R17). Reclaim is deferred to a later commit.
        if (has_active_scan_for_oid(table_oid)) {
            trace(log_,
                  "agent_disk[{}]::maybe_cleanup_inner: oid={} has an active scan cursor — deferring compact",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }

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
        if (dropped_storages_.empty() && manager_dispatcher_addr_ != actor_zeta::address_t::empty_address()) {
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
    void agent_disk_t::set_manager_wal_sync(actor_zeta::address_t address) { manager_wal_addr_ = std::move(address); }

    // GC-slice push-back (see header). Called pre-scheduler-start by base_spaces
    // catalog rebuild and at runtime by mark_storage_dropped_many_inner.
    void agent_disk_t::register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                           uint64_t dropped_at_commit_id,
                                                           std::filesystem::path path,
                                                           std::pmr::vector<std::filesystem::path> sidecar_paths) {
        dropped_storages_.push_back(
            dropped_storage_entry_t{oid, dropped_at_commit_id, std::move(path), std::move(sidecar_paths)});
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
            components::vector::data_chunk_t wal_chunk(resource(), row.types(), row.size());
            wal_chunk.set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk.data[col].set_value(r, row.data[col].value(r));
                }
            }
            std::pmr::vector<components::vector::data_chunk_t> wal_chunks(resource());
            wal_chunks.emplace_back(std::move(wal_chunk));
            // pg_catalog writes route to main_database (ctx.database_oid is always
            // INVALID_OID for catalog writes).
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(manager_wal_addr_,
                                             &wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_chunks),
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
        if (it != storages_.end() && it->second != nullptr && it->second->storage != nullptr && row.size() != 0) {
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
                                casted.set_value(r, src_vec.value(r).cast_as(target_type, ctx.session_tz));
                            } else {
                                casted.validity().set_invalid(r);
                            }
                        }
                        local.data[i] = std::move(casted);
                    }
                }
            }

            // The append chain can surface write_conflict / out_of_memory.
            // append_pg_catalog_row_inner returns a pg_catalog_append_range_t with no error
            // channel; on a failure leave start_row/count at 0 (no rows materialized) and
            // log — the caller treats a zero-count range as a no-op append.
            auto append_r = s->append(local, ctx.txn);
            if (append_r.has_error()) {
                warn(log_,
                     "agent_disk[{}]::append_pg_catalog_row_inner: materialize failed for oid={} — no rows appended",
                     pool_idx_,
                     static_cast<unsigned>(table_oid));
                co_return components::pg_catalog_append_range_t{table_oid, int64_t{0}, 0};
            }
            start_row = append_r.value();
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

        core::pmr::otterbrix_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        detail::inline_scan(entry->table_storage.table(),
                            {oid_col_idx},
                            &scan_resource,
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
    agent_disk_t::unique_future<void>
    agent_disk_t::update_pg_attribute_commit_id_field_inner(execution_context_t ctx,
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

        core::pmr::otterbrix_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        std::pmr::vector<components::types::logical_value_t> row_values(resource());
        row_values.reserve(col_count);

        detail::inline_scan(tbl,
                            all_col_indices,
                            &scan_resource,
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
            auto [_w, wf] = actor_zeta::send(manager_wal_addr_,
                                             &wal::manager_wal_replicate_t::write_physical_update,
                                             ctx.session,
                                             pg_attr_oid,
                                             std::move(wal_row_ids),
                                             std::move(wal_chunks),
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
        register_dropped_storage_inner_sync(table_oid, dropped_at_commit_id, std::move(otbx_path), std::move(sidecars));
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
