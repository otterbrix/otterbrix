#include "agent_disk.hpp"
#include "inline_scan.hpp" // services::disk::detail::inline_scan (catalog DDL on the agent)
#include "manager_disk.hpp"
#include <algorithm>                              // std::min
#include <components/logical_plan/node_group.hpp> // node_group_t::set_pushdown (re-lowering guard)
#include <components/physical_plan/operators/operator_hash_group.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp> // source-swap leaf accessors
#include <components/physical_plan_generator/create_plan.hpp> // create_plan + function_registry + context_storage_t
#include <components/vector/cell_equal.hpp>                   // components::vector::cells_equal (typed FK hash-verify)
#include <components/vector/vector_operations.hpp>
#include <fstream>
#include <services/dispatcher/dispatcher.hpp>

namespace services::disk {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_table_checkpoints{0};
    } // namespace

    uint64_t table_checkpoints() noexcept { return g_table_checkpoints.load(std::memory_order_relaxed); }
    void reset_table_checkpoints() noexcept { g_table_checkpoints.store(0, std::memory_order_relaxed); }
#endif

    using namespace core::filesystem;

    // Test-observable counter of ROWS shipped in the last aggregate-pushdown reduce reply
    // (see agent_disk.hpp). Bumped by the sum of data_chunk_t::size() over the reduced
    // chunks right before they cross the mailbox; tests reset it, run one aggregate,
    // then assert the count is TINY vs. the scanned input. DEV_MODE-only.
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

    // Borrowed pointer — see header. nullptr when the OID isn't owned.
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
        std::vector<components::table::column_definition_t> catalog_columns,
        bool is_computed) noexcept {
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
        // `catalog_columns` is copied rather than moved: it is ALSO the input to the oid-set
        // difference below, which is what re-derives "the catalog has it, this storage does not"
        // on every load. Doing it here rather than only in the bootstrap walk is what covers the
        // LAZY load — after bootstrap only the pg_catalog.* tables are resident, so a user table
        // ALTERed in a previous run is first seen right here.
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), otbx_path, catalog_columns, is_computed);
        // The DISK load ctor records io_error/data_corruption instead of throwing (this helper is noexcept
        // and reachable on the agent thread). Drop a failed-construction entry so we never emplace a
        // half-loaded storage; the manager-side probe has already refused an unopenable file before this.
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

    // Runtime CREATE mailbox handler (see header). The entry is built on the AGENT's
    // OWN resource() here on the agent thread, then emplaced via the existing
    // bootstrap_create_disk_inner_sync helper (now called intra-actor). Returns false on
    // duplicate key, mirroring the helper's contract.
    agent_disk_t::unique_future<bool>
    agent_disk_t::create_storage_disk_inner(components::catalog::oid_t oid,
                                            std::vector<components::table::column_definition_t> columns,
                                            std::filesystem::path otbx_path,
                                            bool is_computed) {
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

    // The ONE refusal the three WAL-replay helpers below share. It is not a routing miss:
    // manager_disk_t picks the agent with pool_idx_for_oid(table_oid) before forwarding, so
    // the owner is settled and this agent is it. An absent entry means the owner holds no
    // storage for the table, and a replay mutation dropped there is a journalled change that
    // recovery quietly declined to apply.
    core::error_t agent_disk_t::no_replay_storage_error(const char* who, components::catalog::oid_t table_oid) {
        std::pmr::string msg{"agent_disk::", resource()};
        msg += std::pmr::string{who, resource()};
        msg += std::pmr::string{": no storage on the owning agent for table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        msg += std::pmr::string{" — the journalled change was NOT replayed", resource()};
        return core::error_t{core::error_code_t::io_error, std::move(msg)};
    }

    // WAL-replay direct_* helpers (see header). Mutation logic is intentionally
    // minimal: schema-adoption / column-expansion / type-promotion run upstream in
    // the mailbox body, and replay records arrive pre-aligned with the table schema,
    // so a direct delete/update against the entry's storage adapter is correct.
    core::error_t agent_disk_t::direct_delete_sync(components::catalog::oid_t table_oid,
                                                   const std::pmr::vector<int64_t>& row_ids,
                                                   uint64_t count,
                                                   const components::table::transaction_data& txn) {
        if (row_ids.empty()) {
            // The one legitimate no-op: the record names no rows.
            return core::error_t::no_error();
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
        for (uint64_t i = 0; i < count && i < row_ids.size(); i++) {
            ids_vec.set_value(i, row_ids[i]);
        }
        entry->storage->delete_rows(ids_vec, count, txn.transaction_id);
        return core::error_t::no_error();
    }

    core::error_t agent_disk_t::direct_update_sync(components::catalog::oid_t table_oid,
                                                   const std::pmr::vector<int64_t>& row_ids,
                                                   components::vector::data_chunk_t& new_data) {
        if (row_ids.empty()) {
            return core::error_t::no_error();
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
        // new_data is on the WAL-replay resource; the storage is on this agent's.
        // update() slices zero-copy refs into the chunk, so deep-copy onto resource()
        // first — else validity_mask_t::operator= asserts resource_ == other.resource_
        // on Debug builds. See docs/wal-recovery-pmr-mismatch.md.
        components::vector::data_chunk_t local(resource(), new_data.types(), new_data.size());
        new_data.copy(local, 0);
        entry->storage->update(ids_vec, local);
        return core::error_t::no_error();
    }

    core::error_t agent_disk_t::direct_add_column_sync(components::catalog::oid_t table_oid,
                                                       const components::vector::data_chunk_t& schema_chunk) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            return no_replay_storage_error("direct_add_column_sync", table_oid);
        }
        auto& entry = it->second;
        auto* s = entry->storage.get();
        // For each schema column, add it unless an equivalent column already exists
        // (idempotent replay). The column type carries its alias = the column name.
        // B1b: presence mirrors stage 1b of storage_append_inner — a computed
        // (relkind='g') table keys columns by (name, type) so each type variant owns
        // its own physical column; name-only matching there silently dropped a
        // replayed variant's ADD_COLUMN and the dependent PHYSICAL_INSERT chunk
        // (one column wider than the table) aborted in collection_t::append.
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
            // RN-oid: a materialised column must carry its pg_attribute.attoid. This replay
            // leg re-applies a schema-growth record, so the identity comes from whatever the
            // catalog published for this table (see
            // collection_storage_entry_t::note_column_identity). 0 means nothing published it
            // — the relkind='g' case, whose columns live in pg_computed_column and which the
            // bootstrap reconciliation never walks.
            def.set_attoid(entry->take_column_identity(name));
            entry->add_column(def, resource());
            // add_column rebuilt the adapter; refresh the local pointer.
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
            default:
                break;
        }
    }

    // Mutation fanout targets. The manager router pre-validates, but the agent
    // re-checks because it owns its slice independently — and the re-check REFUSES rather
    // than no-ops: this agent was selected by pool_idx_for_oid, so "not owned here" means
    // the owner has no storage, and a mutation that did not happen must not report the same
    // (0, 0) an empty request reports.

    agent_disk_t::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
    agent_disk_t::storage_append_inner(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::vector::data_chunk_t> data) {
        const auto txn = ctx.txn;
        // Nothing to write is not a refusal: an empty request has an empty answer, and the
        // whole pipeline below would be a no-op anyway.
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

        // 1b. Dynamic schema growth, on every table. B1a made the .otbx the only
        // substrate, so the mode gate this used to sit behind would have turned growth off
        // for every table — yet growth is load-bearing twice over: computed (relkind='g')
        // tables adopt per-document columns (multi-type variants included), and
        // regular tables materialize an ALTER TABLE ADD COLUMN's storage columns
        // on the first INSERT that carries them (ALTER writes only pg_attribute;
        // without growth the catalog says 4 columns while the storage holds 2 and
        // the next SELECT of the new columns fails). add_column itself is a
        // data_table_t rebuild over the same block manager, and
        // the PHYSICAL_ADD_COLUMN WAL record already replays it.
        // Trigger: alias mismatch at differing chunk/table width = schema
        // growth; equal width = positional rename, handled by column expansion
        // below.
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
                    // RN-oid: stamp the identity the catalog published ahead of this
                    // materialisation. For a regular table that publisher is the ALTER TABLE
                    // ADD COLUMN commit (or bootstrap re-publishing it after a crash); for a
                    // relkind='g' table there is none — its columns are described by
                    // pg_computed_column, and the reconciliation excludes 'g' at the source.
                    col.set_attoid(entry->take_column_identity(col.name()));
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
            // Matching is BY NAME only. There used to be a positional fallback here —
            // when the incoming column count happened to equal the table's, an
            // unmatched name took whatever sat at the same ordinal — and it stood on a
            // path that no longer needs it: the insert operator renames every column to
            // its target and appends the ones the statement omitted, so a chunk arrives
            // full width with the right aliases. A named fallback on the very path being
            // rewritten is rule 6; it is gone, and a name that does not match now stays
            // unmatched instead of quietly picking up a neighbour's data.
            //
            // A column with no incoming vector is filled NULL. DEFAULTs are NOT applied
            // here: they are expanded ABOVE the journal, in operator_insert, from
            // pg_attribute — the one place they are stored. This layer used to
            // substitute them from its own column list, which after a restart carries
            // none (load_from_disk rebuilds column definitions without defaults), so the
            // catalog and the write path disagreed about what an omitted column held.
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
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], data->size());
                    expanded_data.back().validity().set_all_invalid(data->size());
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

        // (There is deliberately NO row-content dedup here. A pre-#460 document-store
        // stage used to full-scan the table per batch and silently drop rows whose
        // `_id` column value already existed — O(table rows) per insert, silent row
        // loss, and it masked a declared UNIQUE/PK on a column named `_id` by
        // filtering the duplicate before the constraint operator's existing-row scan
        // could see it. Uniqueness has exactly one implementation:
        // operator_unique_constraint_t over declared constraints.)

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
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                // WAL-FIRST means the journal is the promise the materialize below keeps. A
                // refused PHYSICAL_INSERT used to come back as a wal_id all the same, so the
                // rows were materialized and reported appended with nothing in the journal to
                // replay them from. Refuse the append instead — nothing is materialized, so
                // there is nothing to unwind.
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
        // A DIRECT WRITE owes no publish. Its deletes were stamped with an
        // immediately-committed version id when they landed (row_group_t::delete_rows,
        // is_txn == false), so there is no pending stamp for commit_all_deletes to find —
        // and asking it to look would tell it to rewrite every slot holding the literal 0.
        // See components::table::DIRECT_WRITE_TXN_ID for the whole contract; this is NOT
        // the "legacy fast path" the comment here used to call it, and the branch must stay.
        if (components::table::is_direct_write_txn(txn_id)) {
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
        // Abort-side twin of the same contract: a DIRECT WRITE is irrevocable by
        // construction — its deletes were committed the instant they landed — so there is
        // nothing to un-stamp, and revert_all_deletes(0) would match on the literal 0 the
        // store never writes. See components::table::DIRECT_WRITE_TXN_ID.
        if (components::table::is_direct_write_txn(txn_id)) {
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
        // Same split as storage_append_inner: an empty request is a success, a missing
        // storage is a refusal.
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
        // No preprocessing here: the manager body already aligned `data` with the entry's
        // canonical schema. The wrapper carries any write_conflict / out_of_memory as a value.
        co_return entry->storage->update(row_ids, *data, txn);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<uint64_t>>
    agent_disk_t::storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                            components::vector::vector_t row_ids,
                                            uint64_t count,
                                            components::table::transaction_data txn) {
        // Nothing asked, nothing marked. An empty request has an empty answer and is not a
        // refusal — unlike the three legs below, which are a delete that DID NOT HAPPEN.
        // They used to return 0 with only a trace line, and 0 is also what a perfectly
        // healthy delete of already-stamped rows returns, so no caller could tell them
        // apart. See the header: the count counts, the wrapper refuses.
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
                                      int64_t limit) {
        std::pmr::vector<components::vector::data_chunk_t> out{resource()};
        // Asking for no rows is not a refusal, whatever the oid — the same split the
        // delete and append legs make.
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
        auto types = entry->storage->types();
        // Fetch in ≤DEFAULT_VECTOR_CAPACITY windows so each produced chunk is born within
        // the capacity bound — no oversized chunk is ever materialized.
        //
        // `limit` is spent HERE, on rows the fetch actually produced, and never on the ids it
        // was handed. Under SNAPSHOT the fetch below drops every row `txn` may not see, so a
        // window of 1024 ids can yield anything from 0 to 1024 rows; a budget deducted from
        // the id count would run out on rows the reader never receives and answer a LIMIT
        // with fewer rows than it asked for. Spending it on produced rows also lets the loop
        // STOP: once the budget is met, the remaining windows are not read at all, which is
        // the whole point of pushing the cap down here instead of truncating upstream.
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
                // First window error aborts the whole batch: the reply must never
                // pair "success" with a chunk whose string cells were left empty
                // by a failed overflow-block read.
                co_return fetch_r.convert_error<std::pmr::vector<components::vector::data_chunk_t>>();
            }
            // The row_ids are NOT stamped here any more. This handler used to memcpy the
            // REQUEST over them, which made the field a restatement of the question rather
            // than a report of the answer — and the answer can be shorter, because the
            // producer drops rows this txn may not see and rows that name no row group at
            // all. collection_t::fetch stamps the ids of the rows it actually gathered, and
            // the guard below is on that pairing.
            assert(chunk.size() <= n && "storage_fetch_inner: a window produced more rows than it was asked for");
            if (capped && produced + chunk.size() > budget) {
                // Truncation, not selection: set_cardinality keeps the chunk's FIRST rows and
                // their stamped row_ids, so the capped reply stays a prefix of the uncapped one.
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
        // A scan that CANNOT BE PERFORMED is an error, never an empty answer. Both legs below
        // used to return an empty batch list, which is also what "no matching rows" looks like:
        // every catalog reader upstream (resolve_namespace / resolve_function_by_name /
        // find_cast_oid / list_namespaces) then reported a read it never made as a negative
        // fact about the catalog. Same wording and the same reasoning as
        // validate_key_col_indices below, which fixed the keyed-read twin of this leg.
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
        //     columns (the optimizer never stamps those).
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
                std::pmr::string what{"storage_fetch_next_batch: no materialized storage to open a scan on: ",
                                      resource()};
                what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                co_return core::error_t{core::error_code_t::missing_table, std::move(what)};
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

    // A4: release a cursor abandoned before it drained. Nothing else erases the entry — the
    // drain paths above only fire when the source keeps pulling — so a source that stops early
    // (error mid-pump, satisfied LIMIT, dropped sub-plan) used to leave the entry alive for the
    // life of the process, and a live entry permanently gates compact() on its oid.
    agent_disk_t::unique_future<void> agent_disk_t::storage_close_cursor_inner(session_id_t /*session*/,
                                                                               components::catalog::oid_t table_oid,
                                                                               uint64_t cursor_id) {
        trace(log_, "agent_disk[{}]::storage_close_cursor_inner: oid={} cursor={}", pool_idx_,
              static_cast<unsigned>(table_oid), cursor_id);
        // Idempotent by construction: an unknown id is already the desired state (the drain
        // paths erase the entry themselves), so this reports nothing and cannot fail.
        active_scans_.erase(cursor_id);
        co_return;
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
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            // NOT an empty fold. The scalar aggregate's empty-input finalize would emit a
            // single COUNT=0 / SUM=NULL row here, indistinguishable from the row a real
            // empty table produces — a statement about a table, made by a read that reached
            // no storage. An empty OWNED slice still folds to that row below.
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
        // Zero keys is not a failure: an empty request has an empty answer, and the
        // one-bucket-per-key invariant still holds at size 0.
        if (nkeys == 0) {
            return result;
        }
        // ARITY GUARD. A key chunk whose column count disagrees with the resolved key
        // columns — or an empty key column set — describes a semi-join that CANNOT BE
        // EVALUATED. This used to answer it with one EMPTY BUCKET PER KEY, and an empty
        // bucket is the affirmative answer "nothing in this table references that key":
        // ON DELETE CASCADE / RESTRICT read it as "this parent has no children" and let
        // the parent row go while its children stayed behind, referencing nothing. The
        // shape of an unevaluable request is an error, never an all-miss answer — the
        // same rule read_chunks_by_keys_inner already states for its own key arity.
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
                norm_keys.data[j] =
                    components::vector::vector_ops::cast_vector(resource, src, stored_key_types[j], nkeys);
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
        // A fetch failure stops the scan and would leave the remaining buckets empty. That
        // shape is indistinguishable from "these keys matched nothing", which is precisely
        // the answer that makes an FK or UNIQUE check pass — so it is returned as an error.
        auto scan_error = for_each_storage_batch(
            storage,
            pos,
            /*filter=*/nullptr,
            &projected_cols,
            txn,
            resource,
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

    // Resolve the key-column NAMES to storage column indices against an owned slice entry.
    // Fills out_indices in name order and returns no_error() on success.
    //
    // Every failure here means the read CANNOT BE PERFORMED — a different fact from "the key
    // matched nothing", and one that must not be collapsed into an empty result: that is how a
    // corrupt or misrouted catalog read surfaces to the user as "Database does not exist". The
    // causes are kept distinct because they are: a misrouted oid is ours, an unknown column
    // name is the caller's.
    // Shared by scan_by_keys_inner and read_chunks_by_keys_inner.
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

    // Validate storage column ORDINALS for a keyed catalog read. The catalog reads address
    // columns by position (components/catalog/helpers.hpp), so no name crosses the mailbox;
    // what makes a position an identity is catalog::system_schemas::column_order_is_pinned.
    // Bounds are still checked: a stale constant must fail loudly, not silently read a
    // neighbouring column.
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
        // result[i] = row_ids matching keys[i]; one (possibly empty) entry per key,
        // preserving input order. Name→index resolution runs once for the whole batch,
        // then the whole key set is answered by ONE streamed hash semi-join pass
        // (fk_hash_semijoin), not one filtered scan per key.
        std::pmr::vector<std::pmr::vector<std::int64_t>> result{resource()};
        result.reserve(keys.size());

        // Resolve the key column NAMES to storage indices once (same column set for every key).
        // A read that cannot be performed is an error, never an empty answer.
        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        std::pmr::vector<std::uint64_t> key_col_indices{resource()};
        if (auto resolved = resolve_key_col_indices(entry, key_col_names, key_col_indices, resource());
            resolved.contains_error()) {
            co_return resolved;
        }

        // Delegate the actual match to the streaming SINGLE-PASS hash semi-join: ONE streamed
        // pass over the table per call, regardless of key count. The helper is a free function
        // so it can be driven directly by a counting storage in unit tests; scan_by_keys_inner
        // is the sole production caller (single path, R6).
        co_return fk_hash_semijoin(resource(), *entry->storage, key_col_indices, keys, txn);
    }

    agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    agent_disk_t::read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                           std::pmr::vector<std::uint64_t> key_col_indices,
                                           components::vector::data_chunk_t keys,
                                           std::pmr::vector<std::uint64_t> projected_cols,
                                           components::table::transaction_data txn) {
        // Single key-tuple: `keys` already carries exactly one row, so it IS the 1-row
        // batch view the plural read_chunks_by_keys_inner expects. Delegate to it inline
        // on this same agent thread (its whole body stays off the mailbox) and unwrap the
        // single entry.
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
        // result[i] = matched chunks for key-tuple i; one (possibly empty) entry per key,
        // preserving input order. Key columns arrive as storage ORDINALS, so there is no name
        // resolution here at all; the whole batch is served by one scan_local call on this
        // agent thread (no self-send).
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> result{resource()};
        result.reserve(keys.size());

        auto it = storages_.find(table_oid);
        const collection_storage_entry_t* entry = (it == storages_.end()) ? nullptr : it->second.get();
        if (auto valid = validate_key_col_indices(entry, key_col_indices, resource()); valid.contains_error()) {
            co_return valid;
        }

        // Columnar keys: column j == key_col_indices[j], row i == key-tuple i. Arity is uniform
        // across the chunk, so a mismatch (chunk column count != key columns) is an error, not an
        // empty answer. Each filter constant is the single
        // materialization of a key cell (keys.value(ki, i)): the filter API requires a
        // logical_value_t, so this is the irreducible floor — no row-major keys cross the mailbox.
        const std::uint64_t nkeys = keys.size();
        if (keys.column_count() != key_col_indices.size()) {
            co_return core::error_t{
                core::error_code_t::invalid_parameter,
                std::pmr::string{"keyed read: key chunk arity does not match key columns", resource()}};
        }
        // ONE pass for the whole batch. The key tuple becomes the same thing a pushed WHERE is —
        // `col == k0 AND col == k1 ...` as a graph with the key cells bound as parameters — and the
        // batch becomes the OR of those tuples, so N keys cost ONE scan instead of N.
        //
        // No pruning is lost by this: check_zonemap_segments (row_group.cpp) unconditionally
        // returns true while a filter is only a graph, so a per-key "filtered scan" also read
        // the whole table and merely selected rows afterwards.
        //
        // Attribution back to individual keys runs the SAME engine the scan itself uses
        // (expressions::run_graph, as in row_group_t::filter_indexing), not a hand-rolled cell
        // comparison, so a row lands in exactly the buckets its own predicate accepts.
        namespace expr = components::expressions;
        const std::size_t narity = key_col_indices.size();

        // Parameter ids are uint16: the combined filter needs nkeys * arity of them.
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

        // Per-key filters: needed for attribution, and for nkeys == 1 the single one IS the scan
        // filter (the OR of one tuple is that tuple).
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

        // No row limit (-1). A scan_error here is a real io_error / data_corruption from a failed
        // block pin, never "this key matched nothing".
#ifdef DEV_MODE
        g_catalog_key_scans.fetch_add(1, std::memory_order_relaxed);
#endif
        // Projection comes from the CALLER: only it knows which columns it reads. An empty set
        // means "all columns" (full_scan's contract) and is what a caller that cannot enumerate
        // its set passes. Non-projected columns stay ordinal-stable placeholders, so a caller
        // that projects too narrowly reads an empty column instead of a value.
        std::vector<std::size_t> scan_projection(projected_cols.begin(), projected_cols.end());
        // The key columns are read by the filter itself, so they must survive the projection.
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
            // The scan filter WAS key 0's predicate, so every returned row is key 0's. Running the
            // same predicate again would return the same rows at the cost of another evaluation.
            result[0] = std::move(matched);
            co_return std::move(result);
        }

        for (auto& chunk : matched) {
            const auto rows = chunk.size();
            if (rows == 0) {
                continue;
            }
            // Hoisted: data_chunk_t::types() copies a complex_logical_type per column, and each
            // copy heap-allocates its alias. Once per chunk, not once per key.
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
                // UNKNOWN drops the row, exactly as false does — same reading as filter_indexing.
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
                // Built with the SAME projection as the scan. Full-width instead would allocate
                // a buffer for a column the scan never materialized, and the copy silently skips
                // a placeholder source — the caller would then read a zero-filled, non-NULL cell
                // where the contract promises a placeholder.
                components::vector::data_chunk_t out =
                    scan_projection.empty()
                        ? components::vector::data_chunk_t{resource(), chunk_types, count}
                        : components::vector::data_chunk_t{resource(), chunk_types, scan_projection, count};
                for (std::size_t c = 0; c < chunk.column_count(); ++c) {
                    if (out.data[c].data() == nullptr && out.data[c].auxiliary() == nullptr) {
                        continue; // placeholder on both sides: nothing to copy, ordinal preserved
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

    // Both accessors said "fallback to manager" in their trace lines. There is no manager
    // fallback and has not been since B4 removed the in-memory store — the manager is a pure
    // router — so the empty list and the zero they returned were the whole answer, and each
    // is also what a real storage legitimately answers.
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

    agent_disk_t::unique_future<checkpoint_result_t>
    agent_disk_t::checkpoint_inner(session_id_t /*session*/, wal::id_t current_wal_id, uint64_t compact_watermark) {
        trace(log_, "agent_disk[{}]::checkpoint_inner: {} entries in local slice", pool_idx_, storages_.size());
        // Per DISK entry, crash-safe checkpoint sequence (order matters):
        //   compact (MVCC-gated), checkpoint(wal_id) — whose header write IS the
        //   atomic commit point under A7.1 shadow paging; no external backup copy
        //   is taken (A7.5) — then persist the .wal_id sidecar via tmp+rename.
        //   Tally min(prev_checkpoint_wal_id_) for the manager's
        //   cross-agent std::min. Null entries are skipped.
        // B6: an entry that is UNCHANGED since its durable root skips the compact and the
        //   rewrite — and nothing else. It still advances its wal-id chain, still writes its
        //   sidecar and still contributes to the min, so the round's WAL floor does not depend
        //   on which entries had work to do. See the gate below.
#ifdef DEV_MODE
        g_table_checkpoints.fetch_add(1, std::memory_order_relaxed);
#endif
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        for (auto& [tbl_oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            if (entry->otbx_path.empty()) {
                continue;
            }

            // Degraded-storage gate, FIRST. The block manager latched a write/fsync that never
            // reached the device, or a free list proven corrupt. Both latches are sticky and
            // both make write_header refuse to commit — so this entry can no longer produce a
            // durable root, and every round spent trying costs a full extra copy of the table
            // inside the .otbx (the released blocks stay quarantined because promotion only
            // happens on a committed header). Nothing used to look at these latches at all;
            // this is where they become visible. Defer the entry, feed its UNCHANGED
            // prev_checkpoint_wal_id into the min so the WAL keeps every record this table
            // still needs, and say so loudly once per round.
            if (entry->table_storage.storage_degraded()) {
                warn(log_,
                     "agent_disk[{}]::checkpoint_inner oid={} block storage is degraded (a write/fsync did "
                     "not reach the device, or the free list is corrupt) — deferring this entry and NOT "
                     "compacting it; the file must be rebuilt",
                     pool_idx_,
                     static_cast<unsigned>(tbl_oid));
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                continue;
            }

            // Cursor gate: defer the WHOLE round for this oid while a streaming fetch-next
            // cursor is open on it — its stored absolute position indexes the un-swapped
            // collection, so the atomic row_groups_ swap a compact performs would shift rows out
            // from under it (R17). The entry is NOT checkpointed either (the `continue` below
            // skips it): it keeps its old file and sidecar, and feeds its UNCHANGED
            // prev_checkpoint_wal_id into the min so the WAL keeps every record it would need
            // for replay. A later round compacts and checkpoints it once the cursor drains.
            if (has_active_scan_for_oid(tbl_oid)) {
                trace(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} has an active scan cursor — deferring the whole entry "
                      "this round (no compact, no checkpoint); its WAL floor is unchanged",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid));
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                continue;
            }

            // B6 — UNCHANGED-TABLE GATE, and the only one here that is not a refusal. Nothing
            // has touched this entry since the header naming its current root committed, so a
            // rebuild would produce the same table in different blocks: compact() allocates a
            // fresh copy of every surviving row (it rebuilds unconditionally — there is no
            // "nothing to do" early exit, and even a table with no dead row pays the full
            // copy), checkpoint() writes it out behind two fsyncs, and the outgoing tree goes
            // to the free list to be reused next round. T1 measured the bill: on 100 tables of
            // 100 rows an EMPTY round cost 205.7 ms against 124.4 ms for the round that had
            // actually written all of them.
            //
            // This is NOT a deferral and the entry does NOT leave the round. It advances its
            // wal-id chain exactly as a rewrite would have (prev <- current, current <- this
            // round's id), persists its sidecar through the same code below, and feeds its
            // prev_checkpoint_wal_id into the min through the same statement at the bottom of
            // the loop. That is deliberate and load-bearing: B2's floor is min(prev) over EVERY
            // entry, and an entry that stopped contributing — or contributed a prev frozen at
            // the last round that happened to write it — would either drop the floor to
            // whatever the rest report or pin it forever, and truncate_before acts on that
            // number. Both halves of the advance are literally true of an unchanged table; the
            // argument is on table_storage_t::advance_wal_id_without_rewrite.
            //
            // What counts as changed is table_storage_t::needs_checkpoint's business, not this
            // loop's. The gate sits AFTER the two refusals above so that a degraded or
            // cursor-held entry keeps its own, more conservative treatment.
            if (!entry->table_storage.needs_checkpoint()) {
                trace(log_,
                      "agent_disk[{}]::checkpoint_inner oid={} is unchanged since its durable root — advancing "
                      "its wal id without rewriting it",
                      pool_idx_,
                      static_cast<unsigned>(tbl_oid));
                entry->table_storage.advance_wal_id_without_rewrite(current_wal_id);
            } else {
                // Failed-round gate: the previous checkpoint attempt on this entry failed. Retry
                // the checkpoint below — a transient error must be able to recover — but do NOT
                // rebuild first. A compact whose header never commits cannot return space under
                // the split free pool, only spend it: the rebuilt tree is allocated by extending
                // the file (reusable_ refills only on a committed header) and the outgoing tree
                // lands in pending_free_ where nothing can reach it. Without this gate a
                // persistent write error at the header offset — which deliberately does not latch,
                // so storage_degraded() stays false — costs a full copy of the table every round,
                // forever, with every health indicator reporting the file healthy.
                const bool skip_compact_this_round = entry->table_storage.last_checkpoint_failed();
                if (skip_compact_this_round) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner oid={} previous checkpoint failed — retrying WITHOUT "
                         "compaction; the rebuild resumes once a checkpoint commits",
                         pool_idx_,
                         static_cast<unsigned>(tbl_oid));
                }

                // MVCC gate FIRST: compact() refuses the rebuild when any version
                // stamp is above the watermark (an active snapshot or an in-flight
                // commit still needs the history, or a positional commit_append is
                // pending). Persisting a non-compacted table would resurrect dead /
                // uncommitted rows on recovery (.otbx has no version metadata), so
                // the entry's checkpoint is deferred to a later round; the WAL keeps
                // its replay records because the old sidecar/prev ids stay in the min.
                if (!skip_compact_this_round && !entry->table_storage.table().compact(compact_watermark)) {
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

                // A7.5: no external backup copy is taken before the round. Shadow paging is the
                // crash protection now — the round writes only fresh blocks (A7.2 split pool keeps
                // every block the durable root names off-limits), the header write is the atomic
                // commit point (A7.1 two-slot root), and a failed round's allocations are rolled
                // back (A7.7) — so on any failure below the durable root N is still on the device,
                // intact, inside the .otbx itself (proven per crash point by the A7.4 matrix).
                //
                // checkpoint(wal_id) returns out_of_memory on a column flush pin failure; it
                // aborts BEFORE the header swap and leaves the wal_id fields unchanged. On error,
                // defer this entry to a later round (same as the MVCC-gate skip above): do NOT
                // persist the sidecar, and feed the unchanged prev_checkpoint_wal_id into the
                // min() so the WAL keeps this table's replay records.
                auto cp_r = entry->table_storage.checkpoint(current_wal_id);
                if (cp_r.has_error()) {
                    warn(log_,
                         "agent_disk[{}]::checkpoint_inner oid={} checkpoint failed (rules 2/9) — deferring this "
                         "round",
                         pool_idx_,
                         static_cast<unsigned>(tbl_oid));
                    min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
                    continue;
                }
            }

            const auto& otbx_path = entry->otbx_path;

            // Persist sidecar wal_id atomically (tmp + rename). Reached by BOTH branches above:
            // the sidecar is the durable half of checkpoint_wal_id_, and an entry that skipped
            // its rewrite advanced that id just the same, so leaving the file behind would put
            // the two halves out of step for no gain.
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

            min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
        }
        co_return checkpoint_result_t{min_prev_id};
    }

    agent_disk_t::unique_future<uint64_t> agent_disk_t::vacuum_inner(session_id_t /*session*/,
                                                                     uint64_t lowest_active_start_time) {
        trace(log_, "agent_disk[{}]::vacuum_inner: {} entries in local slice", pool_idx_, storages_.size());
        // How many entries this pass RENUMBERED — see the header: it is the answer the caller
        // rebuilds indexes on, and it is counted at the line where a renumbering would be
        // performed rather than inferred anywhere above.
        uint64_t renumbered = 0;
        for (auto& slot : storages_) {
            auto& entry = slot.second;
            if (entry == nullptr) {
                continue;
            }
            auto& table = entry->table_storage.table();
            // In-memory MVCC version-chain GC, and the whole of what VACUUM does per entry.
            // Touches no block manager and costs no blocks.
            //
            // ITEM B — nothing is compacted here, and THIS is the line where a compact would
            // stand and where `++renumbered` would stand beside it. See the long note at
            // maybe_cleanup_inner: under A7.2's split pool a compact without a committed
            // header cannot return space, only spend it, and VACUUM used to do exactly that
            // for EVERY entry on EVERY call — even one with nothing dead in it. Compaction
            // belongs to the checkpoint round, which already performs it and is the only place
            // that can commit the release. It used to still run here for the one kind of entry
            // that had no checkpoint round of its own — the in-memory table — and that kind no
            // longer exists.
            //
            // cleanup_versions cannot renumber, and that is a property of what it reaches, not
            // a hope: row_group_collection_t::cleanup_versions -> row_version_manager_t::
            // cleanup_append only ever REPLACES a chunk_info inside vector_info_. No row moves,
            // no row group is rebuilt, and data_table_t::modified_since_checkpoint_ is
            // deliberately left alone by it for the same reason. So `renumbered` stays 0 and
            // the VACUUM statement owes no index rebuild.
            table.cleanup_versions(lowest_active_start_time);
        }
        co_return renumbered;
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

        // ITEM B — THE DELIBERATE DECISION about compacting outside a checkpoint round.
        //
        // A7.2 split the free pool: mark_as_free files a released id into pending_free_, and
        // pending_free_ drains into reusable_ in exactly ONE place — promote_durable_root,
        // reached only once a header naming the NEW root is on the device. free_block_id draws
        // only from reusable_. So a compact() that is not followed by a committed header cannot
        // RETURN space; it can only SPEND it: data_table_t::compact rebuilds the live tree
        // through transition_to_disk -> partial_block_manager_t::get_block_allocation ->
        // free_block_id (an empty reusable_ means "extend the file"), and files the outgoing
        // tree where nothing can reach it. Before A7.2 the released ids went straight back to
        // the single free list and the next allocation reused them, so the footprint plateaued
        // and this cost was invisible. Measured after A7.2: +2.9 MB per VACUUM call on an
        // unchanged 12k-row table.
        //
        // DECISION: this call site, and vacuum_inner, stop compacting DISK-backed storages.
        // Compaction of a DISK-backed table is one indivisible unit with the checkpoint that
        // commits it, and there is exactly one place that performs that unit —
        // agent_disk_t::checkpoint_inner, which already calls compact() itself immediately
        // before checkpointing every DISK entry. Doing half of it anywhere else is all cost.
        //
        // WHY NOT "checkpoint after compacting" here. Two independent reasons:
        //   * it would be WRONG without the WAL bookkeeping. The `.otbx.wal_id` sidecar is what
        //     recovery uses to skip records already absorbed into the durable root
        //     (integration/cpp/base_spaces.cpp: `record.id <= cp_id` -> skip). Committing a root
        //     that contains rows NEWER than the sidecar claims, without advancing it, makes
        //     recovery replay those rows a SECOND time — duplicated rows, which is strictly
        //     worse than a file that is merely too big. Advancing the sidecar needs the current
        //     WAL id, which neither this handler nor vacuum_inner has;
        //   * it would be wrong HERE regardless. maybe_cleanup rides the COMMIT fan-out
        //     (operator_commit_transaction -> maybe_cleanup_many). Turning a commit into a
        //     checkpoint — a full-table rewrite and two fsyncs — is not a trade the write
        //     path can make.
        //
        // WHAT VACUUM STILL IS: cleanup_versions (in-memory version-chain GC) and the index
        // and pg_computed_column GC / index repopulate owned by operator_vacuum_t. Its effect
        // on the file is DEFERRED to the next checkpoint round instead of being immediate and
        // negative.
        //
        // B3c3 settled "deferred, not lost" by measurement rather than by call graph, because
        // that was the one claim the decision above rests on and the one nobody had checked.
        // A counter on every exit of checkpoint_inner, run over the whole integration suite:
        // 13053 DISK entry-rounds, 12962 compacts performed, and dead_rows_left_after_a
        // _completed_round == 0 — not one entry ever finished a round still carrying a dead
        // row. The four skip paths are all DEFERRALS and the numbers say how rare: cursor
        // gate 7 (4 dead rows, next round takes them), MVCC watermark 84 (monotone, so a
        // later round always clears it), failed-round 0 (last_checkpoint_failed_ clears on
        // the next success, so it costs one round), degraded 0 (sticky — but a degraded file
        // can no longer commit ANY root, so its reclaim was already unreachable here too).
        // The end-to-end reading is in test_s3_cleanup_scaling: with the checkpoint compact
        // disabled the durable root keeps 700000 rows where 149988 are live.
        // B4: unconditional. The gate above used to read "not an in-memory entry -> return",
        // and every entry is a file now, so the whole compaction body it protected is gone
        // with it. The handler stays on the contract — operator_commit_transaction sends it
        // per touched oid — and says plainly that the work belongs elsewhere.
        trace(log_,
              "agent_disk[{}]::maybe_cleanup_inner: oid={} — compaction belongs to the checkpoint round that "
              "can commit the release",
              pool_idx_,
              static_cast<unsigned>(table_oid));
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
        // Read otbx_path BEFORE the erase, while the unique_ptr is still live. An empty
        // path (a failed construction that was dropped) skips the remove block.
        // Remove sequence: .otbx +
        // .wal_id sidecar + per-oid directory, all via std::error_code
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
    // rejection and broader is_convertable_to casting, neither of which the
    // catalog-append path applied, so this lighter path keeps WAL-time semantics
    // faithful. (Neither path substitutes DEFAULTs any more: they are expanded above
    // the journal, in operator_insert, and catalog rows are released from that fill —
    // ddl_metadata_builder hands storage a ready-made pg_catalog tuple.)
    agent_disk_t::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
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
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                // Same shape as the three refusals already travelling this wrapper: a catalog
                // row whose journal record was refused must not be reported as appended.
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
                                // THE GUARD ABOVE DOES NOT PROVE THIS CAST SUCCEEDS. It admits
                                // STRING_LITERAL on either side, and 'abc' -> BIGINT is a refusal, not
                                // a conversion. The assert that used to stand here claimed otherwise and
                                // vanished under NDEBUG, leaving .value() to read the value half of an
                                // ERRORED result — a catalog row built out of an undefined cell.
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

            // The append chain can surface write_conflict / out_of_memory. It is REPORTED
            // now: this is the DDL write path, and the range it returns carries no way to
            // tell "wrote nothing" from "wrote nothing because it could not". A CREATE TABLE
            // whose pg_class row never landed used to return a zero-count range, which the
            // caller reads as a no-op append, and the statement reported success over a
            // catalog that does not contain the table it just claimed to create.
            auto append_r = s->append(local, ctx.txn);
            if (append_r.has_error()) {
                co_return append_r.convert_error<components::pg_catalog_append_range_t>();
            }
            start_row = append_r.value();
        } else if (row.size() == 0) {
            // The one legitimate no-op: nothing was asked to be written. Zero rows in,
            // zero rows out, no storage touched.
            trace(log_,
                  "agent_disk[{}]::append_pg_catalog_row_inner: empty row for oid={} — nothing to append",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
        } else {
            // NOT a routing miss. manager_disk_t::append_pg_catalog_row picks this agent with
            // pool_idx_for_oid(table_oid), so the owner is decided before the message is sent
            // and THIS agent is it. What the missing entry means is that the owner has no
            // storage for the table — never created, never loaded, or dropped — and the row
            // has nowhere to go. Refusing is the whole point: the silent version of this leg
            // let DDL report success while the catalog was never written.
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
        // Read the slice directly. Bind entry NON-const so inline_scan binds the
        // non-const data_table_t& overload (no const_cast).
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            // NOT a routing miss, and the same leg append_pg_catalog_row_inner refuses on: the
            // manager picked this agent with pool_idx_for_oid(table_oid), so the owner is decided
            // before the message is sent and THIS agent is it. A missing entry means the owner
            // holds no storage for that catalog table, so the rows the caller asked to be gone
            // were neither found nor removed — and answering "deleted nothing" would be read as
            // a healthy no-op by every caller.
            std::pmr::string msg{"agent_disk::delete_pg_catalog_rows: no storage on the owning agent for catalog "
                                 "oid ",
                                 resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            msg += std::pmr::string{" — nothing was deleted", resource()};
            co_return core::error_t{core::error_code_t::io_error, std::move(msg)};
        }
        auto& entry = it->second;

        // THE SCAN CARRIES ctx.txn, and the delete is only allowed to judge what it can see.
        // Without it the scan ran on transaction_data{0, 0} — direct writes only — while every
        // caller that decides "0 deleted is a refusal" had READ the row it is deleting through
        // a route that DOES carry the transaction (manager_disk_t::read_chunks_by_key ->
        // agent_disk_t::read_chunks_by_key_inner, and the txn-aware resolve funnel). A catalog
        // row written inside an explicit transaction is invisible to {0, 0} until the commit
        // publishes it, so `BEGIN; ALTER TABLE t ADD COLUMN c; ALTER TABLE t DROP COLUMN c;`
        // deleted nothing and the DROP refused a legal sequence — the read saw a row the delete
        // was told did not exist. Gate: integration/cpp/test/test_catalog_delete_refusal.cpp,
        // a_column_added_and_dropped_in_one_transaction_is_dropped.
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
            // The one legitimate emptiness at THIS floor: the scan ran and no row of this table
            // VISIBLE TO ctx.txn carried that oid. Whether that is a healthy no-op or the sign
            // of a catalog the caller has already read and expects to delete from is a question
            // only the caller can answer, so the count travels up unjudged — and because the
            // scan above shares the caller's snapshot, "the caller read it" and "the delete can
            // see it" are now the same claim.
            co_return std::uint64_t{0};
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
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                // REFUSED, and the rows stay. This used to log and fall through to the delete
                // below, which put storage one state ahead of a journal holding no record to
                // replay it from — the delete would simply not exist after a restart. Same rule
                // and same shape as append_pg_catalog_row_inner's WAL leg: the row whose journal
                // record was refused is not written, and the row whose delete record was refused
                // is not deleted.
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
        // The storage was scanned two blocks up to produce `row_ids`, so the refusal
        // direct_delete_sync carries cannot be reached from here. READ IT ANYWAY, say so at
        // error level, and REPORT it: "cannot happen" is exactly what the assert this leg
        // replaced claimed, and the caller now has somewhere to put it.
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

        // transaction_data{} — WRITTEN OUT BECAUSE IT IS WRONG, AND IS NOT SAFE TO CORRECT YET.
        //
        // This patch runs at STEP 4 of operator_commit_transaction_t, whose own comment states
        // the premise it depends on: "the rows still carry insert_id == transaction_id", i.e.
        // they are not published yet, which is what makes patching them invisible to everyone
        // else. transaction_data{} is horizon 0 with no owning transaction and cannot see a row
        // in that state — so EVERY backfill of an in-transaction ALTER logs "attoid not found
        // (skipping)" below and added_at_commit_id keeps its placeholder 0. Zero reads as "added
        // before every snapshot", which shows the column to snapshots older than the ALTER that
        // created it. The delete a few hundred lines up had exactly this defect and ctx.txn is
        // exactly its fix.
        //
        // IT DOES NOT WORK HERE, and the failure is below this file: with ctx.txn the scan finds
        // the row, and the direct_update_sync at the bottom of this body then dies in
        // components::table::update_segment_t::merge_update_loop_internal
        // (components/table/update_segment.hpp:830) on a base_info pointer that is not an
        // address — reproducible on a plain autocommit ALTER ... ADD COLUMN. So this backfill
        // has never once run against a real row, and the update-merge path it would drive does
        // not survive being handed one. Both halves are one defect and need a red test at the
        // components/table floor first; widening this scan before that only turns a silent
        // no-op into a crash. Finding recorded in
        // integration/cpp/test/test_catalog_delete_refusal.cpp,
        // an_in_transaction_add_column_row_survives_the_commit.
        detail::inline_scan(tbl,
                            all_col_indices,
                            &scan_resource,
                            components::table::transaction_data{},
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
            auto wal_result = co_await std::move(wf);
            if (wal_result.has_error()) {
                // This handler's contract is still unique_future<void>, so the refusal has
                // nowhere to travel and is reported at error level rather than dropped. Where
                // delete_pg_catalog_rows_inner stood a moment ago: the same hole, one method
                // over, and closing it is the same kind of signature change through its own
                // callers (operator_commit_transaction_t's backfill drain).
                error(log_,
                      "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: the PHYSICAL_UPDATE did not "
                      "reach the journal for attoid={}: {}",
                      pool_idx_,
                      static_cast<unsigned>(attoid),
                      wal_result.error().what);
            } else if (wal_result.value() == wal::id_t{}) {
                trace(log_,
                      "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: WAL write returned zero id "
                      "for attoid={}",
                      pool_idx_,
                      static_cast<unsigned>(attoid));
            }
        }

        // The row was READ from this slice a few lines up, so the refusal is unreachable here —
        // and, this handler having no error channel of its own yet, it is reported rather than
        // dropped.
        if (auto upd_err = direct_update_sync(pg_attr_oid, row_ids, patch); upd_err.contains_error()) {
            error(log_,
                  "agent_disk[{}]::update_pg_attribute_commit_id_field_inner: the slice it had just read "
                  "refused the update: {}",
                  pool_idx_,
                  upd_err.what);
        }
        co_return;
    }

    // Whole-op intra-agent compaction: read own slice, compute the columns NOT in
    // live_attnames, drop each via entry->drop_column on its own slice, and return the dropped
    // count. This eliminates the per-column manager<->agent round-trips the former manager body
    // did.
    //
    // B4 — WHY THIS RUNS AT ALL NOW. It used to sit behind a "not an in-memory storage ->
    // return 0" gate,
    // and B3c3 measured what that refusal actually did: B1a made every table file-backed, so
    // the gate was refusing EVERY computed table in production and this handler answered 0
    // always. Removing the mode leaves exactly two readings of that gate — "always refuse" or
    // "always act" — and the choice could not be deferred with it.
    //
    // ACT is the right one, and it is safe for a reason B3c established rather than assumed:
    // table_storage_t::drop_column does not free anything here. It NAMES the outgoing column's
    // blocks into pending_released_blocks_ and rebuilds the table by SHARING every surviving
    // column, so the call allocates nothing; the release itself is drained by the next
    // checkpoint, the one place under A7.2's split pool that can commit it. That is the same
    // split B3c1's ALTER TABLE DROP COLUMN leg runs on. The old note here read "un-gating is a
    // follow-up", and it argued from the sibling compact() — which really does spend space
    // outside a committed round. A drop does not.
    //
    // WHAT REFUSING WOULD HAVE COST: nothing else re-derives this drop. A 'g' table's DROP
    // routes to operator_computed_field_unregister_t, which writes only a pg_computed_column
    // refcount=0 tombstone and defers the physical half to VACUUM — to here. The checkpoint
    // round compacts but does not drop columns (compact() enumerates the collection as it
    // stands), and B3c2's bootstrap re-arm excludes 'g' at the source. So a refusal keeps the
    // column in the durable root forever; measured end to end before B4: CREATE TABLE g();
    // INSERT (a,b); ALTER ... DROP COLUMN b; VACUUM; CHECKPOINT; reopen the .otbx offline — the
    // root still named [a, b].
    //
    // THE RISK IT CARRIES, stated plainly: unlike drop_storage_column_inner, which is told WHICH
    // column to drop, this leg is SUBTRACTIVE — it drops the complement of `live_attnames`. Any
    // gap in the caller's derivation of that live set becomes a physical drop of a SURVIVING
    // column. The derivation is operator_vacuum_t's pg_computed_column scan.
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

    // B3c1 — ALTER TABLE DROP COLUMN's physical half on this agent's own slice.
    //
    // The body is deliberately the compact leg's inner loop minus its two VACUUM-shaped
    // parts: the subtractive "everything not in live_attnames" enumeration (the ALTER names
    // its column, and re-deriving a live set here would turn any gap in that derivation into
    // a physical drop of a SURVIVING column). Here the ALTER names its column, so there is no
    // live set to re-derive and no gap to turn into one. Both legs share B3c's split: rebuild
    // now, blocks released by the checkpoint that can commit their release.
    //
    // WHEN this runs is the safety argument, and it is not local: operator_commit_transaction
    // drives it only AFTER the txn's WAL commit marker and the ProcArray publish barrier, so
    // the pg_attribute tombstone is already both durable and visible. A rebuild is not
    // undoable, so it must never precede a tombstone that a ROLLBACK or a lost commit can
    // still take back.
    agent_disk_t::unique_future<core::result_wrapper_t<bool>>
    agent_disk_t::drop_storage_column_inner(components::catalog::oid_t table_oid, std::string attname) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            // Not owned here, or a record-only marker whose storage was never materialized.
            // The caller's tombstone is already committed, so this cannot be reported as a
            // quiet 0: nothing else re-derives the drop, and the blocks would stay named by a
            // root nobody ever revisits.
            std::pmr::string msg{"agent_disk::drop_storage_column: no materialized storage for table oid ",
                                 resource()};
            msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
            co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
        }
        // Same primitive the compact leg calls: rebuild the table without the column and
        // recreate the adapter (which holds a data_table_t& the rebuild invalidates). On a
        // DISK-backed storage the rebuild also NAMES the outgoing column's blocks into
        // table_storage_t::pending_released_blocks_; the checkpoint round drains them (B3c).
        const bool dropped = it->second->drop_column(attname, resource());
        trace(log_,
              "agent_disk[{}]::drop_storage_column_inner: oid={} column='{}' {}",
              pool_idx_,
              static_cast<unsigned>(table_oid),
              attname,
              dropped ? "dropped" : "absent from the storage schema — nothing physical to release");
        co_return core::result_wrapper_t<bool>(dropped);
    }

    // ALTER TABLE RENAME COLUMN's physical half on this agent's own slice — the sibling of
    // drop_storage_column_inner, and it exists for a reason that is not symmetry.
    //
    // manager_disk_t::rearm_dropped_column_blocks_sync reconciles every loaded storage's column
    // names against the live pg_attribute rows at bootstrap and reads a storage-only name as a
    // DROP: it takes the column out of the collection and arms its blocks. That reading is
    // correct only while a catalog name and a storage name cannot diverge. A RENAME that wrote
    // pg_attribute and stopped there would make them diverge by construction, and the next start
    // would physically remove a SURVIVING column together with its data. So the same commit that
    // writes the new name into the catalog writes it here.
    //
    // WHEN this runs is the same argument drop_storage_column_inner makes:
    // operator_commit_transaction drives it only after the txn's WAL commit marker and the
    // ProcArray publish barrier, so an explicit ROLLBACK — which reverts the pg_attribute rows,
    // being ordinary inserts under this txn id — can never leave the storage renamed against a
    // catalog that took the rename back. What it does NOT close, and it is written down rather
    // than glossed: the rename is memory-resident until this table's next checkpoint, while the
    // catalog half is durable at the marker. See the note on table_storage_t::rename_column.
    agent_disk_t::unique_future<core::result_wrapper_t<bool>>
    agent_disk_t::rename_storage_column_inner(components::catalog::oid_t table_oid,
                                              std::string old_attname,
                                              std::string new_attname) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr || it->second->storage == nullptr) {
            // Not owned here, or a record-only marker. The caller's catalog rename is already
            // committed, so a quiet "done" would leave the two halves disagreeing forever with
            // nothing to re-derive the rename from.
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

    // Runtime DROP path, canonical per-oid mark: read otbx_path + derive the .wal_id
    // sidecar from the own slice, then record the GC entry via
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
            }
        }
        // An entry whose construction failed leaves otbx_path/sidecars empty, but we still
        // record a GC entry so disk_has_dropped_ bookkeeping is uniform (sweep no-ops on an
        // empty path).
        register_dropped_storage_inner_sync(table_oid, dropped_at_commit_id, std::move(otbx_path), std::move(sidecars));
    }

    // Batched DROP-mark: one message per agent carries that agent's whole oid slice
    // (manager partitioned by pool_idx_for_oid) plus the shared dropped_at_commit_id.
    // Loops the canonical per-oid mark; an over-routed / not-owned oid records an empty
    // GC entry (no-op sweep).
    agent_disk_t::unique_future<void>
    agent_disk_t::mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                                  uint64_t dropped_at_commit_id) {
        for (auto table_oid : table_oids) {
            mark_storage_dropped_one_local(table_oid, dropped_at_commit_id);
        }
        co_return;
    }

    // RN-oid. See the declaration and collection_storage_entry_t::note_column_identity: this
    // parks the identity of a column the catalog has already created and the storage has not
    // materialised yet, so that whichever INSERT does materialise it stamps the right attoid
    // instead of leaving a 0 the bootstrap reconciliation would have to refuse.
    agent_disk_t::unique_future<void>
    agent_disk_t::note_column_identity_inner(components::catalog::oid_t table_oid,
                                             std::string attname,
                                             std::uint32_t attoid,
                                             components::types::complex_logical_type type) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end() || it->second == nullptr) {
            trace(log_,
                  "agent_disk[{}]::note_column_identity_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        it->second->note_column_identity(std::move(attname), attoid, type);
        co_return;
    }

} //namespace services::disk
