#include "agent_disk.hpp"
#include "manager_disk.hpp"
// Step 8: needed for the on_subscriber_empty(DISK_KIND) ack send in
// on_horizon_advanced_inner — mirrors the include pattern in
// manager_disk.cpp. The header (manager_dispatcher_t method pointer) is only
// reached via this .cpp include so agent_disk.hpp stays light.
#include <services/dispatcher/dispatcher.hpp>

namespace services::disk {

    using namespace core::filesystem;

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource,
                               manager_disk_t* manager,
                               const path_t& path_db,
                               log_t& log)
        // Legacy/default constructor — until manager_disk's create_agent loop
        // (Step 3) starts assigning roles explicitly, every agent defaults to
        // CATALOG with pool_idx 0. This preserves existing behaviour exactly:
        // storages_ slice is empty, role/pool fields are inert, all hot paths
        // continue to consult manager_disk_t::storages_.
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

    // Ownership-transfer entry point — agent slice is the SOLE source of
    // truth after Step 8.11 wrap (manager_disk_t::storages_ deleted) and
    // Step 8.12 final polish (bootstrap_record_oid_sync deleted; every DISK
    // OID now reaches the slice via bootstrap_disk_inner_sync /
    // bootstrap_create_disk_inner_sync with a real entry owning the live
    // SFBM). Step 2/3 historical breadcrumb retained for the slice-member
    // genesis only.
    bool agent_disk_t::has_storage_sync(components::catalog::oid_t oid) const noexcept {
        return storages_.find(oid) != storages_.end();
    }

    // ──────────────── Step 8.5 manager-router delegation helpers ──────────
    //
    // Synchronous probes used by manager_disk_t's public accessors
    // (total_rows_sync / checkpoint_wal_id_sync). Post Step 8.11 wrap +
    // Step 8.12 cleanup the agent slice is the SOLE source of truth — both
    // IN_MEMORY twins and DISK SFBMs live here. Not-owned OIDs return 0;
    // null entries (legacy DISK record-only markers, now unreachable after
    // §8.1.B/C SFBM transfer) are skipped defensively.
    uint64_t agent_disk_t::total_rows_inner_sync(components::catalog::oid_t oid) const noexcept {
        auto it = storages_.find(oid);
        if (it == storages_.end()) {
            return 0;
        }
        const auto& entry = it->second;
        if (entry == nullptr) {
            // Defensive: null entries (legacy DISK record-only markers) are
            // unreachable after §8.1.B/C SFBM transfer.
            return 0;
        }
        return entry->table_storage.table().calculate_size();
    }

    wal::id_t agent_disk_t::checkpoint_wal_id_inner_sync(components::catalog::oid_t oid) const noexcept {
        auto it = storages_.find(oid);
        if (it == storages_.end()) {
            return wal::id_t{0};
        }
        const auto& entry = it->second;
        if (entry == nullptr) {
            // Defensive: null entries (legacy DISK record-only markers) are
            // unreachable after §8.1.B/C SFBM transfer.
            return wal::id_t{0};
        }
        return entry->table_storage.checkpoint_wal_id();
    }

    // ──────────────── Step 8.12 has_in_memory_inner_sync probe ────────────
    //
    // Walk the agent slice looking for any real IN_MEMORY twin. Used by
    // manager_disk_t::checkpoint_all to decide whether the WAL ID floor may
    // be sealed (no IN_MEMORY twins anywhere) or must be suppressed
    // (IN_MEMORY tables still need replay records). Null entries (legacy
    // DISK record-only markers, now dead after the §8.1.B/C SFBM transfer)
    // are skipped — they have no live storage_t to interrogate, and DISK
    // entries are not IN_MEMORY by definition.
    bool agent_disk_t::has_in_memory_inner_sync() const noexcept {
        for (const auto& [oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            if (entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
                return true;
            }
        }
        return false;
    }

    // ──────────────── Step 8.11.A storage_entry_sync accessor ─────────────
    //
    // Borrowed-pointer accessor used by manager-side read sites that scan
    // the catalog through the agent's storages_ slice (now the SOLE source
    // of truth — manager-side storages_ map deleted in Step 8.11 wrap +
    // get_storage() deleted in Step 8.12). Returns nullptr for "OID not in
    // this agent's slice"; null unique_ptr entries (legacy DISK record-only
    // markers, unreachable post §8.1.B/C SFBM transfer) yield nullptr too.
    // The pointer is borrowed — the unique_ptr in the slice retains
    // ownership; the caller MUST NOT keep it across a mailbox-yield. See
    // header comment for the Constraint #11 carve-out justification.
    const collection_storage_entry_t*
    agent_disk_t::storage_entry_sync(components::catalog::oid_t oid) const noexcept {
        auto it = storages_.find(oid);
        if (it == storages_.end()) {
            return nullptr;
        }
        // Defensive: null entries (legacy DISK record-only markers) are
        // unreachable after §8.1.B/C SFBM transfer; report as "not present"
        // so callers see a uniform absence signal.
        return it->second.get();
    }

    bool agent_disk_t::bootstrap_inner_sync(components::catalog::oid_t oid,
                                            std::unique_ptr<collection_storage_entry_t> entry) noexcept {
        // Ownership transfer (Constraint #11: move-only, never shared). Returns
        // false on duplicate-key collision; the existing entry retains
        // ownership and the incoming unique_ptr is destroyed when `entry` goes
        // out of scope. Callers in Step 3 should log + drop on false.
        if (entry == nullptr) {
            return false;
        }
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    // Step 8.12 audit (2026-05-31): `bootstrap_record_oid_sync` definition
    // removed — see header comment for the audit-finding details.
    // All DISK OIDs now reach the agent slice through
    // `bootstrap_disk_inner_sync` (load path, manager_disk_io.cpp 336-354)
    // or `bootstrap_create_disk_inner_sync` (create path, manager_disk_io.
    // cpp 256-273), each of which inserts a REAL entry owning the live SFBM.

    // ──────────────── Step 8.1 DISK ownership constructors ────────────────
    //
    // Construct the SFBM-owning collection_storage_entry_t directly on the
    // agent. Caller (manager_disk_io::create_storage_disk_sync /
    // load_storage_disk_sync) MUST skip the manager-side `storages_.emplace`
    // for the same OID — the exclusive WRITE_LOCK on the `.otbx` means two
    // SFBMs in the same process is a footgun (closing either fd releases the
    // posix-advisory lock for both). The Step 8.1.A commit lands the API
    // surface only; no caller routes through these yet. Wiring lands in 8.1.B
    // (catalog OIDs, requires resolve/bootstrap reader migration) and 8.2
    // (user-table OIDs, requires runtime CREATE TABLE migration).
    //
    // Both helpers return true on insert success, false on duplicate key
    // (existing entry retains ownership; the just-constructed unique_ptr is
    // destroyed when it goes out of scope, closing its file_handle_t — the
    // pre-existing entry's lock is therefore released too due to the posix
    // per-process semantics, which is why we MUST not collide here). The
    // duplicate-key path is reachable only via a programmer error
    // (double-bootstrap) — log+drop is the documented contract on the manager
    // side.
    bool agent_disk_t::bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                                  const std::filesystem::path& otbx_path,
                                                  wal::id_t sidecar_wal_id) noexcept {
        // Probe BEFORE constructing the SFBM so we don't open the file twice
        // on a duplicate-key collision (would release the lock on the
        // existing entry's handle — see the contract note above).
        if (storages_.find(oid) != storages_.end()) {
            // Step 8.1.B diagnostic — duplicate-key path means manager-side
            // (or another agent) already opened this `.otbx`. With the
            // diagnostic-only commit nothing should reach this branch
            // (manager_disk_io still owns construction). When the transfer
            // commit lands, this branch becoming hot is the canonical
            // double-construct symptom we must catch in tests.
            trace(log_,
                  "[step-8.1.B-agent-sfbm-dup] agent[{}] oid {} already in slice — drop "
                  "incoming load (path={})",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
            return false;
        }
        // Step 8.1.B diagnostic — every successful agent-side SFBM open is
        // tagged so test log scans can confirm the post-transfer invariant:
        // exactly one of "[step-8.1.B-sfbm-construct]" (manager side, this
        // commit) OR "[step-8.1.B-agent-sfbm-construct]" (agent side, after
        // transfer) per `.otbx` per process boot. Two of either, OR one of
        // each, signals a double-open and the SFBM WRITE_LOCK footgun.
        trace(log_,
              "[step-8.1.B-agent-sfbm-construct] agent[{}] load oid={} path={} sidecar_wal_id={}",
              pool_idx_,
              static_cast<unsigned>(oid),
              otbx_path.string(),
              static_cast<uint64_t>(sidecar_wal_id));
        // Load-existing constructor: single_file_block_manager_t opens the
        // file with WRITE_LOCK on the agent thread. After 8.1 wiring lands,
        // this is the canonical owner; manager_disk_t::storages_ no longer
        // emplaces for the same OID.
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
            // Step 8.1.B diagnostic — see bootstrap_disk_inner_sync above
            // for the dup-path contract.
            trace(log_,
                  "[step-8.1.B-agent-sfbm-dup] agent[{}] oid {} already in slice — drop "
                  "incoming create (path={})",
                  pool_idx_,
                  static_cast<unsigned>(oid),
                  otbx_path.string());
            return false;
        }
        // Step 8.1.B diagnostic — pairs with manager_disk_io::create_storage_
        // disk_sync's "[step-8.1.B-sfbm-construct] create" tag. Both must not
        // fire for the same OID in the same process.
        trace(log_,
              "[step-8.1.B-agent-sfbm-construct] agent[{}] create oid={} path={}",
              pool_idx_,
              static_cast<unsigned>(oid),
              otbx_path.string());
        // Create-new constructor: single_file_block_manager_t creates the
        // file with WRITE_LOCK on the agent thread. Same ownership contract
        // as bootstrap_disk_inner_sync above.
        auto entry = std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path);
        return storages_.try_emplace(oid, std::move(entry)).second;
    }

    // ──────────────── Step 4 WAL-replay direct_* sync helpers ─────────────
    //
    // Pre-scheduler-start (Phase 4 base_spaces WAL replay) only. Apply the
    // mutation directly against the agent's canonical entry — after Step
    // 8.11 wrap + Step 8.12 cleanup the agent slice IS the sole owner; the
    // historical "dual-write to manager_disk_t::storages_" path is GONE.
    // Missing OIDs and null entries (defensive — legacy DISK record-only
    // markers, unreachable post §8.1.B/C) are logged + no-op'd.
    //
    // The mutation logic here is intentionally minimal — schema-adoption /
    // column-expansion / type-promotion happen upstream in the mailbox
    // body that fans into this slice. Replay records arrive pre-aligned
    // with the table schema, so a direct append/delete/update against the
    // entry's storage_t adapter is correct.
    void agent_disk_t::direct_append_sync(components::catalog::oid_t table_oid,
                                          components::vector::data_chunk_t& data,
                                          core::date::timezone_offset_t /*session_tz*/,
                                          const components::table::transaction_data& txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::direct_append_sync: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            // Defensive: null entries (legacy DISK record-only markers) are
            // unreachable after §8.1.B/C SFBM transfer.
            trace(log_,
                  "agent_disk[{}]::direct_append_sync: oid {} has null entry (unreachable post-§8.1.B/C) — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            return;
        }
        if (data.size() == 0 || entry->storage == nullptr) {
            return;
        }
        entry->storage->append(data, txn);
    }

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
        entry->storage->update(ids_vec, new_data);
    }

    auto agent_disk_t::make_type() const noexcept -> const char* { return "agent_disk"; }

    actor_zeta::behavior_t agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::fix_wal_id>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::fix_wal_id, msg);
                break;
            }
            // Version B* Step 5 — storage_scan mailbox handler. Other storage_*
            // families (append / publish_commits / publish_deletes / etc.) will
            // land in subsequent Step 5 commits.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan, msg);
                break;
            }
            // Version B* Step 5 (mutation set) — append + MVCC commit fanout
            // handlers. Canonical writers post Step 8.11 wrap (manager-side
            // storages_ deleted); the slice owns every mutation.
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
            // Version B* Step 5 (final cleanup) — abort-path + completion
            // handlers. Revert/update/delete/fetch fanout against the
            // agent's IN_MEMORY twins; manager-side bodies remain canonical
            // until Step 8.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_revert_appends_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_revert_appends_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_revert_append_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_revert_append_inner, msg);
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
            // Version B* Step 5 (final cleanup) — batched scan + metadata
            // mailbox handlers. Read-path mirrors for scan_batched /
            // scan_segment / types / total_rows; manager-side bodies remain
            // canonical until Step 8.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan_batched_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan_batched_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::storage_scan_segment_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::storage_scan_segment_inner, msg);
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
            // Version B* Step 6 — fanout-only mailbox handlers. The manager
            // sends one of these per agent for checkpoint_all / vacuum_all /
            // on_horizon_advanced; each agent iterates its local storages_
            // slice in parallel.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::checkpoint_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::checkpoint_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::vacuum_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::vacuum_inner, msg);
                break;
            }
            // Version B* Step 8.7 — single-route maintenance handler. Manager
            // computes pool_idx_for_oid(ctx.table_oid) and sends here so the
            // owning agent runs the compact threshold check against its
            // canonical entry (post Step 8.11/12 the agent slice IS canonical;
            // no manager-side fallback).
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::maybe_cleanup_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::maybe_cleanup_inner, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::on_horizon_advanced_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::on_horizon_advanced_inner, msg);
                break;
            }
            // Version B* Step 7 — runtime DROP path: manager_disk_t::mark_
            // storage_dropped forwards the entry here so the per-agent
            // dropped_storages_ slice receives it without crossing the actor
            // boundary as shared mutable state (Constraint #11). Post
            // Step 8.11 the per-agent slice is the SOLE owner of GC state.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::register_dropped_storage_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::register_dropped_storage_inner, msg);
                break;
            }
            // Version B* Step 8.3 — runtime DROP TABLE storages_ erase. Post
            // Step 8.11/12 the agent slice is the SOLE owner; manager-side
            // drop_storage is a pure router. This handler performs the
            // canonical erase + .otbx removal.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::drop_storage_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::drop_storage_inner, msg);
                break;
            }
            // Version B* Step 8.11.C — physical column compaction routed to
            // the owning agent. compact_relkind_g_storage's mutation half
            // (collection_storage_entry_t::drop_column) is non-const so it
            // cannot run through the storage_entry_sync read accessor; the
            // manager dispatches here so the entry is rebuilt on the agent
            // thread. Post Step 8.11 wrap the agent slice is canonical.
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::drop_column_inner>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::drop_column_inner, msg);
                break;
            }
            default:
                break;
        }
    }

    // Version B* Step 5 — read-path router fanout target.
    //
    // Looks up the OID in this agent's local storages_ slice. Post Step
    // 8.11 wrap + Step 8.12 cleanup the agent slice is the SOLE source of
    // truth — the historical "fall back to manager_disk_t::storages_" path
    // is GONE. Two NOT-OWNED outcomes still return nullptr so the caller
    // (manager_disk_t::storage_scan) returns an empty chunk uniformly:
    //   (a) OID not present in this agent's slice — routing miss; another
    //       agent (or no one) owns the OID.
    //   (b) Defensive: null unique_ptr in the slice (legacy DISK record-
    //       only marker, unreachable after §8.1.B/C SFBM transfer).
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
            // Defensive: null entries (legacy DISK record-only markers) are
            // unreachable after §8.1.B/C SFBM transfer.
            trace(log_,
                  "agent_disk[{}]::storage_scan: oid {} has null entry/storage (unreachable post-§8.1.B/C) — empty result",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto types = entry->storage->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        entry->storage->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    // ──────────────── Step 5 mutation-set fanout targets ─────────────────
    //
    // Apply the canonical mutation against the agent's slice. Post Step
    // 8.11 wrap + Step 8.12 cleanup the agent slice IS the sole owner;
    // no manager-side fallback exists. Not-owned OIDs and null entries
    // (defensive — unreachable post §8.1.B/C) are logged + no-op'd.
    //
    // No exceptions, no shared state across the actor boundary
    // (Constraint #11) — `data` arrives by rvalue unique_ptr, `ranges` /
    // `tables` arrive as PMR-backed by-value vectors. Caller (manager
    // router) already validated `data != nullptr` / range counts before
    // the send, but we re-check defensively here because the agent owns
    // its slice independently.

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_append_inner(components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::vector::data_chunk_t> data,
                                       components::table::transaction_data txn) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_append_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            // Defensive: null entries (legacy DISK record-only markers) are
            // unreachable after §8.1.B/C SFBM transfer.
            trace(log_,
                  "agent_disk[{}]::storage_append_inner: oid {} has null entry (unreachable post-§8.1.B/C) — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        if (!data || data->size() == 0 || entry->storage == nullptr) {
            co_return;
        }
        // The manager-side router has already performed schema-adoption,
        // column-expansion, NOT NULL enforcement, dedup, and type-promotion
        // against the incoming chunk. The post-pipeline `data` chunk is
        // row-compatible with this slice's adapter. Apply the canonical
        // write here.
        if (txn.transaction_id != 0) {
            entry->storage->append(*data, txn);
        } else {
            entry->storage->append(*data);
        }
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_publish_commits_inner(uint64_t commit_id,
                                                std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
        // MVCC visibility flip — canonical writer post Step 8.11 wrap.
        // Skip ranges whose table_oid isn't in this agent's slice (the owning
        // agent receives its own slice via the manager's partitioning send);
        // null entries (defensive — legacy DISK record-only markers,
        // unreachable post §8.1.B/C) are skipped.
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
        // MVCC delete commit — canonical writer post Step 8.11 wrap.
        // txn_id==0 means no real transaction (legacy fast path); short-
        // circuit there, matching the original semantics.
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

    // ──────────────── Step 5 abort-path + completion handlers ─────────────
    //
    // Canonical bodies for storage_revert_appends / storage_revert_append /
    // storage_update / storage_delete_rows / storage_fetch (post Step 8.11
    // wrap: manager-side bodies in manager_disk_storage.cpp are pure
    // routers; the agent slice owns the work). Not-owned OIDs and null
    // entries (defensive — legacy DISK record-only markers, unreachable
    // post §8.1.B/C) are logged + no-op'd (void handlers) or return
    // nullptr (fetch — caller short-circuits to an empty chunk).
    //
    // Constraint #11 recap — `ranges` / `row_ids` / `data` arrive by value
    // (PMR vector / data_chunk_t unique_ptr / vector_t carries its own
    // allocator). No raw pointers cross the actor boundary, no exceptions.

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges) {
        // Batched abort — reverse-iterate so that nested ranges are unwound
        // in append-order opposite (mirrors manager body's `rbegin/rend`
        // walk). Per-range fallback contract matches publish_commits_inner.
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

    agent_disk_t::unique_future<void>
    agent_disk_t::storage_revert_append_inner(components::catalog::oid_t table_oid,
                                              int64_t row_start,
                                              uint64_t count) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_revert_append_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_revert_append_inner: oid {} is a DISK record-only marker — "
                  "deferred to manager-side until Step 8",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        if (entry->storage == nullptr || count == 0) {
            co_return;
        }
        entry->storage->revert_append(row_start, count);
        co_return;
    }

    agent_disk_t::unique_future<void>
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
            co_return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_update_inner: oid {} is a DISK record-only marker — "
                  "deferred to manager-side until Step 8",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        if (!data || entry->storage == nullptr) {
            co_return;
        }
        // Manager-side body has already aligned `data` with the canonical
        // schema (agent twin shares the same column definitions per Step 3
        // bootstrap_inner_sync). Apply the same write here so the IN_MEMORY
        // twin observes matching row mutations.
        entry->storage->update(row_ids, *data, txn);
        co_return;
    }

    agent_disk_t::unique_future<void>
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
            co_return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_delete_rows_inner: oid {} is a DISK record-only marker — "
                  "deferred to manager-side until Step 8",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        if (entry->storage == nullptr || count == 0) {
            co_return;
        }
        if (txn.transaction_id != 0) {
            entry->storage->delete_rows(row_ids, count, txn.transaction_id);
        } else {
            entry->storage->delete_rows(row_ids, count);
        }
        co_return;
    }

    agent_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    agent_disk_t::storage_fetch_inner(components::catalog::oid_t table_oid,
                                      components::vector::vector_t row_ids,
                                      uint64_t count) {
        // Read-path mirror — same nullptr-as-fallback contract as
        // storage_scan above. Manager body owns the read for not-owned OIDs
        // and DISK record-only markers; the agent answers for IN_MEMORY
        // twins planted via bootstrap_inner_sync.
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_fetch_inner: oid {} not owned by this agent — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_fetch_inner: oid {} is a DISK record-only marker — fallback to manager",
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

    // ──────────────── Step 5 batched-scan + metadata inner handlers ───────
    //
    // Final-cleanup read-path mirrors. Manager-side bodies in
    // manager_disk_storage.cpp remain canonical until Step 8; these
    // handlers exist so the agent slice answers from its IN_MEMORY twin
    // when the OID has been planted via bootstrap_inner_sync (Step 3).
    //
    // Fallback sentinels:
    //   - storage_scan_batched_inner → empty pmr-vector
    //   - storage_scan_segment_inner → nullptr (same as storage_scan /
    //     storage_fetch_inner)
    //   - storage_types_inner → empty pmr-vector (matches storage_t::types()
    //     for a schema-less twin)
    //   - storage_total_rows_inner → 0 (matches an empty twin)
    //
    // Constraint #11: PMR vectors by value, no shared mutable state, no
    // exceptions. `filter` arrives as a unique_ptr (mirrors storage_scan),
    // `projected_cols` as a std::vector (mirrors the manager handler's
    // shape — non-PMR because the routing path is shared with the
    // pipeline operator's local vector).

    agent_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    agent_disk_t::storage_scan_batched_inner(components::catalog::oid_t table_oid,
                                             std::unique_ptr<components::table::table_filter_t> filter,
                                             int64_t limit,
                                             std::vector<size_t> projected_cols,
                                             components::table::transaction_data txn) {
        std::pmr::vector<components::vector::data_chunk_t> batches{resource()};
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::storage_scan_batched_inner: oid {} not owned by this agent — fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::move(batches);
        }
        auto& entry = it->second;
        if (entry == nullptr || entry->storage == nullptr) {
            trace(log_,
                  "agent_disk[{}]::storage_scan_batched_inner: oid {} is a DISK record-only marker — "
                  "fallback to manager",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return std::move(batches);
        }
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
        entry->storage->scan_batched(batches, filter.get(), limit, projected_ptr, txn);
        co_return std::move(batches);
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

    // ──────────────── Step 6 fanout-only inner handlers ───────────────────
    //
    // Each handler iterates the agent's local storages_ slice. Post Step
    // 8.11 wrap + Step 8.12 cleanup the slice is the SOLE source of truth
    // (manager_disk_t::storages_ deleted); these handlers own the canonical
    // checkpoint / vacuum / cleanup work.
    //
    // Slice contents (post §8.1.B + §8.1.C SFBM transfer):
    //   - IN_MEMORY catalog/user twins: real
    //     unique_ptr<collection_storage_entry_t> planted via
    //     bootstrap_inner_sync.
    //   - DISK catalog entries: real entries owning the live SFBM, planted
    //     via bootstrap_disk_inner_sync (load) /
    //     bootstrap_create_disk_inner_sync (create).
    //   - DISK user entries: same — every DISK OID now lives here.
    // Null unique_ptr entries (legacy DISK record-only markers) are
    // defensively skipped but unreachable.
    //
    // checkpoint_inner runs the full sequence (compact + checkpoint(wal_id)
    // + sidecar) on the agent thread; the manager's std::min aggregation
    // across agents produces the global tally.
    agent_disk_t::unique_future<wal::id_t> agent_disk_t::checkpoint_inner(session_id_t /*session*/,
                                                                          wal::id_t current_wal_id) {
        trace(log_,
              "agent_disk[{}]::checkpoint_inner: {} entries in local slice",
              pool_idx_,
              storages_.size());
        // Version B* Step 8.1.B/C — agent owns ALL DISK SFBMs (catalog +
        // user). manager.storages_ is gone (Step 8.11 wrap); this body is
        // the canonical checkpoint sequence for DISK entries.
        //
        // Sequence (preserved from the original manager_disk_io.cpp body):
        //   1. Skip non-DISK / empty-path entries.
        //   2. Backup current .otbx → .otbx.prev (copy_file overwrite).
        //   3. Compact + checkpoint(wal_id) — 2 fsync inside.
        //   4. Persist checkpoint_wal_id to .otbx.wal_id sidecar via
        //      tmp+rename atomic write.
        //   5. Delete backup .prev on success.
        //   6. Tally min(prev_checkpoint_wal_id_) across this agent's DISK
        //      entries for return.
        //
        // IN_MEMORY twins are skipped — they have no live SFBM. Null
        // entries (defensive — legacy DISK record-only markers,
        // unreachable post §8.1.B/C) are skipped too.
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        for (auto& [tbl_oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            if (entry->table_storage.mode() != storage_mode_t::DISK) {
                continue;
            }
            if (entry->otbx_path.empty()) {
                continue;
            }
            trace(log_,
                  "agent_disk[{}]::checkpoint_inner checkpointing oid={}",
                  pool_idx_,
                  static_cast<unsigned>(tbl_oid));

            const auto& otbx_path = entry->otbx_path;
            auto prev_path = otbx_path;
            prev_path += ".prev";

            // Step (2) — backup current checkpoint before overwriting.
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

            // Step (3) — compact + checkpoint(wal_id).
            entry->table_storage.table().compact();
            entry->table_storage.checkpoint(current_wal_id);

            // Step (4) — persist sidecar wal_id atomically.
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

            // Step (5) — delete backup after successful checkpoint.
            if (std::filesystem::exists(prev_path)) {
                std::error_code remove_error;
                std::filesystem::remove(prev_path, remove_error);
            }

            // Step (6) — accumulate for the manager-side std::min
            // aggregation across all agents.
            min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
        }
        co_return min_prev_id;
    }

    agent_disk_t::unique_future<void> agent_disk_t::vacuum_inner(session_id_t /*session*/,
                                                                  uint64_t lowest_active_start_time) {
        trace(log_,
              "agent_disk[{}]::vacuum_inner: {} entries in local slice",
              pool_idx_,
              storages_.size());
        // Canonical vacuum body — post Step 8.11 wrap the slice IS the only
        // owner; manager_disk_t::vacuum_all is a pure router. Real entries
        // (IN_MEMORY twins + DISK SFBMs) get cleanup_versions + compact;
        // null entries (defensive — legacy DISK record-only markers,
        // unreachable post §8.1.B/C) are skipped.
        for (auto& [oid, entry] : storages_) {
            if (entry == nullptr) {
                continue;
            }
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
            table.compact();
        }
        co_return;
    }

    // Version B* Step 8.7 — single-route compact threshold handler.
    // Canonical body post Step 8.11 wrap (no manager-side fallback).
    // Manager dispatches via pool_idx_for_oid(ctx.table_oid) so only the
    // owning agent runs the check; not-owned OIDs are no-ops.
    //
    // Body preserves the original semantics: deleted/total > 0.3 threshold,
    // lowest_active_start_time gating against TRANSACTION_ID_START,
    // table.compact() only (cleanup_versions intentionally omitted —
    // scan_committed depends on intact version metadata before compact
    // rebuilds the row_group).
    agent_disk_t::unique_future<void> agent_disk_t::maybe_cleanup_inner(components::catalog::oid_t table_oid,
                                                                         uint64_t lowest_active_start_time) {
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
                  "agent_disk[{}]::maybe_cleanup_inner: oid {} has null entry (unreachable post-§8.1.B/C) — no-op",
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
            if (lowest_active_start_time < components::table::TRANSACTION_ID_START) {
                co_return;
            }
            trace(log_,
                  "agent_disk[{}]::maybe_cleanup_inner: oid={}, deleted {}/{}, running compact",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  deleted,
                  total);
            // Compact alone (no preceding cleanup_versions): scan_committed
            // depends on intact version metadata to filter tombstones;
            // cleanup_versions would strip it before compact rebuilds the
            // row_group.
            table.compact();
        }

        co_return;
    }

    // Version B* Step 7 — per-agent dropped_storages_ GC pass.
    //
    // Walks this agent's slice and physically removes entries whose dropped_at
    // _commit_id < new_horizon (no live snapshot can reference the .otbx).
    // Version B* Step 8.4.D / 8.11: this is the CANONICAL sweep; the manager-
    // side mirror dropped_storages_ field and GC body have been deleted.
    //
    // Version B* Step 8: also fires the on_subscriber_empty(DISK_KIND) ack to
    // the dispatcher once this agent's slice has drained (gated on
    // manager_dispatcher_addr_ != empty_address(), plumbed via set_manager_
    // dispatcher_sync from base_spaces). Each agent emits its own ack; the
    // dispatcher idempotently collapses the N-fold acks into a single
    // disk_has_dropped_ flag flip.
    //
    // Kept-vector rebuild avoids iterator-invalidation on partial erase. All
    // filesystem removes use the std::error_code overload — exceptions are
    // FORBIDDEN (Constraint #1).
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

        // Version B* Step 8 — per-agent on_subscriber_empty(DISK_KIND) ack.
        //
        // When this agent's local dropped_storages_ slice has drained, send
        // the ack directly to the dispatcher so it can clear disk_has_dropped_
        // and stop broadcasting on_horizon_advanced once every subscriber has
        // confirmed. The manager-side body (manager_disk_t::on_horizon_
        // advanced) ALSO sends the ack today — Step 8 N+1 will drop the
        // manager ack once every agent slice is the canonical owner.
        //
        // This is intentionally idempotent at the dispatcher: receiving the
        // ack while the flag is already cleared is a no-op. The dual-path
        // safety net keeps the GC subscriber clearing reliably during the
        // incremental Step 8 migration even if some agent slices are still
        // empty (in which case THIS handler sees an already-empty slice and
        // fires the ack immediately — same observable behaviour as the
        // manager body).
        //
        // Gated on != empty_address() — base_spaces wires the dispatcher
        // address via set_manager_dispatcher_sync before scheduler.start; test
        // fixtures that skip the wire-up (no dispatcher present) still pass
        // through cleanly.
        if (dropped_storages_.empty()
            && manager_dispatcher_addr_ != actor_zeta::address_t::empty_address()) {
            // dec 38 ack flag flip — dispatcher clears disk_has_dropped_ on
            // receipt. DISK_KIND = 1 matches the manager-side constant and the
            // dispatcher's subscriber-kind enum.
            constexpr uint8_t DISK_KIND = 1;
            [[maybe_unused]] auto _ = actor_zeta::send(manager_dispatcher_addr_,
                                                       &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                                       DISK_KIND);
        }
        co_return;
    }

    // Version B* Step 8 — dispatcher address plumbing.
    //
    // base_spaces calls this after spawning the agents and before scheduler.
    // start so each agent owns its own copy of the manager_dispatcher_t
    // mailbox handle. Single-threaded by construction at the bootstrap call
    // site (no scheduler running yet); constraint #11 — address_t is a
    // mailbox handle, not mutable shared state. After scheduler.start, the
    // address is read-only from inside on_horizon_advanced_inner.
    void agent_disk_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        manager_dispatcher_addr_ = std::move(address);
    }

    // Version B* Step 7 — per-agent dropped_storages_ slice bootstrap.
    //
    // Pre-scheduler-start callers only (dec 18 V1 carve-out): base_spaces
    // PHASE 2c catalog scan rebuild calls into manager_disk_t::register_
    // dropped_storage_sync which forwards here while schedulers are still
    // idle. After scheduler.start, the runtime DROP path
    // (manager_disk_t::mark_storage_dropped mailbox handler) must use the
    // mailbox-handler overload below (Constraint #11 — no sync method calls
    // across actor boundaries once mailboxes are live).
    void agent_disk_t::register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                            uint64_t dropped_at_commit_id,
                                                            std::filesystem::path path,
                                                            std::pmr::vector<std::filesystem::path> sidecar_paths) {
        dropped_storages_.push_back(dropped_storage_entry_t{oid,
                                                             dropped_at_commit_id,
                                                             std::move(path),
                                                             std::move(sidecar_paths)});
    }

    // Mailbox handler — runtime DROP path forwarded from manager_disk_t::
    // mark_storage_dropped via actor_zeta::otterbrix::send. Delegates to the
    // sync helper which performs the push_back; the actor mailbox guarantees
    // serialization w.r.t. on_horizon_advanced_inner.
    agent_disk_t::unique_future<void>
    agent_disk_t::register_dropped_storage_inner(components::catalog::oid_t oid,
                                                  uint64_t dropped_at_commit_id,
                                                  std::filesystem::path path,
                                                  std::pmr::vector<std::filesystem::path> sidecar_paths) {
        register_dropped_storage_inner_sync(oid,
                                             dropped_at_commit_id,
                                             std::move(path),
                                             std::move(sidecar_paths));
        co_return;
    }

    // Version B* Step 8.3 — runtime DROP TABLE storages_ erase.
    //
    // Forwarded from manager_disk_t::drop_storage's mailbox body (a pure
    // router post Step 8.11 wrap) via actor_zeta::otterbrix::send. The
    // agent slice is the SOLE owner of the storage entry; this handler
    // performs the canonical erase + physical .otbx removal. Idempotent:
    // erasing a missing key is a no-op (DROP IF EXISTS / WAL replay
    // re-drop / not-routed-to-this-agent OIDs all reduce to the same call
    // here). IN_MEMORY twins and DISK entries are handled uniformly —
    // std::pmr::unordered_map::erase drops the unique_ptr which closes the
    // owned file_handle_t at most once per process.
    //
    // Constraint #11: mailbox-only (entry point via dispatch), no shared
    // mutable state crosses the actor boundary (oid by value), no
    // exceptions. The mailbox guarantees serialization w.r.t. other slice
    // mutations on this agent (bootstrap_inner_sync runs pre-start; runtime
    // mutators are storage_*_inner which only read the slice).
    agent_disk_t::unique_future<void>
    agent_disk_t::drop_storage_inner(components::catalog::oid_t oid) {
        // Read otbx_path from the agent's own entry BEFORE the erase so the
        // unique_ptr is still live. Empty path (IN_MEMORY twins planted via
        // bootstrap_inner_sync) skips the filesystem remove block by
        // construction — only DISK entries planted via bootstrap_disk_inner_
        // sync / bootstrap_create_disk_inner_sync ever carry a non-empty
        // otbx_path. Not-owned OIDs and null entries (defensive — legacy
        // DISK record-only markers, unreachable post §8.1.B/C) both fall
        // through to the no-op log branch.
        //
        // Filesystem remove sequence matches the pre-8.4.B manager body:
        // .otbx + .wal_id sidecar + .prev sidecar + per-oid parent
        // directory. std::error_code overloads on every remove — exceptions
        // FORBIDDEN (Constraint #1).
        std::filesystem::path otbx_path;
        if (auto it = storages_.find(oid); it != storages_.end()) {
            if (it->second != nullptr) {
                otbx_path = it->second->otbx_path;
            }
        }
        const auto erased = storages_.erase(oid);
        if (erased == 0) {
            // Logging at trace because manager_disk_t::drop_storage routes
            // to a single agent (idx = pool_idx_for_oid(oid)), so this path
            // is only reached for "OID truly missing" — benign (idempotent
            // DROP).
            trace(log_,
                  "agent_disk[{}]::drop_storage_inner: oid {} not in local slice (no-op)",
                  pool_idx_,
                  static_cast<unsigned>(oid));
        } else {
            trace(log_,
                  "agent_disk[{}]::drop_storage_inner: erased oid {} from local slice",
                  pool_idx_,
                  static_cast<unsigned>(oid));
        }
        if (!otbx_path.empty()) {
            // Physically remove the .otbx file (and its sidecars + per-oid
            // directory). Otherwise a restart would see the surviving .otbx,
            // WAL replay would synthesise a phantom storage, and re-CREATE
            // TABLE could collide with the recycled oid. The mark_storage_
            // dropped / on_horizon_advanced GC sweep is the secondary safety
            // net — this immediate removal is the primary cleanup.
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
        co_return;
    }

    // Version B* Step 8.11.C — physical column compaction routed to the
    // owning agent. The mutation half of compact_relkind_g_storage (manager
    // side reads the surviving column set through storage_entry_sync,
    // Step 8.11.A) lives here because collection_storage_entry_t::drop_
    // column is non-const: it calls table_storage_t::drop_column and then
    // rebuilds the storage_t adapter (the adapter holds a data_table_t&
    // that becomes dangling after the rebuild) against the agent's own
    // arena via resource(). Post Step 8.11 wrap the agent slice is the
    // SOLE source of truth — no manager-side fallback exists.
    //
    // Outcomes:
    //   (a) OID not in this agent's slice (routing miss / WAL replay before
    //       pool assignment stabilises) — logged no-op,
    //   (b) Null entry (defensive — legacy DISK record-only marker,
    //       unreachable post §8.1.B/C) — logged no-op,
    //   (c) DISK-mode storage — drop_column is out-of-scope per
    //       table_storage_t::drop_column (returns false in DISK mode; see
    //       test_table_storage::drop_column_disk_is_noop),
    //   (d) the column is already gone (idempotent re-issue) — the inner
    //       drop_column returns false and we log "not found".
    // Constraint #11: oid by value, column_name moved by value (std::pmr::
    // string). No shared state crosses the actor boundary; no exceptions.
    agent_disk_t::unique_future<void>
    agent_disk_t::drop_column_inner(components::catalog::oid_t table_oid,
                                     std::pmr::string column_name) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            trace(log_,
                  "agent_disk[{}]::drop_column_inner: oid {} not owned by this agent — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        auto& entry = it->second;
        if (entry == nullptr) {
            // Defensive: null entries (legacy DISK record-only markers) are
            // unreachable after §8.1.B/C SFBM transfer. In any case, DISK-
            // mode drop_column is out-of-scope per table_storage_t::drop_
            // column (returns false on DISK mode).
            trace(log_,
                  "agent_disk[{}]::drop_column_inner: oid {} has null entry (unreachable post-§8.1.B/C) — no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid));
            co_return;
        }
        // collection_storage_entry_t::drop_column takes std::string; the
        // mailbox payload is std::pmr::string (Constraint #11 — PMR
        // containers across actor boundaries). Build a std::string view of
        // the same bytes; the inner call copies into the table-name lookup
        // path.
        const std::string attname{column_name.data(), column_name.size()};
        const bool dropped = entry->drop_column(attname, resource());
        if (!dropped) {
            // Either the column is already gone (idempotent re-issue) or
            // table_storage is DISK-mode (returns false unconditionally).
            // Both reduce to "no work to do here".
            trace(log_,
                  "agent_disk[{}]::drop_column_inner: oid {} column '{}' not found / DISK no-op",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  attname);
        } else {
            trace(log_,
                  "agent_disk[{}]::drop_column_inner: oid {} dropped column '{}'",
                  pool_idx_,
                  static_cast<unsigned>(table_oid),
                  attname);
        }
        co_return;
    }

} //namespace services::disk
