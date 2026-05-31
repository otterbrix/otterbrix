#pragma once

#include "agent_disk.hpp"
#include "disk_contract.hpp"
#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>
#include <actor-zeta/mailbox/make_message.hpp>
#include <actor-zeta/mailbox/message.hpp>
#include <atomic>
#include <chrono>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/catalog/results/resolve_result.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/configuration/configuration.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/storage/storage.hpp>
#include <components/storage/table_storage_adapter.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/executor.hpp>
#include <limits>
#include <mutex>
#include <optional>
#include <services/wal/base.hpp>
#include <set>
#include <thread>
#include <unordered_set>
#include <vector>

namespace services::disk {

    using session_id_t = ::components::session::session_id_t;

    enum class storage_mode_t : uint8_t
    {
        IN_MEMORY = 0,
        DISK = 1
    };

    /// Owns data_table_t + its supporting storage infrastructure.
    /// Supports both in-memory (schema-less computing tables) and disk-backed (table.otbx) storage.
    class table_storage_t {
    public:
        /// In-memory mode: computing tables (schema-less)
        explicit table_storage_t(std::pmr::memory_resource* resource);

        /// In-memory mode with explicit columns
        explicit table_storage_t(std::pmr::memory_resource* resource,
                                 std::vector<components::table::column_definition_t> columns);

        /// Disk mode: create new table.otbx
        table_storage_t(std::pmr::memory_resource* resource,
                        std::vector<components::table::column_definition_t> columns,
                        const std::filesystem::path& otbx_path);

        /// Disk mode: load existing table.otbx
        table_storage_t(std::pmr::memory_resource* resource, const std::filesystem::path& otbx_path);

        components::table::data_table_t& table() { return *table_; }
        storage_mode_t mode() const { return mode_; }

        /// Checkpoint (disk mode only, no-op for in-memory).
        /// W-TORN: writes data blocks + fsync, then header + fsync (2 fsync — durability before header swap).
        void checkpoint();
        /// Same as checkpoint() + tracks W-TORN per-table wal_id snapshot.
        /// prev_checkpoint_wal_id_ ← old checkpoint_wal_id_; checkpoint_wal_id_ ← new_wal_id.
        void checkpoint(wal::id_t new_wal_id);

        /// W-TORN: latest committed checkpoint wal_id for this DISK table (0 if never checkpointed / IN_MEMORY).
        wal::id_t checkpoint_wal_id() const noexcept { return checkpoint_wal_id_; }
        /// Used by load path to seed checkpoint_wal_id_ from sidecar before WAL replay
        /// decides which records this storage already includes.
        void set_checkpoint_wal_id(wal::id_t v) noexcept { checkpoint_wal_id_ = v; }
        /// W-TORN: previous checkpoint wal_id (the state in the .prev backup); 0 before first overwrite.
        /// Used by checkpoint_all to compute min(prev) for safe WAL truncation.
        wal::id_t prev_checkpoint_wal_id() const noexcept { return prev_checkpoint_wal_id_; }

        /// Add a new column to the live in-memory table. Replaces table_ with a new data_table_t
        /// constructed from the current one + col. Retained as a primitive for tests and
        /// future in-memory-sync paths; the SQL ALTER TABLE ADD COLUMN flow no longer calls
        /// it (resolve_table reads columns from pg_attribute on every lookup).
        void add_column(components::table::column_definition_t& col);

        /// Physical column compaction. Drops the column whose name matches
        /// `attname` from the IN_MEMORY data_table_t, reclaiming its physical storage.
        /// Implemented via the data_table_t(parent, removed_column) rebuild constructor —
        /// row_groups are rebuilt without the dropped column (collection_t::remove_column
        /// per-segment). Used by VACUUM after pg_computed_column GC: columns that no
        /// longer have any live attrefcount>0 row are physically dead and can be reclaimed.
        ///
        /// No-op for DISK-backed storages (would require segment rewrites + checkpoint
        /// coordination). No-op if the column is missing.
        ///
        /// Returns true if the column was found and removed; false otherwise (column
        /// missing OR storage is DISK-mode).
        bool drop_column(const std::string& attname);

    private:
        storage_mode_t mode_;
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        std::unique_ptr<components::table::storage::block_manager_t> block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
        wal::id_t checkpoint_wal_id_{0};
        wal::id_t prev_checkpoint_wal_id_{0};
    };

    // Storage entries per collection.
    //
    // Version B* Step 2 (Pass 9): promoted from a private nested type of
    // manager_disk_t to namespace scope so agent_disk_t can own its slice of
    // storages_ as `std::pmr::unordered_map<oid_t, std::unique_ptr<
    // collection_storage_entry_t>>`. Constraint #11: ownership migration is
    // by-rvalue std::unique_ptr move only — no shared raw pointers cross the
    // actor boundary.
    struct collection_storage_entry_t {
        table_storage_t table_storage;
        std::unique_ptr<components::storage::storage_t> storage;
        // Actual on-disk path for DISK-mode tables. Empty for IN_MEMORY entries.
        // Used by checkpoint_all (sidecar lands next to .otbx) and drop_storage
        // (physical file removal).
        std::filesystem::path otbx_path;
        // Computing (relkind='g', dynamic-schema) table: created schema-less via
        // create_storage. Only such tables may hold several columns with the same
        // name but different types (multi-type fields); regular tables coerce.
        bool is_computed = false;

        /// In-memory: schema-less (computing / relkind='g' dynamic schema)
        explicit collection_storage_entry_t(std::pmr::memory_resource* resource)
            : table_storage(resource)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), resource))
            , is_computed(true) {}

        /// In-memory: with columns
        explicit collection_storage_entry_t(std::pmr::memory_resource* resource,
                                            std::vector<components::table::column_definition_t> columns)
            : table_storage(resource, std::move(columns))
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), resource)) {
        }

        /// Disk: create new table.otbx
        collection_storage_entry_t(std::pmr::memory_resource* resource,
                                   std::vector<components::table::column_definition_t> columns,
                                   const std::filesystem::path& otbx_path_in)
            : table_storage(resource, std::move(columns), otbx_path_in)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), resource))
            , otbx_path(otbx_path_in) {}

        /// Disk: load existing table.otbx
        collection_storage_entry_t(std::pmr::memory_resource* resource, const std::filesystem::path& otbx_path_in)
            : table_storage(resource, otbx_path_in)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), resource))
            , otbx_path(otbx_path_in) {}

        /// Update live in-memory schema: add new column to table_ and recreate the storage adapter.
        void add_column(components::table::column_definition_t& col, std::pmr::memory_resource* res) {
            table_storage.add_column(col);
            storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), res);
        }

        /// Physical column compaction: drop column from in-memory table_ and
        /// recreate the storage adapter (the adapter holds a data_table_t& that becomes
        /// dangling after the rebuild). Returns true if the column was found and removed.
        bool drop_column(const std::string& attname, std::pmr::memory_resource* res) {
            if (!table_storage.drop_column(attname)) {
                return false;
            }
            storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), res);
            return true;
        }
    };

    // Block C §3.5 dec 33 V1 deferred DROP TABLE GC entry: file path + commit_id
    // of the DROP plus the standard sidecars (`.wal_id`, `.prev`). on_horizon_
    // advanced iterates the per-actor slice and physically removes entries whose
    // dropped_at_commit_id < new_horizon (no live snapshot can reference them).
    //
    // Version B* Step 7: promoted from a private nested type of manager_disk_t
    // to namespace scope (mirrors collection_storage_entry_t in Step 2) so
    // agent_disk_t can own its routed slice as
    // `std::pmr::vector<dropped_storage_entry_t>`. Constraint #11: passed
    // by-value across the actor boundary (path / pmr-vector members move).
    struct dropped_storage_entry_t {
        components::catalog::oid_t oid;
        uint64_t dropped_at_commit_id;
        std::filesystem::path path;
        std::pmr::vector<std::filesystem::path> sidecar_paths;
    };

    class manager_disk_t final : public actor_zeta::actor::actor_mixin<manager_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t>;

        using run_fn_t = std::function<void()>;

        manager_disk_t(
            std::pmr::memory_resource*,
            actor_zeta::scheduler_raw scheduler,
            actor_zeta::scheduler_raw scheduler_disk,
            configuration::config_disk config,
            log_t& log,
            run_fn_t run_fn = [] { std::this_thread::yield(); });
        ~manager_disk_t();

        void set_run_fn(run_fn_t fn) { run_fn_ = std::move(fn); }

        // True if a storage entry is registered for `table_oid` (used by WAL replay to lazily
        // create in-memory storages on the first PHYSICAL_INSERT for tables without an .otbx).
        //
        // Version B* Step 8.11 wrap (2026-05-31): manager-side storages_ map
        // has been DELETED — the routed agent slice is the SOLE source of
        // truth. All call sites (test_d4_lazy_load bootstrap inspection,
        // base_spaces Phase 2c bootstrap,
        // manager_disk_io::peek_checkpoint_wal_id_from_disk bootstrap path)
        // are either single-threaded bootstrap or already inside the manager's
        // mailbox lock — the sync probe into the agent is safe (Constraint #11
        // pre-scheduler carve-out).
        bool has_storage(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return false;
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return false;
            return agents_[idx]->has_storage_sync(table_oid);
        }
        // Sync row-count probe used by base_spaces WAL replay to avoid duplicating already-
        // checkpointed records. Returns 0 for unknown OIDs.
        //
        // Step 8.11 wrap: routed agent slice is the sole source of truth.
        uint64_t total_rows_sync(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return 0;
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return 0;
            return agents_[idx]->total_rows_inner_sync(table_oid);
        }
        // Returns the persisted checkpoint_wal_id for `table_oid` (0 if never checkpointed or unknown).
        //
        // Step 8.11 wrap: routed agent slice is the sole source of truth.
        wal::id_t checkpoint_wal_id_sync(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return wal::id_t{0};
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return wal::id_t{0};
            return agents_[idx]->checkpoint_wal_id_inner_sync(table_oid);
        }

        // Read the .otbx.wal_id sidecar directly from disk without loading the storage.
        wal::id_t peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                                   components::catalog::oid_t database_oid) const noexcept;

        // Load a user-table storage from its .otbx file on demand. Called by WAL replay
        // when it encounters a record for a disk-backed table that hasn't been loaded yet.
        void load_storage_for_wal_replay_sync(components::catalog::oid_t table_oid,
                                              components::catalog::oid_t database_oid);

        // Synchronous storage creation for initialization (before schedulers start).
        void create_storage_with_columns_sync(components::catalog::oid_t table_oid,
                                              components::catalog::oid_t database_oid,
                                              std::vector<components::table::column_definition_t> columns);
        // System catalog (pg_*) bootstrap and load. Called from base_spaces during PHASE 1
        // before any actor is spawned. bootstrap creates the 9 .otbx files for the system
        // tables; load picks them up on subsequent starts. Both are idempotent w.r.t. the
        // resulting `storages_` map — collections are keyed by `pg_catalog.<name>`.
        void bootstrap_system_tables_sync();
        void load_system_tables_sync();
        // Walk config_.path looking for user-table .otbx files
        // (${db_oid}/${tbl_oid}/table.otbx where tbl_oid >= FIRST_USER_OID) and
        // load each into storages_ via load_storage_disk_sync. Called by
        // base_spaces after bootstrap_system_tables_sync so that subsequent
        // WAL replay can (1) read each user table's checkpoint_wal_id sidecar
        // for filtering and (2) avoid synthesising phantom storages with
        // possibly-wrong schemas from a single WAL chunk.
        void load_user_table_storages_sync();
        // Synchronous scan of pg_class.oid column, returning the set
        // of user-table OIDs (oid >= FIRST_USER_OID) currently alive in the
        // catalog. Called by base_spaces between system-record replay and
        // user-record replay so user WAL records targeting a dropped table
        // (whose .otbx and pg_class row are gone) are skipped instead of
        // resurrecting a phantom storage.
        std::unordered_set<components::catalog::oid_t> alive_user_oids_sync() const;

        // dec 37 V1 catalog scan: returns (oid, delete_id) for every pg_class
        // row whose row-version is tombstoned (deleted but not yet physically
        // GC'd). Called by base_spaces Phase 2c after WAL replay, before
        // scheduler.start, to rebuild the per-agent dropped_storages_ slices
        // (Version B* Step 8.11: manager-side mirror deleted; routing goes
        // through register_dropped_storage_sync into the owning agent slice)
        // from on-disk state so on_horizon_advanced can finish the deferred
        // GC of user-table .otbx files left behind by a crash mid-DROP.
        //
        // pg_class has no `dropped_at_commit_id` column (dec 32 V2 added that
        // to pg_attribute, not pg_class). The tombstone marker is the
        // row-version-manager's per-row delete_id, which is not exposed
        // through a public API. Returned delete_id is a sentinel (1) — at boot
        // lowest_active_start_time=1, so any delete_id > 1 is GC-eligible;
        // sentinel 1 means "GC on the first horizon advance past 1", i.e. the
        // first commit that bumps published_horizon_.
        std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>> scan_dropped_oids_sync();

        // Read-only accessor for the on-disk root directory.
        // dec 37 V1 base_spaces uses this to derive dropped storage paths.
        const std::filesystem::path& path_db() const noexcept { return config_.path; }
        // After load, scan pg_class/pg_attribute/pg_type/pg_proc/pg_constraint and seed
        // oid_gen_ to max(oid)+1 so future allocate() never collides with on-disk OIDs.
        // Scans pg_class/pg_attribute/pg_type/pg_proc/pg_constraint/pg_index for the max
        // OID across all system tables, then seeds oid_gen_ to max+1 so future allocate()
        // never collides with on-disk OIDs.
        void restore_oid_generator_sync();

        // Read the value of a named setting from pg_settings. Returns the most recently
        // appended value for the given name, or empty string if not found.
        // Synchronous — called at startup before actor schedulers start.
        std::string read_setting_sync(std::string_view name);

        // Public accessor — ddl_* methods take their OIDs from this generator.
        components::catalog::oid_generator& oid_gen() noexcept { return oid_gen_; }

        // Per-item resolve methods. Каждый метод сканирует соответствующую pg_*-таблицу
        // на disk actor thread и возвращает найденный объект (или {found=false}).
        // Параметр since_version сохранён для совместимости с message dispatch (всегда
        // игнорируется — версионирование больше не используется).
        unique_future<resolve_namespace_result_t>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);
        unique_future<resolve_table_result_t> resolve_table(execution_context_t ctx,
                                                            components::catalog::oid_t namespace_oid,
                                                            std::string name,
                                                            std::uint64_t since_version);
        unique_future<resolve_type_result_t> resolve_type(execution_context_t ctx,
                                                          components::catalog::oid_t namespace_oid,
                                                          std::string name,
                                                          std::uint64_t since_version);
        // Sync internal: pg_type → pg_class fallback. Self-recurses for composite STRUCT
        // field references (UNKNOWN-by-name) without going through actor messaging.
        resolve_type_result_t resolve_type_sync(components::catalog::oid_t namespace_oid, const std::string& name);
        unique_future<resolve_function_result_t> resolve_function(execution_context_t ctx,
                                                                  components::catalog::oid_t namespace_oid,
                                                                  std::string name,
                                                                  std::uint64_t since_version);

        // Cross-namespace function lookup: returns ALL pg_proc rows whose proname matches
        // `name`, regardless of pronamespace. Used by the UDF admin paths (#41 Path 2/4):
        // register_udf needs to detect cross-namespace conflicts; drop_udf needs to purge
        // every row sharing the name. The single-namespace resolve_function above is the
        // hot-path query API; this one is admin-scope and may return an empty vector.
        unique_future<std::pmr::vector<resolve_function_result_t>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);

        // V4 admin-path enumerators. Bypass the per-name cache (cache is per-(name, ns_oid)
        // keyed; enumeration of "all namespaces" / "all tables in ns" cannot be served by
        // it). Used by the dispatcher's collections_ rebuild and UDF namespace pick.
        unique_future<std::pmr::vector<std::string>> list_namespaces(execution_context_t ctx);

        // Allocate a batch of fresh OIDs from the disk-local oid_gen_. Called by the
        // dispatcher before invoking planner_t::create_plan for DDL statements, so that
        // the planner can build pg_class / pg_attribute rows without needing async access
        // to the disk actor. Wasted OIDs (plan rejected before execution) are acceptable —
        // same trade-off as PostgreSQL's pre-allocation approach.
        unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        // WAL-safe append of a single pre-built row into a pg_catalog table.
        unique_future<components::pg_catalog_append_range_t>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                   components::catalog::oid_t table_oid,
                                                   std::int64_t oid_col_idx,
                                                   components::catalog::oid_t target_oid);

        // Block C §3.5 dec 32 V2 OPTION X: patch the pg_attribute row identified by
        // `attoid` (column index 0) with `commit_id` written into column index 10
        // (added_at_commit_id) when kind == added_at, or column index 11
        // (dropped_at_commit_id) when kind == dropped_at. operator_alter_column_
        // {add,drop,rename} insert pg_attribute rows with placeholder 0 (commit_id
        // isn't allocated until transaction_manager_t::commit() runs at commit
        // time); operator_commit_transaction_t drains the per-txn backfill
        // markers and dispatches one of these calls per marker after the
        // commit_id is known but BEFORE storage_publish_commits flips the MVCC
        // visibility bit. The rows still carry insert_id == txn_id so the
        // partial-column update is a metadata-only write on rows nobody else
        // can observe. WAL pairing: emits a physical_update record so replay
        // re-applies the backfill after the matching physical_insert.
        unique_future<void>
        update_pg_attribute_commit_id_field(execution_context_t ctx,
                                            components::catalog::oid_t attoid,
                                            components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                            std::uint64_t commit_id);

        // Pure storage scan: row_ids of txn-visible rows in the table with `table_oid`
        // where key_col_names[i] == key_values[i] for every i.
        unique_future<std::pmr::vector<std::int64_t>>
        scan_by_key(execution_context_t ctx,
                    components::catalog::oid_t table_oid,
                    std::pmr::vector<std::string> key_col_names,
                    std::pmr::vector<components::types::logical_value_t> key_values);

        // Full row-data scan: returns all column values for every txn-visible row
        // where key_col_names[i] == key_values[i].
        unique_future<std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>>
        read_rows_by_key(execution_context_t ctx,
                         components::catalog::oid_t table_oid,
                         std::pmr::vector<std::string> key_col_names,
                         std::pmr::vector<components::types::logical_value_t> key_values);

        // Physical column compaction. For an IN_MEMORY relkind='g' storage,
        // drop every physical column whose name is NOT in `live_attnames`. Called by
        // operator_vacuum_t after pg_computed_column GC: columns whose
        // attrefcount<=0 rows have been deleted are physically dead and can be
        // reclaimed. Returns the number of columns physically dropped (0 if storage
        // is DISK-mode, missing, or already compact). DISK-backed storages would
        // need segment rewrites + checkpoint coordination.
        unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::set<std::string> live_attnames);

        // ALTER TABLE ADD COLUMN owned by operator_alter_column_add_t; computed
        // tables maintained via operator_computed_field_register_t.

        // Synchronous direct replay methods for physical WAL (before schedulers start).
        uint64_t direct_append_sync(components::catalog::oid_t table_oid,
                                    components::vector::data_chunk_t& data,
                                    core::date::timezone_offset_t session_tz);
        uint64_t direct_append_sync(components::catalog::oid_t table_oid,
                                    components::vector::data_chunk_t& data,
                                    core::date::timezone_offset_t session_tz,
                                    const components::table::transaction_data& txn);
        void direct_delete_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count);
        void direct_delete_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count,
                                const components::table::transaction_data& txn);
        void direct_update_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                components::vector::data_chunk_t& new_data);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char* { return "manager_disk"; }

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
        requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>) [[nodiscard]] ReturnType
            enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args);

        void sync(address_pack pack);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        unique_future<wal::id_t> checkpoint_all(session_id_t session, wal::id_t current_wal_id);
        unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        unique_future<void> maybe_cleanup(execution_context_t ctx, uint64_t lowest_active_start_time);

        // dec 33 V1 event-driven GC subscriber. Version B* Step 8.4.D /
        // 8.11: manager body is a pure fanout — every agent's
        // on_horizon_advanced_inner walks its OWN dropped_storages_ slice,
        // physically removes entries whose dropped_at_commit_id <
        // new_horizon (no live snapshot can reference them anymore), and
        // emits on_subscriber_empty(DISK_KIND) to dispatcher on slice drain
        // so the selective-broadcast flag clears.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        /// Bootstrap helper — dec 37 V1 catalog scan rebuild populates this.
        /// Also called internally by the mark_storage_dropped mailbox handler
        /// once it has derived path + sidecars from the live `storages_` entry.
        /// NOT a mailbox handler — single-threaded callers only.
        void register_dropped_storage_sync(components::catalog::oid_t oid,
                                           uint64_t dropped_at_commit_id,
                                           std::filesystem::path path,
                                           std::pmr::vector<std::filesystem::path> sidecar_paths);

        /// Runtime DROP TABLE path — sent from operator_dynamic_cascade_delete
        /// (inside the executor actor) BEFORE the existing drop_storage send.
        /// Reads the live `storages_` entry to derive the .otbx path + the
        /// standard sidecars (wal_id, prev), then records them via
        /// register_dropped_storage_sync. Does NOT touch the `storages_` map
        /// or any files — drop_storage continues to perform the immediate file
        /// removal; the GC entry exists so on_horizon_advanced can reconcile
        /// any leftover state and so dispatcher disk_has_dropped_ gets flipped
        /// via on_drop_resource_marked.
        unique_future<void> mark_storage_dropped(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t dropped_at_commit_id);

        /// Bootstrap helper — base_spaces wires dispatcher address before
        /// scheduler.start, and the manager fans it out to every agent so
        /// per-slice on_horizon_advanced_inner can fire
        /// on_subscriber_empty(DISK_KIND) directly once its dropped_storages_
        /// slice drains (Version B* Step 8.4.D / 8.11 — no manager mirror).
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Storage management
        unique_future<void> create_storage(session_id_t session,
                                           components::catalog::oid_t table_oid,
                                           components::catalog::oid_t database_oid);
        unique_future<void> create_storage_with_columns(session_id_t session,
                                                        components::catalog::oid_t table_oid,
                                                        components::catalog::oid_t database_oid,
                                                        std::vector<components::table::column_definition_t> columns);
        unique_future<void> create_storage_disk(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t database_oid,
                                                std::vector<components::table::column_definition_t> columns);
        unique_future<void> drop_storage(session_id_t session, components::catalog::oid_t table_oid);

        // Storage queries
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<uint64_t> storage_total_rows(session_id_t session, components::catalog::oid_t table_oid);

        // Storage data operations
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     components::catalog::oid_t table_oid,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);
        // Batched + projected variant: returns a vector of chunks (PR #483 multi-chunk)
        // and applies index-based column projection at the storage layer (PR #477).
        // Empty `projected_cols` means "read all columns" (pass-through).
        unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        storage_scan_batched(session_id_t session,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::table::table_filter_t> filter,
                             int64_t limit,
                             std::vector<size_t> projected_cols,
                             components::table::transaction_data txn);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session, components::catalog::oid_t table_oid, int64_t start, uint64_t count);
        unique_future<std::pair<uint64_t, uint64_t>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::vector::data_chunk_t> data);

        unique_future<std::pair<int64_t, uint64_t>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       components::vector::vector_t row_ids,
                       std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t> storage_delete_rows(execution_context_t ctx,
                                                    components::catalog::oid_t table_oid,
                                                    components::vector::vector_t row_ids,
                                                    uint64_t count);
        // MVCC commit/revert
        unique_future<void> storage_publish_commit(execution_context_t ctx,
                                                  components::catalog::oid_t table_oid,
                                                  uint64_t commit_id,
                                                  int64_t row_start,
                                                  uint64_t count);
        unique_future<void> storage_revert_append(execution_context_t ctx,
                                                  components::catalog::oid_t table_oid,
                                                  int64_t row_start,
                                                  uint64_t count);
        unique_future<void>
        storage_publish_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);

        // Batched MVCC swap. Each range carries its own table_oid.
        unique_future<void> storage_publish_commits(execution_context_t ctx,
                                                   uint64_t commit_id,
                                                   std::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void> storage_publish_deletes(execution_context_t ctx,
                                                   uint64_t commit_id,
                                                   std::set<components::catalog::oid_t> tables);

        unique_future<void> storage_revert_appends(execution_context_t ctx,
                                                   std::vector<components::pg_catalog_append_range_t> ranges);

        using dispatch_traits = actor_zeta::implements<disk_contract,
                                                       &manager_disk_t::flush,
                                                       &manager_disk_t::checkpoint_all,
                                                       &manager_disk_t::vacuum_all,
                                                       &manager_disk_t::maybe_cleanup,
                                                       // Storage management
                                                       &manager_disk_t::create_storage,
                                                       &manager_disk_t::create_storage_with_columns,
                                                       &manager_disk_t::create_storage_disk,
                                                       &manager_disk_t::drop_storage,
                                                       // Storage queries
                                                       &manager_disk_t::storage_types,
                                                       &manager_disk_t::storage_total_rows,
                                                       // Storage data operations
                                                       &manager_disk_t::storage_scan,
                                                       &manager_disk_t::storage_scan_batched,
                                                       &manager_disk_t::storage_fetch,
                                                       &manager_disk_t::storage_scan_segment,
                                                       &manager_disk_t::storage_append,
                                                       &manager_disk_t::storage_update,
                                                       &manager_disk_t::storage_delete_rows,
                                                       // MVCC commit/revert
                                                       &manager_disk_t::storage_publish_commit,
                                                       &manager_disk_t::storage_revert_append,
                                                       &manager_disk_t::storage_publish_delete,
                                                       &manager_disk_t::storage_publish_commits,
                                                       &manager_disk_t::storage_publish_deletes,
                                                       &manager_disk_t::storage_revert_appends,
                                                       // resolve + invalidation pull
                                                       &manager_disk_t::resolve_namespace,
                                                       &manager_disk_t::resolve_table,
                                                       &manager_disk_t::resolve_type,
                                                       &manager_disk_t::resolve_function,
                                                       &manager_disk_t::resolve_function_by_name,
                                                       &manager_disk_t::list_namespaces,
                                                       &manager_disk_t::allocate_oids_batch,
                                                       &manager_disk_t::append_pg_catalog_row,
                                                       &manager_disk_t::delete_pg_catalog_rows,
                                                       &manager_disk_t::update_pg_attribute_commit_id_field,
                                                       &manager_disk_t::scan_by_key,
                                                       &manager_disk_t::read_rows_by_key,
                                                       &manager_disk_t::compact_relkind_g_storage,
                                                       &manager_disk_t::on_horizon_advanced,
                                                       &manager_disk_t::mark_storage_dropped>;

    private:
        // Disk storage helpers — used only by bootstrap / io / recovery paths
        // inside services/disk/. Not part of the actor's public interface.
        void create_storage_disk_sync(components::catalog::oid_t table_oid,
                                      components::catalog::oid_t database_oid,
                                      std::vector<components::table::column_definition_t> columns,
                                      const std::filesystem::path& otbx_path);
        void load_storage_disk_sync(components::catalog::oid_t table_oid,
                                    components::catalog::oid_t database_oid,
                                    const std::filesystem::path& otbx_path);

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        actor_zeta::scheduler_raw scheduler_disk_;
        run_fn_t run_fn_;
        std::mutex mutex_;

        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        // dec 33/38 selective broadcast — base_spaces wires this before
        // scheduler.start via set_manager_dispatcher_sync. Version B* Step
        // 8.4.D / 8.11: kept only so the manager can fan the dispatcher
        // address out to every agent during bootstrap (each agent emits its
        // own on_subscriber_empty(DISK_KIND) ack when its dropped_storages_
        // slice drains; the manager itself no longer acks or mirrors).
        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};
        log_t log_;
        configuration::config_disk config_;
        // Version B* (Pass 9 USER DECISION) storages_ migration — COMPLETE.
        //
        // Terminal state (post Step 8.11 wrap + Step 8.12 final polish,
        // 2026-05-31): manager_disk_t::storages_ has been DELETED. Every
        // IN_MEMORY twin and DISK SFBM lives in the routed agent's slice;
        // the manager is a pure router. has_storage / total_rows_sync /
        // checkpoint_wal_id_sync delegate directly to
        // agents_[pool_idx_for_oid]. checkpoint_all uses agent->
        // has_in_memory_inner_sync() to preserve the WAL-seal suppression
        // semantic that the pre-cutover body derived from manager.storages_.
        // get_storage(oid) is DELETED; manager-side dead fallback bodies
        // (direct_*_sync, storage_fetch, storage_scan_segment,
        // storage_revert_append, storage_publish_*) collapsed to pure
        // routers.
        //
        // Ownership shape:
        //   - agent_disk_0 (CATALOG): pg_* system tables (pg_class,
        //     pg_attribute, pg_type, pg_proc, pg_namespace, pg_constraint,
        //     pg_index, pg_database, pg_settings, pg_computed_column),
        //     oid_gen_, stored_catalog_, file_wal_id_.
        //   - agents_[1..N-1] (USER_POOL): user tables hash-routed by
        //     table_oid % (N-1).
        //
        // Routing helper: pool_idx_for_oid(oid_t, pool_size) — see below.
        //   Returns 0 for catalog OIDs, 1+(oid % (N-1)) for user OIDs.
        std::pmr::vector<agent_disk_ptr> agents_{resource_};
        components::catalog::oid_generator oid_gen_;
        components::catalog::session_catalog_t stored_catalog_;

        // Block C §3.5 dec 33 V1 deferred DROP TABLE GC.
        //
        // Version B* Step 8.11: the manager-side `dropped_storages_` mirror
        // has been DELETED. The per-agent dropped_storages_ slices (owned by
        // each agent_disk_t — see agent_disk.hpp) are the CANONICAL and SOLE
        // owner of GC state. Writers (register_dropped_storage_sync /
        // mark_storage_dropped) are pure routers that forward into the
        // routed agent slice; the per-agent on_horizon_advanced_inner sweep
        // physically removes .otbx + sidecars and emits
        // on_subscriber_empty(DISK_KIND) directly. DO NOT reintroduce a
        // manager-side mirror — soak validation confirmed no reader remains.
        // (`dropped_storage_entry_t` is defined at namespace scope above so
        // agent_disk_t owns slices of the same type.)

        // Step 8.12 (2026-05-31): `get_storage(oid)` has been DELETED along
        // with `storages_`. Every storage access now probes the routed agent
        // slice (`agents_[pool_idx_for_oid(oid)]->storage_entry_sync(oid)`
        // for sync paths; agent storage_*_inner mailbox handlers otherwise).
        void create_agent(int count_agents);
        auto agent() -> actor_zeta::address_t;

        // Version B* (Pass 9): hash-route by table_oid.
        // - catalog tables (oid < FIRST_USER_OID) → agent 0 (catalog co-located)
        // - user tables → agents_[1..N-1] by oid modulo (N-1).
        // Currently a stub: returns 0 always until agents_ pool is bootstrapped.
        static constexpr std::size_t pool_idx_for_oid(components::catalog::oid_t oid,
                                                      std::size_t pool_size) noexcept;

        actor_zeta::behavior_t current_behavior_;
    };

    template<typename ReturnType, typename... Args>
    requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>)
        ReturnType manager_disk_t::enqueue_impl(actor_zeta::actor::address_t sender,
                                                actor_zeta::mailbox::message_id cmd,
                                                Args&&... args) {
        using R = typename actor_zeta::type_traits::is_unique_future<ReturnType>::value_type;

        auto [msg, future] =
            actor_zeta::detail::make_message<R>(resource(), std::move(sender), cmd, std::forward<Args>(args)...);

        std::lock_guard<std::mutex> guard(mutex_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return std::move(future);
    }

} //namespace services::disk