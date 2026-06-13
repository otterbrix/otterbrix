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
#include <boost/lockfree/queue.hpp>
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
#include <components/logical_plan/node_create_index.hpp>
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
#include <condition_variable>
#include <limits>
#include <list>
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

    // Storage entry per collection. Namespace-scope so agent_disk_t can own a
    // `unordered_map<oid_t, unique_ptr<collection_storage_entry_t>>` slice.
    // Ownership migrates across actors by rvalue unique_ptr move only.
    struct collection_storage_entry_t {
        table_storage_t table_storage;
        std::unique_ptr<components::storage::storage_t> storage;
        // Actual on-disk path for DISK-mode tables. Empty for IN_MEMORY entries.
        // Used by checkpoint_all (sidecar lands next to .otbx) and
        // drop_storage_one_local (physical file removal).
        std::filesystem::path otbx_path;
        // Computing (relkind='g', dynamic-schema) table, created schema-less. Only
        // these may hold several columns with the same name but different types
        // (multi-type fields); regular tables coerce.
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

    // Deferred DROP TABLE GC entry: file path + commit_id of the DROP plus the
    // standard sidecars (`.wal_id`, `.prev`). on_horizon_advanced iterates the
    // per-agent slice and physically removes entries whose
    // dropped_at_commit_id < new_horizon (no live snapshot can reference them).
    // Passed by-value across the actor boundary.
    struct dropped_storage_entry_t {
        components::catalog::oid_t oid;
        uint64_t dropped_at_commit_id;
        std::filesystem::path path;
        std::pmr::vector<std::filesystem::path> sidecar_paths;
    };

    // Index-bootstrap row: one entry per live pg_index row, populated by
    // scan_alive_pg_index_sync() and consumed by base_spaces to spawn
    // index_agent_disk_t actors. Non-1:1 mappings from pg_index:
    //   keys        ← indkey, a CSV of attoids resolved to attnames via pg_attribute.
    //   ready_since ← indisvalid sentinel: 1 if valid, 0 if backfill uncommitted
    //                 (base_spaces skips ready_since==0 as an unfinished build).
    //   name        ← pg_class.relname for indexrelid (no pg_index column).
    //   type        — pg_index has no indtype column; defaults to index_type::single,
    //                 which base_spaces uses to pick the on-disk extension
    //                 (.bitcask vs .btree) when reconstructing the agent.
    struct pg_index_row_t {
        components::catalog::oid_t oid;
        components::catalog::oid_t table_oid;
        std::pmr::string name;
        components::logical_plan::index_type type;
        components::logical_plan::keys_base_storage_t keys;
        std::uint64_t ready_since;

        explicit pg_index_row_t(std::pmr::memory_resource* resource)
            : oid(components::catalog::INVALID_OID)
            , table_oid(components::catalog::INVALID_OID)
            , name(resource)
            , type(components::logical_plan::index_type::single)
            , keys(resource)
            , ready_since(0) {}
    };

    class manager_disk_t final : public actor_zeta::actor::actor_mixin<manager_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Bootstrap address bundle for sync() (plain named struct — no std::tuple,
        // mirrors services::wal::wal_sync_pack_t). Carries the WAL manager's address
        // so the disk manager can address it after spawn.
        struct disk_sync_pack_t {
            actor_zeta::address_t wal = actor_zeta::address_t::empty_address();
        };

        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };

        manager_disk_t(
            std::pmr::memory_resource*,
            actor_zeta::scheduler_raw scheduler,
            actor_zeta::scheduler_raw scheduler_disk,
            configuration::config_disk config,
            log_t& log);
        ~manager_disk_t();

        // True if a storage entry is registered for `table_oid` (used by WAL replay to lazily
        // create in-memory storages on the first PHYSICAL_INSERT for tables without an .otbx).
        // The sync probe into the agent slice is only safe single-threaded: callers must
        // be pre-scheduler-start bootstrap or already inside the manager's mailbox lock.
        bool has_storage(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return false;
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return false;
            return agents_[idx]->has_storage_sync(table_oid);
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
        // System catalog (pg_*) bootstrap. Called from base_spaces during PHASE 1
        // before any actor is spawned. Creates the system-table .otbx files on a fresh
        // start and picks up existing ones on subsequent starts; idempotent w.r.t. the
        // resulting `storages_` map — collections are keyed by `pg_catalog.<name>`.
        void bootstrap_system_tables_sync();
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

        // Index-bootstrap helper: scan pg_class for every live user-OID whose
        // relkind is 'r' (regular table) or 'm' (materialized view). These are
        // the OIDs for which manager_index_t needs an empty engine populated
        // at startup (before any CREATE INDEX-driven rebuild can populate
        // per-index data). Called by base_spaces between
        // load_user_table_storages_sync and bootstrap_indexes_sync, pre-
        // scheduler-start (single-threaded).
        //
        // Excludes system OIDs (oid < FIRST_USER_OID) and tombstoned rows.
        // Independent of (but consistent with) alive_user_oids_sync() which
        // has no relkind filter and is used by WAL replay.
        std::pmr::vector<components::catalog::oid_t> scan_live_table_oids_sync() const;

        // Index-bootstrap helper: one pg_index_row_t per live pg_index row (see
        // that struct for field mapping). Called by base_spaces immediately after
        // scan_live_table_oids_sync to spawn per-index disk agents and register
        // them with manager_index_t.
        std::pmr::vector<pg_index_row_t> scan_alive_pg_index_sync() const;

        // Sync full-storage scan for post-bootstrap index rebuild. CHECKPOINT
        // compaction renumbers physical row_ids contiguously from 0 (see
        // data_table_t::compact), so pre-compact row_ids persisted in on-disk
        // index btrees go stale; base_spaces feeds this scan into
        // manager_index_t::bootstrap_repopulate_sync to rebuild against current
        // row_ids. Single-threaded bootstrap only. Returns a default-constructed
        // unique_ptr when the oid is unknown or its storage is empty.
        std::unique_ptr<components::vector::data_chunk_t>
        scan_storage_for_rebuild_sync(components::catalog::oid_t table_oid,
                                      std::pmr::memory_resource* resource) const;

        // Catalog scan returning (oid, delete_id) for every tombstoned pg_class
        // row. base_spaces calls it after WAL replay to rebuild the per-agent
        // dropped_storages_ slices (via register_dropped_storage_sync) so
        // on_horizon_advanced can finish GC of .otbx files left by a crash mid-DROP.
        //
        // pg_class has no dropped_at_commit_id column, so the tombstone is the
        // row-version delete_id (no public API). Returned delete_id is sentinel 1:
        // at boot lowest_active_start_time=1, so anything > 1 is already GC-eligible
        // and sentinel 1 means "GC on the first horizon advance past 1".
        std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>> scan_dropped_oids_sync();

        // Index-bootstrap alias for scan_dropped_oids_sync — identical body because
        // pg_class is the only relation whose tombstones matter for index GC.
        std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>> scan_dropped_table_oids_sync() {
            return scan_dropped_oids_sync();
        }

        // Read-only accessor for the on-disk root directory.
        // base_spaces uses this to derive dropped storage paths.
        const std::filesystem::path& path_db() const noexcept { return config_.path; }
        // Scans pg_class/pg_attribute/pg_type/pg_proc/pg_constraint/pg_index for the max
        // OID across all system tables, then seeds oid_gen_ to max+1 so future allocate()
        // never collides with on-disk OIDs.
        void restore_oid_generator_sync();

        // Read the value of a named setting from pg_settings. Returns the most recently
        // appended value for the given name, or empty string if not found.
        // Synchronous — called at startup before actor schedulers start.
        std::string read_setting_sync(std::string_view name);

        // Per-item resolve methods. Each method scans the corresponding pg_* table
        // on the disk actor thread and returns the found object (or {found=false}).
        // The since_version parameter is kept for message-dispatch compatibility
        // (always ignored — versioning is no longer used).
        unique_future<resolve_namespace_result_t>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);

        // Cross-namespace function lookup: returns ALL pg_proc rows whose proname matches
        // `name`, regardless of pronamespace. Used by the UDF admin paths (#41 Path 2/4):
        // register_udf needs to detect cross-namespace conflicts; drop_udf needs to purge
        // every row sharing the name. The single-namespace resolve_function above is the
        // hot-path query API; this one is admin-scope and may return an empty vector.
        unique_future<std::pmr::vector<resolve_function_result_t>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);

        // V4 admin-path enumerators. Bypass the per-name cache (cache is per-(name, ns_oid)
        // keyed; enumeration of "all namespaces" / "all tables in ns" cannot be served by
        // it). Used by catalog-resolve enumeration paths and the UDF namespace pick.
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

        // Batched delete_pg_catalog_rows: loops the singular inner logic per spec,
        // emitting the same WAL records as N singular calls.
        unique_future<void> delete_pg_catalog_rows_many(execution_context_t ctx,
                                                        std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Patch each backfill's pg_attribute row keyed by `attoid` (col 0): write the
        // shared `commit_id` into col 10 (added_at_commit_id) when kind==added_at, else
        // col 11 (dropped_at_commit_id). operator_alter_column_{add,drop,rename} insert
        // these rows with placeholder 0 (commit_id isn't allocated until commit);
        // operator_commit_transaction_t drains the per-txn backfill markers and
        // dispatches one batched call, after the commit_id is known but BEFORE
        // storage_publish_commits flips MVCC visibility. The rows still carry
        // insert_id == txn_id, so each is a metadata-only write nobody else can
        // observe. Emits one physical_update WAL record per backfill so replay
        // re-applies each after the matching physical_insert.
        unique_future<void>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        // Batched keyed scan: result[i] = match row_ids for key-tuple i. Keys are
        // columnar — `keys` is a data_chunk (column j = key_col_names[j], row i = i-th
        // key-tuple). All keys share `table_oid` (one owning agent), so the per-key loop
        // runs intra-agent via a single scan_by_keys_inner message.
        unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Columnar row-data scan for ONE key-tuple: returns the txn-visible rows where
        // key_col_names[j] == keys.value(j, 0) as batched data_chunk_t (each <=
        // DEFAULT_VECTOR_CAPACITY rows). `keys` is a 1-row columnar carrier (column j ==
        // key_col_names[j]), so no row-major logical_value_t crosses the boundary. Thin
        // router: one read_chunks_by_key_inner message to the owning agent.
        unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::string> key_col_names,
                           components::vector::data_chunk_t keys);

        // Batched multi-key columnar row-data scan: result[i] = matched chunks for key-tuple i
        // (each <= DEFAULT_VECTOR_CAPACITY rows). `keys` is an N-row columnar carrier (column j =
        // key_col_names[j], row i = i-th key-tuple), so no row-major logical_value_t crosses the
        // boundary. All keys share `table_oid` (one owning agent), so the per-key loop runs
        // intra-agent via a single read_chunks_by_keys_inner message. result.size() ==
        // keys.size() (one possibly-empty entry per key, in input order). Thin router.
        unique_future<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::string> key_col_names,
                            components::vector::data_chunk_t keys);

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
        void direct_delete_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count);
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

        void sync(disk_sync_pack_t pack);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        // compact_watermark (here and below): the dispatcher's visible-to-all
        // horizon (txn_compact_watermark_msg / txn_publish_msg return) handed to
        // data_table_t::compact(); any version stamp above it makes the compact
        // a no-op, and checkpoint_inner then skips that entry for the round.
        unique_future<wal::id_t>
        checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);
        unique_future<void>
        vacuum_all(session_id_t session, uint64_t lowest_active_start_time, uint64_t compact_watermark);
        // Batched GC-threshold check + compact. Routes each table_oid to its owning
        // agent's maybe_cleanup_inner with the shared compact_watermark, grouped per
        // agent and dispatched two-phase (send all, then await all).
        unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                               std::pmr::vector<components::catalog::oid_t> table_oids,
                                               uint64_t compact_watermark);

        // Event-driven GC subscriber. Manager fans out to every agent; each
        // agent's on_horizon_advanced_inner walks its OWN dropped_storages_ slice,
        // removes entries whose dropped_at_commit_id < new_horizon (no live
        // snapshot can reference them), and acks on_subscriber_empty(DISK_KIND) to
        // the dispatcher on slice drain so the selective-broadcast flag clears.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        /// Bootstrap-only helper — the crash-recovery catalog scan rebuild populates the
        /// per-agent dropped_storages_ slices through this (base_spaces, pre-scheduler-start).
        /// The RUNTIME DROP path does NOT use this: it goes mark_storage_dropped_many
        /// (mailbox) -> agent mark_storage_dropped_many_inner -> register_dropped_storage_inner_sync
        /// on the agent's own thread. NOT a mailbox handler — single-threaded callers only.
        void register_dropped_storage_sync(components::catalog::oid_t oid,
                                           uint64_t dropped_at_commit_id,
                                           std::filesystem::path path,
                                           std::pmr::vector<std::filesystem::path> sidecar_paths);

        /// Runtime DROP TABLE path — sent from operator_dynamic_cascade_delete
        /// BEFORE the drop_storage_many send, so the owning agents can still read the
        /// live storage entries to derive the .otbx path + sidecars (wal_id, prev) and
        /// record them via register_dropped_storage_inner_sync. Touches no files
        /// (drop_storage_many does the removal); the GC entry lets on_horizon_advanced
        /// reconcile leftovers and flips dispatcher disk_has_dropped_ via
        /// on_drop_resource_marked. Batched: a single cascade DROP marks every storage
        /// with the SAME dropped_at_commit_id, so we partition the oids per owning
        /// agent (pool_idx_for_oid) and fan out one mark_storage_dropped_many_inner per
        /// agent in parallel — N per-oid manager round-trips collapse to one (at most
        /// num_agents parallel sends), mirroring drop_storage_many.
        unique_future<void> mark_storage_dropped_many(session_id_t session,
                                                      std::pmr::vector<components::catalog::oid_t> table_oids,
                                                      uint64_t dropped_at_commit_id);

        /// DROP-GC value-space remap. mark_storage_dropped_many recorded the GC entry's
        /// dropped_at_commit_id in TXN-ID space (>= 2^62, the only id the cascade
        /// operator had). After the transaction commits and a real commit_id is
        /// allocated, operator_commit_transaction sends this; the manager fans out
        /// storage_dropped_committed_inner(txn_id, commit_id) to EVERY agent so the
        /// owning slice can rewrite dropped_at_commit_id into commit-id space — the
        /// value space the on_horizon_advanced sweep horizon is compared against.
        unique_future<void> storage_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        /// DROP-rollback un-mark — the abort mirror of storage_dropped_committed.
        /// mark_storage_dropped_many recorded the GC entry's dropped_at_commit_id in TXN-ID
        /// space (>= 2^62). If the transaction ABORTS instead of committing, the table
        /// must remain live, so operator_abort_transaction sends this; the manager fans
        /// out storage_drop_aborted_inner(txn_id) to EVERY agent so the owning slice can
        /// ERASE its dropped_storages_ entries whose dropped_at_commit_id == txn_id,
        /// un-marking the DROP so on_horizon_advanced never removes the .otbx.
        unique_future<void> storage_drop_aborted(session_id_t session, uint64_t txn_id);

        /// Bootstrap helper — base_spaces wires dispatcher address before
        /// scheduler.start, and the manager fans it out to every agent so
        /// per-slice on_horizon_advanced_inner can fire
        /// on_subscriber_empty(DISK_KIND) directly once its dropped_storages_
        /// slice drains (no manager-side mirror).
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
        // Batched DROP: partition the oids per owning agent (pool_idx_for_oid) and
        // fan out one drop_storage_many_inner per agent in parallel — N per-oid
        // manager round-trips collapse to one (at most num_agents parallel sends).
        // Each agent's inner is idempotent for not-owned oids. Caller MUST ensure
        // all index unregisters complete BEFORE invoking this (cross-manager order).
        unique_future<void> drop_storage_many(session_id_t session,
                                              std::pmr::vector<components::catalog::oid_t> table_oids);

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
        // Batched + projected variant: returns a vector of chunks and applies
        // index-based column projection at the storage layer.
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
        // Batched MVCC swap. Each range carries its own table_oid.
        unique_future<void> storage_publish_commits(execution_context_t ctx,
                                                   uint64_t commit_id,
                                                   std::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void> storage_publish_deletes(execution_context_t ctx,
                                                   uint64_t commit_id,
                                                   std::set<components::catalog::oid_t> tables);

        unique_future<void> storage_revert_appends(execution_context_t ctx,
                                                   std::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void> storage_revert_deletes(execution_context_t ctx,
                                                   std::vector<components::catalog::oid_t> tables);

        using dispatch_traits = actor_zeta::implements<disk_contract,
                                                       &manager_disk_t::flush,
                                                       &manager_disk_t::checkpoint_all,
                                                       &manager_disk_t::vacuum_all,
                                                       &manager_disk_t::maybe_cleanup_many,
                                                       // Storage management
                                                       &manager_disk_t::create_storage,
                                                       &manager_disk_t::create_storage_with_columns,
                                                       &manager_disk_t::create_storage_disk,
                                                       &manager_disk_t::drop_storage_many,
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
                                                       &manager_disk_t::storage_publish_commits,
                                                       &manager_disk_t::storage_publish_deletes,
                                                       &manager_disk_t::storage_revert_appends,
                                                       &manager_disk_t::storage_revert_deletes,
                                                       // resolve + invalidation pull
                                                       &manager_disk_t::resolve_namespace,
                                                       &manager_disk_t::resolve_function_by_name,
                                                       &manager_disk_t::list_namespaces,
                                                       &manager_disk_t::allocate_oids_batch,
                                                       &manager_disk_t::append_pg_catalog_row,
                                                       &manager_disk_t::delete_pg_catalog_rows,
                                                       &manager_disk_t::delete_pg_catalog_rows_many,
                                                       &manager_disk_t::update_pg_attribute_commit_id_fields,
                                                       &manager_disk_t::scan_by_keys,
                                                       &manager_disk_t::read_chunks_by_key,
                                                       &manager_disk_t::read_chunks_by_keys,
                                                       &manager_disk_t::compact_relkind_g_storage,
                                                       &manager_disk_t::on_horizon_advanced,
                                                       &manager_disk_t::mark_storage_dropped_many,
                                                       &manager_disk_t::storage_dropped_committed,
                                                       &manager_disk_t::storage_drop_aborted>;

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
        // ALL message processing happens on loop_thread_ (see ctor); mutex_/pump_cv_
        // serve only the loop's idle sleep + early wake from enqueue_impl.
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // Stores raw message* (boost::lockfree requires trivially-copyable): release()
        // on push, re-wrapped into message_ptr by the loop. Nodes are non-PMR.
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_;
        // Wakes the loop thread out of its idle sleep when a new message arrives.
        std::condition_variable pump_cv_;

        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        // Held only to fan the dispatcher address out to every agent at bootstrap;
        // the manager itself never acks or mirrors — each agent emits its own
        // on_subscriber_empty(DISK_KIND) when its dropped_storages_ slice drains.
        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};
        log_t log_;
        configuration::config_disk config_;
        // Storage ownership shape (manager has NO storages_ map — pure router):
        //   - agent_disk_0 (CATALOG): all pg_* system tables, oid_gen_,
        //     stored_catalog_, file_wal_id_.
        //   - agents_[1..N-1] (USER_POOL): user tables hash-routed by table_oid.
        // Routing via pool_idx_for_oid below.
        std::pmr::vector<agent_disk_ptr> agents_{resource_};
        components::catalog::oid_generator oid_gen_;
        components::catalog::session_catalog_t stored_catalog_;

        // The per-agent dropped_storages_ slices are the SOLE owner of GC state;
        // writers here are pure routers. DO NOT reintroduce a manager-side mirror.

        // Storage access path: sync probes go through
        // `agents_[pool_idx_for_oid(oid)]->storage_entry_sync(oid)`; all other
        // access goes through agent storage_*_inner mailbox handlers.
        void create_agent(int count_agents);
        auto agent() -> actor_zeta::address_t;

        // Hash-route by table_oid. Catalog tables (oid < FIRST_USER_OID) → agent 0;
        // user tables hash across agents_[1..N-1].
        static constexpr std::size_t pool_idx_for_oid(components::catalog::oid_t oid,
                                                      std::size_t pool_size) noexcept {
            if (pool_size == 0) return 0;
            if (static_cast<std::uint32_t>(oid) < components::catalog::FIRST_USER_OID) return 0;
            if (pool_size == 1) return 0;
            return 1 + (static_cast<std::size_t>(oid) % (pool_size - 1));
        }
    };

    template<typename ReturnType, typename... Args>
    requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>)
        ReturnType manager_disk_t::enqueue_impl(actor_zeta::actor::address_t sender,
                                                actor_zeta::mailbox::message_id cmd,
                                                Args&&... args) {
        using R = typename actor_zeta::type_traits::is_unique_future<ReturnType>::value_type;

        auto [msg, future] =
            actor_zeta::detail::make_message<R>(resource(), std::move(sender), cmd, std::forward<Args>(args)...);

        auto enqueue_status = enqueue_impl(std::move(msg));
        static_cast<void>(enqueue_status);

        return std::move(future);
    }

} //namespace services::disk