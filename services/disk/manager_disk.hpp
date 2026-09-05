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
#include <condition_variable>
#include <core/executor.hpp>
#include <limits>
#include <list>
#include <mutex>
#include <optional>
#include <services/wal/base.hpp>
#include <set>
#include <thread>
#include <unordered_map>
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

        /// Disk mode: load existing table.otbx.
        ///
        /// A7.6: `catalog_columns` is the schema overlay for a NEVER-CHECKPOINTED file — one
        /// whose header carries meta_block == INVALID_INDEX and whose youth the block manager
        /// has proven (the file is exactly BLOCK_START bytes; see load_existing_database).
        /// Such a file holds no serialized schema of its own, so the catalog's is the only
        /// one there is: the table constructs as legitimately EMPTY with these columns. For a
        /// checkpointed file the parameter is ignored — the file's own schema is
        /// authoritative. A proven-young file with an EMPTY overlay is a construction error
        /// (rule 6: the catalog must know a disk table's columns; guessing a 0-column schema
        /// would make every INSERT fail later and further away) — UNLESS the caller passes
        /// `allow_schemaless`, the B1a contract for computed (relkind='g') tables whose
        /// catalog schema is legitimately empty: their columns are adopted from appended
        /// chunks and serialized by the first checkpoint, so a young computed file opens as
        /// an empty schema-less table.
        table_storage_t(std::pmr::memory_resource* resource,
                        const std::filesystem::path& otbx_path,
                        std::vector<components::table::column_definition_t> catalog_columns,
                        bool allow_schemaless = false);

        components::table::data_table_t& table() { return *table_; }
        storage_mode_t mode() const { return mode_; }

        /// A7.6: true when the load ctor opened a proven-young .otbx (no checkpointed content;
        /// schema overlaid from the catalog). Used by the manager to cross-check the `.wal_id`
        /// sidecar in the REFUSING direction: a sidecar claiming a checkpoint over a young
        /// file is a contradiction, never grounds to guess.
        [[nodiscard]] bool never_checkpointed() const noexcept { return never_checkpointed_; }

        // DISK ctors do file I/O (create/open/header read) + metadata-chain deserialize, all of which can
        // fail with io_error/data_corruption. A constructor cannot return a result_wrapper_t and MUST NOT
        // throw -- the DISK ctors run on the agent thread via bootstrap_create_disk_inner_sync (noexcept),
        // so a throw would std::terminate. Instead the ctor records the error here; the caller
        // (bootstrap_*_inner_sync / the manager probe) checks construction_failed(), drops the entry and
        // reports the refusal loudly (A7.5: the file is left byte-identical — no external backup recovery
        // exists; the two-slot root inside the .otbx is the only recovery mechanism). IN_MEMORY ctors
        // never set this.
        bool construction_failed() const noexcept { return construction_error_.contains_error(); }
        [[nodiscard]] const core::error_t& construction_error() const noexcept { return construction_error_; }

        /// Has the underlying block manager latched a failure it cannot recover from? Both of
        /// its latches (a write/fsync that never reached the device; a free list proven
        /// corrupt) are STICKY by design, and both make write_header refuse to commit -- which
        /// means the manager never promotes its pending free pool again. A caller that keeps
        /// compacting such a table pays a full extra copy of it every single round, for the
        /// life of the process; agent_disk_t::checkpoint_inner reads this and defers the entry
        /// instead. Always false for IN_MEMORY. Nothing is sealed away from a degraded table:
        /// the deferral feeds its unchanged prev_checkpoint_wal_id into the WAL floor.
        [[nodiscard]] bool storage_degraded() const noexcept;

        /// Did the LAST checkpoint attempt on this table fail? Distinct from
        /// storage_degraded(): a failed header write whose previous root still stands
        /// deliberately does NOT latch, because the retry is meant to reach the same slot
        /// again and recover a transient error. The cost of that choice is that a PERSISTENT
        /// error there is retried forever, and each retry runs compact() first — which, under
        /// the split free pool, can only SPEND space when no header commits. So the entry
        /// keeps attempting its checkpoint (transient errors still recover) but stops
        /// REBUILDING until one succeeds. Always false for IN_MEMORY.
        [[nodiscard]] bool last_checkpoint_failed() const noexcept { return last_checkpoint_failed_; }

        /// Checkpoint (disk mode only, no-op/success for in-memory).
        /// W-TORN: writes data blocks + fsync, then header + fsync (2 fsync — durability before header swap).
        /// Returns out_of_memory when a column flush pin fails in
        /// data_table_t::checkpoint; true on success (or IN_MEMORY no-op).
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint();
        /// Same as checkpoint() + tracks W-TORN per-table wal_id snapshot.
        /// prev_checkpoint_wal_id_ ← old checkpoint_wal_id_; checkpoint_wal_id_ ← new_wal_id.
        /// Propagates the checkpoint() error; on error the wal_id fields stay unchanged.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(wal::id_t new_wal_id);

        /// W-TORN: latest committed checkpoint wal_id for this DISK table (0 if never checkpointed / IN_MEMORY).
        wal::id_t checkpoint_wal_id() const noexcept { return checkpoint_wal_id_; }
        /// Used by load path to seed checkpoint_wal_id_ from sidecar before WAL replay
        /// decides which records this storage already includes.
        void set_checkpoint_wal_id(wal::id_t v) noexcept { checkpoint_wal_id_ = v; }
        /// W-TORN: previous checkpoint wal_id (the state of the superseded root, i.e. the root the
        /// two-slot header still recovers if the current round's commit is lost); 0 before first overwrite.
        /// Used by checkpoint_all to compute min(prev) for safe WAL truncation.
        wal::id_t prev_checkpoint_wal_id() const noexcept { return prev_checkpoint_wal_id_; }

        /// Add a new column to the live in-memory table. Replaces table_ with a new data_table_t
        /// constructed from the current one + col. Retained as a primitive for tests and
        /// future in-memory-sync paths; the SQL ALTER TABLE ADD COLUMN flow no longer calls
        /// it (resolve_table reads columns from pg_attribute on every lookup).
        void add_column(components::table::column_definition_t& col);

        /// Physical column compaction. Drops the column whose name matches `attname` from the
        /// live data_table_t, in BOTH storage modes. Implemented via the
        /// data_table_t(parent, removed_column) rebuild constructor — row_groups are rebuilt
        /// without the dropped column (collection_t::remove_column per-segment). Used by VACUUM
        /// after pg_computed_column GC: columns that no longer have any live attrefcount>0 row
        /// are physically dead and can be reclaimed.
        ///
        /// B3c — WHAT IS AND IS NOT DONE HERE, on a DISK-backed table. The rebuild itself runs
        /// immediately and costs nothing: it SHARES every surviving column with the successor
        /// collection and simply forgets the dropped one, so not a single block is allocated.
        /// Returning the dropped column's blocks does NOT run here, because outside a checkpoint
        /// round it could only SPEND space and never return it — A7.2's split pool drains
        /// pending_free_ into reusable_ in exactly one place, promote_durable_root, reached only
        /// once a header naming the new root is on the device. (See the long DECISION note at
        /// agent_disk_t::maybe_cleanup_inner; it is the same reasoning, measured at +2.9 MB per
        /// VACUUM call for the sibling case.) So the ids are NAMED here — the last moment they
        /// are knowable, since the rebuild destroys the column object that holds them — and
        /// released by checkpoint(), the one place that can commit the release.
        ///
        /// No-op if the column is missing.
        ///
        /// Returns true if the column was found and removed; false if it was missing.
        bool drop_column(const std::string& attname);

    private:
        /// B3c — the deferred half of drop_column, run from checkpoint() once the new root's
        /// pointer stream is on the device and immediately before the free list that the
        /// committing header will name is serialized. Frees only the blocks it can PROVE the
        /// dropped column owned alone; anything else is left to its owner. See the definition.
        void release_dropped_column_blocks();

        storage_mode_t mode_;
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        std::unique_ptr<components::table::storage::block_manager_t> block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
        // B3c: disk block ids named by a column that drop_column has already removed from
        // table_, still to be released. Filled by drop_column (the only moment they can be
        // enumerated — the rebuild destroys the column object that knows them) and drained by
        // release_dropped_column_blocks() inside the checkpoint that can commit the release.
        // Not durable, and deliberately so: a crash before that checkpoint leaves the durable
        // root still naming those blocks and the catalog tombstone still hiding the column, i.e.
        // exactly the state this branch has always shipped for a DISK-backed drop. Space is
        // leaked until something re-derives the drop; nothing is corrupt and nothing is lost.
        std::pmr::vector<uint64_t> pending_released_blocks_;
        wal::id_t checkpoint_wal_id_{0};
        wal::id_t prev_checkpoint_wal_id_{0};
        // See last_checkpoint_failed(). Cleared by a successful checkpoint, so a transient
        // failure costs exactly one un-compacted round.
        bool last_checkpoint_failed_{false};
        // Set by the DISK ctors on file/metadata failure instead of throwing (see construction_failed()).
        core::error_t construction_error_{core::error_t::no_error()};
        // A7.6: set by the DISK load ctor when the .otbx was proven young (never checkpointed)
        // and constructed empty with the catalog's schema. See never_checkpointed().
        bool never_checkpointed_{false};
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

        /// Disk: create new table.otbx. B1b: the computed (relkind='g') flag is passed
        /// EXPLICITLY by every caller — the old "zero columns ⇒ computed" inference is
        /// gone because this ctor also serves WAL-replay synthesis, where a computed
        /// table's storage is rebuilt from a WAL chunk's NON-empty column list (the
        /// inference silently dropped the flag there and the next type-variant insert
        /// was glued into the wrong column). Callers derive the flag from the fact:
        /// pg_class.relkind where the row exists (replay synthesis), the planner's own
        /// relkind derivation where it does not yet (runtime CREATE).
        collection_storage_entry_t(std::pmr::memory_resource* resource,
                                   std::vector<components::table::column_definition_t> columns,
                                   const std::filesystem::path& otbx_path_in,
                                   bool is_computed_create)
            : table_storage(resource, std::move(columns), otbx_path_in)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), resource))
            , otbx_path(otbx_path_in)
            , is_computed(is_computed_create) {}

        /// Disk: load existing table.otbx. `catalog_columns` is the A7.6 schema overlay for a
        /// proven-young (never-checkpointed) file; ignored for a checkpointed one.
        /// `is_computed_load` marks a computed (relkind='g') table: the empty catalog
        /// schema is legal for it (allow_schemaless) and the entry keeps its dynamic-schema
        /// append semantics across restarts.
        collection_storage_entry_t(std::pmr::memory_resource* resource,
                                   const std::filesystem::path& otbx_path_in,
                                   std::vector<components::table::column_definition_t> catalog_columns,
                                   bool is_computed_load = false)
            : table_storage(resource, otbx_path_in, std::move(catalog_columns), is_computed_load)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), resource))
            , otbx_path(otbx_path_in)
            , is_computed(is_computed_load) {}

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
    // standard sidecar (`.wal_id`). on_horizon_advanced iterates the
    // per-agent slice and physically removes entries whose
    // dropped_at_commit_id < new_horizon (no live snapshot can reference them).
    // Passed by-value across the actor boundary.
    struct dropped_storage_entry_t {
        components::catalog::oid_t oid;
        uint64_t dropped_at_commit_id;
        std::filesystem::path path;
        std::pmr::vector<std::filesystem::path> sidecar_paths;
    };

    // A7.5: the engine owns the `<table>.otbx.*` sidecar namespace and this build writes
    // exactly one sidecar (`.wal_id`, staged via `.wal_id.tmp`). Any other name in that
    // namespace — the whole-file backup / quarantine sidecars of builds predating shadow
    // paging included — makes the on-disk state ambiguous: returns data_corruption naming
    // the stray file, and touches NOTHING (the stray is the operator's evidence, rule 6
    // forbids guessing). no_error() when the namespace is clean. Called by
    // load_storage_disk_sync before any probe open; free-standing so tests can assert the
    // refusal's error value directly.
    [[nodiscard]] core::error_t verify_otbx_sidecars(const std::filesystem::path& otbx_path,
                                                     std::pmr::memory_resource* resource);

    // Index-bootstrap row: one entry per live pg_index row, populated by
    // scan_alive_pg_index_sync() and consumed by base_spaces to spawn
    // index_agent_disk_t actors. Non-1:1 mappings from pg_index:
    //   keys        ← indkey, a CSV of attoids resolved to attnames via pg_attribute.
    //   ready_since ← indisvalid sentinel: 1 if valid, 0 if backfill uncommitted
    //                 (base_spaces skips ready_since==0 as an unfinished build).
    //   type        ← indtype, decoded via index_type_from_indtype_code. NOT
    //                 defaulted: a pg_index row whose indtype is missing or
    //                 outside the alphabet is catalog corruption — the scan
    //                 fails LOUDLY (error log + abort) instead of guessing a
    //                 backend and handing a bitcask directory to a B+tree reader.
    // No name field: the on-disk index layout and every layer below the planner
    // are keyed by (table_oid, indexrelid); the human-readable name lives only
    // in pg_class.
    struct pg_index_row_t {
        components::catalog::oid_t oid;
        components::catalog::oid_t table_oid;
        components::logical_plan::index_type type;
        components::logical_plan::keys_base_storage_t keys;
        std::uint64_t ready_since;

        explicit pg_index_row_t(std::pmr::memory_resource* resource)
            : oid(components::catalog::INVALID_OID)
            , table_oid(components::catalog::INVALID_OID)
            , type(components::logical_plan::index_type::no_valid)
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

        manager_disk_t(std::pmr::memory_resource*,
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
        // A4 observability: does any OPEN fetch-next cursor still target `table_oid`? A live
        // cursor gates compact() on that oid, so this is how a test observes the gate going up
        // and coming back down. Same single-threaded constraint as has_storage above.
        bool has_active_scan_for_oid_sync(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return false;
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return false;
            return agents_[idx]->has_active_scan_for_oid(table_oid);
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
        // Synchronous DISK storage creation (before schedulers start): new .otbx at
        // `otbx_path`. Used by bootstrap (system tables), rehydrate, and base_spaces'
        // WAL-replay synthesis (B1a: replay synthesises DISK storages). B1b:
        // `is_computed` is the pg_class.relkind='g' fact, resolved by the caller —
        // replay synthesis reads it via relkind_for_oid_sync (pg_class is final by
        // then); bootstrap and rehydrate pass false (system tables are never
        // computed; rehydrate's scan is filtered to relkind 'r'/'m').
        void create_storage_disk_sync(components::catalog::oid_t table_oid,
                                      components::catalog::oid_t database_oid,
                                      std::vector<components::table::column_definition_t> columns,
                                      const std::filesystem::path& otbx_path,
                                      bool is_computed);
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
        // Recreate the missing .otbx for every alive user table that is present
        // in the persisted pg_class catalog but whose row storage was not loaded
        // by load_user_table_storages_sync (B1a: every table is disk-backed, so a
        // missing storage means the file was lost — a freshly created .otbx's
        // directory entry is not fsynced, so a crash can durably keep the catalog
        // row while losing the file). pg_class persists unconditionally, so on
        // reopen a CREATE TABLE IF NOT EXISTS sees the table "exists" and skips
        // storage creation, and resolve_table returns the schema, yet the disk
        // agent owns no storage at that oid — so storage_append no-ops (returns
        // 0,0) and scans see nothing. Recreates each missing storage from its
        // pg_attribute column definitions so the catalog and the storage layer
        // agree. Pre-scheduler-start, single-threaded (same window as
        // load_user_table_storages_sync). Skips relkinds without pg_attribute
        // row storage (views, sequences; computed tables are recovered by WAL
        // replay synthesis, their schema is not in pg_attribute) and any oid
        // already loaded.
        void rehydrate_missing_user_storages_sync();
        // Synchronous scan of pg_class.oid column, returning the set
        // of user-table OIDs (oid >= FIRST_USER_OID) currently alive in the
        // catalog. Called by base_spaces between system-record replay and
        // user-record replay so user WAL records targeting a dropped table
        // (whose .otbx and pg_class row are gone) are skipped instead of
        // resurrecting a phantom storage.
        std::unordered_set<components::catalog::oid_t> alive_user_oids_sync() const;
        // Resolve a single table's pg_class.relkind (single-threaded bootstrap
        // scan of pg_class cols {0=oid, 3=relkind} on agents_[0]). Returns '\0'
        // when the catalog does not (yet) know the oid — callers treat that as
        // "unknown", never as a relkind. B1a: load_storage_disk_sync uses it to
        // recognise computed (relkind='g') tables, whose catalog schema is
        // legitimately empty and whose entries keep dynamic-schema semantics.
        char relkind_for_oid_sync(components::catalog::oid_t table_oid) const;

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
        // row_ids. Single-threaded bootstrap only. Returns the storage as a batch of
        // ≤DEFAULT_VECTOR_CAPACITY chunks (empty when the oid is unknown or its storage
        // is empty).
        std::pmr::vector<components::vector::data_chunk_t>
        scan_storage_for_rebuild_sync(components::catalog::oid_t table_oid, std::pmr::memory_resource* resource) const;

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

        // Scan the persisted catalog for the maximum MVCC commit-id, so reopen can
        // re-seed the dispatcher's commit clock (transaction_manager_t::seed_commit_clock).
        // The authoritative source is pg_attribute columns added_at_commit_id (index 10)
        // and dropped_at_commit_id (index 11) — the only commit-id columns in the whole
        // catalog schema. Returns the max non-null int64 across both columns (0 if none).
        // Pre-scheduler-start, single-threaded — mirrors restore_oid_generator_sync.
        std::uint64_t max_persisted_commit_id_sync() const;

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
        // every row sharing the name. Admin-scope (register/drop UDF); may return an empty vector.
        unique_future<std::pmr::vector<resolve_function_result_t>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);

        // Bookkeeping lookup (NOT query-time cast resolution — that is the in-memory
        // cast_registry_). Finds the pg_cast row identified by its (castsource,
        // casttarget) pair and returns the cast's own oid (col 0), or INVALID_OID if
        // absent. Admin path only: unregister-cast uses it to find the row to delete.
        unique_future<components::catalog::oid_t> find_cast_oid(execution_context_t ctx,
                                                                components::catalog::oid_t source_oid,
                                                                components::catalog::oid_t target_oid);

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
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Columnar row-data scan for ONE key-tuple: returns the txn-visible rows where
        // key_col_names[j] == keys.value(j, 0) as batched data_chunk_t (each <=
        // DEFAULT_VECTOR_CAPACITY rows). `keys` is a 1-row columnar carrier (column j ==
        // key_col_names[j]), so no row-major logical_value_t crosses the boundary. Thin
        // router: one read_chunks_by_key_inner message to the owning agent.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::uint64_t> key_col_indices,
                           components::vector::data_chunk_t keys,
                           std::pmr::vector<std::uint64_t> projected_cols);

        // Batched multi-key columnar row-data scan: result[i] = matched chunks for key-tuple i
        // (each <= DEFAULT_VECTOR_CAPACITY rows). `keys` is an N-row columnar carrier (column j =
        // key_col_names[j], row i = i-th key-tuple), so no row-major logical_value_t crosses the
        // boundary. All keys share `table_oid` (one owning agent), so the per-key loop runs
        // intra-agent via a single read_chunks_by_keys_inner message. result.size() ==
        // keys.size() (one possibly-empty entry per key, in input order). Thin router.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::uint64_t> key_col_indices,
                            components::vector::data_chunk_t keys,
                            std::pmr::vector<std::uint64_t> projected_cols);

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
        uint64_t direct_append_sync(components::catalog::oid_t table_oid, components::vector::data_chunk_t& data);
        void direct_delete_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count);
        void direct_update_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                components::vector::data_chunk_t& new_data);
        // WAL-replay of a PHYSICAL_ADD_COLUMN record: re-apply each schema column to
        // the owned storage ahead of the dependent PHYSICAL_INSERT. `schema_chunk` is
        // a 0-row chunk whose columns ARE the new columns (alias-tagged types).
        // Idempotent: columns already present (by name) are skipped.
        void direct_add_column_sync(components::catalog::oid_t table_oid,
                                    const components::vector::data_chunk_t& schema_chunk);

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
        // B1b: `is_computed` marks a computed (relkind='g') table. It is derived by the
        // caller from the fact that DEFINES relkind — the CREATE TABLE operator passes the
        // planner's own derivation (planner.cpp rewrite_create_table: relkind='g' ⇔ empty
        // column list, applied to the same list), the matview operator passes false
        // (relkind='m'; plan-gen refuses an empty inferred column set). The pg_class row
        // does not exist yet at storage-create time, so it cannot be scanned here.
        unique_future<void> create_storage_disk(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t database_oid,
                                                std::vector<components::table::column_definition_t> columns,
                                                bool is_computed);
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

        // Storage data operations.
        // Streaming fetch-next scan source (STEP 3 / phase B). Transparent router:
        // pool_idx_for_oid -> owning agent's storage_fetch_next_batch_inner, forwarding
        // the reply (batch + minted/advanced cursor_id) unchanged. The agent holds the
        // LIVE per-cursor scan state; this manager only routes. cursor_id==0 OPENs,
        // non-zero ADVANCEs.
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        // Release an abandoned fetch-next cursor (A4). Transparent router to the owning
        // agent's storage_close_cursor_inner.
        unique_future<void> storage_close_cursor(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t cursor_id);
        // Aggregate-pushdown REDUCE: transparent router to the owning agent's
        // storage_reduce_inner — one reply carrying ALL final aggregated rows (see
        // disk_contract for the protocol + the single-owner invariant).
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce(session_id_t session,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::table::table_filter_t> filter,
                       std::vector<size_t> projected_cols,
                       components::table::transaction_data txn,
                       components::operators::pushed_aggregate_spec_t spec);
        // storage_fetch returns the fetched rows as a vector of ≤ DEFAULT_VECTOR_CAPACITY chunks.
        // The wrapper forwards the owning agent's buffer-pool OOM / data_corruption
        // unchanged; callers read has_error() before .value().
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols);
        // Appends every chunk in order. Appends within one txn are contiguous, so the
        // result is the single coalesced range [range_start, range_start + total_count).
        // Reply wraps (start_row, count) so a write_conflict / out_of_memory from the
        // table-layer append chain reaches operator_insert as a value.
        unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::data_chunk_t> data);

        // Updates every chunk in order; row_ids[i] are the storage row-ids for data[i]
        // (the two vectors are positionally aligned and must have equal length). Returns
        // the coalesced new-row range [range_start, range_start + total_count).
        // Reply wraps (updated, appended) so a write_conflict / out_of_memory from the
        // table-layer MVCC update reaches operator_update / fk_cascade as a value.
        unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data);
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
                                                       &manager_disk_t::storage_fetch_next_batch,
                                                       &manager_disk_t::storage_close_cursor,
                                                       &manager_disk_t::storage_reduce,
                                                       &manager_disk_t::storage_fetch,
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
                                                       &manager_disk_t::find_cast_oid,
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
        // Returns no_error() on success. Returns data_corruption/io_error — instead of throwing —
        // when the .otbx is missing, refuses to open (both header slots unusable), or sits next to
        // a stray legacy sidecar (verify_otbx_sidecars): this runs on the single-threaded
        // bootstrap/recovery path whose callers (bootstrap_system_tables_sync,
        // load_user_table_storages_sync, load_storage_for_wal_replay_sync) propagate / log the error.
        // A7.5 contract: on every refusal the file set is left byte-identical — recovery is the
        // two-slot root inside the .otbx, and there is no external backup to fall back to.
        //
        // A7.6: `catalog_columns` is the schema overlay for a never-checkpointed .otbx (see
        // table_storage_t's load ctor). System-table callers pass the builtin schema; user-table
        // callers pass {} and the columns are resolved from pg_attribute here. When the file is
        // young and no columns can be resolved yet (bootstrap walk runs before WAL replay has
        // repopulated the catalog), the load is DEFERRED — no_error, no storage — and the
        // post-replay walk picks the table up once the catalog knows it.
        [[nodiscard]] core::error_t
        load_storage_disk_sync(components::catalog::oid_t table_oid,
                               components::catalog::oid_t database_oid,
                               const std::filesystem::path& otbx_path,
                               std::vector<components::table::column_definition_t> catalog_columns);

        // A7.6: one scan of pg_attribute (agents_[0], bootstrap thread) grouping live columns of
        // every `wanted` relid into attnum-ordered column_definition_t lists. Shared by
        // rehydrate_missing_user_storages_sync and the young-.otbx schema overlay; oids with no
        // live columns are absent from the result.
        [[nodiscard]] std::unordered_map<components::catalog::oid_t,
                                         std::vector<components::table::column_definition_t>>
        collect_catalog_columns_sync(const std::unordered_set<components::catalog::oid_t>& wanted) const;

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

        // Single manager-side scan funnel over the owning agent's
        // storage_scan_inner, so there is ONE place that issues a catalog
        // scan. `filter` null = "see all rows"; `projected_cols` empty = "all
        // columns"; returns an empty batch vector when there is no owning agent.
        // txn defaults to transaction_data{} = "see all committed".
        unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        scan_table(components::catalog::oid_t table_oid,
                   std::unique_ptr<components::table::table_filter_t> filter,
                   std::vector<std::size_t> projected_cols,
                   components::table::transaction_data txn = components::table::transaction_data{});

        // Hash-route by table_oid. Catalog tables (oid < FIRST_USER_OID) → agent 0;
        // user tables hash across agents_[1..N-1].
        static constexpr std::size_t pool_idx_for_oid(components::catalog::oid_t oid, std::size_t pool_size) noexcept {
            if (pool_size == 0)
                return 0;
            if (static_cast<std::uint32_t>(oid) < components::catalog::FIRST_USER_OID)
                return 0;
            if (pool_size == 1)
                return 0;
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