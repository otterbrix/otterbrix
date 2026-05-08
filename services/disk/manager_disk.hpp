#pragma once

#include <limits>
#include "agent_disk.hpp"
#include <components/catalog/dependency_walker.hpp>
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
#include <optional>
#include <components/configuration/configuration.hpp>
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
#include "ddl_result.hpp"
#include "invalidation_ring_buffer.hpp"
#include "resolve_result.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <core/executor.hpp>
#include <mutex>
#include <services/wal/base.hpp>
#include <thread>

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
        /// constructed from the current one + col. Used by ddl_add_column to keep loaded storages
        /// in sync with pg_attribute when ALTER TABLE ADD COLUMN runs on an already-loaded table.
        void add_column(components::table::column_definition_t& col);

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

        // True if a storage entry is registered for `name` (used by WAL replay to lazily
        // create in-memory storages on the first PHYSICAL_INSERT for tables without an .otbx).
        bool has_storage(const collection_full_name_t& name) const noexcept {
            return storages_.find(name) != storages_.end();
        }
        // Sync row-count probe used by base_spaces WAL replay to avoid duplicating already-
        // checkpointed records. Returns 0 for unknown names.
        uint64_t total_rows_sync(const collection_full_name_t& name) const noexcept {
            auto it = storages_.find(name);
            if (it == storages_.end()) return 0;
            return it->second->table_storage.table().calculate_size();
        }
        // Returns the persisted checkpoint_wal_id for `name` (0 if never checkpointed or unknown).
        // Read at startup from the .wal_id sidecar; lets WAL replay skip records already covered
        // by the .otbx state without depending on a row-count heuristic.
        wal::id_t checkpoint_wal_id_sync(const collection_full_name_t& name) const noexcept {
            auto it = storages_.find(name);
            if (it == storages_.end()) return wal::id_t{0};
            return it->second->table_storage.checkpoint_wal_id();
        }

        // Read the .otbx.wal_id sidecar directly from disk without loading the storage.
        // Falls back to the in-memory value if the storage is already loaded.
        // Returns 0 when the table has never been checkpointed or for in-memory-only tables.
        // Used by the D4-lazy WAL replay path to skip already-checkpointed records without
        // requiring restore_user_storages_sync() to pre-populate storages_.
        wal::id_t peek_checkpoint_wal_id_from_disk(const collection_full_name_t& name) const noexcept;

        // Load a user-table storage from its .otbx file on demand, without requiring a prior
        // restore_user_storages_sync(). Called by WAL replay when it encounters a record for a
        // disk-backed table that hasn't been loaded yet. No-op if storage already loaded or if
        // no .otbx exists (in-memory table, handled by WAL replay's create_storage path).
        void load_storage_for_wal_replay_sync(const collection_full_name_t& name);

        // Synchronous storage creation for initialization (before schedulers start).
        void create_storage_with_columns_sync(const collection_full_name_t& name,
                                              std::vector<components::table::column_definition_t> columns);
        // Disk storage: create new or load existing
        void create_storage_disk_sync(const collection_full_name_t& name,
                                      std::vector<components::table::column_definition_t> columns,
                                      const std::filesystem::path& otbx_path);
        void load_storage_disk_sync(const collection_full_name_t& name, const std::filesystem::path& otbx_path);

        // System catalog (pg_*) bootstrap and load. Called from base_spaces during PHASE 1
        // before any actor is spawned. bootstrap creates the 9 .otbx files for the system
        // tables; load picks them up on subsequent starts. Both are idempotent w.r.t. the
        // resulting `storages_` map — collections are keyed by `pg_catalog.<name>`.
        void bootstrap_system_tables_sync();
        void load_system_tables_sync();
        // After load, scan pg_class/pg_attribute/pg_type/pg_proc/pg_constraint and seed
        // oid_gen_ to max(oid)+1 so future allocate() never collides with on-disk OIDs.
        // Scans pg_class/pg_attribute/pg_type/pg_proc/pg_constraint/pg_index for the max
        // OID across all system tables, then seeds oid_gen_ to max+1 so future allocate()
        // never collides with on-disk OIDs.
        void restore_oid_generator_sync();

        // Restart helper: scan pg_class for user relations (relkind='r'/'g'/'c') and load each
        // collection's storage from disk so DML calls (storage_append / WAL replay) find
        // the entry in storages_ without going through resolve_table first. Idempotent.
        void restore_user_storages_sync();

        // Public accessor — ddl_* methods take their OIDs from this generator.
        components::catalog::oid_generator& oid_gen() noexcept { return oid_gen_; }

        // Catalog version monotonic counter: bumped on every successful DDL.
        // Public read accessor (single producer = the disk actor; readers see eventual values).
        std::uint64_t catalog_version() const noexcept { return catalog_version_; }

        // V4 resolve invocation counters — used by integration tests to verify cache hit/miss
        // behavior (e.g. "warm cache → 0 roundtrips"). Bumped at the start of each resolve_*
        // handler. Atomic so a test thread can read while the actor thread writes.
        struct resolve_counters_t {
            std::atomic<std::uint64_t> resolve_namespace{0};
            std::atomic<std::uint64_t> resolve_table{0};
            std::atomic<std::uint64_t> resolve_type{0};
            std::atomic<std::uint64_t> resolve_function{0};
            std::atomic<std::uint64_t> resolve_function_by_name{0};
        };
        const resolve_counters_t& resolve_counters() const noexcept { return resolve_counters_; }
        void reset_resolve_counters() noexcept {
            resolve_counters_.resolve_namespace.store(0);
            resolve_counters_.resolve_table.store(0);
            resolve_counters_.resolve_type.store(0);
            resolve_counters_.resolve_function.store(0);
            resolve_counters_.resolve_function_by_name.store(0);
        }

        // Per-item resolve methods (V4: lazy resolve from pg_catalog).
        // Each method scans the relevant pg_* tables on the disk actor thread, returns the
        // resolved object plus the invalidation tail since ctx-supplied since_version (so
        // the plan cache (M5) gets cache-key + delta in one roundtrip).
        unique_future<resolve_namespace_result_t> resolve_namespace(execution_context_t ctx,
                                                                     std::string name,
                                                                     std::uint64_t since_version);
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
        resolve_type_result_t resolve_type_sync(components::catalog::oid_t namespace_oid,
                                                 const std::string& name);
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
        resolve_function_by_name(execution_context_t ctx,
                                  std::string name,
                                  std::uint64_t since_version);

        // V4 admin-path enumerators. Bypass the per-name cache (cache is per-(name, ns_oid)
        // keyed; enumeration of "all namespaces" / "all tables in ns" cannot be served by
        // it). Used by the dispatcher's collections_ rebuild and UDF namespace pick.
        unique_future<std::pmr::vector<std::string>>
        list_namespaces(execution_context_t ctx);

        // Returns (oid, relname) pairs for all relations in the given namespace whose
        // relkind is a "live storage" type ('r' regular, 'g' computing). Other relkinds
        // (indexes, sequences, views, macros, composites) are filtered out.
        unique_future<std::pmr::vector<std::pair<components::catalog::oid_t, std::string>>>
        list_tables_in_namespace(execution_context_t ctx, components::catalog::oid_t namespace_oid);

        // Pull-based invalidation tail. The plan cache calls this to keep
        // its (plan_hash, catalog_version) keyspace consistent with on-disk truth.
        unique_future<invalidation_ring_buffer_t::snapshot_t>
        recent_invalidations_since(session_id_t session, std::uint64_t since_version);

        // Flip MVCC tags from txn_id → commit_id for every pg_catalog.* row appended
        // under ctx.txn.transaction_id. Called by dispatcher after WAL commit_txn +
        // txn_manager.commit. No-op when txn_id == 0 (no tracking happens for bootstrap).
        unique_future<void>
        commit_pg_catalog_appends(execution_context_t ctx, std::uint64_t commit_id);

        // Revert pg_catalog.* appends made under ctx.txn.transaction_id (used on DDL
        // failure path before WAL commit_txn). No-op when txn_id == 0.
        unique_future<void>
        revert_pg_catalog_appends(execution_context_t ctx);

        // Allocate a batch of fresh OIDs from the disk-local oid_gen_. Called by the
        // dispatcher before invoking planner_t::create_plan for DDL statements, so that
        // the planner can build pg_class / pg_attribute rows without needing async access
        // to the disk actor. Wasted OIDs (plan rejected before execution) are acceptable —
        // same trade-off as PostgreSQL's pre-allocation approach.
        unique_future<std::vector<components::catalog::oid_t>>
        allocate_oids_batch(std::size_t count);

        // WAL-safe append of a single pre-built row into a pg_catalog table.
        // Called by operator_primitive_write when executing planner-emitted DDL plans.
        // Semantics: WAL physical_insert + direct_append_sync (same as internal DDL methods).
        unique_future<void> append_pg_catalog_row(execution_context_t ctx,
                                                   collection_full_name_t name,
                                                   components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        // Emits WAL write_physical_delete before the MVCC tombstone so drops survive
        // crash-before-checkpoint. Called by operator_primitive_delete and execute_ddl's
        // sequence handler for planner-built drop plans.
        unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                    collection_full_name_t name,
                                                    std::int64_t oid_col_idx,
                                                    components::catalog::oid_t target_oid);

        // Pure storage scan: row_ids of txn-visible rows in `name` where
        // key_col_names[i] == key_values[i] for every i.
        unique_future<std::pmr::vector<std::int64_t>>
        scan_by_key(execution_context_t ctx,
                    collection_full_name_t name,
                    std::vector<std::string> key_col_names,
                    std::vector<components::types::logical_value_t> key_values);

        // Index lookup: first txn-visible row_id in the table indexed by index_oid
        // where indexed columns == key_values (indkey order). nullopt on no match.
        unique_future<std::optional<std::int64_t>>
        point_lookup_by_index(execution_context_t ctx,
                              components::catalog::oid_t index_oid,
                              std::vector<components::types::logical_value_t> key_values);

        // Full row-data scan: returns all column values for every txn-visible row
        // where key_col_names[i] == key_values[i]. Same filter as scan_by_key but
        // returns complete row data instead of row_ids.
        unique_future<std::vector<std::vector<components::types::logical_value_t>>>
        read_rows_by_key(execution_context_t ctx,
                          collection_full_name_t name,
                          std::vector<std::string> key_col_names,
                          std::vector<components::types::logical_value_t> key_values);

        // OID-keyed scan: resolves table_oid → collection internally, then scans.
        // Returns row_ids of matching txn-visible rows. Empty on unknown OID.
        unique_future<std::pmr::vector<std::int64_t>>
        scan_by_table_oid(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::vector<std::string> key_col_names,
                           std::vector<components::types::logical_value_t> key_values);

        unique_future<ddl_result_t> ddl_adopt_computing_schema(execution_context_t ctx,
                                                                components::catalog::oid_t table_oid,
                                                                std::vector<components::table::column_definition_t> columns);

        // Computing tables (relkind='g') hold versioned, ref-counted fields in
        // pg_computed_column rather than fixed pg_attribute rows. Lifecycle:
    //   ddl_computed_append        → INSERT new (attversion=max+1, attrefcount=1)
        //                                 OR bump attrefcount when an identical (name, type)
        //                                 row is already live
        //   ddl_computed_drop          → decrement attrefcount; delete row when refcount==0
        // ddl_adopt_computing_schema turns a computing table back into a regular one
        // (relkind 'g' → 'r') by promoting the latest types into pg_attribute.
        unique_future<ddl_result_t> ddl_computed_append(execution_context_t ctx,
                                                         components::catalog::oid_t table_oid,
                                                         std::string field_name,
                                                         components::catalog::oid_t type_oid);
        unique_future<ddl_result_t> ddl_computed_drop(execution_context_t ctx,
                                                       components::catalog::oid_t table_oid,
                                                       std::string field_name);

        // Column lifecycle DDL — pg_attribute mutations under MVCC.
        // attnum is never reused. ddl_add_column allocates next_column_oid via the table's
        // metadata counter.
        unique_future<ddl_result_t> ddl_add_column(execution_context_t ctx,
                                                    components::catalog::oid_t table_oid,
                                                    components::table::column_definition_t column);

        // Synchronous direct replay methods for physical WAL (before schedulers start).
        // The txn overload takes an explicit transaction_data: pass {0, 0} for
        // committed-at-txn=0 semantics, or an active transaction for MVCC-aware appends.
        // Returns the row offset where the append began (for later commit_append).
        uint64_t direct_append_sync(const collection_full_name_t& name, components::vector::data_chunk_t& data);
        uint64_t direct_append_sync(const collection_full_name_t& name,
                                    components::vector::data_chunk_t& data,
                                    const components::table::transaction_data& txn);
        void direct_delete_sync(const collection_full_name_t& name,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count);
        void direct_delete_sync(const collection_full_name_t& name,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count,
                                const components::table::transaction_data& txn);
        void direct_update_sync(const collection_full_name_t& name,
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

        // Storage management
        unique_future<void> create_storage(session_id_t session, collection_full_name_t name);
        unique_future<void> create_storage_with_columns(session_id_t session,
                                                        collection_full_name_t name,
                                                        std::vector<components::table::column_definition_t> columns);
        unique_future<void> create_storage_disk(session_id_t session,
                                                collection_full_name_t name,
                                                std::vector<components::table::column_definition_t> columns);
        unique_future<void> drop_storage(session_id_t session, collection_full_name_t name);

        // Storage queries
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, collection_full_name_t name);
        unique_future<uint64_t> storage_total_rows(session_id_t session, collection_full_name_t name);
        unique_future<uint64_t> storage_calculate_size(session_id_t session, collection_full_name_t name);

        // Storage data operations
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     collection_full_name_t name,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      collection_full_name_t name,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session, collection_full_name_t name, int64_t start, uint64_t count);
        unique_future<std::pair<uint64_t, uint64_t>>
        storage_append(execution_context_t ctx, std::unique_ptr<components::vector::data_chunk_t> data);

        unique_future<std::pair<int64_t, uint64_t>>
        storage_update(execution_context_t ctx,
                       components::vector::vector_t row_ids,
                       std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t>
        storage_delete_rows(execution_context_t ctx, components::vector::vector_t row_ids, uint64_t count);
        // MVCC commit/revert
        unique_future<void>
        storage_commit_append(execution_context_t ctx, uint64_t commit_id, int64_t row_start, uint64_t count);
        unique_future<void> storage_revert_append(execution_context_t ctx, int64_t row_start, uint64_t count);
        unique_future<void> storage_commit_delete(execution_context_t ctx, uint64_t commit_id);

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
                                                       &manager_disk_t::storage_calculate_size,
                                                       // Storage data operations
                                                       &manager_disk_t::storage_scan,
                                                       &manager_disk_t::storage_fetch,
                                                       &manager_disk_t::storage_scan_segment,
                                                       &manager_disk_t::storage_append,
                                                       &manager_disk_t::storage_update,
                                                       &manager_disk_t::storage_delete_rows,
                                                       // MVCC commit/revert
                                                       &manager_disk_t::storage_commit_append,
                                                       &manager_disk_t::storage_revert_append,
                                                       &manager_disk_t::storage_commit_delete,
                                                       // DDL pipeline
                                                       &manager_disk_t::ddl_adopt_computing_schema,
                                                       &manager_disk_t::ddl_computed_append,
                                                       &manager_disk_t::ddl_computed_drop,
                                                       &manager_disk_t::ddl_add_column,
                                                       // resolve + invalidation pull
                                                       &manager_disk_t::resolve_namespace,
                                                       &manager_disk_t::resolve_table,
                                                       &manager_disk_t::resolve_type,
                                                       &manager_disk_t::resolve_function,
                                                       &manager_disk_t::resolve_function_by_name,
                                                       &manager_disk_t::list_namespaces,
                                                       &manager_disk_t::list_tables_in_namespace,
                                                       &manager_disk_t::recent_invalidations_since,
                                                       &manager_disk_t::commit_pg_catalog_appends,
                                                       &manager_disk_t::revert_pg_catalog_appends,
                                                       &manager_disk_t::allocate_oids_batch,
                                                       &manager_disk_t::append_pg_catalog_row,
                                                       &manager_disk_t::delete_pg_catalog_rows,
                                                       &manager_disk_t::scan_by_key,
                                                       &manager_disk_t::point_lookup_by_index,
                                                       &manager_disk_t::read_rows_by_key,
                                                       &manager_disk_t::scan_by_table_oid>;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        actor_zeta::scheduler_raw scheduler_disk_;
        run_fn_t run_fn_;
        std::mutex mutex_;

        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        log_t log_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        components::catalog::oid_generator oid_gen_;
        std::uint64_t catalog_version_{0};
        invalidation_ring_buffer_t invalidations_;
        mutable resolve_counters_t resolve_counters_;

        // Internal helper — pushes the result's first invalidation event into the
        // ring buffer (so M5's plan cache sees it on its next pull) and returns the result
        // unchanged. Called from every ddl_* coroutine: `co_return finalize_ddl(result);`.
        ddl_result_t finalize_ddl(ddl_result_t r) noexcept;

        // MVCC-delete every row in `name` whose column at `oid_col_idx` equals `target_oid`.
        // Issued under txn so the delete tombstones are visible only to this transaction
        // until commit. Used for both pg_class/pg_namespace/pg_type/pg_proc/pg_index drops.
        void delete_system_rows_by_oid_match(const collection_full_name_t& name,
                                              std::int64_t oid_col_idx,
                                              components::catalog::oid_t target_oid,
                                              const components::table::transaction_data& txn);

        // Return true if any row in `table_name` has oid_col == target_oid (committed rows only).
        bool pg_oid_exists(const collection_full_name_t& table_name,
                           std::uint64_t oid_col,
                           components::catalog::oid_t target_oid) const;


        // Storage entries per collection
        struct collection_storage_entry_t {
            table_storage_t table_storage;
            std::unique_ptr<components::storage::storage_t> storage;

            /// In-memory: schema-less
            explicit collection_storage_entry_t(std::pmr::memory_resource* resource)
                : table_storage(resource)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                         resource)) {}

            /// In-memory: with columns
            explicit collection_storage_entry_t(std::pmr::memory_resource* resource,
                                                std::vector<components::table::column_definition_t> columns)
                : table_storage(resource, std::move(columns))
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                         resource)) {}

            /// Disk: create new table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource,
                                       std::vector<components::table::column_definition_t> columns,
                                       const std::filesystem::path& otbx_path)
                : table_storage(resource, std::move(columns), otbx_path)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                         resource)) {}

            /// Disk: load existing table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource, const std::filesystem::path& otbx_path)
                : table_storage(resource, otbx_path)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                         resource)) {}

            /// Update live in-memory schema: add new column to table_ and recreate the storage adapter.
            void add_column(components::table::column_definition_t& col, std::pmr::memory_resource* res) {
                table_storage.add_column(col);
                storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), res);
            }
        };
        std::unordered_map<collection_full_name_t, std::unique_ptr<collection_storage_entry_t>, collection_name_hash>
            storages_;

        // Track pg_catalog.* appends made under a real (non-zero) txn so that after
        // WAL commit_txn + txn_manager.commit, we can flip MVCC tags from txn_id → commit_id
        // by calling storage_t::commit_append on each tracked range. Cleared per-txn after
        // commit_pg_catalog_appends runs.
        struct pending_pg_catalog_append_t {
            collection_full_name_t name;
            int64_t start_row{0};
            uint64_t count{0};
        };
        std::unordered_map<std::uint64_t, std::vector<pending_pg_catalog_append_t>> pending_pg_catalog_appends_;

        // O(1) namespace name ↔ OID indexes. Populated by load_system_tables_sync and every
        // ddl_create/drop_namespace call. Used by resolve_namespace and the lazy-load path
        // in resolve_table to avoid per-resolve pg_namespace scans.
        std::unordered_map<std::string, components::catalog::oid_t> ns_name_to_oid_;
        std::unordered_map<components::catalog::oid_t, std::string> ns_oid_to_name_;

        // O(1) (namespace_oid, table_name) → {oid, relkind} index.
        struct table_index_entry_t {
            components::catalog::oid_t oid{components::catalog::INVALID_OID};
            char relkind{'r'};
        };
        struct ns_table_key_hash_t {
            std::size_t operator()(const std::pair<components::catalog::oid_t, std::string>& k) const noexcept {
                std::size_t h = std::hash<std::uint32_t>{}(k.first);
                h ^= std::hash<std::string>{}(k.second) + 0x9e3779b9u + (h << 6u) + (h >> 2u);
                return h;
            }
        };
        using ns_table_key_t = std::pair<components::catalog::oid_t, std::string>;
        std::unordered_map<ns_table_key_t, table_index_entry_t, ns_table_key_hash_t> table_to_oid_;
        // Reverse map: table OID → key.
        std::unordered_map<components::catalog::oid_t, ns_table_key_t> table_oid_to_key_;

        // Helper: rebuild ns and table indexes from pg_namespace/pg_class.
        // Called by load_system_tables_sync after loading .otbx files, and after WAL replay.
        void rebuild_lookup_indexes();

        components::storage::storage_t* get_storage(const collection_full_name_t& name);

        void create_agent(int count_agents);
        auto agent() -> actor_zeta::address_t;

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