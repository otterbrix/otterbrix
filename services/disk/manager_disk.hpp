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
#include <components/catalog/results/ddl_result.hpp>
#include <components/catalog/results/resolve_result.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <core/executor.hpp>
#include <set>
#include <unordered_set>
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
        /// constructed from the current one + col. Retained as a primitive for tests and
        /// future in-memory-sync paths; the SQL ALTER TABLE ADD COLUMN flow no longer calls
        /// it (resolve_table reads columns from pg_attribute on every lookup).
        void add_column(components::table::column_definition_t& col);

        /// Phase 7.5b physical column compaction. Drops the column whose name matches
        /// `attname` from the IN_MEMORY data_table_t, reclaiming its physical storage.
        /// Implemented via the data_table_t(parent, removed_column) rebuild constructor —
        /// row_groups are rebuilt without the dropped column (collection_t::remove_column
        /// per-segment). Used by VACUUM after pg_computed_column GC: columns that no
        /// longer have any live attrefcount>0 row are physically dead and can be reclaimed.
        ///
        /// No-op for DISK-backed storages (out of scope for Phase 7.5b — would require
        /// segment rewrites + checkpoint coordination). No-op if the column is missing.
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
        bool has_storage(components::catalog::oid_t table_oid) const noexcept {
            return storages_.find(table_oid) != storages_.end();
        }
        // Sync row-count probe used by base_spaces WAL replay to avoid duplicating already-
        // checkpointed records. Returns 0 for unknown OIDs.
        uint64_t total_rows_sync(components::catalog::oid_t table_oid) const noexcept {
            auto it = storages_.find(table_oid);
            if (it == storages_.end()) return 0;
            return it->second->table_storage.table().calculate_size();
        }
        // Returns the persisted checkpoint_wal_id for `table_oid` (0 if never checkpointed or unknown).
        wal::id_t checkpoint_wal_id_sync(components::catalog::oid_t table_oid) const noexcept {
            auto it = storages_.find(table_oid);
            if (it == storages_.end()) return wal::id_t{0};
            return it->second->table_storage.checkpoint_wal_id();
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
        // Phase 11.C: walk config_.path looking for user-table .otbx files
        // (${db_oid}/${tbl_oid}/table.otbx where tbl_oid >= FIRST_USER_OID) and
        // load each into storages_ via load_storage_disk_sync. Called by
        // base_spaces after bootstrap_system_tables_sync so that subsequent
        // WAL replay can (1) read each user table's checkpoint_wal_id sidecar
        // for filtering and (2) avoid synthesising phantom storages with
        // possibly-wrong schemas from a single WAL chunk.
        void load_user_table_storages_sync();
        // Phase 11.C: synchronous scan of pg_class.oid column, returning the set
        // of user-table OIDs (oid >= FIRST_USER_OID) currently alive in the
        // catalog. Called by base_spaces between system-record replay and
        // user-record replay so user WAL records targeting a dropped table
        // (whose .otbx and pg_class row are gone) are skipped instead of
        // resurrecting a phantom storage.
        std::unordered_set<components::catalog::oid_t> alive_user_oids_sync() const;
        // After load, scan pg_class/pg_attribute/pg_type/pg_proc/pg_constraint and seed
        // oid_gen_ to max(oid)+1 so future allocate() never collides with on-disk OIDs.
        // Scans pg_class/pg_attribute/pg_type/pg_proc/pg_constraint/pg_index for the max
        // OID across all system tables, then seeds oid_gen_ to max+1 so future allocate()
        // never collides with on-disk OIDs.
        void restore_oid_generator_sync();

        // Public accessor — ddl_* methods take their OIDs from this generator.
        components::catalog::oid_generator& oid_gen() noexcept { return oid_gen_; }

        // Per-item resolve methods. Каждый метод сканирует соответствующую pg_*-таблицу
        // на disk actor thread и возвращает найденный объект (или {found=false}).
        // Параметр since_version сохранён для совместимости с message dispatch (всегда
        // игнорируется — версионирование больше не используется).
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

        // Allocate a batch of fresh OIDs from the disk-local oid_gen_. Called by the
        // dispatcher before invoking planner_t::create_plan for DDL statements, so that
        // the planner can build pg_class / pg_attribute rows without needing async access
        // to the disk actor. Wasted OIDs (plan rejected before execution) are acceptable —
        // same trade-off as PostgreSQL's pre-allocation approach.
        unique_future<std::vector<components::catalog::oid_t>>
        allocate_oids_batch(std::size_t count);

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

        // Phase 7.5b physical column compaction. For an IN_MEMORY relkind='g' storage,
        // drop every physical column whose name is NOT in `live_attnames`. Called by
        // operator_vacuum_t step 5b after pg_computed_column GC: columns whose
        // attrefcount<=0 rows have been deleted are physically dead and can be
        // reclaimed. Returns the number of columns physically dropped (0 if storage
        // is DISK-mode, missing, or already compact). Out of scope: DISK-backed
        // storages — that would need segment rewrites + checkpoint coordination.
        unique_future<std::uint64_t>
        compact_relkind_g_storage(execution_context_t ctx,
                                   components::catalog::oid_t table_oid,
                                   std::set<std::string> live_attnames);

        // ALTER TABLE ADD COLUMN owned by operator_alter_column_add_t; computed
        // tables maintained via operator_computed_field_register_t.

        // Synchronous direct replay methods for physical WAL (before schedulers start).
        uint64_t direct_append_sync(components::catalog::oid_t table_oid, components::vector::data_chunk_t& data);
        uint64_t direct_append_sync(components::catalog::oid_t table_oid,
                                    components::vector::data_chunk_t& data,
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
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session,
                              components::catalog::oid_t table_oid,
                              int64_t start,
                              uint64_t count);
        unique_future<std::pair<uint64_t, uint64_t>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::vector::data_chunk_t> data);

        unique_future<std::pair<int64_t, uint64_t>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       components::vector::vector_t row_ids,
                       std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t>
        storage_delete_rows(execution_context_t ctx,
                             components::catalog::oid_t table_oid,
                             components::vector::vector_t row_ids,
                             uint64_t count);
        // MVCC commit/revert
        unique_future<void>
        storage_commit_append(execution_context_t ctx,
                               components::catalog::oid_t table_oid,
                               uint64_t commit_id,
                               int64_t row_start,
                               uint64_t count);
        unique_future<void> storage_revert_append(execution_context_t ctx,
                                                   components::catalog::oid_t table_oid,
                                                   int64_t row_start,
                                                   uint64_t count);
        unique_future<void> storage_commit_delete(execution_context_t ctx,
                                                   components::catalog::oid_t table_oid,
                                                   uint64_t commit_id);

        // Phase 5b: batched MVCC swap. Each range carries its own table_oid.
        unique_future<void>
        storage_commit_appends(execution_context_t ctx,
                               uint64_t commit_id,
                               std::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void>
        storage_commit_deletes(execution_context_t ctx,
                               uint64_t commit_id,
                               std::set<components::catalog::oid_t> tables);

        unique_future<void>
        storage_revert_appends(execution_context_t ctx,
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
                                                       &manager_disk_t::storage_fetch,
                                                       &manager_disk_t::storage_scan_segment,
                                                       &manager_disk_t::storage_append,
                                                       &manager_disk_t::storage_update,
                                                       &manager_disk_t::storage_delete_rows,
                                                       // MVCC commit/revert
                                                       &manager_disk_t::storage_commit_append,
                                                       &manager_disk_t::storage_revert_append,
                                                       &manager_disk_t::storage_commit_delete,
                                                       &manager_disk_t::storage_commit_appends,
                                                       &manager_disk_t::storage_commit_deletes,
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
                                                       &manager_disk_t::scan_by_key,
                                                       &manager_disk_t::read_rows_by_key,
                                                       &manager_disk_t::compact_relkind_g_storage>;

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
        log_t log_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        components::catalog::oid_generator oid_gen_;

        // Storage entries per collection
        struct collection_storage_entry_t {
            table_storage_t table_storage;
            std::unique_ptr<components::storage::storage_t> storage;
            // Phase 11.C: the actual on-disk path for DISK-mode tables. Empty for
            // IN_MEMORY entries. Used by checkpoint_all (sidecar lands next to .otbx)
            // and drop_storage (physical file removal). Before this field existed,
            // checkpoint_all synthesised a path under `main_database/<oid>/` which
            // was correct only for system tables — user tables silently lost their
            // sidecar to a non-existent directory, breaking restart filtering.
            std::filesystem::path otbx_path;

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
                                       const std::filesystem::path& otbx_path_in)
                : table_storage(resource, std::move(columns), otbx_path_in)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                         resource))
                , otbx_path(otbx_path_in) {}

            /// Disk: load existing table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource, const std::filesystem::path& otbx_path_in)
                : table_storage(resource, otbx_path_in)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                         resource))
                , otbx_path(otbx_path_in) {}

            /// Update live in-memory schema: add new column to table_ and recreate the storage adapter.
            void add_column(components::table::column_definition_t& col, std::pmr::memory_resource* res) {
                table_storage.add_column(col);
                storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(), res);
            }

            /// Phase 7.5b physical column compaction: drop column from in-memory table_ and
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
        std::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>>
            storages_;

        components::storage::storage_t* get_storage(components::catalog::oid_t table_oid);

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