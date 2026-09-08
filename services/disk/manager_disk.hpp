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

    /// Owns a table's data_table_t and the storage stack behind its table.otbx.
    class table_storage_t {
    public:
        /// Create new table.otbx
        table_storage_t(std::pmr::memory_resource* resource,
                        std::vector<components::table::column_definition_t> columns,
                        const std::filesystem::path& otbx_path);

        /// Load existing table.otbx; `catalog_columns` overlay is required unless `allow_schemaless`.
        table_storage_t(std::pmr::memory_resource* resource,
                        const std::filesystem::path& otbx_path,
                        std::vector<components::table::column_definition_t> catalog_columns,
                        bool allow_schemaless = false);

        components::table::data_table_t& table() { return *table_; }

        /// Refuses a `.wal_id` sidecar that claims a checkpoint over a never-checkpointed file.
        [[nodiscard]] bool never_checkpointed() const noexcept { return never_checkpointed_; }

        bool construction_failed() const noexcept { return construction_error_.contains_error(); }
        [[nodiscard]] const core::error_t& construction_error() const noexcept { return construction_error_; }

        /// Sticky by design, never clears: write_header then refuses to commit forever.
        [[nodiscard]] bool storage_degraded() const noexcept;

        /// Unlike storage_degraded(), does not latch — a transient error must stay retryable.
        [[nodiscard]] bool last_checkpoint_failed() const noexcept { return last_checkpoint_failed_; }

        /// Asked only on the failed-round retry path (walks every segment); over-reporting is safe.
        [[nodiscard]] bool has_pending_update_overlay();

        /// A .otbx carries no version metadata: checkpointing over a stamp above `watermark` resurrects the row.
        [[nodiscard]] bool has_versions_above(uint64_t watermark) const;

        /// W-TORN: fsyncs data then header; out_of_memory / unimplemented_yet (unfolded overlay) / true.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint();
        /// Same as checkpoint(), plus rolls prev_checkpoint_wal_id_ forward before storing new_wal_id.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(wal::id_t new_wal_id);

        /// True when this round has physical work to do; false still advances the wal-id chain + sidecar.
        /// An empty round took 205.7 ms against 124.4 ms for one that wrote (100 tables x 100 rows).
        [[nodiscard]] bool needs_checkpoint() const noexcept;

        /// Same effect as checkpoint(wal::id_t), minus the write — wal-id bookkeeping only.
        void advance_wal_id_without_rewrite(wal::id_t new_wal_id) noexcept;

        /// W-TORN: latest committed wal_id (0 = never checkpointed); valid only if checkpoint_wal_id_known().
        wal::id_t checkpoint_wal_id() const noexcept { return checkpoint_wal_id_; }
        /// Seeds checkpoint_wal_id_ from the sidecar on load, before WAL replay decides what to skip.
        void set_checkpoint_wal_id(wal::id_t v) noexcept {
            checkpoint_wal_id_ = v;
            checkpoint_wal_id_known_ = true;
        }

        /// A third state 0 can't carry: unreadable must NOT report 0, or replay re-applies absorbed records.
        [[nodiscard]] bool checkpoint_wal_id_known() const noexcept { return checkpoint_wal_id_known_; }
        void set_checkpoint_wal_id_unreadable() noexcept {
            checkpoint_wal_id_ = wal::id_t{0};
            checkpoint_wal_id_known_ = false;
        }
        /// W-TORN: previous checkpoint wal_id (0 before first overwrite); used for min(prev) WAL truncation.
        wal::id_t prev_checkpoint_wal_id() const noexcept { return prev_checkpoint_wal_id_; }

        /// Rebuilds table_, invalidating old references; kept for tests/WAL-replay, not SQL ALTER TABLE ADD COLUMN.
        void add_column(components::table::column_definition_t& col);

        /// Rebuilds without col; block release waits for checkpoint(). Outside a checkpoint round the split
        /// free pool only spends space (measured +2.9 MB per VACUUM at agent_disk_t::maybe_cleanup_inner).
        bool drop_column(const std::string& attname);

        /// Storage half of ALTER TABLE RENAME COLUMN: in-memory only until the next checkpoint (a
        /// crash reloads the OLD name); closed by comparing attoid, not name, in rearm_dropped_column_blocks_sync.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_attname,
                                                                 const std::string& new_attname);

    private:
        /// Deferred half of drop_column, in checkpoint() before the free list serializes; PROVEN-orphan blocks only.
        void release_dropped_column_blocks();

        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        std::unique_ptr<components::table::storage::block_manager_t> block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
        // Blocks drop_column removed but not yet released; not durable, so a crash just leaks space.
        std::pmr::vector<uint64_t> pending_released_blocks_;
        wal::id_t checkpoint_wal_id_{0};
        bool checkpoint_wal_id_known_{true};
        wal::id_t prev_checkpoint_wal_id_{0};
        bool last_checkpoint_failed_{false};
        core::error_t construction_error_{core::error_t::no_error()};
        bool never_checkpointed_{false};
#ifdef DEV_MODE
        // DEV_MODE safety net: fingerprint of the durable root, checked whenever needs_checkpoint() answers false.
        struct clean_fingerprint_t {
            uint64_t total_rows = 0;
            uint64_t committed_rows = 0;
            uint64_t column_count = 0;
        };
        clean_fingerprint_t clean_fingerprint_{};
        void capture_clean_fingerprint() noexcept;
#endif
    };

    // Namespace-scope (not nested) so agent_disk_t can own a map of these; ownership moves only by unique_ptr.
    struct collection_storage_entry_t {
        table_storage_t table_storage;
        // Declared BEFORE `storage`: every adapter built below borrows it (see note_column_identity).
        std::vector<components::table::column_definition_t> unmaterialized_columns;
        std::unique_ptr<components::storage::storage_t> storage;
        std::filesystem::path otbx_path;
        bool is_computed = false;

        /// Create new table.otbx; `is_computed_create` is explicit rather than inferred (WAL-replay would misfile it).
        collection_storage_entry_t(std::pmr::memory_resource* resource,
                                   std::vector<components::table::column_definition_t> columns,
                                   const std::filesystem::path& otbx_path_in,
                                   bool is_computed_create)
            : table_storage(resource, std::move(columns), otbx_path_in)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     resource,
                                                                                     &unmaterialized_columns))
            , otbx_path(otbx_path_in)
            , is_computed(is_computed_create) {}

        /// Load existing table.otbx; `catalog_columns` overlays a never-checkpointed schema, ignored otherwise.
        collection_storage_entry_t(std::pmr::memory_resource* resource,
                                   const std::filesystem::path& otbx_path_in,
                                   std::vector<components::table::column_definition_t> catalog_columns,
                                   bool is_computed_load = false)
            : table_storage(resource, otbx_path_in, std::move(catalog_columns), is_computed_load)
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     resource,
                                                                                     &unmaterialized_columns))
            , otbx_path(otbx_path_in)
            , is_computed(is_computed_load) {}

        void add_column(components::table::column_definition_t& col, std::pmr::memory_resource* res) {
            table_storage.add_column(col);
            drop_unmaterialized(col.name());
            storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     res,
                                                                                     &unmaterialized_columns);
        }

        /// Recreates the storage adapter too — its data_table_t& would dangle after the rebuild.
        bool drop_column(const std::string& attname, std::pmr::memory_resource* res) {
            if (!table_storage.drop_column(attname)) {
                return false;
            }
            storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     res,
                                                                                     &unmaterialized_columns);
            return true;
        }

        /// Unlike drop_column, does NOT recreate the adapter — a rename mutates table_ in place.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_attname,
                                                                 const std::string& new_attname) {
            return table_storage.rename_column(old_attname, new_attname);
        }

        void note_column_identity(std::string attname,
                                  std::uint32_t attoid,
                                  const components::types::complex_logical_type& type,
                                  const std::optional<components::types::logical_value_t>& default_value = {}) {
            if (attname.empty() || attoid == 0) {
                return;
            }
            for (const auto& column : table_storage.table().columns()) {
                if (column.name() == attname) {
                    return;
                }
            }
            for (auto& p : unmaterialized_columns) {
                if (p.name() == attname) {
                    if (p.attoid() == 0) {
                        p.set_attoid(attoid);
                    }
                    if (!p.has_default_value() && default_value.has_value()) {
                        p.set_default_value(default_value);
                    }
                    return;
                }
            }
            components::table::column_definition_t def(std::move(attname), type);
            def.set_attoid(attoid);
            def.set_default_value(default_value);
            unmaterialized_columns.push_back(std::move(def));
        }

        // Unlike take_column_identity, does NOT consume the entry — caller reads DEFAULT before materialising.
        [[nodiscard]] const components::table::column_definition_t*
        find_unmaterialized(const std::string& attname) const noexcept {
            for (const auto& p : unmaterialized_columns) {
                if (p.name() == attname) {
                    return &p;
                }
            }
            return nullptr;
        }

        // 0 = nothing published for this name. Consumes the entry, so the adapter stops answering NULLs.
        std::uint32_t take_column_identity(const std::string& attname) {
            for (auto it = unmaterialized_columns.begin(); it != unmaterialized_columns.end(); ++it) {
                if (it->name() == attname) {
                    const auto attoid = it->attoid();
                    unmaterialized_columns.erase(it);
                    return attoid;
                }
            }
            return 0;
        }

        void drop_unmaterialized(const std::string& attname) {
            for (auto it = unmaterialized_columns.begin(); it != unmaterialized_columns.end(); ++it) {
                if (it->name() == attname) {
                    unmaterialized_columns.erase(it);
                    return;
                }
            }
        }

        void adopt_catalog_columns(const std::vector<components::table::column_definition_t>& catalog_columns) {
            for (const auto& def : catalog_columns) {
                if (def.attoid() == 0) {
                    continue;
                }
                bool in_storage = false;
                for (const auto& column : table_storage.table().columns()) {
                    if (column.attoid() == def.attoid() || column.name() == def.name()) {
                        in_storage = true;
                        break;
                    }
                }
                if (!in_storage) {
                    // Must decode attdefspec on the manager's resource, not the scan arena, or the default dangles.
                    note_column_identity(def.name(), def.attoid(), def.type(), def.default_value_opt());
                }
            }
        }
    };

    struct dropped_class_row_t {
        components::catalog::oid_t oid;
        components::catalog::oid_t namespace_oid;
        std::uint64_t delete_id;
    };

    struct dropped_storage_entry_t {
        components::catalog::oid_t oid;
        uint64_t dropped_at_commit_id;
        std::filesystem::path path;
        std::pmr::vector<std::filesystem::path> sidecar_paths;
    };

    // Exactly one sidecar (`.wal_id`) is valid; any other name is data_corruption.
    [[nodiscard]] core::error_t verify_otbx_sidecars(const std::filesystem::path& otbx_path,
                                                     std::pmr::memory_resource* resource);

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

        bool has_storage(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return false;
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return false;
            return agents_[idx]->has_storage_sync(table_oid);
        }
        bool has_active_scan_for_oid_sync(components::catalog::oid_t table_oid) const noexcept {
            if (agents_.empty())
                return false;
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx >= agents_.size() || agents_[idx] == nullptr)
                return false;
            return agents_[idx]->has_active_scan_for_oid(table_oid);
        }
        // Wrapped: a bare 0 would be ambiguous with "never checkpointed".
        core::result_wrapper_t<wal::id_t>
        peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                         components::catalog::oid_t database_oid) const;

        // Loads a user-table storage on demand; "no file" is no_error(), "failed to load" is an error.
        [[nodiscard]] core::error_t load_storage_for_wal_replay_sync(components::catalog::oid_t table_oid,
                                                                     components::catalog::oid_t database_oid);

        // Disambiguated here: no_error (already owns the oid) vs io_error (could not build).
        [[nodiscard]] core::error_t create_storage_disk_sync(components::catalog::oid_t table_oid,
                                                             components::catalog::oid_t database_oid,
                                                             std::vector<components::table::column_definition_t> columns,
                                                             const std::filesystem::path& otbx_path,
                                                             bool is_computed);
        void bootstrap_system_tables_sync();
        void load_user_table_storages_sync();
        // Rebuilds the .otbx for tables load_user_table_storages_sync couldn't load; returns divergences not closed.
        [[nodiscard]] core::result_wrapper_t<std::size_t> rehydrate_missing_user_storages_sync();
        // Re-derives a column drop whose release a crash discarded; runs after both user-table walks and WAL replay.
        void rearm_dropped_column_blocks_sync();
        std::unordered_set<components::catalog::oid_t> alive_user_oids_sync() const;
        // '\0' means "no such row" only — an unreadable pg_class travels the error wrapper instead, or
        // a DOCUMENT table's dynamic schema would silently vanish.
        core::result_wrapper_t<char> relkind_for_oid_sync(components::catalog::oid_t table_oid) const;

        // Names a table's `.otbx` directory, not well_known_oid::main_database.
        components::catalog::oid_t relnamespace_for_oid_sync(components::catalog::oid_t table_oid) const;

        std::pmr::vector<components::catalog::oid_t> scan_live_table_oids_sync() const;

        std::pmr::vector<pg_index_row_t> scan_alive_pg_index_sync() const;

        std::pmr::vector<components::vector::data_chunk_t>
        scan_storage_for_rebuild_sync(components::catalog::oid_t table_oid, std::pmr::memory_resource* resource) const;

        std::pmr::vector<dropped_class_row_t> scan_dropped_oids_sync();

        std::pmr::vector<dropped_class_row_t> scan_dropped_table_oids_sync() { return scan_dropped_oids_sync(); }

        const std::filesystem::path& path_db() const noexcept { return config_.path; }

        // Directory oid every SYSTEM table's `.otbx` sits under — a fixed convention, not a resolved namespace.
        static constexpr components::catalog::oid_t system_dir_oid() noexcept {
            return components::catalog::well_known_oid::main_database;
        }
        void restore_oid_generator_sync();

        std::uint64_t max_persisted_commit_id_sync() const;

        // Most recent value for `name` in pg_settings, empty only if no such row exists (else throws).
        std::string read_setting_sync(std::string_view name);

        unique_future<core::result_wrapper_t<resolve_namespace_result_t>>
        resolve_namespace(execution_context_t ctx, std::string name);

        unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
        resolve_function_by_name(execution_context_t ctx, std::string name);

        // Bookkeeping lookup, not query-time cast resolution (that's cast_registry_); admin path only.
        unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        find_cast_oid(execution_context_t ctx,
                      components::catalog::oid_t source_oid,
                      components::catalog::oid_t target_oid);

        unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>> list_namespaces(execution_context_t ctx);

        unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                   components::catalog::oid_t table_oid,
                                                   std::int64_t oid_col_idx,
                                                   components::catalog::oid_t target_oid);

        unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
        delete_pg_catalog_rows_many(execution_context_t ctx, std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Patches backfilled pg_attribute rows after commit_id is known, before storage_publish_commits
        // flips visibility.
        unique_future<core::error_t>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Columnar row-data scan for one key-tuple; `keys` stays columnar to avoid a row-major crossing.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::uint64_t> key_col_indices,
                           components::vector::data_chunk_t keys,
                           std::pmr::vector<std::uint64_t> projected_cols);

        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::uint64_t> key_col_indices,
                            components::vector::data_chunk_t keys,
                            std::pmr::vector<std::uint64_t> projected_cols);

        // Drops every relkind='g' column not in `live_attnames` — subtractive, unlike drop_storage_column.
        unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::set<std::string> live_attnames);

        // ALTER TABLE DROP COLUMN's physical half; must run after the WAL commit marker and ProcArray
        // publish barrier, or a release could outlive a reverted tombstone.
        unique_future<core::result_wrapper_t<bool>>
        drop_storage_column(session_id_t session, components::catalog::oid_t table_oid, std::string attname);

        // ALTER TABLE RENAME COLUMN's physical half; ordering mirrors drop_storage_column — a reverted
        // ALTER can never leave storage renamed against a catalog that took the rename back.
        unique_future<core::result_wrapper_t<bool>> rename_storage_column(session_id_t session,
                                                                          components::catalog::oid_t table_oid,
                                                                          std::string old_attname,
                                                                          std::string new_attname);

        // ALTER TABLE ADD COLUMN: operator_alter_column_add_t; computed tables: operator_computed_field_register_t.

        core::result_wrapper_t<uint64_t> direct_append_sync(components::catalog::oid_t table_oid,
                                                            components::vector::data_chunk_t& data);
        // These three refuse (not no-op) with no storage: on WAL replay, a dropped mutation never re-derives.
        [[nodiscard]] core::error_t direct_delete_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       uint64_t count);
        [[nodiscard]] core::error_t direct_update_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       components::vector::data_chunk_t& new_data);
        [[nodiscard]] core::error_t direct_add_column_sync(components::catalog::oid_t table_oid,
                                                           const components::vector::data_chunk_t& schema_chunk);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char* { return "manager_disk"; }

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
        requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>) [[nodiscard]] ReturnType
            enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args);

        // `flush` was removed (a no-op promising durability). Do not reintroduce — checkpoint_all owns durability.

        // compact_watermark (here and below): the dispatcher's visible-to-all horizon for compact().
        unique_future<wal::id_t>
        checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);
        // Fans cleanup_versions to every agent; renumbers nothing, so VACUUM owes no index rebuild.
        unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                               std::pmr::vector<components::catalog::oid_t> table_oids,
                                               uint64_t compact_watermark);

        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        /// Bootstrap-only: crash-recovery rebuild populates dropped_storages_ (runtime DROP uses
        /// mark_storage_dropped_many).
        void register_dropped_storage_sync(components::catalog::oid_t oid,
                                           uint64_t dropped_at_commit_id,
                                           std::filesystem::path path,
                                           std::pmr::vector<std::filesystem::path> sidecar_paths);

        /// Runtime DROP TABLE path, sent BEFORE drop_storage_many while agents can still read live entries.
        unique_future<void> mark_storage_dropped_many(session_id_t session,
                                                      std::pmr::vector<components::catalog::oid_t> table_oids,
                                                      uint64_t dropped_at_commit_id);

        /// Rewrites a DROP's dropped_at_commit_id from TXN-ID space into commit-id space once commit allocates one.
        unique_future<void> storage_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        /// Abort mirror of storage_dropped_committed: erases dropped_storages_ entries for an aborted txn.
        unique_future<void> storage_drop_aborted(session_id_t session, uint64_t txn_id);

        /// Bootstrap helper: fans the dispatcher address to every agent (no manager-side mirror).
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        /// Bootstrap helper: fans the WAL address to every agent (can't be a ctor arg — born after the disk manager).
        void set_manager_wal_sync(actor_zeta::address_t address);

        // `is_computed` (relkind='g') is derived by the caller — pg_class doesn't exist yet to check here.
        unique_future<void> create_storage_disk(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t database_oid,
                                                std::vector<components::table::column_definition_t> columns,
                                                bool is_computed);
        // Batched DROP, partitioned per owning agent. Caller must finish index unregisters before this.
        unique_future<void> drop_storage_many(session_id_t session,
                                              std::pmr::vector<components::catalog::oid_t> table_oids);

        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(session_id_t session,
                                                                           components::catalog::oid_t table_oid);

        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        unique_future<void> storage_close_cursor(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t cursor_id);
        unique_future<core::result_wrapper_t<uint64_t>> storage_open_scan_hold(session_id_t session,
                                                                               components::catalog::oid_t table_oid);
        unique_future<core::result_wrapper_t<uint64_t>> storage_compact_epoch(session_id_t session,
                                                                              components::catalog::oid_t table_oid);
        // Aggregate-pushdown REDUCE: one reply carries ALL final rows (see disk_contract's single-owner invariant).
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce(session_id_t session,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::table::table_filter_t> filter,
                       std::vector<size_t> projected_cols,
                       components::table::transaction_data txn,
                       components::operators::pushed_aggregate_spec_t spec);
        // Fetched rows as chunks, or buffer-pool OOM/data_corruption, or a routing refusal (check
        // has_error() first); under SNAPSHOT the reply is paired by row_ids, never by position.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols,
                      components::table::transaction_data txn,
                      components::table::fetch_visibility_t visibility,
                      int64_t limit,
                      uint64_t expected_compact_epoch);
        unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::data_chunk_t> data);

        unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data);
        // Wrapped so a routing refusal is distinguishable from a legitimate "0 marks set".
        unique_future<core::result_wrapper_t<uint64_t>> storage_delete_rows(execution_context_t ctx,
                                                                            components::catalog::oid_t table_oid,
                                                                            components::vector::vector_t row_ids,
                                                                            uint64_t count);
        unique_future<void> storage_publish_commits(execution_context_t ctx,
                                                    uint64_t commit_id,
                                                    std::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void> storage_publish_deletes(execution_context_t ctx,
                                                    uint64_t commit_id,
                                                    std::set<components::catalog::oid_t> tables);

        unique_future<core::error_t> storage_revert_appends(execution_context_t ctx,
                                                            std::vector<components::pg_catalog_append_range_t> ranges,
                                                            bool tail_only);

        unique_future<void> storage_revert_deletes(execution_context_t ctx,
                                                   std::vector<components::catalog::oid_t> tables);

        using dispatch_traits = actor_zeta::implements<disk_contract,
                                                       &manager_disk_t::checkpoint_all,
                                                       &manager_disk_t::vacuum_all,
                                                       &manager_disk_t::maybe_cleanup_many,
                                                       &manager_disk_t::create_storage_disk,
                                                       &manager_disk_t::drop_storage_many,
                                                       &manager_disk_t::storage_types,
                                                       &manager_disk_t::storage_total_rows,
                                                       &manager_disk_t::storage_fetch_next_batch,
                                                       &manager_disk_t::storage_close_cursor,
                                                       &manager_disk_t::storage_reduce,
                                                       &manager_disk_t::storage_fetch,
                                                       &manager_disk_t::storage_append,
                                                       &manager_disk_t::storage_update,
                                                       &manager_disk_t::storage_delete_rows,
                                                       &manager_disk_t::storage_publish_commits,
                                                       &manager_disk_t::storage_publish_deletes,
                                                       &manager_disk_t::storage_revert_appends,
                                                       &manager_disk_t::storage_revert_deletes,
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
                                                       &manager_disk_t::drop_storage_column,
                                                       &manager_disk_t::rename_storage_column,
                                                       &manager_disk_t::on_horizon_advanced,
                                                       &manager_disk_t::mark_storage_dropped_many,
                                                       &manager_disk_t::storage_dropped_committed,
                                                       &manager_disk_t::storage_drop_aborted,
                                                       // Appended last — positional msg ids (see
                                                       // disk_contract::dispatch_traits).
                                                       &manager_disk_t::storage_open_scan_hold,
                                                       &manager_disk_t::storage_compact_epoch>;

    private:
        // Returns no_error(), or data_corruption/io_error instead of throwing, when the .otbx is
        // missing, unopenable, or has a stray sidecar; `catalog_columns` may defer the load if unresolved yet.
        [[nodiscard]] core::error_t
        load_storage_disk_sync(components::catalog::oid_t table_oid,
                               components::catalog::oid_t database_oid,
                               const std::filesystem::path& otbx_path,
                               std::vector<components::table::column_definition_t> catalog_columns);

        [[nodiscard]] std::unordered_map<components::catalog::oid_t,
                                         std::vector<components::table::column_definition_t>>
        collect_catalog_columns_sync(const std::unordered_set<components::catalog::oid_t>& wanted) const;

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        actor_zeta::scheduler_raw scheduler_disk_;
        // All message processing happens on loop_thread_; mutex_/pump_cv_ only gate its idle sleep.
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // Needs to stay trivially-copyable for boost::lockfree; re-wrapped into an owning pointer by the loop.
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_;
        std::condition_variable pump_cv_;

        log_t log_;
        configuration::config_disk config_;
        // No storages_ map here (pure router): agent 0 takes system oids, others split the user pool.
        std::pmr::vector<agent_disk_ptr> agents_{resource_};
        components::catalog::oid_generator oid_gen_;
        components::catalog::session_catalog_t stored_catalog_;

        // dropped_storages_ per-agent slices are the sole owner of GC state — no manager-side mirror.

        void create_agent(int count_agents);

        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        scan_table(components::catalog::oid_t table_oid,
                   std::unique_ptr<components::table::table_filter_t> filter,
                   std::vector<std::size_t> projected_cols,
                   components::table::transaction_data txn = components::table::transaction_data{});

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

        if (enqueue_impl(std::move(msg)).second != actor_zeta::detail::enqueue_result::success) {
            assert(future.is_ready() && "a refused enqueue must complete the future as abandoned");
        }

        return std::move(future);
    }

} //namespace services::disk