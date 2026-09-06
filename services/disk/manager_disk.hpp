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

    /// Owns data_table_t + its supporting storage infrastructure. Every table is backed by a
    /// `table.otbx`; the two constructors differ only in create vs. open.
    class table_storage_t {
    public:
        /// Create new table.otbx
        table_storage_t(std::pmr::memory_resource* resource,
                        std::vector<components::table::column_definition_t> columns,
                        const std::filesystem::path& otbx_path);

        /// Load existing table.otbx. `catalog_columns` overlays the schema only for a
        /// never-checkpointed file (meta_block == INVALID_INDEX); ignored otherwise. An empty
        /// overlay is a construction error unless `allow_schemaless` (relkind='g' tables).
        table_storage_t(std::pmr::memory_resource* resource,
                        const std::filesystem::path& otbx_path,
                        std::vector<components::table::column_definition_t> catalog_columns,
                        bool allow_schemaless = false);

        components::table::data_table_t& table() { return *table_; }

        /// True when the load ctor opened a never-checkpointed .otbx. Used to refuse a
        /// `.wal_id` sidecar that claims a checkpoint over a young file (contradiction).
        [[nodiscard]] bool never_checkpointed() const noexcept { return never_checkpointed_; }

        // Ctors MUST NOT throw: they run via bootstrap_create_disk_inner_sync (noexcept), so a
        // throw would std::terminate. Errors are recorded here instead; see construction_failed().
        bool construction_failed() const noexcept { return construction_error_.contains_error(); }
        [[nodiscard]] const core::error_t& construction_error() const noexcept { return construction_error_; }

        /// True once the block manager's write/fsync or free-list latch is stuck (sticky by
        /// design, never clears): write_header then refuses to commit forever. checkpoint_inner
        /// defers such entries instead of paying a full extra copy every round.
        [[nodiscard]] bool storage_degraded() const noexcept;

        /// Did the LAST checkpoint attempt fail? Unlike storage_degraded(), does NOT latch — a
        /// transient error must stay retryable. Gates whether the retry may skip its rebuild:
        /// under the split free pool a compact that never commits a header can only SPEND space.
        [[nodiscard]] bool last_checkpoint_failed() const noexcept { return last_checkpoint_failed_; }

        /// True when a column carries a committed-update overlay a rebuild has not folded in
        /// (column_data_t::updates_ / WAL-replay PHYSICAL_UPDATE); checkpoint refuses to write
        /// pre-update bytes. Asked ONLY on the failed-round retry path — it walks every segment
        /// of every column via data_table_t::get_column_segment_info(), too costly for the
        /// ordinary round. Over-reports safe: a rolled-back update still counts.
        [[nodiscard]] bool has_pending_update_overlay();

        /// True when any row's version stamp is above `watermark` (uncommitted, or a commit no
        /// older snapshot may see). Same predicate data_table_t::compact gates on, asked here too
        /// because the failed-round retry path skips compact() entirely — and a .otbx carries no
        /// version metadata, so checkpointing over such a stamp resurrects the row at restart.
        [[nodiscard]] bool has_versions_above(uint64_t watermark) const;

        /// Checkpoint.
        /// W-TORN: writes data blocks + fsync, then header + fsync (2 fsync — durability before header swap).
        /// Returns out_of_memory on a column flush pin failure, unimplemented_yet when a column
        /// still carries a committed-update overlay no rebuild folded in; true on success.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint();
        /// Same as checkpoint() + tracks W-TORN per-table wal_id snapshot.
        /// prev_checkpoint_wal_id_ ← old checkpoint_wal_id_; checkpoint_wal_id_ ← new_wal_id.
        /// Propagates the checkpoint() error; on error the wal_id fields stay unchanged.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(wal::id_t new_wal_id);

        /// True when this round has physical work to do for this table. Measured: 100 tables x
        /// 100 rows, an EMPTY round took 205.7 ms against 124.4 ms for a round that wrote them
        /// all — so a false answer skips only the rebuild; the entry still advances its wal-id
        /// chain, persists its sidecar and contributes to the round's min. See
        /// data_table_t::modified_since_checkpoint for what counts as changed.
        [[nodiscard]] bool needs_checkpoint() const noexcept;

        /// wal-id bookkeeping for a round that skipped rewriting this table — the same effect as
        /// checkpoint(wal::id_t) without the write. Recovery relies on `current` covering every
        /// record already absorbed: integration/cpp/base_spaces.cpp filters `record.id <= cp_id`.
        void advance_wal_id_without_rewrite(wal::id_t new_wal_id) noexcept;

        /// W-TORN: latest committed checkpoint wal_id for this table (0 if never checkpointed).
        /// Only meaningful while checkpoint_wal_id_known() is true -- see below.
        wal::id_t checkpoint_wal_id() const noexcept { return checkpoint_wal_id_; }
        /// Used by load path to seed checkpoint_wal_id_ from sidecar before WAL replay
        /// decides which records this storage already includes.
        void set_checkpoint_wal_id(wal::id_t v) noexcept {
            checkpoint_wal_id_ = v;
            checkpoint_wal_id_known_ = true;
        }

        /// A third state wal::id_t can't carry: 0 means "never checkpointed" to the replay
        /// filter, so a sidecar that exists but could not be read must NOT report 0 — that would
        /// re-replay already-absorbed records. Unknown makes replay drop that table's records
        /// loudly instead of guessing; self-heals on the next committed checkpoint.
        [[nodiscard]] bool checkpoint_wal_id_known() const noexcept { return checkpoint_wal_id_known_; }
        void set_checkpoint_wal_id_unreadable() noexcept {
            checkpoint_wal_id_ = wal::id_t{0};
            checkpoint_wal_id_known_ = false;
        }
        /// W-TORN: previous checkpoint wal_id (the state of the superseded root, i.e. the root the
        /// two-slot header still recovers if the current round's commit is lost); 0 before first overwrite.
        /// Used by checkpoint_all to compute min(prev) for safe WAL truncation.
        wal::id_t prev_checkpoint_wal_id() const noexcept { return prev_checkpoint_wal_id_; }

        /// Add a new column to the live table. Replaces table_ with a new data_table_t
        /// constructed from the current one + col. Retained as a primitive for tests and for
        /// the WAL-replay schema-growth path; the SQL ALTER TABLE ADD COLUMN flow no longer
        /// calls it (resolve_table reads columns from pg_attribute on every lookup).
        void add_column(components::table::column_definition_t& col);

        /// Drops the column matching `attname` from the live table (VACUUM after
        /// pg_computed_column GC). The rebuild is free — every surviving column is SHARED with
        /// the successor, nothing is allocated. Block release does NOT run here: outside a
        /// checkpoint round the split free pool can only SPEND space, never return it (measured
        /// +2.9 MB per VACUUM at agent_disk_t::maybe_cleanup_inner). Ids are named here — the
        /// rebuild destroys the column object that knows them — and released by checkpoint().
        ///
        /// Returns true if the column was found and removed; false (no-op) if it was missing.
        bool drop_column(const std::string& attname);

        /// Rename ONE column of the live table (storage half of ALTER TABLE RENAME COLUMN). The
        /// rename stays in-memory until this table's next checkpoint, while the catalog half is
        /// durable at the WAL commit marker; a crash in between reloads the OLD name against a
        /// catalog carrying the NEW one. Closed by comparing attoid, not name, in
        /// rearm_dropped_column_blocks_sync — a name-keyed walk would read that as a drop.
        ///
        /// true = renamed; false = this storage never carried `old_attname`;
        /// error = `new_attname` is already a column here, or no table is loaded.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_attname,
                                                                 const std::string& new_attname);

    private:
        /// Deferred half of drop_column: runs inside checkpoint() once the new root's block
        /// stream is on the device, immediately before the free list is serialized. Frees only
        /// the blocks it can PROVE the dropped column owned alone.
        void release_dropped_column_blocks();

        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        std::unique_ptr<components::table::storage::block_manager_t> block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
        // Block ids drop_column has removed from table_ but not yet released; drained by
        // release_dropped_column_blocks() at checkpoint. Deliberately NOT durable: a crash before
        // that leaks space but corrupts nothing (rearm_dropped_column_blocks_sync re-derives the
        // drop on the next start).
        std::pmr::vector<uint64_t> pending_released_blocks_;
        wal::id_t checkpoint_wal_id_{0};
        // A storage built by the CREATE ctor has never been checkpointed and knows it; only a
        // LOAD whose sidecar could not be read clears this (see checkpoint_wal_id_known()).
        bool checkpoint_wal_id_known_{true};
        wal::id_t prev_checkpoint_wal_id_{0};
        // See last_checkpoint_failed(). Cleared by a successful checkpoint, so a transient
        // failure costs exactly one un-compacted round.
        bool last_checkpoint_failed_{false};
        // Set by the DISK ctors on file/metadata failure instead of throwing (see construction_failed()).
        core::error_t construction_error_{core::error_t::no_error()};
        // Set by the DISK load ctor when the .otbx was proven young (never checkpointed)
        // and constructed empty with the catalog's schema. See never_checkpointed().
        bool never_checkpointed_{false};
#ifdef DEV_MODE
        // DEV_MODE safety net, same shape as segment_tree_t::flush's dirty-flag check: a cheap
        // fingerprint of the durable root, re-checked whenever needs_checkpoint() is about to
        // answer false. A mutation path that forgot to mark dirty would otherwise reach restart
        // silently losing data; this aborts on the spot instead.
        struct clean_fingerprint_t {
            uint64_t total_rows = 0;
            uint64_t committed_rows = 0;
            uint64_t column_count = 0;
        };
        clean_fingerprint_t clean_fingerprint_{};
        void capture_clean_fingerprint() noexcept;
#endif
    };

    // Storage entry per collection. Namespace-scope so agent_disk_t can own a
    // `unordered_map<oid_t, unique_ptr<collection_storage_entry_t>>` slice.
    // Ownership migrates across actors by rvalue unique_ptr move only.
    struct collection_storage_entry_t {
        table_storage_t table_storage;
        // Columns pg_attribute publishes that this storage has not materialised yet. Declared
        // BEFORE `storage` because every adapter built below borrows it; see note_column_identity.
        std::vector<components::table::column_definition_t> unmaterialized_columns;
        std::unique_ptr<components::storage::storage_t> storage;
        // Actual on-disk path of this table's .otbx. Used by checkpoint_all (sidecar
        // lands next to .otbx) and drop_storage_one_local (physical file removal).
        std::filesystem::path otbx_path;
        // Computing (relkind='g', dynamic-schema) table, created schema-less. Only
        // these may hold several columns with the same name but different types
        // (multi-type fields); regular tables coerce.
        bool is_computed = false;

        /// Create new table.otbx. `is_computed_create` is passed EXPLICITLY rather than
        /// inferred from an empty column list: WAL-replay synthesis rebuilds a computed table's
        /// storage from a chunk with a NON-empty column list, where that inference would drop
        /// the flag and glue the next type-variant insert into the wrong column.
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

        /// Load existing table.otbx. `catalog_columns` overlays a never-checkpointed file's
        /// schema; ignored otherwise. `is_computed_load` allows an empty schema (relkind='g').
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

        /// Update the live schema: add new column to table_ and recreate the storage adapter.
        void add_column(components::table::column_definition_t& col, std::pmr::memory_resource* res) {
            table_storage.add_column(col);
            // The column now HAS rows; it must stop being answered with NULLs.
            drop_unmaterialized(col.name());
            storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     res,
                                                                                     &unmaterialized_columns);
        }

        /// Physical column compaction: drop the column from the live table_ and
        /// recreate the storage adapter (the adapter holds a data_table_t& that becomes
        /// dangling after the rebuild). Returns true if the column was found and removed.
        bool drop_column(const std::string& attname, std::pmr::memory_resource* res) {
            if (!table_storage.drop_column(attname)) {
                return false;
            }
            storage = std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     res,
                                                                                     &unmaterialized_columns);
            return true;
        }

        /// Rename a column of the live table_. Deliberately does NOT recreate the storage
        /// adapter (unlike drop_column): a rename mutates table_ in place, so the adapter's
        /// existing data_table_t& already reads the new name through columns().
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_attname,
                                                                 const std::string& new_attname) {
            return table_storage.rename_column(old_attname, new_attname);
        }

        // Parks a column's pg_attribute identity BEFORE it materialises: ALTER TABLE ADD COLUMN
        // writes only pg_attribute, and the storage column is materialised later by the
        // schema-growth stage of storage_append_inner, which has no catalog to ask (pg_attribute
        // is agent 0's; an append handler may not take a second cross-actor await). Keyed by
        // name, filled by two publishers (the ALTER's own commit, and bootstrap's
        // rearm_dropped_column_blocks_sync by oid-set difference after a crash).
        // Also answers reads until materialisation: table_storage_adapter_t presents these as
        // trailing constant columns (the default, or NULL) — the same PostgreSQL 11+ device as
        // pg_attribute.attmissingval, and row_group_t::add_column backfills from the same default
        // on materialisation so the answer never flips.
        void note_column_identity(std::string attname,
                                  std::uint32_t attoid,
                                  const components::types::complex_logical_type& type,
                                  const std::optional<components::types::logical_value_t>& default_value = {}) {
            if (attname.empty() || attoid == 0) {
                return;
            }
            // A column the storage already carries is materialised, whatever a stale note says.
            // Claiming it here would give every chunk a second, all-NULL copy of a column that
            // has data.
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
                    // A publisher that KNOWS the default completes an entry that does not;
                    // neither publisher overwrites one that already has it.
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

        // The parked publication for `attname`, or nullptr. Unlike take_column_identity, does
        // NOT consume the entry — the caller reads the DEFAULT here before materialising drops it.
        [[nodiscard]] const components::table::column_definition_t*
        find_unmaterialized(const std::string& attname) const noexcept {
            for (const auto& p : unmaterialized_columns) {
                if (p.name() == attname) {
                    return &p;
                }
            }
            return nullptr;
        }

        // 0 = nothing published for this name. Consumes the entry: the caller is materialising
        // the column, so the adapter must stop answering it with NULLs.
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

        // Re-derive the published set from a catalog column list, by the same oid-set difference
        // rearm_dropped_column_blocks_sync uses. NAME is checked alongside oid as a guard: a
        // storage whose columns carry attoid 0 would otherwise match nothing and double every
        // column in every chunk.
        void adopt_catalog_columns(const std::vector<components::table::column_definition_t>& catalog_columns) {
            for (const auto& def : catalog_columns) {
                if (def.attoid() == 0) {
                    continue; // relkind='g' columns live in pg_computed_column and carry none
                }
                bool in_storage = false;
                for (const auto& column : table_storage.table().columns()) {
                    if (column.attoid() == def.attoid() || column.name() == def.name()) {
                        in_storage = true;
                        break;
                    }
                }
                if (!in_storage) {
                    // collect_catalog_columns_sync must decode attdefspec on the MANAGER's
                    // resource, not its scan arena: a logical_value_t copy keeps its source
                    // resource pointer, so a scan-arena default would dangle after this call.
                    note_column_identity(def.name(), def.attoid(), def.type(), def.default_value_opt());
                }
            }
        }
    };

    // One tombstoned pg_class row, as scan_dropped_oids_sync reports it: the table's own oid,
    // the namespace oid its `.otbx` directory is keyed by, and the sentinel delete_id.
    struct dropped_class_row_t {
        components::catalog::oid_t oid;
        components::catalog::oid_t namespace_oid;
        std::uint64_t delete_id;
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

    // This build writes exactly one sidecar (`.wal_id`, staged via `.wal_id.tmp`) next to a
    // `.otbx`. Any other name there — e.g. a backup/quarantine sidecar from a build predating
    // shadow paging — returns data_corruption naming the stray file and touches nothing.
    // Called by load_storage_disk_sync before any probe open.
    [[nodiscard]] core::error_t verify_otbx_sidecars(const std::filesystem::path& otbx_path,
                                                     std::pmr::memory_resource* resource);

    // Index-bootstrap row: one entry per live pg_index row, populated by
    // scan_alive_pg_index_sync() and consumed by base_spaces to spawn
    // index agent actors. Non-1:1 mappings from pg_index:
    //   keys        ← indkey, a CSV of attoids resolved to attnames via pg_attribute.
    //   ready_since ← indisvalid sentinel: 1 if valid, 0 if backfill uncommitted
    //                 (base_spaces skips ready_since==0 as an unfinished build).
    //   type        ← indtype, decoded via index_type_from_indtype_code. NOT defaulted: a
    //                 missing/invalid indtype fails LOUDLY (error log + abort) rather than
    //                 guessing a backend and handing a bitcask directory to a B+tree reader.
    // No name field: the on-disk layout is keyed by (table_oid, indexrelid); the name lives
    // only in pg_class.
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

        // True if a storage entry is registered for `table_oid` (used by WAL replay to decide
        // whether the first PHYSICAL_INSERT for a table has to synthesise its .otbx).
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
        // Observability: does any OPEN fetch-next cursor still target `table_oid`? A live
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
        // Read the .otbx.wal_id sidecar directly from disk without loading the storage, or the
        // reason it could not be read.
        // THE WRAPPER IS THE WHOLE POINT. wal::id_t{0} is the replay filter's word for "this table
        // has never been checkpointed, replay every record it has", so answering an unreadable
        // sidecar with 0 does not lose a diagnostic — it re-applies records already absorbed into
        // the checkpointed .otbx. Here 0 means only "no sidecar exists, so no checkpoint ever
        // committed"; every other way of not getting an answer (a short or zero-length sidecar, an
        // unnamed namespace, no configured path, a loaded entry whose floor came up unknown)
        // travels the error side.
        // "PRESENT BUT SHORT" IS A CRASH IMAGE, not corruption: even with the atomic writer
        // (stage_checkpoint_sidecar + publish_checkpoint_sidecar in agent_disk.cpp) a crash
        // between the staging write and the rename is legitimate. That is why an unreadable
        // sidecar is reported rather than made fatal to the table's open — see
        // table_storage_t::checkpoint_wal_id_known().
        core::result_wrapper_t<wal::id_t>
        peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                         components::catalog::oid_t database_oid) const;

        // Load a user-table storage from its .otbx file on demand. Called by WAL replay
        // when it encounters a record for a disk-backed table that hasn't been loaded yet.
        // NO FILE IS no_error(): replay legitimately runs ahead of a table's first checkpoint
        // and synthesises the storage from the record's own chunk. A FILE THAT DID NOT LOAD is
        // the error, and the two must not arrive as the same answer — the caller's next move
        // after "no storage" is to CREATE one at that very path, which over a file that exists
        // but did not open is how a table that could still be repaired stops being one.
        [[nodiscard]] core::error_t load_storage_for_wal_replay_sync(components::catalog::oid_t table_oid,
                                                                     components::catalog::oid_t database_oid);

        // Synchronous DISK storage creation (before schedulers start): new .otbx at `otbx_path`.
        // Used by bootstrap (system tables), rehydrate, and base_spaces' WAL-replay synthesis.
        // `is_computed` is the pg_class.relkind='g' fact, resolved by the caller — replay synthesis
        // reads it via relkind_for_oid_sync (pg_class is final by then); bootstrap and rehydrate
        // pass false (system tables are never computed; rehydrate's scan is filtered to 'r'/'m').
        // AND IT REPORTS. The agent answers a create with one bool covering two unrelated outcomes
        // — "this agent already owns the oid" and "the .otbx could not be built at all". The manager
        // tells them apart without changing that contract: after a false, the owning agent either
        // holds the oid (a legitimate skip, no_error) or does not (the create failed, io_error). Its
        // callers are recovery walks that go on to append into whatever came up, so the difference
        // is the difference between a no-op and rows written nowhere.
        [[nodiscard]] core::error_t create_storage_disk_sync(components::catalog::oid_t table_oid,
                                                             components::catalog::oid_t database_oid,
                                                             std::vector<components::table::column_definition_t> columns,
                                                             const std::filesystem::path& otbx_path,
                                                             bool is_computed);
        // System catalog (pg_*) bootstrap. Called from base_spaces during PHASE 1
        // before any actor is spawned. Creates the system-table .otbx files on a fresh
        // start and picks up existing ones on subsequent starts; idempotent w.r.t. the
        // resulting `storages_` map — collections are keyed by `pg_catalog.<name>`.
        void bootstrap_system_tables_sync();
        // Walk config_.path for user-table .otbx files (${db_oid}/${tbl_oid}/table.otbx with
        // tbl_oid >= FIRST_USER_OID) and load each into storages_ via load_storage_disk_sync.
        // Called by base_spaces after bootstrap_system_tables_sync so that WAL replay can read each
        // table's checkpoint_wal_id sidecar for filtering, instead of synthesising phantom storages
        // with possibly-wrong schemas from a single WAL chunk.
        void load_user_table_storages_sync();
        // Recreate the missing .otbx for every alive user table present in the persisted pg_class
        // catalog but whose storage load_user_table_storages_sync did not load: a freshly created
        // .otbx's directory entry is not fsynced, so a crash can durably keep the catalog row while
        // losing the file. pg_class persists unconditionally, so on reopen a CREATE TABLE IF NOT
        // EXISTS sees the table "exists" and skips storage creation, and resolve_table returns the
        // schema, yet the disk agent owns no storage at that oid — so storage_append REFUSES and
        // every scan refuses with it: answering (0,0) and an empty result would make the state
        // reachable AND silent, with nothing above the storage layer able to notice. Each missing
        // storage is rebuilt from its pg_attribute column definitions. Pre-scheduler-start,
        // single-threaded (same window as load_user_table_storages_sync). Skips relkinds without
        // pg_attribute row storage (views, sequences; computed tables are recovered by WAL replay
        // synthesis, their schema is not in pg_attribute) and any oid already loaded.
        // ANSWERS WITH THE NUMBER OF DIVERGENCES IT COULD NOT CLOSE. A table whose pg_attribute
        // columns do not resolve cannot be rebuilt — a zero-column storage is worse than none, and
        // refusing the start would repeat on every start over a catalog nothing inside this process
        // can repair — so the skip is right and stays; what was wrong is that it was SILENT, in the
        // one walk written specifically to notice this state. Four legs return before a single table
        // is examined (no disk agents, an empty config path, pg_class not loaded, pg_class too short
        // to scan) and a count of 0 from any of them is byte-for-byte the answer a healthy start
        // gives, while the one production caller reads "> 0"; those four travel the error side, so 0
        // means only "every alive table has storage behind it".
        [[nodiscard]] core::result_wrapper_t<std::size_t> rehydrate_missing_user_storages_sync();
        // Re-derive a column drop whose physical release a crash discarded.
        // The commit path drops the column from the live table and NAMES its blocks into
        // table_storage_t::pending_released_blocks_; the checkpoint releases them. That set lives
        // only in memory, so a crash in between loses it while the disk keeps the pg_attribute
        // tombstone (durable through the WAL commit marker) AND the physically present column. The
        // table then reloads with the column back in its collection and nothing downstream can
        // re-derive the drop — compact() least of all, since after the reload the column is
        // genuinely part of the collection. The space leaks forever.
        // This is the one place that can notice: it compares each loaded user table's own
        // (checkpointed) column names against the LIVE pg_attribute set and hands every
        // storage-only column to table_storage_t::drop_column — the same primitive the commit path
        // calls. The rebuild allocates nothing (every surviving column is SHARED with the
        // successor); the bytes move at the next checkpoint, exactly as on the live path. Re-arming
        // WITHOUT dropping would be a no-op: the release proves non-ownership by asking whether the
        // live collection still names the id, and a column left in place answers yes to all of them.
        // The measurement behind that no-op, and why the key is the attoid and NOT the column name,
        // are at the definition in manager_disk_bootstrap.cpp.
        // ORDERING (base_spaces): after BOTH user-table walks — the storage half must be loaded —
        // and after WAL replay, since the catalog half is not final until the tombstone is replayed
        // and running it earlier would read an un-replayed ADD COLUMN as a drop and physically
        // remove a surviving column. Before bootstrap_indexes_sync. Pre-scheduler-start and
        // single-threaded, which is why the comparison lives here and not in the checkpoint round,
        // where the disk agent holds no catalog.
        // Computed (relkind='g') tables are excluded at the source (scan_live_table_oids_sync yields
        // only 'r'/'m'): their schema is in pg_computed_column, so an empty pg_attribute set would
        // read as "every column dropped". An unreadable or contradictory catalog is reported at
        // error level and NOTHING is dropped — a leak is recoverable on the next start, an emptied
        // table is not.
        void rearm_dropped_column_blocks_sync();
        // Synchronous scan of pg_class.oid column, returning the set
        // of user-table OIDs (oid >= FIRST_USER_OID) currently alive in the
        // catalog. Called by base_spaces between system-record replay and
        // user-record replay so user WAL records targeting a dropped table
        // (whose .otbx and pg_class row are gone) are skipped instead of
        // resurrecting a phantom storage.
        std::unordered_set<components::catalog::oid_t> alive_user_oids_sync() const;
        // Resolve a single table's pg_class.relkind (single-threaded bootstrap scan of pg_class cols
        // {0=oid, 3=relkind} on agents_[0]). Answers '\0' when the catalog does not know the oid —
        // callers read that as "not computed", which is right for a table pg_class carries no row
        // for. load_storage_disk_sync uses it to recognise computed (relkind='g') tables, whose
        // catalog schema is legitimately empty and whose entries keep dynamic-schema semantics.
        // '\0' STAYS IN BAND FOR "NO SUCH ROW" AND ONLY THAT. "pg_class is not loaded, or is too
        // short to carry a relkind column" is not an answer at all: every consumer turns '\0' into
        // is_computed = false, so a DOCUMENT table recovered through such a path would come back as
        // an ordinary row-storage table with its dynamic-schema semantics silently gone. That leg
        // travels the wrapper. It should be unreachable in production — bootstrap_system_tables_sync
        // refuses to start unless every system table came up — and an unreachable state that
        // reports is exactly what makes the claim checkable.
        core::result_wrapper_t<char> relkind_for_oid_sync(components::catalog::oid_t table_oid) const;

        // Resolve a single table's pg_class.relnamespace (same single-threaded bootstrap scan shape,
        // cols {0=oid, 2=relnamespace}). Returns INVALID_OID when the catalog does not know the oid.
        // This is what names the directory a table's `.otbx` lives in:
        // `${db_root}/${relnamespace}/${table_oid}/table.otbx`. Every recovery path that has to
        // REBUILD that path — WAL-replay synthesis, the deferred-DROP GC sweep, the rehydrate of a
        // lost file — must resolve it here rather than substitute well_known_oid::main_database (4),
        // which is not a namespace oid at all and is one no user table can carry (CREATE DATABASE
        // allocates its namespace from FIRST_USER_OID upward), so such a path misses the real file
        // every time.
        components::catalog::oid_t relnamespace_for_oid_sync(components::catalog::oid_t table_oid) const;

        // Index-bootstrap helper: scan pg_class for every live user-OID whose relkind is 'r'
        // (regular table) or 'm' (materialized view) — the OIDs for which manager_index_t needs an
        // empty engine populated at startup. Called by base_spaces between
        // load_user_table_storages_sync and bootstrap_indexes_sync, pre-scheduler-start. Excludes
        // system OIDs and tombstoned rows; independent of (but consistent with) alive_user_oids_sync,
        // which has no relkind filter and is used by WAL replay.
        std::pmr::vector<components::catalog::oid_t> scan_live_table_oids_sync() const;

        // Index-bootstrap helper: one pg_index_row_t per live pg_index row (see
        // that struct for field mapping). Called by base_spaces immediately after
        // scan_live_table_oids_sync to spawn per-index disk agents and register
        // them with manager_index_t.
        std::pmr::vector<pg_index_row_t> scan_alive_pg_index_sync() const;

        // Sync full-storage scan for post-bootstrap index rebuild. CHECKPOINT compaction renumbers
        // physical row_ids contiguously from 0 (see data_table_t::compact), so pre-compact row_ids
        // persisted in on-disk indexes go stale; this hands a table's live rows back so an index can
        // be rebuilt against current ids. Single-threaded bootstrap only. Returns the storage as a
        // batch of <=DEFAULT_VECTOR_CAPACITY chunks (empty when the oid is unknown or its storage is
        // empty).
        // NO CALLER TODAY: the bootstrap index rebuild this fed was provably a no-op (it refilled a
        // per-transaction buffer and then erased it, having no in-memory index left to rebuild).
        // Repairing the stale ids for real means clearing and refilling the index AGENT's store,
        // which is a mailbox round trip and cannot happen in this pre-scheduler-start window.
        std::pmr::vector<components::vector::data_chunk_t>
        scan_storage_for_rebuild_sync(components::catalog::oid_t table_oid, std::pmr::memory_resource* resource) const;

        // Catalog scan returning (oid, relnamespace, delete_id) for every tombstoned pg_class row.
        // base_spaces calls it after WAL replay to rebuild the per-agent dropped_storages_ slices
        // (via register_dropped_storage_sync) so on_horizon_advanced can finish GC of .otbx files
        // left by a crash mid-DROP.
        // pg_class has no dropped_at_commit_id column, so the tombstone is the row-version delete_id
        // (no public API). Returned delete_id is sentinel 1: at boot lowest_active_start_time=1, so
        // anything > 1 is already GC-eligible and sentinel 1 means "GC on the first horizon advance
        // past 1".
        // `relnamespace` (col 2) comes back with it because the caller has to rebuild
        // `${db_root}/${relnamespace}/${oid}/`, and well_known_oid::main_database (4) is not a value
        // any user table carries. It has to be read here rather than looked up afterwards: the row
        // is a TOMBSTONE, and every ordinary catalog read omits permanently-deleted rows.
        std::pmr::vector<dropped_class_row_t> scan_dropped_oids_sync();

        // Index-bootstrap alias for scan_dropped_oids_sync — identical body because
        // pg_class is the only relation whose tombstones matter for index GC.
        std::pmr::vector<dropped_class_row_t> scan_dropped_table_oids_sync() { return scan_dropped_oids_sync(); }

        // Read-only accessor for the on-disk root directory.
        // base_spaces uses this to derive dropped storage paths.
        const std::filesystem::path& path_db() const noexcept { return config_.path; }

        // The directory oid every SYSTEM table's `.otbx` sits under:
        // `${path_db()}/${system_dir_oid()}/${tbl_oid}/table.otbx`. bootstrap_system_tables_sync
        // both writes and reads that layout, and it is a fixed convention rather than a
        // resolved namespace — a system table has no pg_class row of its own to carry a
        // `relnamespace` (bootstrap seeds self-descriptions for a handful of tables only).
        // Exposed so recovery paths can name the layout instead of repeating the constant.
        static constexpr components::catalog::oid_t system_dir_oid() noexcept {
            return components::catalog::well_known_oid::main_database;
        }
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
        // appended value for the given name, or empty string ONLY when no row with that
        // name exists. An unloaded or malformed pg_settings throws the pre-scheduler
        // std::runtime_error startup refusal instead of masquerading as "setting absent"
        // (bootstrap_system_tables_sync seeds pg_settings first, so those states are
        // sequencing bugs or corruption, never a legitimate empty).
        // Synchronous — called at startup before actor schedulers start.
        std::string read_setting_sync(std::string_view name);

        // Per-item resolve methods. Each method scans the corresponding pg_* table
        // on the disk actor thread and returns the found object (or {found=false}).
        // All four carry core::result_wrapper_t because the SCAN can fail, and "the read
        // failed" is not "the catalog does not have it". {found=false} / an empty vector /
        // INVALID_OID stay the honest NEGATIVE answers, inside the wrapper.
        unique_future<core::result_wrapper_t<resolve_namespace_result_t>>
        resolve_namespace(execution_context_t ctx, std::string name);

        // Cross-namespace function lookup: returns ALL pg_proc rows whose proname matches
        // `name`, regardless of pronamespace. Used by the UDF admin paths (#41 Path 2/4):
        // register_udf needs to detect cross-namespace conflicts; drop_udf needs to purge
        // every row sharing the name. Admin-scope (register/drop UDF); may return an empty vector.
        unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
        resolve_function_by_name(execution_context_t ctx, std::string name);

        // Bookkeeping lookup (NOT query-time cast resolution — that is the in-memory
        // cast_registry_). Finds the pg_cast row identified by its (castsource,
        // casttarget) pair and returns the cast's own oid (col 0), or INVALID_OID if
        // absent. Admin path only: unregister-cast uses it to find the row to delete.
        unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        find_cast_oid(execution_context_t ctx,
                      components::catalog::oid_t source_oid,
                      components::catalog::oid_t target_oid);

        // Admin-path enumerators. Bypass the per-name cache (cache is per-(name, ns_oid)
        // keyed; enumeration of "all namespaces" / "all tables in ns" cannot be served by
        // it). Used by catalog-resolve enumeration paths and the UDF namespace pick.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>> list_namespaces(execution_context_t ctx);

        // Allocate a batch of fresh OIDs from the disk-local oid_gen_. Called by the
        // dispatcher before invoking planner_t::create_plan for DDL statements, so that
        // the planner can build pg_class / pg_attribute rows without needing async access
        // to the disk actor. Wasted OIDs (plan rejected before execution) are acceptable —
        // same trade-off as PostgreSQL's pre-allocation approach.
        unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        // WAL-safe append of a single pre-built row into a pg_catalog table, or the reason
        // the row was not written. The wrapper is what separates "appended nothing" from
        // "could not append": a zero-count range reads as a no-op at every call site, so
        // without it a refused catalog write left the DDL statement reporting success.
        unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                   components::catalog::oid_t table_oid,
                                                   std::int64_t oid_col_idx,
                                                   components::catalog::oid_t target_oid);

        // Batched delete_pg_catalog_rows: loops the singular inner logic per spec, emitting the
        // same WAL records as N singular calls, and reports how many rows EACH spec deleted (in
        // spec order) or the reason the scrub did not happen. See disk_contract.hpp: a zero
        // count is an honest "nothing carried that oid" and the caller decides whether that is
        // legitimate; every refusal travels the wrapper instead of being reported as a no-op.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
        delete_pg_catalog_rows_many(execution_context_t ctx, std::pmr::vector<pg_catalog_delete_spec_t> specs);

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
        // Answers with the FIRST refusal any marker met, after attempting them all: one
        // unpatchable marker must not cost the others their stamp, and a backfill that did not
        // happen must not be a log line under a COMMIT that says success. See agent_disk.hpp
        // for why the caller reports rather than refuses.
        unique_future<core::error_t>
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

        // Physical column compaction for a relkind='g' storage: drop every physical column
        // whose name is NOT in `live_attnames`. Called by operator_vacuum_t after
        // pg_computed_column GC: columns whose attrefcount<=0 rows have been deleted are
        // physically dead and can be reclaimed. Returns the number of columns physically
        // dropped (0 if the storage is missing or already compact).
        // Acts unconditionally — see the long note at
        // agent_disk_t::compact_relkind_g_storage_inner for why that is the safe reading and
        // what a refusal would cost. The leg is SUBTRACTIVE (it drops the complement of
        // `live_attnames`); ALTER TABLE DROP COLUMN, which names its column, has its own leg —
        // see drop_storage_column below.
        unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::set<std::string> live_attnames);

        // ALTER TABLE DROP COLUMN's physical half: release the ONE column `attname`
        // from the storage of `table_oid`. Thin router to the owning agent; see the contract
        // in disk_contract.hpp for why this is a sibling of compact_relkind_g_storage rather
        // than a flag on it, and for the three-way answer (true / false / error).
        // ORDERING, and it is the whole safety argument: this is driven by
        // operator_commit_transaction_t AFTER the txn's WAL commit marker and the ProcArray
        // publish barrier, in the same place the commit-time physical DROP TABLE runs. The
        // pg_attribute tombstone is the durable record of the drop; the physical release must
        // never be able to outlive a tombstone that a ROLLBACK or a crashed txn takes away.
        unique_future<core::result_wrapper_t<bool>>
        drop_storage_column(session_id_t session, components::catalog::oid_t table_oid, std::string attname);

        // ALTER TABLE RENAME COLUMN's physical half: rename ONE column of `table_oid`'s
        // storage. Thin router to the owning agent; the three-way answer and the reason this
        // leg has to exist at all are in disk_contract.hpp.
        // ORDERING mirrors drop_storage_column and for the same reason in the ROLLBACK
        // direction: operator_commit_transaction_t drives it only after the WAL commit marker
        // and the publish barrier, so a reverted ALTER can never leave the storage renamed
        // against a catalog that took the rename back — and the bootstrap walk would read that
        // divergence as a DROP of a surviving column.
        unique_future<core::result_wrapper_t<bool>> rename_storage_column(session_id_t session,
                                                                          components::catalog::oid_t table_oid,
                                                                          std::string old_attname,
                                                                          std::string new_attname);

        // ALTER TABLE ADD COLUMN owned by operator_alter_column_add_t; computed
        // tables maintained via operator_computed_field_register_t.

        // Synchronous direct replay methods for physical WAL (before schedulers start).
        // The append answers with the START ROW of what it wrote, or the reason it wrote nothing.
        // The value alone cannot carry that: 0 is simultaneously "the owning agent holds no storage
        // for this oid", "the chunk was empty", "the append was refused" and "the first row of a
        // fresh table landed at row 0" — so a record of COMMITTED rows replayed into a table with no
        // storage would disappear with nothing above the storage layer able to notice. Same refusal
        // the three routers below make; an EMPTY chunk stays the one legitimate no-op and answers 0
        // without an error.
        core::result_wrapper_t<uint64_t> direct_append_sync(components::catalog::oid_t table_oid,
                                                            components::vector::data_chunk_t& data);
        // These three REFUSE rather than no-op when the table has no storage on its owning
        // agent: they run on the WAL-replay path, where a dropped mutation is a journalled
        // change that recovery declined to restore and nothing re-derives later. See
        // agent_disk_t's declarations for the full reasoning.
        [[nodiscard]] core::error_t direct_delete_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       uint64_t count);
        [[nodiscard]] core::error_t direct_update_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       components::vector::data_chunk_t& new_data);
        // WAL-replay of a PHYSICAL_ADD_COLUMN record: re-apply each schema column to
        // the owned storage ahead of the dependent PHYSICAL_INSERT. `schema_chunk` is
        // a 0-row chunk whose columns ARE the new columns (alias-tagged types).
        // Idempotent: columns already present (by name) are skipped.
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

        // `flush` USED TO SIT HERE AND IS GONE. It was a registered contract method whose body
        // traced and returned: no buffer flushed, no file synced, no entry touched — a name that
        // promised durability and delivered nothing. Durability of a table is checkpoint_all's,
        // and only its. The three call sites that named it (operator_commit_transaction.cpp,
        // operator_update.cpp, operator_delete.cpp) are gone with it in the same edit, which is
        // what removal always needed: it renumbers every method below it in dispatch_traits, so
        // contract, actor, behavior() and senders move as ONE change.
        // Do not reintroduce a no-op under this name. If a caller needs "the writes are on the
        // device now", it has to say WHAT it wants durable and the answer is checkpoint_all.

        // compact_watermark (here and below): the dispatcher's visible-to-all
        // horizon (txn_compact_watermark_msg / txn_publish_msg return) handed to
        // data_table_t::compact(); any version stamp above it makes the compact
        // a no-op, and checkpoint_inner then skips that entry for the round.
        unique_future<wal::id_t>
        checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);
        // Fans cleanup_versions out to every agent. Renumbers nothing — see
        // agent_disk_t::vacuum_inner — so the VACUUM statement owes no index rebuild.
        unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
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

        /// Runtime DROP TABLE path — sent from operator_dynamic_cascade_delete BEFORE the
        /// drop_storage_many send, so the owning agents can still read the live storage entries to
        /// derive the .otbx path + sidecars (wal_id, prev) and record them via
        /// register_dropped_storage_inner_sync. Touches no files (drop_storage_many does the
        /// removal); the GC entry lets on_horizon_advanced reconcile leftovers and flips dispatcher
        /// disk_has_dropped_ via on_drop_resource_marked. Batched: one cascade DROP marks every
        /// storage with the SAME dropped_at_commit_id, so the oids are partitioned per owning agent
        /// (pool_idx_for_oid) and fanned out as one mark_storage_dropped_many_inner per agent.
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

        /// Bootstrap helper — base_spaces wires the WAL address before scheduler.start,
        /// and the manager fans it out to every agent (the CATALOG agent writes physical
        /// WAL records for catalog DDL on its own thread). The WAL manager is born after
        /// the disk manager — its constructor takes the disk mailbox — so this one
        /// address cannot be a constructor argument here. no_mailbox() when the WAL is
        /// off; the agents' empty-address guards then skip every WAL write.
        void set_manager_wal_sync(actor_zeta::address_t address);

        // Storage management
        // `is_computed` marks a computed (relkind='g') table. It is derived by the
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

        // Storage queries. Both wrap: an empty type list and a zero row count are real answers
        // about a real table, so they cannot double as the answer for an oid no agent owns. See
        // the contract note on disk_contract::storage_types.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(session_id_t session,
                                                                           components::catalog::oid_t table_oid);

        // Storage data operations.
        // Streaming fetch-next scan source. Transparent router:
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
        // Release an abandoned fetch-next cursor. Transparent router to the owning agent's
        // storage_close_cursor_inner.
        unique_future<void> storage_close_cursor(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 uint64_t cursor_id);
        // Compact-hold open (see disk_contract): transparent router to the owning agent's
        // storage_open_scan_hold_inner. Released with storage_close_cursor.
        unique_future<core::result_wrapper_t<uint64_t>> storage_open_scan_hold(session_id_t session,
                                                                               components::catalog::oid_t table_oid);
        // Current compact epoch of the table (see disk_contract::storage_compact_epoch):
        // transparent router to the owning agent's storage_compact_epoch_inner.
        unique_future<core::result_wrapper_t<uint64_t>> storage_compact_epoch(session_id_t session,
                                                                              components::catalog::oid_t table_oid);
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
        // unchanged, and its routing refusal (an oid no agent has a storage for); callers
        // read has_error() before .value().
        // `txn` + `visibility` ride this same message and neither has a default: under
        // SNAPSHOT rows invisible to `txn` are dropped, so the reply is SHORTER than the
        // request and is paired with it through each chunk's row_ids, never by position.
        // `limit` is the POST-VISIBILITY row cap (-1 == uncapped) the index scan pushes down,
        // the counterpart of storage_fetch_next_batch's post-filter matched-row cap.
        // See the contract note on disk_contract::storage_fetch.
        // `expected_compact_epoch` (no default, same rule as `visibility`): the epoch the row ids
        // were minted against, or k_fetch_epoch_unchecked for ids the caller minted itself. See
        // the contract note on disk_contract::storage_fetch.
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
        // Appends every chunk in order. Appends within one txn are contiguous, so the
        // result is the single coalesced range [range_start, range_start + total_count).
        // Reply wraps (start_row, count) so a write_conflict / out_of_memory from the
        // table-layer append chain — and the routing refusal, which a zero-length range
        // could not be told apart from — reaches operator_insert as a value.
        unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::data_chunk_t> data);

        // Updates every chunk in order; row_ids[i] are the storage row-ids for data[i]
        // (the two vectors are positionally aligned and must have equal length). Returns
        // the coalesced new-row range [range_start, range_start + total_count).
        // Reply wraps (updated, appended) so a write_conflict / out_of_memory from the
        // table-layer MVCC update — and the routing refusal — reaches operator_update /
        // fk_cascade as a value.
        unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data);
        // Marks rows deleted under ctx.txn; the reply wraps the count so a refusal (no
        // agent owns the oid) is distinguishable from "0 marks set", which is a legitimate
        // outcome for already-deleted or duplicate ids. See disk_contract.
        unique_future<core::result_wrapper_t<uint64_t>> storage_delete_rows(execution_context_t ctx,
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
                                                       &manager_disk_t::checkpoint_all,
                                                       &manager_disk_t::vacuum_all,
                                                       &manager_disk_t::maybe_cleanup_many,
                                                       // Storage management
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
                                                       &manager_disk_t::drop_storage_column,
                                                       &manager_disk_t::rename_storage_column,
                                                       &manager_disk_t::on_horizon_advanced,
                                                       &manager_disk_t::mark_storage_dropped_many,
                                                       &manager_disk_t::storage_dropped_committed,
                                                       &manager_disk_t::storage_drop_aborted,
                                                       // Appended LAST — positional msg ids (see
                                                       // disk_contract::dispatch_traits).
                                                       &manager_disk_t::storage_open_scan_hold,
                                                       &manager_disk_t::storage_compact_epoch>;

    private:
        // Returns no_error() on success. Returns data_corruption/io_error — instead of throwing —
        // when the .otbx is missing, refuses to open (both header slots unusable), or sits next to
        // a stray legacy sidecar (verify_otbx_sidecars): this runs on the single-threaded
        // bootstrap/recovery path whose callers (bootstrap_system_tables_sync,
        // load_user_table_storages_sync, load_storage_for_wal_replay_sync) propagate / log the error.
        // On every refusal the file set is left byte-identical — recovery is the two-slot root
        // inside the .otbx, and there is no external backup to fall back to.
        // `catalog_columns` is the schema overlay for a never-checkpointed .otbx (see
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

        // One scan of pg_attribute (agents_[0], bootstrap thread) grouping live columns of
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

        // Held only to fan the dispatcher address out to every agent at bootstrap;
        // the manager itself never acks or mirrors — each agent emits its own
        // on_subscriber_empty(DISK_KIND) when its dropped_storages_ slice drains.
        log_t log_;
        configuration::config_disk config_;
        // Storage ownership shape (manager has NO storages_ map — pure router):
        //   - agent_disk_0 (CATALOG): all pg_* system tables, oid_gen_,
        //     stored_catalog_.
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

        // Single manager-side scan funnel over the owning agent's
        // storage_scan_inner, so there is ONE place that issues a catalog
        // scan. `filter` null = "see all rows"; `projected_cols` empty = "all
        // columns"; txn defaults to transaction_data{} = "see all committed".
        // REFUSES (io_error) when there is no owning agent and passes the agent's
        // scan error through: an empty batch vector means "no matching rows" and
        // nothing else.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
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

        // The status is read, not (void)-discarded. A refused enqueue already
        // destroyed the message in the .cpp overload (logged there), so `future` is
        // completed as abandoned and returning it hands the caller a readable failure
        // instead of a silent hang; a successful enqueue returns the same future live.
        if (enqueue_impl(std::move(msg)).second != actor_zeta::detail::enqueue_result::success) {
            assert(future.is_ready() && "a refused enqueue must complete the future as abandoned");
        }

        return std::move(future);
    }

} //namespace services::disk