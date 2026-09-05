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
    /// `table.otbx` — B1a made the file the only substrate and B4 removed the mode enum that
    /// used to select an alternative — so the two constructors below differ only in whether
    /// the file is created or opened.
    class table_storage_t {
    public:
        /// Create new table.otbx
        table_storage_t(std::pmr::memory_resource* resource,
                        std::vector<components::table::column_definition_t> columns,
                        const std::filesystem::path& otbx_path);

        /// Load existing table.otbx.
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

        /// A7.6: true when the load ctor opened a proven-young .otbx (no checkpointed content;
        /// schema overlaid from the catalog). Used by the manager to cross-check the `.wal_id`
        /// sidecar in the REFUSING direction: a sidecar claiming a checkpoint over a young
        /// file is a contradiction, never grounds to guess.
        [[nodiscard]] bool never_checkpointed() const noexcept { return never_checkpointed_; }

        // Both ctors do file I/O (create/open/header read) + metadata-chain deserialize, all of which can
        // fail with io_error/data_corruption. A constructor cannot return a result_wrapper_t and MUST NOT
        // throw -- they run on the agent thread via bootstrap_create_disk_inner_sync (noexcept),
        // so a throw would std::terminate. Instead the ctor records the error here; the caller
        // (bootstrap_*_inner_sync / the manager probe) checks construction_failed(), drops the entry and
        // reports the refusal loudly (A7.5: the file is left byte-identical — no external backup recovery
        // exists; the two-slot root inside the .otbx is the only recovery mechanism).
        bool construction_failed() const noexcept { return construction_error_.contains_error(); }
        [[nodiscard]] const core::error_t& construction_error() const noexcept { return construction_error_; }

        /// Has the underlying block manager latched a failure it cannot recover from? Both of
        /// its latches (a write/fsync that never reached the device; a free list proven
        /// corrupt) are STICKY by design, and both make write_header refuse to commit -- which
        /// means the manager never promotes its pending free pool again. A caller that keeps
        /// compacting such a table pays a full extra copy of it every single round, for the
        /// life of the process; agent_disk_t::checkpoint_inner reads this and defers the entry
        /// instead. Nothing is sealed away from a degraded table:
        /// the deferral feeds its unchanged prev_checkpoint_wal_id into the WAL floor.
        [[nodiscard]] bool storage_degraded() const noexcept;

        /// Did the LAST checkpoint attempt on this table fail? Distinct from
        /// storage_degraded(): a failed header write whose previous root still stands
        /// deliberately does NOT latch, because the retry is meant to reach the same slot
        /// again and recover a transient error. The cost of that choice is that a PERSISTENT
        /// error there is retried forever, and each retry runs compact() first — which, under
        /// the split free pool, can only SPEND space when no header commits. So the entry
        /// keeps attempting its checkpoint (transient errors still recover) but stops
        /// REBUILDING until one succeeds.
        [[nodiscard]] bool last_checkpoint_failed() const noexcept { return last_checkpoint_failed_; }

        /// Checkpoint.
        /// W-TORN: writes data blocks + fsync, then header + fsync (2 fsync — durability before header swap).
        /// Returns out_of_memory when a column flush pin fails in
        /// data_table_t::checkpoint; true on success.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint();
        /// Same as checkpoint() + tracks W-TORN per-table wal_id snapshot.
        /// prev_checkpoint_wal_id_ ← old checkpoint_wal_id_; checkpoint_wal_id_ ← new_wal_id.
        /// Propagates the checkpoint() error; on error the wal_id fields stay unchanged.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(wal::id_t new_wal_id);

        /// B6 — DOES THIS ROUND HAVE ANY PHYSICAL WORK TO DO FOR THIS TABLE?
        ///
        /// False means the .otbx on the device already describes this table exactly, so
        /// compacting and rewriting it would produce the same table in different blocks at the
        /// cost of a full copy and two fsyncs. T1 measured that: 100 tables x 100 rows, an
        /// EMPTY round took 205.7 ms against 124.4 ms for the round that had actually written
        /// them all.
        ///
        /// WHAT COUNTS AS CHANGED. Everything that changes what a checkpoint would serialize:
        /// appends (committed or not), deletes, updates, reverts, schema growth and the ALTER
        /// rebuilds, a compact, and a table that has never been written at all. That set is
        /// enforced one level down, by data_table_t::modified_since_checkpoint — see the long
        /// note there for why the bit lives where the mutations are instead of where the
        /// decision is. Two pieces of state live up HERE and are named separately:
        ///   * pending_released_blocks_ — B3c files a dropped column's block ids here and only
        ///     checkpoint() can commit their release. Today a drop also rebuilds the
        ///     data_table_t, so the entry is dirty anyway and this conjunct never fires on its
        ///     own; it is stated all the same because this is the ONLY durable effect a round
        ///     owes that is not derivable from the table, and a skip that dropped it would
        ///     leak the blocks silently;
        ///   * a failed previous checkpoint needs no conjunct: the flag is cleared only by a
        ///     COMMITTED header, so a failed round leaves the entry dirty by construction.
        /// A construction that failed has no table to write and answers false; its caller drops
        /// the entry and reports the refusal (see construction_failed()).
        ///
        /// Answering false does NOT take the entry out of the round: it still advances its
        /// wal-id chain (advance_wal_id_without_rewrite), persists its sidecar and contributes
        /// its prev_checkpoint_wal_id to the round's min. Only the rebuild is skipped.
        [[nodiscard]] bool needs_checkpoint() const noexcept;

        /// B6 — the wal-id bookkeeping of a round that had nothing to write for this table.
        /// Exactly what checkpoint(wal::id_t) does on success, minus the writing:
        /// prev_checkpoint_wal_id_ <- checkpoint_wal_id_, checkpoint_wal_id_ <- new_wal_id.
        ///
        /// Both halves are literally true of a skipped entry, which is why the skip is
        /// invisible to WAL sealing. `prev` means "the root a lost commit this round would fall
        /// back to": no commit happened, so that root is the one already on the device, taken
        /// at the old checkpoint_wal_id_. `current` means "every WAL record at or below this id
        /// for this table is already in the file": true up to the round's id, because the table
        /// is unchanged since the last one. Recovery filters records on exactly that
        /// (integration/cpp/base_spaces.cpp: `record.id <= cp_id` -> skip) and skips nothing it
        /// would have needed, since an unchanged table has no records above its old id.
        void advance_wal_id_without_rewrite(wal::id_t new_wal_id) noexcept;

        /// W-TORN: latest committed checkpoint wal_id for this table (0 if never checkpointed).
        wal::id_t checkpoint_wal_id() const noexcept { return checkpoint_wal_id_; }
        /// Used by load path to seed checkpoint_wal_id_ from sidecar before WAL replay
        /// decides which records this storage already includes.
        void set_checkpoint_wal_id(wal::id_t v) noexcept { checkpoint_wal_id_ = v; }
        /// W-TORN: previous checkpoint wal_id (the state of the superseded root, i.e. the root the
        /// two-slot header still recovers if the current round's commit is lost); 0 before first overwrite.
        /// Used by checkpoint_all to compute min(prev) for safe WAL truncation.
        wal::id_t prev_checkpoint_wal_id() const noexcept { return prev_checkpoint_wal_id_; }

        /// Add a new column to the live table. Replaces table_ with a new data_table_t
        /// constructed from the current one + col. Retained as a primitive for tests and for
        /// the WAL-replay schema-growth path; the SQL ALTER TABLE ADD COLUMN flow no longer
        /// calls it (resolve_table reads columns from pg_attribute on every lookup).
        void add_column(components::table::column_definition_t& col);

        /// Physical column compaction. Drops the column whose name matches `attname` from the
        /// live data_table_t. Implemented via the
        /// data_table_t(parent, removed_column) rebuild constructor — row_groups are rebuilt
        /// without the dropped column (collection_t::remove_column per-segment). Used by VACUUM
        /// after pg_computed_column GC: columns that no longer have any live attrefcount>0 row
        /// are physically dead and can be reclaimed.
        ///
        /// B3c — WHAT IS AND IS NOT DONE HERE. The rebuild itself runs
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

        /// Rename ONE column of the live table — the storage half of ALTER TABLE RENAME COLUMN.
        ///
        /// The storage's name for a column is a CACHE of the catalog's; the identity is the
        /// column's pg_attribute.attoid, which a rename does not move (RN-oid). Keeping the two
        /// in step here is what lets the append path's column expansion and drop_column — both
        /// of which still address columns by name — go on working straight after the statement,
        /// without waiting for a restart.
        ///
        /// Nothing is allocated, moved or released. Unlike drop_column this builds no successor
        /// data_table_t — a name is not part of any segment or block — so there are no ids to
        /// name into pending_released_blocks_ and no adapter to rebuild.
        ///
        /// CRASH WINDOW, stated rather than hidden: the rename is in memory until this table's
        /// next checkpoint serializes the schema, while the catalog half is durable at the WAL
        /// commit marker, so a crash in between reloads a storage carrying the OLD name against
        /// a catalog carrying the NEW one. That state used to COST THE COLUMN — the bootstrap
        /// walk compared names and read it as a drop. It no longer can: the walk compares
        /// attoids, sees no drop, and repairs the stale name from the catalog
        /// (rearm_dropped_column_blocks_sync). The window is closed by IDENTITY, not by making
        /// this call replayable.
        ///
        /// true = renamed; false = this storage never carried `old_attname`;
        /// error = `new_attname` is already a column here, or no table is loaded.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_attname,
                                                                 const std::string& new_attname);

    private:
        /// B3c — the deferred half of drop_column, run from checkpoint() once the new root's
        /// pointer stream is on the device and immediately before the free list that the
        /// committing header will name is serialized. Frees only the blocks it can PROVE the
        /// dropped column owned alone; anything else is left to its owner. See the definition.
        void release_dropped_column_blocks();

        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        std::unique_ptr<components::table::storage::block_manager_t> block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
        // B3c: block ids named by a column that drop_column has already removed from
        // table_, still to be released. Filled by drop_column (the only moment they can be
        // enumerated — the rebuild destroys the column object that knows them) and drained by
        // release_dropped_column_blocks() inside the checkpoint that can commit the release.
        // Not durable, and deliberately so: a crash before that checkpoint leaves the durable
        // root still naming those blocks and the catalog tombstone still hiding the column, i.e.
        // exactly the state this branch has always shipped for a drop. Space is
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
#ifdef DEV_MODE
        // B6 — the DEV_MODE safety net for a hand-maintained flag, in the shape
        // segment_tree_t::flush uses: a cheap description of what the durable root was written
        // from, captured whenever the table is known clean, and re-checked every time
        // needs_checkpoint() is about to answer false. A mutation path that forgot to mark
        // never reaches the disk and is lost at restart, silently — this is what turns that
        // into an abort on the spot. See capture_clean_fingerprint().
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
        // BEFORE `storage` because every adapter built below borrows it; see the long note at
        // note_column_identity for who fills it and why the list is authoritative rather than a
        // hint. Owned here, so it survives every add_column / drop_column adapter rebuild.
        std::vector<components::table::column_definition_t> unmaterialized_columns;
        std::unique_ptr<components::storage::storage_t> storage;
        // Actual on-disk path of this table's .otbx. Used by checkpoint_all (sidecar
        // lands next to .otbx) and drop_storage_one_local (physical file removal).
        std::filesystem::path otbx_path;
        // Computing (relkind='g', dynamic-schema) table, created schema-less. Only
        // these may hold several columns with the same name but different types
        // (multi-type fields); regular tables coerce.
        bool is_computed = false;

        /// Create new table.otbx. B1b: the computed (relkind='g') flag is passed
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
            , storage(std::make_unique<components::storage::table_storage_adapter_t>(table_storage.table(),
                                                                                     resource,
                                                                                     &unmaterialized_columns))
            , otbx_path(otbx_path_in)
            , is_computed(is_computed_create) {}

        /// Load existing table.otbx. `catalog_columns` is the A7.6 schema overlay for a
        /// proven-young (never-checkpointed) file; ignored for a checkpointed one.
        /// `is_computed_load` marks a computed (relkind='g') table: the empty catalog
        /// schema is legal for it (allow_schemaless) and the entry keeps its dynamic-schema
        /// append semantics across restarts.
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
            // The column now HAS rows; it must stop being answered with NULLs. take_column_identity
            // already drops it on the materialising path, but the erase belongs next to the
            // materialisation itself so no future caller can add a column and leave the claim behind.
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
        /// adapter, and that is not an omission: drop_column has to because its rebuild
        /// move-assigns a NEW data_table_t over table_ and leaves the adapter's data_table_t&
        /// dangling. A rename mutates the SAME object in place, and the adapter reads columns()
        /// straight through that reference, so it already answers with the new name.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_attname,
                                                                 const std::string& new_attname) {
            return table_storage.rename_column(old_attname, new_attname);
        }

        // RN-oid — IDENTITY FOR A COLUMN THAT DOES NOT EXIST YET.
        //
        // Every storage column must carry its pg_attribute.attoid, because that is what the
        // bootstrap reconciliation compares on (see rearm_dropped_column_blocks_sync). Three of
        // the four ways a storage column comes into being are handed the catalog row that
        // created it — CREATE TABLE / CREATE MATERIALIZED VIEW through
        // build_create_table_writes, and both catalog-driven load paths through
        // collect_catalog_columns_sync. The fourth is not: ALTER TABLE ADD COLUMN writes only
        // pg_attribute, and the storage column is materialised LATER, by the schema-growth
        // stage of storage_append_inner, out of an INSERT chunk that carries nothing but an
        // alias-tagged type. That stage has no catalog to ask (pg_attribute is agent 0's, and
        // an append handler may not take a second cross-actor await).
        //
        // So the identity is DELIVERED AHEAD OF THE COLUMN and parked here, keyed by the only
        // thing the future INSERT chunk will carry — the name. Two publishers, and between them
        // they cover the live case and every crash:
        //   * the ALTER's own commit (manager_disk_t::update_pg_attribute_commit_id_fields
        //     routes each added_at marker to the owning agent), reaching this entry before the
        //     client's next statement can, by mailbox FIFO;
        //   * bootstrap (rearm_dropped_column_blocks_sync), for every live catalog attoid the
        //     storage does not already carry — which is an OID-set difference, not a name
        //     match, and re-publishes whatever a crash discarded before any INSERT can run.
        // A pending entry is consumed on use. It is NOT durable and does not need to be: it
        // describes a column that does not exist yet, and bootstrap re-derives it from the two
        // durable facts every time.
        //
        // THE SAME LIST ALSO ANSWERS THE READS. A published-but-unmaterialised column is not only
        // an identity waiting for its column: for the duration it is a column pg_attribute SHOWS
        // and no row group holds, and every reader has to survive naming it. So the entry carries
        // the column's TYPE alongside its identity and hands the whole list to the storage
        // adapter, which presents those columns as trailing all-NULL ones (see
        // table_storage_adapter_t). Publishing therefore has to be complete, not best-effort, and
        // it is: the ALTER's own commit covers the live case, and adopt_catalog_columns() re-derives
        // the set on EVERY load — bootstrap and lazy alike — from the same oid-set difference.
        void note_column_identity(std::string attname,
                                  std::uint32_t attoid,
                                  const components::types::complex_logical_type& type) {
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
                    return;
                }
            }
            components::table::column_definition_t def(std::move(attname), type);
            def.set_attoid(attoid);
            unmaterialized_columns.push_back(std::move(def));
        }

        // 0 = nothing published for this name. The caller is the one materialising the column,
        // so the entry is dropped on the way out: it has served its single purpose — and dropping
        // it is also what stops the adapter answering NULLs for a column that now has rows.
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

        // Re-derive the published set from a catalog column list, by the SAME oid-set difference
        // rearm_dropped_column_blocks_sync uses: every live pg_attribute column whose attoid no
        // storage column carries is a column the catalog has and this storage has not materialised.
        // Called on every LOAD (bootstrap and lazy), which is what makes the set complete after a
        // restart — the pg_attribute row is durable, the parked note is not.
        //
        // The NAME is checked alongside the oid, and not as a second identity: it is the guard
        // against a storage whose columns carry attoid 0 (a state the bootstrap walk refuses
        // rather than pretends away). On the oid alone such a storage would match nothing and the
        // whole catalog would be published as unmaterialised, doubling every column in every chunk
        // this adapter fills. A name collision means the column is physically there whatever its
        // identity says, and the reader must not invent a second one.
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
                    note_column_identity(def.name(), def.attoid(), def.type());
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
    // index agent actors. Non-1:1 mappings from pg_index:
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
        // agent owns no storage at that oid — so storage_append REFUSES and every scan
        // refuses with it. They used to answer (0,0) and an empty result instead, which is
        // exactly why this walk had to be written: the state was reachable AND silent, so
        // nothing above the storage layer could notice it. Recreates each missing storage from its
        // pg_attribute column definitions so the catalog and the storage layer
        // agree. Pre-scheduler-start, single-threaded (same window as
        // load_user_table_storages_sync). Skips relkinds without pg_attribute
        // row storage (views, sequences; computed tables are recovered by WAL
        // replay synthesis, their schema is not in pg_attribute) and any oid
        // already loaded.
        void rehydrate_missing_user_storages_sync();
        // B3c2 — re-derive a column drop whose physical release a crash discarded.
        //
        // B3c1's commit path drops the column from the live table and NAMES its blocks into
        // table_storage_t::pending_released_blocks_; B3c's checkpoint releases them. That set
        // lives only in memory, so a crash in between loses it while the disk keeps the
        // pg_attribute tombstone (durable through the WAL commit marker) AND the physically
        // present column. The table then reloads with the column back in its collection and
        // nothing downstream can ever re-derive the drop — compact() least of all, since after
        // the reload the column is genuinely part of the collection. The space leaks forever.
        //
        // This is the one place that can notice: it compares each loaded user table's own
        // (checkpointed) column names against the LIVE pg_attribute set and hands every
        // storage-only column to table_storage_t::drop_column — the same primitive the commit
        // path calls, which names the blocks and rebuilds the collection without the column.
        // The rebuild allocates nothing (every surviving column is SHARED with the successor);
        // the bytes move at the next checkpoint, exactly as on the live path. Re-arming WITHOUT
        // dropping would be a no-op: the release proves non-ownership by asking whether the
        // live collection still names the id, and a column left in place answers yes to all of
        // them. See the long note at the definition.
        //
        // ORDERING (base_spaces): after BOTH user-table walks — the storage half must be
        // loaded — and after WAL replay — the catalog half is not final until the tombstone is
        // replayed; running it earlier would read an un-replayed ADD COLUMN as a drop and
        // physically remove a surviving column. Before bootstrap_indexes_sync, so the index
        // rebuild scans the same layout every later scan will. Pre-scheduler-start,
        // single-threaded (same window as the walks above); no cross-actor message is needed,
        // which is why the comparison lives here and not in the checkpoint round, where the
        // disk agent holds no catalog.
        //
        // Computed (relkind='g') tables are excluded at the source (scan_live_table_oids_sync
        // yields only 'r'/'m'): their schema is in pg_computed_column, so an empty
        // pg_attribute set for them would read as "every column dropped". Rule 6: an
        // unreadable or contradictory catalog is reported at error level and NOTHING is
        // dropped — a leak is recoverable on the next start, an emptied table is not.
        void rearm_dropped_column_blocks_sync();
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
        //
        // '\0' DELIBERATELY STAYS IN-BAND, decided rather than overlooked. It used to conflate
        // three things, one of which was "pg_class is not loaded". That one is gone:
        // bootstrap_system_tables_sync now refuses to start unless every system table came up,
        // so the `entry == nullptr` leg is unreachable at bootstrap and everything downstream
        // runs after it. The two survivors are both honest — an empty pg_class on a fresh
        // database, and "no pg_class row names this oid" — and the sole production consumer
        // (manager_disk_io.cpp:315) reads '\0' as "not computed", which is right for both.
        char relkind_for_oid_sync(components::catalog::oid_t table_oid) const;

        // Resolve a single table's pg_class.relnamespace (same single-threaded bootstrap
        // scan shape, cols {0=oid, 2=relnamespace}). Returns INVALID_OID when the catalog
        // does not know the oid.
        //
        // This is what names the directory a table's `.otbx` lives in:
        // `${db_root}/${relnamespace}/${table_oid}/table.otbx`, written by
        // create_storage_disk from the namespace oid the planner resolved. Every recovery
        // path that has to REBUILD that path — WAL-replay synthesis, the deferred-DROP GC
        // sweep, the rehydrate of a lost file — used to substitute
        // well_known_oid::main_database (4) for it, which is not a namespace oid at all and
        // is one no user table can carry: CREATE DATABASE allocates its namespace from
        // FIRST_USER_OID upward. The path missed the real file every time.
        components::catalog::oid_t relnamespace_for_oid_sync(components::catalog::oid_t table_oid) const;

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

        // Sync full-storage scan for post-bootstrap index rebuild. CHECKPOINT compaction
        // renumbers physical row_ids contiguously from 0 (see data_table_t::compact), so
        // pre-compact row_ids persisted in on-disk indexes go stale; this hands a table's
        // live rows back so an index can be rebuilt against current ids. Single-threaded
        // bootstrap only. Returns the storage as a batch of ≤DEFAULT_VECTOR_CAPACITY
        // chunks (empty when the oid is unknown or its storage is empty).
        //
        // NO CALLER TODAY. base_spaces fed it into manager_index_t's bootstrap rebuild,
        // and that rebuild was removed once it became provably a no-op (it refilled a
        // per-transaction buffer and then erased it, having no in-memory index left to
        // rebuild). Repairing the stale ids for real means clearing and refilling the
        // index AGENT's store, which is a mailbox round trip and cannot happen in this
        // pre-scheduler-start window; it belongs to the runtime repopulate path.
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
        //
        // B4: the row's `relnamespace` (pg_class col 2) comes back with it. The caller has to
        // rebuild `${db_root}/${relnamespace}/${oid}/`, which is where create_storage_disk put
        // the file, and it used to substitute well_known_oid::main_database (4) there — a value
        // no user table ever carries, since CREATE DATABASE allocates its namespace from
        // FIRST_USER_OID upward. It has to be read here rather than looked up afterwards: the
        // row is a TOMBSTONE, and every ordinary catalog read omits permanently-deleted rows.
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
        // appended value for the given name, or empty string if not found.
        // Synchronous — called at startup before actor schedulers start.
        std::string read_setting_sync(std::string_view name);

        // Per-item resolve methods. Each method scans the corresponding pg_* table
        // on the disk actor thread and returns the found object (or {found=false}).
        // The since_version parameter is kept for message-dispatch compatibility
        // (always ignored — versioning is no longer used).
        //
        // All four carry core::result_wrapper_t because the SCAN can fail, and "the read
        // failed" is not "the catalog does not have it". {found=false} / an empty vector /
        // INVALID_OID stay the honest NEGATIVE answers, inside the wrapper.
        unique_future<core::result_wrapper_t<resolve_namespace_result_t>>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);

        // Cross-namespace function lookup: returns ALL pg_proc rows whose proname matches
        // `name`, regardless of pronamespace. Used by the UDF admin paths (#41 Path 2/4):
        // register_udf needs to detect cross-namespace conflicts; drop_udf needs to purge
        // every row sharing the name. Admin-scope (register/drop UDF); may return an empty vector.
        unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);

        // Bookkeeping lookup (NOT query-time cast resolution — that is the in-memory
        // cast_registry_). Finds the pg_cast row identified by its (castsource,
        // casttarget) pair and returns the cast's own oid (col 0), or INVALID_OID if
        // absent. Admin path only: unregister-cast uses it to find the row to delete.
        unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        find_cast_oid(execution_context_t ctx,
                      components::catalog::oid_t source_oid,
                      components::catalog::oid_t target_oid);

        // V4 admin-path enumerators. Bypass the per-name cache (cache is per-(name, ns_oid)
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

        // Physical column compaction for a relkind='g' storage: drop every physical column
        // whose name is NOT in `live_attnames`. Called by operator_vacuum_t after
        // pg_computed_column GC: columns whose attrefcount<=0 rows have been deleted are
        // physically dead and can be reclaimed. Returns the number of columns physically
        // dropped (0 if the storage is missing or already compact).
        //
        // B4 un-gated this. It used to refuse every non-in-memory storage, which after B1a was
        // every storage — see the long note at agent_disk_t::compact_relkind_g_storage_inner
        // for why acting is the safe reading and what a refusal cost. The leg is SUBTRACTIVE
        // (it drops the complement of `live_attnames`); ALTER TABLE DROP COLUMN, which names
        // its column, has its own leg — see drop_storage_column below.
        unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::set<std::string> live_attnames);

        // B3c1 — ALTER TABLE DROP COLUMN's physical half: release the ONE column `attname`
        // from the storage of `table_oid`. Thin router to the owning agent; see the contract
        // in disk_contract.hpp for why this is a sibling of compact_relkind_g_storage rather
        // than a flag on it, and for the three-way answer (true / false / error).
        //
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
        //
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
        uint64_t direct_append_sync(components::catalog::oid_t table_oid, components::vector::data_chunk_t& data);
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

        void sync(disk_sync_pack_t pack);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        // compact_watermark (here and below): the dispatcher's visible-to-all
        // horizon (txn_compact_watermark_msg / txn_publish_msg return) handed to
        // data_table_t::compact(); any version stamp above it makes the compact
        // a no-op, and checkpoint_inner then skips that entry for the round.
        unique_future<wal::id_t>
        checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);
        // Answers how many storages this VACUUM RENUMBERED — see agent_disk_t::vacuum_inner,
        // which produces the count where a renumbering would happen. The VACUUM statement
        // rebuilds indexes only on a non-zero answer, instead of assuming one.
        unique_future<uint64_t> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
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

        // Storage queries. Both wrap: an empty type list and a zero row count are real
        // answers about a real table AND were the answer for an oid no agent owns. See the
        // contract note on disk_contract::storage_types.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(session_id_t session,
                                                                           components::catalog::oid_t table_oid);

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
        // unchanged, and its routing refusal (an oid no agent has a storage for); callers
        // read has_error() before .value().
        // `txn` + `visibility` ride this same message (C4b) and neither has a default: under
        // SNAPSHOT rows invisible to `txn` are dropped, so the reply is SHORTER than the
        // request and is paired with it through each chunk's row_ids, never by position.
        // `limit` is the POST-VISIBILITY row cap (-1 == uncapped) the index scan pushes down,
        // the counterpart of storage_fetch_next_batch's post-filter matched-row cap.
        // See the contract note on disk_contract::storage_fetch.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols,
                      components::table::transaction_data txn,
                      components::table::fetch_visibility_t visibility,
                      int64_t limit);
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
                                                       &manager_disk_t::flush,
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

        auto enqueue_status = enqueue_impl(std::move(msg));
        static_cast<void>(enqueue_status);

        return std::move(future);
    }

} //namespace services::disk