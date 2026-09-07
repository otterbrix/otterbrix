#pragma once

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include "disk_contract.hpp" // fetch_batch_t reply payload for storage_fetch_next_batch_inner
#include <atomic>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/log/log.hpp>
#include <components/storage/storage.hpp> // scan_position_t for the index-resume active_scan_t
#include <components/table/data_table.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/timezones.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <set>
#include <string>
#include <unordered_map>

namespace services::disk {

#ifdef DEV_MODE
    // Test-observable round counter (not tables); a round rewrites every table whole (O(data)).
    uint64_t table_checkpoints() noexcept;
    void reset_table_checkpoints() noexcept;

    // Test-observable count of publish/revert legs with no storage on the owner (missed flip/unwind, not a misroute).
    uint64_t publish_revert_misses() noexcept;
    void reset_publish_revert_misses() noexcept;

    // Test-observable checkpoint-round tallies: entries deferred vs. rewritten (checkpoint_result_t).
    uint64_t checkpoint_entries_deferred() noexcept;
    uint64_t checkpoint_entries_rewritten() noexcept;
    void reset_checkpoint_entry_tallies() noexcept;

    // DEV_MODE-only deterministic pause between cursor fetches, for cursor-vs-concurrent-DDL
    // interleaving tests. Checked before every ADVANCE, never OPEN; hold()==true drives a
    // non-blocking round-trip instead of a wait, since blocking here would deadlock (the agent
    // serving the cursor also serves a concurrent DDL's physical half).
    struct scan_advance_gate_t {
        virtual ~scan_advance_gate_t() = default;
        // true = keep holding this scan between batches; false = let the next fetch go.
        virtual bool hold(components::catalog::oid_t table_oid, uint64_t cursor_id) = 0;
    };
    void dev_set_scan_advance_gate(scan_advance_gate_t* gate); // nullptr = off
    scan_advance_gate_t* dev_scan_advance_gate();
#endif

    using path_t = std::filesystem::path;

    using session_id_t = ::components::session::session_id_t;
    using execution_context_t = ::components::execution_context_t;

    // Test-observable rows storage_reduce_inner ships to the coordinator (mirrors dml_flush_count()).
#ifdef DEV_MODE
    uint64_t pushdown_reply_rows() noexcept;
    void reset_pushdown_reply_rows() noexcept;

    // Test-observable scan count for read_chunks_by_keys_inner, one bump per table pass regardless of key count.
    uint64_t catalog_key_scans() noexcept;
    void reset_catalog_key_scans() noexcept;
#endif

    // Forward-declared (manager_disk.hpp); incomplete here only because agent_disk_t's destructor defers the map.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    // Hash semi-join backing scan_by_keys_inner; namespace-scope so tests can call it directly.
    core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    fk_hash_semijoin(std::pmr::memory_resource* resource,
                     components::storage::storage_t& storage,
                     const std::pmr::vector<std::uint64_t>& key_col_indices,
                     components::vector::data_chunk_t& keys,
                     components::table::transaction_data txn);

    // Cross-mailbox result of checkpoint_inner; min_prev_checkpoint_wal_id is the min over this agent's entries.
    struct checkpoint_result_t {
        wal::id_t min_prev_checkpoint_wal_id;
        // deferred = degraded/cursor/MVCC/failed gate; rewritten = new root; advanced = wal-id chain moved, no rewrite.
        uint64_t deferred{0};
        uint64_t rewritten{0};
        uint64_t advanced{0};
    };

    /// storages_ partition role; MUST align with manager_disk_t::pool_idx_for_oid (idx 0 ↔ CATALOG).
    enum class agent_role_t : std::uint8_t
    {
        CATALOG = 0,
        USER_POOL = 1
    };

    class agent_disk_t final : public actor_zeta::basic_actor<agent_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        /// Default-constructed agent: CATALOG role, pool_idx = 0.
        agent_disk_t(std::pmr::memory_resource* resource, const path_t& path_db, log_t& log);

        /// Role-aware constructor (role/pool_idx must match pool_idx_for_oid's assignment).
        agent_disk_t(std::pmr::memory_resource* resource,
                     const path_t& path_db,
                     log_t& log,
                     agent_role_t role,
                     std::size_t pool_idx);

        ~agent_disk_t();

        /// Bootstrap-only probe for whether this agent owns `oid`'s storage (not a mailbox handler).
        [[nodiscard]] bool has_storage_sync(components::catalog::oid_t oid) const noexcept;

        // An open cursor holds an ABSOLUTE row position that compact()'s row swap would shift; checkpoint_inner defers.
        [[nodiscard]] bool has_active_scan_for_oid(components::catalog::oid_t oid) const noexcept {
            for (const auto& [_cursor, scan] : active_scans_) {
                if (scan.table_oid == oid) {
                    return true;
                }
            }
            return false;
        }

        // Raw pointer into storages_, nullptr if not owned, race-free (mailbox serializes writes); borrowed only.
        [[nodiscard]] const collection_storage_entry_t*
        storage_entry_sync(components::catalog::oid_t oid) const noexcept;

        [[nodiscard]] bool
        bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                  const std::filesystem::path& otbx_path,
                                  wal::id_t sidecar_wal_id,
                                  bool sidecar_readable,
                                  std::vector<components::table::column_definition_t> catalog_columns,
                                  bool is_computed) noexcept;

        // is_computed (relkind='g') is resolved by the caller, never inferred from an empty column set.
        [[nodiscard]] bool bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                                            std::vector<components::table::column_definition_t> columns,
                                                            const std::filesystem::path& otbx_path,
                                                            bool is_computed) noexcept;

        // Built with the agent's own resource(), so nothing crosses the mailbox; false on a duplicate key.
        unique_future<bool> create_storage_disk_inner(components::catalog::oid_t oid,
                                                      std::vector<components::table::column_definition_t> columns,
                                                      std::filesystem::path otbx_path,
                                                      bool is_computed);

        // WAL-replay only, before scheduler.start; the storage_* handlers replace these after.
        [[nodiscard]] core::error_t no_replay_storage_error(const char* who, components::catalog::oid_t table_oid);
        [[nodiscard]] core::error_t direct_delete_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       uint64_t count,
                                                       const components::table::transaction_data& txn);
        [[nodiscard]] core::error_t direct_update_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       components::vector::data_chunk_t& new_data);
        // Re-applies schema_chunk's columns (0-row) ahead of the dependent PHYSICAL_INSERT; idempotent by name.
        [[nodiscard]] core::error_t direct_add_column_sync(components::catalog::oid_t table_oid,
                                                           const components::vector::data_chunk_t& schema_chunk);

        // Mutation handlers: a not-owned oid REFUSES on every leg below; only an empty request succeeds as a no-op.
        unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append_inner(execution_context_t ctx,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::vector::data_chunk_t> data);

        unique_future<void>
        storage_publish_commits_inner(uint64_t commit_id,
                                      std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void> storage_publish_deletes_inner(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          std::pmr::vector<components::catalog::oid_t> tables);

        // MVCC delete abort — un-stamps this txn's pending deletes back to NOT_DELETED_ID.
        unique_future<void> storage_revert_deletes_inner(uint64_t txn_id,
                                                         std::pmr::vector<components::catalog::oid_t> tables);

        unique_future<void>
        storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        // Wraps storage_t::update's (updated, appended) pair; (0, 0) means an EMPTY chunk, not "no storage".
        unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update_inner(components::catalog::oid_t table_oid,
                             components::vector::vector_t row_ids,
                             std::unique_ptr<components::vector::data_chunk_t> data,
                             components::table::transaction_data txn);

        // A count below what was requested is not a refusal — an already-stamped row is skipped by design.
        unique_future<core::result_wrapper_t<uint64_t>>
        storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                  components::vector::vector_t row_ids,
                                  uint64_t count,
                                  components::table::transaction_data txn);

        // SNAPSHOT drops invisible rows; RAW keeps all. `expected_compact_epoch` is checked atomically with the read.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch_inner(components::catalog::oid_t table_oid,
                            components::vector::vector_t row_ids,
                            uint64_t count,
                            std::vector<size_t> projected_cols,
                            components::table::transaction_data txn,
                            components::table::fetch_visibility_t visibility,
                            int64_t limit,
                            uint64_t expected_compact_epoch);

        // Reply wraps ≤DEFAULT_VECTOR_CAPACITY batches; scan OOM/corruption travels back as a value, never a throw.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_scan_inner(components::catalog::oid_t table_oid,
                           std::unique_ptr<components::table::table_filter_t> filter,
                           int64_t limit,
                           std::vector<size_t> projected_cols,
                           components::table::transaction_data txn);

        // cursor_id==0 opens (mints a cursor, first batch); nonzero advances. A drained cursor
        // (exhausted / limit reached) erases the entry and replies an EMPTY chunk + cursor_id.
        // OPEN on a not-owned oid REFUSES, so it can't be misread as "this table is empty".
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch_inner(session_id_t session,
                                       components::catalog::oid_t table_oid,
                                       uint64_t cursor_id,
                                       std::unique_ptr<components::table::table_filter_t> filter,
                                       int64_t limit,
                                       std::vector<size_t> projected_cols,
                                       components::table::transaction_data txn);

        // Releases an abandoned fetch-next cursor and lifts the compact() gate; idempotent (unknown id = no-op).
        unique_future<void> storage_close_cursor_inner(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       uint64_t cursor_id);

        // Mints a position-less hold so checkpoint_inner defers compact(); released by storage_close_cursor_inner.
        unique_future<core::result_wrapper_t<uint64_t>>
        storage_open_scan_hold_inner(session_id_t session, components::catalog::oid_t table_oid);

        unique_future<core::result_wrapper_t<uint64_t>>
        storage_compact_epoch_inner(session_id_t session, components::catalog::oid_t table_oid);

        // Replies ALL final GROUP BY rows in ONE reply; refuses rather than folding empty input into a false "COUNT=0".
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce_inner(session_id_t session,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::table::table_filter_t> filter,
                             std::vector<size_t> projected_cols,
                             components::table::transaction_data txn,
                             components::operators::pushed_aggregate_spec_t spec);

        // Batched keyed scan for one owned table via fk_hash_semijoin.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys_inner(components::catalog::oid_t table_oid,
                           std::pmr::vector<std::string> key_col_names,
                           components::vector::data_chunk_t keys,
                           components::table::transaction_data txn);

        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::uint64_t> key_col_indices,
                                 components::vector::data_chunk_t keys,
                                 std::pmr::vector<std::uint64_t> projected_cols,
                                 components::table::transaction_data txn);

        // result.size() == keys.size() always; an empty entry means only that key matched nothing.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                  std::pmr::vector<std::uint64_t> key_col_indices,
                                  components::vector::data_chunk_t keys,
                                  std::pmr::vector<std::uint64_t> projected_cols,
                                  components::table::transaction_data txn);

        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types_inner(components::catalog::oid_t table_oid);

        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows_inner(components::catalog::oid_t table_oid);

        unique_future<checkpoint_result_t>
        checkpoint_inner(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);

        unique_future<void> vacuum_inner(session_id_t session, uint64_t lowest_active_start_time);

        // Compacts nothing (see vacuum_inner); kept only because operator_commit_transaction still sends it.
        unique_future<void> maybe_cleanup_inner(components::catalog::oid_t table_oid, uint64_t compact_watermark);

        unique_future<void> on_horizon_advanced_inner(uint64_t new_horizon);

        unique_future<void> storage_dropped_committed_inner(uint64_t txn_id, uint64_t commit_id);

        unique_future<void> storage_drop_aborted_inner(uint64_t txn_id);

        // Not a mailbox handler; called pre-start by base_spaces and at runtime by mark_storage_dropped_many_inner.
        void register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                 uint64_t dropped_at_commit_id,
                                                 std::filesystem::path path,
                                                 std::pmr::vector<std::filesystem::path> sidecar_paths);

        unique_future<void> drop_storage_many_inner(std::pmr::vector<components::catalog::oid_t> oids);

        unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row_inner(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    components::vector::data_chunk_t row);

        unique_future<core::result_wrapper_t<std::uint64_t>>
        delete_pg_catalog_rows_inner(execution_context_t ctx,
                                     components::catalog::oid_t table_oid,
                                     std::int64_t oid_col_idx,
                                     components::catalog::oid_t target_oid);

        // STEP 4 of operator_commit_transaction_t, BELOW the commit marker: a failure is reported, never un-commits.
        unique_future<core::error_t>
        update_pg_attribute_commit_id_field_inner(execution_context_t ctx,
                                                  components::catalog::oid_t attoid,
                                                  components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                                  std::uint64_t commit_id);

        // Missing/already-compact returns 0; no gate refuses file-backed tables (see the definition).
        unique_future<std::uint64_t> compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                                     std::set<std::string> live_attnames);

        // Same entry->drop_column primitive as the compact leg above, but NAMES the column, so
        // there's no live set to re-derive and no gap that could drop a surviving one instead.
        //   true  = the column was in the schema and is gone;
        //   false = the storage exists but never carried it (an ALTER ADD COLUMN that never
        //           materialized), so nothing physical to release;
        //   error = no materialized storage for the oid here (disk_contract.hpp).
        unique_future<core::result_wrapper_t<bool>> drop_storage_column_inner(components::catalog::oid_t table_oid,
                                                                             std::string attname);

        // Physical half of ALTER TABLE RENAME COLUMN (keeps the storage's cached name in step with the catalog's).
        //   true  = renamed;
        //   false = the storage exists but never carried `old_attname` (an ALTER ADD COLUMN
        //           with no materializing INSERT yet is legitimately nothing to rename);
        //   error = no materialized storage for the oid, or `new_attname` already exists.
        unique_future<core::result_wrapper_t<bool>> rename_storage_column_inner(components::catalog::oid_t table_oid,
                                                                                std::string old_attname,
                                                                                std::string new_attname);

        unique_future<void> mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                                            uint64_t dropped_at_commit_id);

        // Also parks the column's TYPE AND DEFAULT (ENCODED, decoded here) for table_storage_adapter_t.
        unique_future<void>
        note_column_identity_inner(components::catalog::oid_t table_oid,
                                   std::string attname,
                                   std::uint32_t attoid,
                                   components::pg_attribute_commit_id_backfill_t::added_column_type_t type);

        // Bootstrap-only, wired before scheduler.start, read-only after; a mailbox handle, so copying it is safe.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap-only; the CATALOG agent uses it to write physical WAL records on the agent thread directly.
        void set_manager_wal_sync(actor_zeta::address_t address);

        using dispatch_traits = actor_zeta::dispatch_traits<&agent_disk_t::storage_append_inner,
                                                            &agent_disk_t::storage_publish_commits_inner,
                                                            &agent_disk_t::storage_publish_deletes_inner,
                                                            &agent_disk_t::storage_revert_deletes_inner,
                                                            &agent_disk_t::storage_revert_appends_inner,
                                                            &agent_disk_t::storage_update_inner,
                                                            &agent_disk_t::storage_delete_rows_inner,
                                                            &agent_disk_t::storage_fetch_inner,
                                                            &agent_disk_t::storage_scan_inner,
                                                            &agent_disk_t::storage_fetch_next_batch_inner,
                                                            &agent_disk_t::storage_close_cursor_inner,
                                                            &agent_disk_t::storage_reduce_inner,
                                                            &agent_disk_t::scan_by_keys_inner,
                                                            &agent_disk_t::read_chunks_by_key_inner,
                                                            &agent_disk_t::read_chunks_by_keys_inner,
                                                            &agent_disk_t::storage_types_inner,
                                                            &agent_disk_t::storage_total_rows_inner,
                                                            &agent_disk_t::checkpoint_inner,
                                                            &agent_disk_t::vacuum_inner,
                                                            &agent_disk_t::maybe_cleanup_inner,
                                                            &agent_disk_t::on_horizon_advanced_inner,
                                                            &agent_disk_t::storage_dropped_committed_inner,
                                                            &agent_disk_t::storage_drop_aborted_inner,
                                                            &agent_disk_t::drop_storage_many_inner,
                                                            &agent_disk_t::append_pg_catalog_row_inner,
                                                            &agent_disk_t::delete_pg_catalog_rows_inner,
                                                            &agent_disk_t::update_pg_attribute_commit_id_field_inner,
                                                            &agent_disk_t::compact_relkind_g_storage_inner,
                                                            &agent_disk_t::drop_storage_column_inner,
                                                            &agent_disk_t::rename_storage_column_inner,
                                                            &agent_disk_t::mark_storage_dropped_many_inner,
                                                            &agent_disk_t::note_column_identity_inner,
                                                            &agent_disk_t::create_storage_disk_inner,
                                                            // Appended LAST — positional msg ids.
                                                            &agent_disk_t::storage_open_scan_hold_inner,
                                                            &agent_disk_t::storage_compact_epoch_inner>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // Non-mailbox, called directly on the agent thread; `filter`/`projected_cols` may be nullptr (all columns).
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
        scan_local(components::catalog::oid_t table_oid,
                   components::table::table_filter_t* filter,
                   int64_t limit,
                   const std::vector<std::size_t>* projected_cols,
                   const components::table::transaction_data& txn);

        void drop_storage_one_local(components::catalog::oid_t oid);

        void mark_storage_dropped_one_local(components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);

        log_t log_;
        path_t path_;

        // Role isn't stored: it's `pool_idx_ == 0` by construction (idx 0 = CATALOG; see agent_role_t).
        std::size_t pool_idx_;

        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // Per-cursor state for storage_fetch_next_batch_inner. POSITION-ONLY: holds no buffered
        // batches, only the resume position `pos`; each fetch rebuilds a TRANSIENT scan state
        // from it, so peak memory is one batch and no pins survive a mailbox round-trip.
        struct active_scan_t {
            // attoid (0 if never stamped) is the primary column identity (survives a RENAME);
            // when 0, (name, type) disambiguates instead.
            struct open_column_t {
                std::uint32_t attoid{0};
                std::string name;
            };
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID}; // gates compact() on this oid
            components::storage::scan_position_t pos; // absolute resume position
            std::unique_ptr<components::table::table_filter_t>
                filter;                                    // owned; rebound per fetch
            std::vector<std::size_t> projected_cols;       // empty == all columns
            components::table::transaction_data txn{0, 0}; // MVCC snapshot for the whole scan
            int64_t matched_limit{-1};                     // post-filter matched-row cap (-1 == unbounded)
            uint64_t matched_emitted{0};                   // running matched rows handed out (enforces matched_limit)
            // OPEN-time schema snapshot: projection/filter are bound positionally against it, so
            // every fetch re-checks the live schema against this snapshot before answering.
            std::vector<open_column_t> open_columns;
            std::vector<components::types::complex_logical_type> open_types;
        };
        std::pmr::unordered_map<uint64_t, active_scan_t> active_scans_;
        uint64_t next_scan_cursor_id_{1}; // (session, counter); 0 is the OPEN-request sentinel


        std::pmr::vector<dropped_storage_entry_t> dropped_storages_;

        // Empty by default so test fixtures without a dispatcher pass cleanly (gates the ack path).
        actor_zeta::address_t manager_dispatcher_addr_{actor_zeta::address_t::empty_address()};

        actor_zeta::address_t manager_wal_addr_{actor_zeta::address_t::empty_address()};
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
