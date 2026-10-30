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
#include <span>
#include <string>
#include <unordered_map>

namespace services::disk {

#ifdef DEV_MODE
    // Counts checkpoint rounds, not tables; a round rewrites every table whole (O(data)).
    uint64_t table_checkpoints() noexcept;
    void reset_table_checkpoints() noexcept;

    uint64_t publish_revert_misses() noexcept;
    void reset_publish_revert_misses() noexcept;

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

    using session_id_t = components::session::session_id_t;
    using execution_context_t = components::execution_context_t;

#ifdef DEV_MODE
    // The rows of every page the disk answered of a taken block (storage_run_block).
    uint64_t disk_block_rows() noexcept;
    void reset_disk_block_rows() noexcept;

    uint64_t catalog_key_scans() noexcept;
    void reset_catalog_key_scans() noexcept;
#endif

    // Forward-declared in manager_disk.hpp.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    fk_hash_semijoin(std::pmr::memory_resource* resource,
                     components::storage::storage_t& storage,
                     const std::pmr::vector<std::uint64_t>& key_col_indices,
                     components::vector::data_chunk_t& keys,
                     components::table::transaction_data txn);

    // min_prev_checkpoint_wal_id is the min over this agent's entries.
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

        agent_disk_t(std::pmr::memory_resource* resource, const path_t& path_db, log_t& log);

        agent_disk_t(std::pmr::memory_resource* resource,
                     const path_t& path_db,
                     log_t& log,
                     agent_role_t role,
                     std::size_t pool_idx);

        ~agent_disk_t();

        // Bootstrap and WAL replay reach the slice only through this; it is built from manager_disk_t::bootstrap_t.
        class bootstrap_t;

        unique_future<core::result_wrapper_t<bool>>
        // `relname`: what the table's refusals name it by (PostgreSQL 18's relation "t"); empty keeps the default.
        create_storage_disk_inner(components::catalog::oid_t oid,
                                  std::vector<components::table::column_definition_t> columns,
                                  std::filesystem::path otbx_path,
                                  bool is_computed,
                                  std::string relname);

        // Mutation handlers: a not-owned oid REFUSES on every leg below; only an empty request succeeds as a no-op.
        // `attoids` name the payload's columns: the table's visible ones, in order (a computed table: none).
        // One message is one write: every chunk is checked before any is journalled, and a chunk that fails to
        // materialize takes the ones before it back with it.
        unique_future<core::result_wrapper_t<write_receipt_t>>
        storage_append_inner(execution_context_t ctx,
                             components::catalog::oid_t table_oid,
                             std::pmr::vector<components::catalog::oid_t> attoids,
                             std::pmr::vector<components::vector::data_chunk_t> data,
                             bool returning);

        unique_future<void>
        storage_publish_commits_inner(uint64_t commit_id,
                                      std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<void> storage_publish_deletes_inner(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          std::pmr::vector<components::catalog::oid_t> tables);

        // Un-stamps back to NOT_DELETED_ID.
        unique_future<void> storage_revert_deletes_inner(uint64_t txn_id,
                                                         uint64_t from_sub_id,
                                                         std::pmr::vector<components::catalog::oid_t> tables);

        unique_future<core::error_t>
        storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        unique_future<core::result_wrapper_t<update_receipt_t>>
        storage_update_inner(components::catalog::oid_t table_oid,
                             std::pmr::vector<components::vector::vector_t> row_ids,
                             std::pmr::vector<components::vector::data_chunk_t> data,
                             components::table::transaction_data txn,
                             bool returning);

        unique_future<core::result_wrapper_t<write_receipt_t>>
        storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                  components::table::row_id_batch_t row_ids,
                                  components::table::transaction_data txn,
                                  bool returning);

        // SNAPSHOT drops invisible rows; RAW keeps all. `expected_compact_epoch` is checked atomically with the read.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch_inner(components::catalog::oid_t table_oid,
                            components::table::row_id_batch_t row_ids,
                            std::vector<size_t> projected_cols,
                            components::table::transaction_data txn,
                            components::table::fetch_visibility_t visibility,
                            int64_t limit,
                            uint64_t expected_compact_epoch);

        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_scan_inner(components::catalog::oid_t table_oid,
                           int64_t limit,
                           std::vector<size_t> projected_cols,
                           components::table::transaction_data txn);

        // cursor_id==0 opens (mints a cursor); nonzero advances. A drained cursor erases the entry and
        // replies an EMPTY chunk + cursor_id; OPEN on a not-owned oid REFUSES (never reads as "empty table").
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch_inner(session_id_t session,
                                       components::catalog::oid_t table_oid,
                                       uint64_t cursor_id,
                                       int64_t limit,
                                       std::vector<size_t> projected_cols,
                                       components::table::transaction_data txn);

        // Lifts the compact() gate; idempotent (unknown id = no-op).
        unique_future<void>
        storage_close_cursor_inner(session_id_t session, components::catalog::oid_t table_oid, uint64_t cursor_id);

        // Mints a position-less hold so checkpoint_inner defers compact(); released by storage_close_cursor_inner.
        unique_future<core::result_wrapper_t<uint64_t>>
        storage_open_scan_hold_inner(session_id_t session, components::catalog::oid_t table_oid);

        unique_future<core::result_wrapper_t<uint64_t>>
        storage_compact_epoch_inner(session_id_t session, components::catalog::oid_t table_oid);

        // A block the disk took (manager_disk_t::storage_offer), as a SELECT block of one table with only the clauses
        // the disk runs: a WHERE, and a GROUP BY. Its first page; a missing storage refuses with internal_error.
        // A GROUP BY is reduced whole in this one call (no compact can move a row under it), its rows answered page
        // by page; a WHERE reads under a cursor gated like a scan's. The cursor carries this agent's pool index in its
        // top 8 bits.
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_run_block_inner(execution_context_t ctx,
                                components::logical_plan::detached_block_t block,
                                components::vector::storage_parameters parameters);
        // The next page of a taken block; a drained cursor answers an empty chunk.
        unique_future<core::result_wrapper_t<fetch_batch_t>> storage_block_page_inner(execution_context_t ctx,
                                                                                      uint64_t cursor_id);
        unique_future<void> storage_close_block_inner(uint64_t cursor_id);

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
                                  std::pmr::vector<components::vector::data_chunk_t> keys,
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

        unique_future<void> storage_drop_aborted_inner(uint64_t txn_id,
                                                       std::pmr::vector<components::catalog::oid_t> oids);

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

        unique_future<core::result_wrapper_t<components::storage::appended_range_t>>
        update_pg_attribute_commit_id_field_inner(execution_context_t ctx,
                                                  components::catalog::oid_t attoid,
                                                  components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                                  std::uint64_t commit_id);

        unique_future<core::result_wrapper_t<std::uint64_t>>
        compact_relkind_g_storage_inner(components::catalog::oid_t table_oid, std::set<std::string> live_attnames);

        // NAMES the column instead of taking the live set: no set to re-derive, so no gap that
        // could drop a surviving column instead.
        //   true  = the column was in the schema and is gone;
        //   false = the storage exists but never carried it (an ALTER ADD COLUMN that never
        //           materialized), so nothing physical to release;
        //   error = no materialized storage for the oid here (disk_contract.hpp).
        unique_future<core::result_wrapper_t<bool>> drop_storage_column_inner(components::catalog::oid_t table_oid,
                                                                              std::string attname);

        // Keeps the storage's cached name in step with the catalog's.
        //   true  = renamed;
        //   false = the storage exists but never carried `old_attname` (an ALTER ADD COLUMN
        //           with no materializing INSERT yet is legitimately nothing to rename);
        //   error = no materialized storage for the oid, or `new_attname` already exists.
        unique_future<core::result_wrapper_t<bool>> rename_storage_column_inner(components::catalog::oid_t table_oid,
                                                                                std::string old_attname,
                                                                                std::string new_attname);

        unique_future<void> mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                                            uint64_t dropped_at_commit_id);

        unique_future<core::error_t> add_storage_column_inner(execution_context_t ctx,
                                                              components::catalog::oid_t table_oid,
                                                              components::table::column_definition_t column);

        unique_future<core::error_t> stamp_column_dropped_inner(components::catalog::oid_t table_oid,
                                                                components::catalog::oid_t attoid,
                                                                components::table::transaction_data txn);

        unique_future<void> publish_column_stamps_inner(uint64_t txn_id,
                                                        uint64_t commit_id,
                                                        std::pmr::vector<components::catalog::oid_t> tables);

        unique_future<void> revert_column_stamps_inner(uint64_t txn_id,
                                                       uint64_t from_sub_id,
                                                       std::pmr::vector<components::catalog::oid_t> tables);

        unique_future<core::error_t> storage_prepare_inner(components::table::transaction_data txn,
                                                           std::pmr::vector<components::catalog::oid_t> tables);

        unique_future<void> storage_release_prepared_inner(uint64_t txn_id,
                                                           std::pmr::vector<components::catalog::oid_t> tables);

        unique_future<core::error_t> storage_set_not_null_inner(components::table::transaction_data txn,
                                                                components::catalog::oid_t table_oid,
                                                                components::catalog::oid_t attoid);

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
                                                            &agent_disk_t::add_storage_column_inner,
                                                            &agent_disk_t::stamp_column_dropped_inner,
                                                            &agent_disk_t::publish_column_stamps_inner,
                                                            &agent_disk_t::create_storage_disk_inner,
                                                            // Appended LAST — positional msg ids.
                                                            &agent_disk_t::storage_open_scan_hold_inner,
                                                            &agent_disk_t::storage_compact_epoch_inner,
                                                            &agent_disk_t::revert_column_stamps_inner,
                                                            &agent_disk_t::storage_prepare_inner,
                                                            &agent_disk_t::storage_release_prepared_inner,
                                                            &agent_disk_t::storage_set_not_null_inner,
                                                            &agent_disk_t::storage_run_block_inner,
                                                            &agent_disk_t::storage_block_page_inner,
                                                            &agent_disk_t::storage_close_block_inner>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // An open cursor holds an absolute row position that compact()'s row swap would shift; checkpoint_inner defers.
        [[nodiscard]] bool has_active_scan_for_oid(components::catalog::oid_t oid) const noexcept {
            for (const auto& [_cursor, scan] : active_scans_) {
                if (scan.table_oid == oid) {
                    return true;
                }
            }
            return false;
        }

        // is_computed (relkind='g') is resolved by the caller, never inferred from an empty column set.
        // false: the oid is already in this slice; an error: the storage could not be constructed.
        [[nodiscard]] core::result_wrapper_t<bool>
        create_local(components::catalog::oid_t oid,
                     std::vector<components::table::column_definition_t> columns,
                     const std::filesystem::path& otbx_path,
                     bool is_computed) noexcept;

        // false: the oid is already in this slice; an error: the .otbx could not be opened.
        [[nodiscard]] core::result_wrapper_t<bool>
        load_local(components::catalog::oid_t oid,
                   const std::filesystem::path& otbx_path,
                   wal::id_t sidecar_wal_id,
                   bool sidecar_readable,
                   std::vector<components::table::column_definition_t> catalog_columns,
                   bool is_computed) noexcept;

        [[nodiscard]] collection_storage_entry_t* entry_local(components::catalog::oid_t oid) noexcept;

        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
        scan_local(components::catalog::oid_t table_oid,
                   components::table::table_filter_t* filter,
                   int64_t limit,
                   const std::vector<std::size_t>* projected_cols,
                   const components::table::transaction_data& txn);

        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
        read_by_keys_local(components::catalog::oid_t table_oid,
                           const std::pmr::vector<std::uint64_t>& key_col_indices,
                           std::span<const components::vector::data_chunk_t> keys,
                           const std::pmr::vector<std::uint64_t>& projected_cols,
                           const components::table::transaction_data& txn);

        void drop_storage_one_local(components::catalog::oid_t oid);

        // A cursor of `entry`'s table at its first row, under txn's snapshot; `limit` caps the rows `filter` keeps.
        void open_scan_(uint64_t cursor_id,
                        components::catalog::oid_t table_oid,
                        collection_storage_entry_t& entry,
                        std::unique_ptr<components::table::table_filter_t> filter,
                        int64_t limit,
                        std::vector<size_t> projected_cols,
                        components::table::transaction_data txn);
        // The cursor's next page; a drained or unknown cursor answers an empty chunk and is forgotten.
        core::result_wrapper_t<fetch_batch_t> advance_scan_(uint64_t cursor_id);
        // Every cursor of the agent (a scan's, a hold's, a taken block's): its pool index in the top 8 bits, by which
        // the manager routes it, then one counter; never 0, never a live one.
        uint64_t mint_cursor_();
        // The next page of a taken block: of its GROUP BY's rows, or of its WHERE's cursor.
        core::result_wrapper_t<fetch_batch_t> next_block_page_(uint64_t cursor_id);

        void mark_storage_dropped_one_local(components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);
        // The checkpoint sidecars to sweep are derived from `path` here, the one place both DROP paths share.
        void register_dropped_local(components::catalog::oid_t oid,
                                    uint64_t dropped_at_commit_id,
                                    std::filesystem::path path);

        log_t log_;
        path_t path_;

        std::size_t pool_idx_;

        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // POSITION-ONLY: holds just `pos`; each fetch rebuilds a transient scan, so peak memory is one batch.
        struct active_scan_t {
            // attoid (0 = never stamped) is the identity that survives a RENAME; 0 falls back to (name, type).
            struct open_column_t {
                std::uint32_t attoid{0};
                std::string name;
            };
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID}; // gates compact() on this oid
            components::storage::scan_position_t pos;                               // absolute resume position
            std::unique_ptr<components::table::table_filter_t> filter;              // owned; rebound per fetch
            std::vector<std::size_t> projected_cols;                                // empty == all columns
            components::table::transaction_data txn{0, 0};                          // MVCC snapshot for the whole scan
            int64_t matched_limit{-1};   // post-filter matched-row cap (-1 == unbounded)
            uint64_t matched_emitted{0}; // running matched rows handed out (enforces matched_limit)
            // OPEN-time schema snapshot; projection/filter bind positionally against it, re-checked each fetch.
            std::vector<open_column_t> open_columns;
            std::vector<components::types::complex_logical_type> open_types;
        };
        std::pmr::unordered_map<uint64_t, active_scan_t> active_scans_;

        // The rows of a taken GROUP BY not yet answered.
        struct block_pages_t {
            std::pmr::vector<components::vector::data_chunk_t> pages;
            std::size_t next{0};
        };
        std::pmr::unordered_map<uint64_t, block_pages_t> block_pages_;
        uint64_t next_cursor_{1};

        std::pmr::vector<dropped_storage_entry_t> dropped_storages_;

        actor_zeta::address_t manager_dispatcher_addr_{actor_zeta::address_t::empty_address()};

        actor_zeta::address_t manager_wal_addr_{actor_zeta::address_t::empty_address()};
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
