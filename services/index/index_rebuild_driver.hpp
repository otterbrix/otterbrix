#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/date/timezones.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::index {

    // THE ONE DRIVER FOR "THE TABLES WERE JUST COMPACTED, REBUILD THEIR INDEXES".
    //
    // data_table_t::compact rebuilds a table at row id 0 and renumbers every surviving row, and a
    // physical row id is what an index entry stores. Both shapes of the resulting wrong answer are
    // SILENT: an id that maps to no row group is dropped by collection_t::fetch (a short answer),
    // and an id that now belongs to a different surviving row is gathered as if it were the match.
    //
    // A SHARED FUNCTION, NOT A LOOP IN AN OPERATOR. Compaction happens in exactly one place
    // (agent_disk_t::checkpoint_inner) but is REACHED from two: operator_checkpoint_t and
    // manager_wal_replicate_t::run_auto_checkpoint. Written out longhand inside one, the other
    // silently does not have it -- and an auto-checkpoint without it renumbers indexed tables and
    // leaves their indexes behind.
    //
    // Per table reported by manager_index_t::all_indexed_oids (which already excludes oids
    // mid-DROP): drain the streaming scan leg (storage_fetch_next_batch), then hand every chunk to
    // manager_index_t::repopulate_table, which clears the agents' stores and re-stages each row
    // under the PHYSICAL id the scan stamped into chunk.row_ids. A table with no rows still goes
    // through: the clear is what removes the stale entries.
    //
    // WHERE THE CALL GOES IN THE ROUND, the half a shared function cannot enforce for its callers
    // and therefore the half they diverged on twice:
    //
    //     compaction (agent_disk_t::checkpoint_inner)  ->  THIS CALL  ->  WAL truncation
    //
    // AFTER the compaction, which is what renumbers the rows this re-stages. BEFORE the truncation,
    // the round's only destructive step: while the durable state is a post-compact table under
    // pre-compact indexes, whatever ends the round -- an error returned from here, or a kill -9 --
    // must not ALSO have destroyed journal segments. test_checkpoint_rebuild_before_truncate pins
    // this for operator_checkpoint_t; run_auto_checkpoint keeps it at step (c2) ahead of step (d).
    //
    // The order does not SHORTEN that window; what reaches past the round's own death is a durable
    // "renumbered and not yet rebuilt" marker, armed inside flush_all_indexes and cleared per table
    // inside repopulate_table once that table's agents have published and force_flushed.
    // base_spaces::bootstrap_indexes_sync READS it and declines to wire the indexes it names, so a
    // crash here costs full scans and an error line instead of silent wrong answers. See
    // manager_index_t::rebuild_marker_path_ for the file, the union rule and the price.
    //
    // DRAINED-OR-RELEASED, never abandoned: a live fetch-next cursor gates compact() on its oid, so
    // a leaked one would wedge the very table the next round has to reclaim. The error exit closes
    // the cursor before returning.
    //
    // `txn` is the snapshot the rebuild scan reads under. An index answers a SUPERSET and never
    // filters by visibility, so the right snapshot is "every committed row, no uncommitted one" --
    // see committed_rows_snapshot() below. Returns the first error any leg reported, or no_error();
    // RETURNED rather than logged because only the caller knows whether a stale index fails a
    // statement or aborts a maintenance round.
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    repopulate_indexes_after_compaction(std::pmr::memory_resource* resource,
                                        actor_zeta::actor::address_t disk_address,
                                        actor_zeta::actor::address_t index_address,
                                        components::session::session_id_t session,
                                        components::table::transaction_data txn,
                                        core::date::timezone_offset_t session_tz);

    // The snapshot for a rebuild driven by something that is not a SQL statement and so has no
    // transaction of its own (the WAL auto-checkpoint). Not a "see everything" wildcard:
    // transaction_version_operator::use_inserted_version reads exactly three fields, and this
    // pins all three:
    //   * transaction_id 0      -- no self-writes to admit;
    //   * snapshot_horizon      -- one below TRANSACTION_ID_START, i.e. at or above every
    //                              commit id that can exist and below every PENDING txn id, so
    //                              committed rows pass rule 3 and uncommitted ones are already
    //                              refused by rule 2;
    //   * in_flight_snapshot {} -- nothing committed-but-unpublished is held back.
    [[nodiscard]] components::table::transaction_data committed_rows_snapshot() noexcept;

} // namespace services::index
