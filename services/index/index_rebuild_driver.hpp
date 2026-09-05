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
    // data_table_t::compact rebuilds a table at row id 0 and renumbers every surviving row.
    // Physical row ids are what an index entry stores -- in the agent's tree and in its
    // on-disk directory alike -- so the instant a compacting checkpoint round commits, every
    // index of every table it touched names the wrong row. The two shapes of the resulting
    // wrong answer are both SILENT: an id that maps to no row group is dropped by
    // collection_t::fetch (a short answer), and an id that now belongs to a different
    // surviving row is gathered as if it were the match (a wrong answer).
    //
    // WHY THIS IS A SHARED FUNCTION AND NOT A LOOP IN AN OPERATOR. Compaction happens in
    // exactly one place (agent_disk_t::checkpoint_inner) but is REACHED from two:
    // operator_checkpoint_t, which owns the SQL statement and the shutdown checkpoint, and
    // manager_wal_replicate_t::run_auto_checkpoint, which earns a WAL truncation boundary
    // when the log outgrows auto_checkpoint_threshold_bytes. The rebuild used to be written
    // out longhand inside the operator, and the second caller -- which describes itself as
    // the "self-orchestrated analogue" of the first -- simply did not have it. Every
    // auto-checkpoint therefore renumbered indexed tables and left their indexes behind. One
    // function, called by both, is what makes that class of omission impossible rather than
    // merely fixed once.
    //
    // WHAT IT DOES, per table reported by manager_index_t::all_indexed_oids (which already
    // excludes oids mid-DROP): drain the streaming scan leg (storage_fetch_next_batch) to
    // completion, then hand every chunk to manager_index_t::repopulate_table, which clears
    // the agents' stores and re-stages each row under the PHYSICAL id the scan stamped into
    // chunk.row_ids. A table with no rows still goes through: the clear is what removes the
    // stale entries, and repopulate_table expects the empty feed.
    //
    // WHERE THE CALL GOES IN THE ROUND, which is the half a shared function cannot enforce
    // for its callers and therefore the half they diverged on twice.
    //
    //     compaction (agent_disk_t::checkpoint_inner)  ->  THIS CALL  ->  WAL truncation
    //
    // AFTER the compaction, because that is what renumbers the rows this re-stages.
    // BEFORE the truncation, because the truncation is the round's only destructive step and
    // this call is the round's last chance to fail recoverably. Between the compact
    // committing its .otbx header and this driver's force_flush landing, the durable state is
    // a post-compact table under pre-compact indexes; whatever ends the round inside that
    // window — an error returned from here, or a kill -9 — must not ALSO have destroyed
    // journal segments. operator_checkpoint_t ran the truncation FIRST until
    // test_checkpoint_rebuild_before_truncate pinned the order;
    // manager_wal_replicate_t::run_auto_checkpoint has always had it right, at step (c2)
    // ahead of step (d).
    //
    // WHAT THE ORDER DOES NOT BUY, so nobody reads more into it than it says: it does not
    // shorten the window. A crash after the compaction is durable and before this driver's
    // flush lands leaves indexes naming pre-compact rows whether the journal was trimmed or
    // not, and NOTHING repairs that on the next start — base_spaces rebuilds no index at
    // startup (manager_index.hpp records the removal of the pass that used to look like it
    // did) and WAL replay maintains none either. Closing that window needs a durable
    // "these oids were renumbered and not yet rebuilt" fact that bootstrap can act on; it
    // is not something a call order can express.
    //
    // DRAINED-OR-RELEASED, never abandoned: a live fetch-next cursor gates compact() on its
    // oid, so a leaked one would wedge the very table the next round has to reclaim. The
    // error exit closes the cursor before returning.
    //
    // `txn` is the snapshot the rebuild scan reads under. An index answers a SUPERSET and
    // never filters by visibility (the table decides what a reader may see), so the right
    // snapshot is "every committed row, no uncommitted one" -- see
    // committed_rows_snapshot() below for a caller that has no statement of its own.
    //
    // Returns the first error any leg reported, or no_error(). It is RETURNED rather than
    // logged because an index that could not be rebuilt disagrees with its table, and only
    // the caller knows whether that fails a statement or aborts a maintenance round.
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    repopulate_indexes_after_compaction(std::pmr::memory_resource* resource,
                                        actor_zeta::actor::address_t disk_address,
                                        actor_zeta::actor::address_t index_address,
                                        components::session::session_id_t session,
                                        components::table::transaction_data txn,
                                        core::date::timezone_offset_t session_tz);

    // The snapshot for a rebuild driven by something that is not a SQL statement and so has
    // no transaction of its own (the WAL auto-checkpoint). It is not a "see everything"
    // wildcard -- transaction_version_operator::use_inserted_version reads exactly three
    // fields, and this pins all three:
    //   * transaction_id 0      -- no self-writes to admit;
    //   * snapshot_horizon      -- one below TRANSACTION_ID_START, i.e. at or above every
    //                              commit id that can exist and below every PENDING txn id,
    //                              so committed rows pass rule 3 and uncommitted ones are
    //                              already refused by rule 2;
    //   * in_flight_snapshot {} -- nothing committed-but-unpublished is held back.
    // The result is precisely "every committed row and no uncommitted one", which is the
    // superset an index is supposed to hold.
    [[nodiscard]] components::table::transaction_data committed_rows_snapshot() noexcept;

} // namespace services::index
