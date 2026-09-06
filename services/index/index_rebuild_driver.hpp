#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/date/timezones.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::index {

    class committed_rows_snapshot_t;

    // The one constructor of committed_rows_snapshot_t. Not a "see everything" wildcard:
    // transaction_version_operator::use_inserted_version reads exactly three fields, pinned here:
    //   * transaction_id 0      -- no self-writes to admit;
    //   * snapshot_horizon      -- one below TRANSACTION_ID_START, so committed rows pass case 3
    //                              and uncommitted ones are already refused by case 2;
    //   * in_flight_snapshot {} -- nothing committed-but-unpublished held back.
    [[nodiscard]] committed_rows_snapshot_t committed_rows_snapshot() noexcept;

    // The snapshot the rebuild scan reads under: "every committed row, no uncommitted one".
    // A distinct type rather than a raw transaction_data: repopulate_table clears every store
    // before the refill, so a scan under a STATEMENT snapshot (ctx->txn) silently drops from
    // the index every row a neighbour committed after that snapshot's horizon (pinned by
    // test_checkpoint_rebuild_snapshot.cpp). Only committed_rows_snapshot() can construct one,
    // so a caller holding ctx->txn does not compile.
    class committed_rows_snapshot_t {
    public:
        [[nodiscard]] const components::table::transaction_data& txn() const noexcept { return txn_; }

    private:
        explicit committed_rows_snapshot_t(components::table::transaction_data txn) noexcept
            : txn_(std::move(txn)) {}
        friend committed_rows_snapshot_t committed_rows_snapshot() noexcept;

        components::table::transaction_data txn_;
    };

    // The one driver for "the tables were just compacted, rebuild their indexes".
    //
    // data_table_t::compact renumbers every surviving row to start at row id 0, and a physical
    // row id is what an index entry stores. The resulting wrong answer is silent either way: an
    // id mapping to no row group is dropped by collection_t::fetch, and an id now belonging to a
    // different surviving row is gathered as if it were the match.
    //
    // A shared function, not a loop duplicated per caller: compaction happens in one place
    // (agent_disk_t::checkpoint_inner) but is reached from two (operator_checkpoint_t and
    // manager_wal_replicate_t::run_auto_checkpoint); written out longhand in one, the other
    // would silently lack it.
    //
    // Per table from manager_index_t::all_indexed_oids (excludes oids mid-DROP): drains the
    // streaming scan (storage_fetch_next_batch), then hands every chunk to
    // manager_index_t::repopulate_table, which clears the agents' stores and re-stages each row
    // under the physical id the scan stamped into chunk.row_ids. A table with no rows still goes
    // through -- the clear is what removes the stale entries.
    //
    // Where the call goes in the round -- the half a shared function can't enforce, and the half
    // both callers diverged on before this existed:
    //
    //     compaction (agent_disk_t::checkpoint_inner)  ->  THIS CALL  ->  WAL truncation
    //
    // After compaction (which renumbers the rows re-staged here), before WAL truncation (the
    // round's only destructive step): while durable state is a post-compact table under
    // pre-compact indexes, nothing ending the round early may also destroy journal segments.
    // Pinned by test_checkpoint_rebuild_before_truncate for operator_checkpoint_t;
    // run_auto_checkpoint keeps this call at step (c2), ahead of truncation's step (d).
    //
    // The order doesn't shorten that window; what survives a crash in it is a durable
    // "renumbered and not yet rebuilt" marker (armed in flush_all_indexes, cleared per table in
    // repopulate_table). base_spaces::bootstrap_indexes_sync reads it and declines to wire the
    // indexes it names, trading silent wrong answers for a full scan. See
    // manager_index_t::rebuild_marker_path_ for the file and the union rule.
    //
    // Drained-or-released, never abandoned: a live fetch-next cursor gates compact() on its oid,
    // so a leaked one would wedge the very table the next round needs to reclaim.
    //
    // `snapshot` is what the rebuild scan reads under, since an index answers a superset and
    // never filters by visibility. Returns the first error any leg reported -- returned rather
    // than logged, since only the caller knows whether a stale index fails a statement or a round.
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    repopulate_indexes_after_compaction(std::pmr::memory_resource* resource,
                                        actor_zeta::actor::address_t disk_address,
                                        actor_zeta::actor::address_t index_address,
                                        components::session::session_id_t session,
                                        committed_rows_snapshot_t snapshot,
                                        core::date::timezone_offset_t session_tz);

} // namespace services::index
