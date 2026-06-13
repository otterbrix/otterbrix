#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // CHECKPOINT — global no-arg operation.
    //
    // Steps (in await_async_and_resume):
    //   1. flush_all_indexes (if index_address present) — materialize dirty btree pages.
    //   2. current_wal_id (if wal_address present) — read max wal_id before checkpoint
    //      so the checkpoint marker pins a known recovery boundary.
    //   3. checkpoint_all on disk — copy + fsync per-table data, then 2nd fsync barrier
    //      so the W-TORN per-table prev/current wal-id snapshot is durable.
    //   4. truncate_before on wal (if checkpoint_wal_id > 0) — drop old WAL segments.
    //   5. Index rebuild (if index_address present) — checkpoint_inner compact()s
    //      each table, renumbering row ids; the in-memory index engines hold
    //      positional refs into the pre-compact layout. For every all_indexed_oids
    //      table: storage_total_rows -> storage_scan_segment(0, total) ->
    //      repopulate_table (clears on-disk index backing + in-memory engine, then
    //      re-inserts at post-compact ids). total==0 still repopulates to wipe
    //      stale entries. MUST follow checkpoint_all (step 3).
    //
    // WAL recovery semantics: identical to the legacy dispatcher.cpp checkpoint_t case.
    // The checkpoint_all return value is min(prev_checkpoint_wal_id_) across tables;
    // truncate_before is gated on > 0 (IN_MEMORY-only DBs leave WAL untrimmed).
    class operator_checkpoint_t final : public read_write_operator_t {
    public:
        operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
