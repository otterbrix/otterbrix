#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // CHECKPOINT — global no-arg operation.
    //
    // Steps (in await_async_and_resume):
    //   1. flush_all_indexes (if index_address present) — materialize dirty btree pages.
    //   2. current_wal_id (if wal_address present) — read max wal_id before checkpoint so the checkpoint
    //      marker pins a known recovery boundary.
    //   3. checkpoint_all on disk — copy + fsync per-table data, then 2nd fsync barrier so the W-TORN
    //      per-table prev/current wal-id snapshot is durable.
    //   4. Index rebuild (if index_address present) — checkpoint_inner compact()s each table, renumbering
    //      row ids; every index of every table it touched stores the PRE-compact ids.
    //      services::index::repopulate_indexes_after_compaction is the shared driver, and it is DURABLE
    //      when it returns (both agent families end commit_inserts with force_flush).
    //   5. truncate_before on wal (if checkpoint_wal_id > 0) — drop old WAL segments.
    //
    // STEPS 4 AND 5 ARE IN THIS ORDER ON PURPOSE. The truncation is the one step of the round that destroys
    // something, so it goes last: a rebuild that refuses must be able to end the round without the journal
    // having already been trimmed behind an index that still names pre-compact rows. It is the same order
    // manager_wal_replicate_t::run_auto_checkpoint keeps (its rebuild is step (c2), its truncate step (d)).
    //
    // The checkpoint_all return value is min(prev_checkpoint_wal_id_) across tables; truncate_before is gated
    // on > 0 (a round in which no table checkpointed reports 0 and leaves the WAL untrimmed).
    class operator_checkpoint_t final : public read_write_operator_t {
    public:
        operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
