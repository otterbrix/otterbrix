#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // VACUUM — global no-arg operation.
    //
    // Steps (in await_async_and_resume):
    //   1. manager_disk_t::vacuum_all — for every user storage, cleanup_versions (drop tuple versions older
    //      than lowest_active_start_time). Implemented globally on the disk side; called once. It ANSWERS how
    //      many storages it renumbered.
    //   2. manager_index_t::cleanup_all_versions — called once if index_address is set.
    //   3. An index rebuild, IF AND ONLY IF step 1 reported a renumbering.
    //   4. pg_computed_column GC (tombstones + stale versions) and the physical column compaction for
    //      relkind='g' tables, driven by a drained pg_class scan.
    //
    // ON STEP 3, AND WHY IT IS CONDITIONAL. A full rebuild is owed for exactly one reason: something moved a
    // physical row id, because that is what an index entry stores. Only data_table_t::compact does, and its
    // single call site is agent_disk_t::checkpoint_inner — a route VACUUM does not take, so an unconditional
    // rebuild pays a drained scan of every table plus a clear-and-refill of every index for a renumbering
    // that never happened. The condition is a FACT rather than a guess: vacuum_all returns the count,
    // produced inside vacuum_inner at the line a compact would occupy, so re-enabling compaction there
    // re-arms this rebuild without anyone having to remember. And when it does fire it calls
    // services::index::repopulate_indexes_after_compaction — the SAME driver the CHECKPOINT statement and
    // the WAL auto-checkpoint use — rather than a third longhand copy of the loop, which is how the
    // auto-checkpoint came to have no rebuild at all.
    //
    // Reads pipeline_context.lowest_active_start_time (set by the executor from txn_manager_t).
    class operator_vacuum_t final : public read_write_operator_t {
    public:
        operator_vacuum_t(std::pmr::memory_resource* resource, log_t log);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
