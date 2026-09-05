#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // VACUUM — global no-arg operation.
    //
    // Steps: 1) manager_disk_t::vacuum_all (cleanup_versions per storage; answers how many were
    // renumbered). 2) manager_index_t::cleanup_all_versions. 3) index rebuild, ONLY IF step 1
    // reported a renumbering. 4) pg_computed_column GC + physical column compaction for relkind='g'.
    //
    // Step 3 fires only because compact() moves row ids (indexes store them), and compact()'s only
    // call site (checkpoint_inner) is a route VACUUM doesn't take — so an unconditional rebuild would
    // almost always be wasted work. Uses services::index::repopulate_indexes_after_compaction, the
    // same driver as CHECKPOINT/auto-checkpoint.
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
