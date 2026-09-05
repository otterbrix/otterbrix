#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // CHECKPOINT — global no-arg operation.
    //
    // Steps: 1) flush_all_indexes. 2) current_wal_id (pins the recovery boundary before checkpointing).
    // 3) checkpoint_all (copy+fsync per table, then a barrier fsync). 4) index rebuild — checkpoint_inner
    // compacts each table, renumbering row ids that indexes still hold pre-compact; the shared driver
    // repopulate_indexes_after_compaction is durable when it returns. 5) truncate_before, gated on
    // checkpoint_all's returned watermark > 0. Steps 4 and 5 are in this order deliberately: truncation is
    // destructive and runs last, so a refused rebuild can still stop the round before the WAL is trimmed
    // (same order as manager_wal_replicate_t::run_auto_checkpoint).
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
