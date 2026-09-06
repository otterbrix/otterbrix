#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // Measurement seam (stale-index window probe): hold the CHECKPOINT round EXACTLY between the
    // end of checkpoint_all (every table is compacted, physical row ids renumbered) and the start
    // of repopulate_indexes_after_compaction (the indexes still hold pre-compact ids). A reader
    // that ARRIVES inside this span opens its compact-hold too late to defer anything and the
    // index answers stale ids. Plain virtual (no std::function), process-wide, DEV_MODE-only; the
    // operator polls it by ONE no-op cross-actor round-trip per ask, so no actor thread ever
    // blocks while it holds (same shape as index_fetch_gate_t / delete_wal_apply_gate_t).
    struct checkpoint_repopulate_gate_t {
        virtual ~checkpoint_repopulate_gate_t() = default;
        // true = keep holding the round between compaction and index rebuild; false = proceed.
        virtual bool hold() = 0;
    };
    void dev_set_checkpoint_repopulate_gate(checkpoint_repopulate_gate_t* gate); // nullptr = off
    checkpoint_repopulate_gate_t* dev_checkpoint_repopulate_gate();
#endif

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
