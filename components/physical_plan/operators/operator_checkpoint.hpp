#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // Measurement seam: holds the CHECKPOINT round between checkpoint_all and
    // repopulate_indexes_after_compaction, to probe readers arriving in that stale-index window.
    struct checkpoint_repopulate_gate_t {
        virtual ~checkpoint_repopulate_gate_t() = default;
        // true = keep holding the round between compaction and index rebuild; false = proceed.
        virtual bool hold() = 0;
    };
    void dev_set_checkpoint_repopulate_gate(checkpoint_repopulate_gate_t* gate); // nullptr = off
    checkpoint_repopulate_gate_t* dev_checkpoint_repopulate_gate();
#endif

    // Index rebuild must run before WAL truncation, so a refused rebuild stops the round
    // before the WAL is trimmed (same order as manager_wal_replicate_t::run_auto_checkpoint).
    class operator_checkpoint_t final : public read_write_operator_t {
    public:
        operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log);

        // Sourceless SINK leaf: the executor drives await_async_and_resume via the bottom-up needs_async_finalize pass.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
