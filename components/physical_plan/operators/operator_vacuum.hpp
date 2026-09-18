#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Touches manager_disk_t::vacuum_all, manager_index_t::cleanup_all_versions, and relkind='g' GC/compaction.
    // No index rebuild: compact() is what moves row ids, and its only call site (checkpoint_inner) is
    // a route VACUUM doesn't take. Reads pipeline_context.lowest_active_start_time (set by the executor
    // from txn_manager_t).
    class operator_vacuum_t final : public read_write_operator_t {
    public:
        operator_vacuum_t(std::pmr::memory_resource* resource, log_t log);

        // The executor admits this sourceless sink leaf as a streaming sink-root, driving
        // await_async_and_resume via the bottom-up needs_async_finalize pass.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
