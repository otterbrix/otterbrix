#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <cstddef>

namespace components::operators {

    // Pipeline replacement for dispatcher's inline
    // manager_disk_t::allocate_oids_batch calls. At Pass 1 execute time the
    // operator sends `allocate_oids_batch(count)` to the disk actor and publishes
    // the resulting OIDs on the normal DATA channel: output() carries one
    // UINTEGER column, one row per OID, in allocation order. The DDL planner
    // consumes them positionally from there.
    class operator_allocate_oids_t final : public read_write_operator_t {
    public:
        operator_allocate_oids_t(std::pmr::memory_resource* resource, log_t log, std::size_t count);

        // Sourceless SINK leaf (no data pipeline, no children): the single
        // allocate_oids_batch round-trip to the disk actor + the set_output run in
        // await_async_and_resume. The executor admits it as a streaming sink-root and
        // drives await_async_and_resume via the bottom-up needs_async_finalize pass.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::size_t count_;
    };

} // namespace components::operators
