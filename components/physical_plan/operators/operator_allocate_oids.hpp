#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <cstddef>

namespace components::logical_plan {
    class node_allocate_oids_t;
} // namespace components::logical_plan

namespace components::operators {

    // Pipeline replacement for dispatcher's inline
    // manager_disk_t::allocate_oids_batch calls. At Pass 1 execute time the
    // operator sends `allocate_oids_batch(count)` to the disk actor and
    // stamps the resulting vector onto the back-pointed logical node so the
    // DDL planner can read OIDs via node_allocate_oids_t::oids().
    class operator_allocate_oids_t final : public read_write_operator_t {
    public:
        operator_allocate_oids_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 std::size_t count,
                                 components::logical_plan::node_allocate_oids_t* target_node);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::size_t count_;
        components::logical_plan::node_allocate_oids_t* target_node_;
    };

} // namespace components::operators