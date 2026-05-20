#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <vector>

namespace components::operators {

    // Executes a list of operators sequentially, ignoring data output.
    // Used for DDL pipelines where multiple catalog-write operators run in order.
    class operator_sequence_t final : public read_write_operator_t {
    public:
        operator_sequence_t(std::pmr::memory_resource* resource, log_t log, std::vector<operator_ptr> steps);

        const std::vector<operator_ptr>& steps() const { return steps_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::vector<operator_ptr> steps_;
    };

} // namespace components::operators