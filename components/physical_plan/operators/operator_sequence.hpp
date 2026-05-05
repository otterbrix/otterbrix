#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <vector>

namespace components::operators {

    // Executes an ordered list of operators sequentially, without forwarding
    // output between them. Used for DDL pipelines and DELETE+FK-cascade sequences.
    // Unlike operator_t (binary tree), this holds an arbitrary-length list.
    class operator_sequence_t final : public read_write_operator_t {
    public:
        operator_sequence_t(std::pmr::memory_resource* resource,
                             log_t log,
                             std::vector<operator_ptr> ops);

        void prepare_ops();

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::vector<operator_ptr> ops_;
    };

} // namespace components::operators
