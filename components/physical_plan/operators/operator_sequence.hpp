#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // A DDL statement's steps, one after another: the executor runs each step as a plan of its own, in order, and
    // stops at the first that fails; the sequence itself does nothing. The steps are not its children, so a statement
    // of thousands of steps is a flat list, never a chain that deep.
    class operator_sequence_t final : public read_write_operator_t {
    public:
        operator_sequence_t(std::pmr::memory_resource* resource, log_t log);

        void append_step(operator_ptr step) { steps_.push_back(std::move(step)); }
        void drop_last_step() { steps_.pop_back(); }
        const std::pmr::vector<operator_ptr>& steps() const noexcept { return steps_; }

    private:
        std::pmr::vector<operator_ptr> steps_;
    };

} // namespace components::operators
