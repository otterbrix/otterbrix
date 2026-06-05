#pragma once

#include "operator.hpp"
#include "operator_data.hpp"

namespace components::operators {

    class operator_recursive_cte_t final : public read_only_operator_t {
    public:
        operator_recursive_cte_t(std::pmr::memory_resource* resource, log_t log, bool all);

        // Returns a pointer to the working-set slot so operator_cte_scan_t can point into it.
        operator_data_ptr* working_set_slot() noexcept { return &working_set_; }

    private:
        bool all_;
        operator_data_ptr working_set_;

        void on_execute_impl(pipeline::context_t* context) override;

        static void reset_subtree(const operator_ptr& op);
    };

} // namespace components::operators
