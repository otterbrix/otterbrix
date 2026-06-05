#pragma once

#include "operator.hpp"
#include "operator_data.hpp"

namespace components::operators {

    // Leaf operator that outputs the current recursive-CTE working set.
    // The pointer slot is owned by operator_recursive_cte_t and updated each iteration.
    class operator_cte_scan_t final : public read_only_operator_t {
    public:
        operator_cte_scan_t(std::pmr::memory_resource* resource, log_t log, operator_data_ptr* working_set);

    private:
        operator_data_ptr* working_set_;

        void on_execute_impl(pipeline::context_t*) override;
    };

} // namespace components::operators
