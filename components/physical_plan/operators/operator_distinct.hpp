#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class operator_distinct_t final : public read_only_operator_t {
    public:
        operator_distinct_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
