#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <vector>

namespace components::operators {

    // Checks NOT NULL constraints over the incoming chunk.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                     log_t                      log,
                                     std::vector<std::string>   not_null_columns);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::vector<std::string> not_null_columns_;
    };

} // namespace components::operators