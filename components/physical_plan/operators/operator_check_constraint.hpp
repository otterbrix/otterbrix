#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    // Checks NOT NULL constraints and CHECK expressions over the incoming chunk.
    // CHECK expressions are compiled to predicate_ptr once in the constructor;
    // evaluated per-row without re-parsing.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::vector<std::string> not_null_columns,
                                    std::vector<std::pair<std::string, std::string>> check_exprs = {});

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::vector<std::string> not_null_columns_;
        std::vector<std::pair<std::string, predicates::predicate_ptr>> check_predicates_; // (name, compiled)
    };

} // namespace components::operators