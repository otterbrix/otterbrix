#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/catalog/constraint_evaluator.hpp>

namespace components::operators {

    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::row_predicate_fn predicate,
                                     std::string conexpr);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        catalog::row_predicate_fn predicate_;
        std::string               conexpr_;
    };

} // namespace components::operators
