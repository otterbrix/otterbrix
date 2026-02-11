#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>

namespace components::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        explicit operator_delete(services::collection::context_collection_t* collection,
                                 expressions::compare_expression_ptr expr = nullptr);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        expressions::compare_expression_ptr compare_expression_;
    };

} // namespace components::operators