#pragma once

#include <components/expressions/compare_expression.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::table::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(services::collection::context_collection_t* collection,
                   expressions::compare_expression_ptr expr,
                   logical_plan::limit_t limit);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;
        void on_resume_impl(pipeline::context_t* pipeline_context) override;

        const expressions::compare_expression_ptr expr_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::table::operators
