#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

#include <components/expressions/expression.hpp>
#include <components/logical_plan/node_limit.hpp>

namespace components::table::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(services::collection::context_collection_t* context,
                         const expressions::expression_ptr& expression,
                         logical_plan::limit_t limit);

    private:
        const expressions::expression_ptr expression_;
        const logical_plan::limit_t limit_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::table::operators
