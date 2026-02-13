#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(services::collection::context_collection_t* context,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        expressions::expression_ptr expr = nullptr);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::expression_ptr expr_;
        bool upsert_;
    };

} // namespace components::operators
