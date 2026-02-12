#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <expressions/compare_expression.hpp>

namespace components::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit);

    private:
        const expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
