#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators::merge {

    class operator_merge_t : public read_only_operator_t {
    public:
        explicit operator_merge_t(services::collection::context_collection_t* context, logical_plan::limit_t limit);

    protected:
        logical_plan::limit_t limit_;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;
        virtual void on_merge_impl(pipeline::context_t* pipeline_context) = 0;
    };

    using operator_merge_ptr = boost::intrusive_ptr<operator_merge_t>;

    bool is_operator_merge(const expressions::compare_expression_ptr& expr);
    operator_merge_ptr create_operator_merge(services::collection::context_collection_t* context,
                                             const expressions::compare_type& type,
                                             logical_plan::limit_t limit);
    operator_merge_ptr create_operator_merge(services::collection::context_collection_t* context,
                                             const expressions::compare_expression_ptr& expr,
                                             logical_plan::limit_t limit);

} // namespace components::collection::operators::merge
