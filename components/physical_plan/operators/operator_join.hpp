#pragma once

#include "predicates/predicate.hpp"
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <expressions/compare_expression.hpp>

namespace components::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_join_t(std::pmr::memory_resource* resource, log_t log,
                        type join_type,
                        const expressions::expression_ptr& expression);

    private:
        type join_type_;
        expressions::expression_ptr expression_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        void on_execute_impl(pipeline::context_t* context) override;
        void inner_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void outer_full_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void outer_left_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void outer_right_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void cross_join_(pipeline::context_t* context);
    };

} // namespace components::operators
