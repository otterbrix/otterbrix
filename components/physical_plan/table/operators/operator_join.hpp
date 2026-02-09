#pragma once

#include "predicates/predicate.hpp"
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <expressions/compare_expression.hpp>

namespace components::table::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        explicit operator_join_t(services::collection::context_collection_t* context,
                                 type join_type,
                                 const expressions::expression_ptr& expression);

    private:
        type join_type_;
        expressions::expression_ptr expression_;
        // TODO: check if it is necessary after introduction of plan validation
        std::unordered_map<std::string, size_t> name_index_map_left_;
        std::unordered_map<std::string, size_t> name_index_map_right_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        void on_execute_impl(pipeline::context_t* context) override;
        void inner_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void outer_full_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void outer_left_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void outer_right_join_(const predicates::predicate_ptr&, pipeline::context_t* context);
        void cross_join_(pipeline::context_t* context);
    };

} // namespace components::table::operators
