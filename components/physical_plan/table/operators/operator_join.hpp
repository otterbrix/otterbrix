#pragma once

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <expressions/compare_expression.hpp>

namespace components::table::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        explicit operator_join_t(services::collection::context_collection_t* context,
                                 type join_type,
                                 const expressions::compare_expression_ptr& expression);

    private:
        type join_type_;
        expressions::compare_expression_ptr expression_;
        std::unordered_map<std::string, size_t> name_index_map_left_;
        std::unordered_map<std::string, size_t> name_index_map_right_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        bool check_predicate_(pipeline::context_t* context, size_t row_left, size_t row_right) const;
        void on_execute_impl(pipeline::context_t* context) final;
        void inner_join_(pipeline::context_t* context);
        void outer_full_join_(pipeline::context_t* context);
        void outer_left_join_(pipeline::context_t* context);
        void outer_right_join_(pipeline::context_t* context);
        void cross_join_(pipeline::context_t* context);
    };

} // namespace components::table::operators
