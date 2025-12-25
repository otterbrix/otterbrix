#pragma once

#include "predicates/predicate.hpp"

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        explicit operator_join_t(services::collection::context_collection_t* context,
                                 type join_type,
                                 predicates::predicate_ptr&& predicate);

    private:
        type join_type_;
        predicates::predicate_ptr predicate_;

        bool check_expressions_(const document::document_ptr& left,
                                const document::document_ptr& right,
                                pipeline::context_t* context);
        void on_execute_impl(pipeline::context_t* context) override;
        void inner_join_(pipeline::context_t* context);
        void outer_full_join_(pipeline::context_t* context);
        void outer_left_join_(pipeline::context_t* context);
        void outer_right_join_(pipeline::context_t* context);
        void cross_join_(pipeline::context_t* context);
    };

} // namespace components::collection::operators
