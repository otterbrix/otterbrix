#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

#include "predicates/predicate.hpp"

#include <components/logical_plan/node_limit.hpp>

namespace components::collection::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(services::collection::context_collection_t* context,
                         predicates::predicate_ptr predicate,
                         logical_plan::limit_t limit);

    private:
        const predicates::predicate_ptr predicate_;
        const logical_plan::limit_t limit_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::collection::operators
