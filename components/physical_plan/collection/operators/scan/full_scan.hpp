#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/collection/operators/predicates/predicate.hpp>

namespace components::collection::operators {

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(services::collection::context_collection_t* collection,
                  predicates::predicate_ptr predicate,
                  logical_plan::limit_t limit);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        const predicates::predicate_ptr predicate_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::collection::operators
