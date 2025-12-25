#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

#include <components/physical_plan/collection/operators/predicates/predicate.hpp>

namespace components::collection::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        explicit operator_delete(services::collection::context_collection_t* collection,
                                 predicates::predicate_ptr&& match_predicate = nullptr);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        predicates::predicate_ptr match_predicate_;
    };

} // namespace components::collection::operators