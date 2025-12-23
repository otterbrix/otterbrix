#pragma once

#include <components/document/document.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

#include "predicates/predicate.hpp"

namespace components::collection::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(services::collection::context_collection_t* context,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        const expressions::compare_expression_ptr& comp_expr);

        operator_update(services::collection::context_collection_t* context,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        predicates::predicate_ptr match_predicate_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
        bool upsert_;
    };

} // namespace components::collection::operators
