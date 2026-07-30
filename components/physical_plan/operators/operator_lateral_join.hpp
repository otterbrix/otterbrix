#pragma once

#include "operator.hpp"
#include "operator_data.hpp"

#include <components/expressions/expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_join.hpp>

namespace components::operators {

    // LATERAL (correlated) join: the right (inner) side references columns of the
    // left (outer) side and must be re-evaluated per outer row. Modeled on
    // operator_recursive_cte_t — outer_/inner_ are PRIVATE sub-plan roots (not
    // left_/right_), so the executor treats this as a sourceless async-finalize
    // sink and drives it through await_async_and_resume, where it owns running both
    // sub-plans via ctx->runner->run_subplan.
    //
    // Per outer row: write each correlated parameter slot in ctx->parameters from
    // the outer row's bound column, reset + rerun the inner sub-plan, and emit
    // (outer columns ++ inner columns) for every inner row. join_type::left keeps
    // an outer row with no inner rows, NULL-padded on the right.
    //
    // The output layout is fixed at plan time: outer_schema/inner_schema are the
    // resolved output column lists of the two sub-plans (stamped on the logical
    // nodes by validate_schema), each record naming its column beside its type.
    // Building the join layout, ON predicate, and
    // correlation bindings from these — rather than discovering them from the first
    // inner run's chunks — is what lets join_type::left NULL-pad an outer row whose
    // inner side yields zero rows for EVERY outer row (there is no runtime chunk to
    // learn the inner column count from in that case).
    class operator_lateral_join_t final : public read_only_operator_t {
    public:
        using correlation_t = components::logical_plan::node_join_t::correlation_t;

        operator_lateral_join_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::logical_plan::join_type type,
                                std::pmr::vector<correlation_t> correlations,
                                expressions::expression_ptr on_expression,
                                vector::schema_t outer_schema,
                                vector::schema_t inner_schema);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        void set_lateral_terms(operator_ptr outer, operator_ptr inner) noexcept {
            outer_ = std::move(outer);
            inner_ = std::move(inner);
        }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        // EXPLAIN: outer_/inner_ are PRIVATE sub-plan roots (left()==right()==nullptr) — recurse into
        // them here, else the walk would render this as a childless leaf.
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, catalog::INVALID_OID);
            if (outer_) {
                outer_->explain(s);
            }
            if (inner_) {
                inner_->explain(s);
            }
            s.end();
        }

        actor_zeta::unique_future<core::error_t> drive_(pipeline::context_t* ctx);
        static void reset_subtree(const operator_ptr& op);

        components::logical_plan::join_type type_;
        std::pmr::vector<correlation_t> correlations_;
        // ON predicate over (outer columns ++ inner columns). all_true for the
        // comma / ON true forms; a real predicate filters each inner row per outer row.
        expressions::expression_ptr on_expression_;
        // Plan-time resolved output schemas of the two sides; the output layout is
        // (outer_schema_ ++ inner_schema_). Each record carries its column's name
        // beside its type (M3-B5).
        vector::schema_t outer_schema_;
        vector::schema_t inner_schema_;
        operator_ptr outer_{nullptr};
        operator_ptr inner_{nullptr};
    };

} // namespace components::operators
