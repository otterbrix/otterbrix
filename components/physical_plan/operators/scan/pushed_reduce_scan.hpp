#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp>
#include <core/result_wrapper.hpp>

namespace components::operators {

    // Aggregate-pushdown source. Lowered by create_plan_aggregate for a
    // pushdown-stamped aggregate: ships the POD reduce spec (plus the lowered WHERE
    // filter and the projection) on the DEDICATED manager_disk_t::storage_reduce
    // protocol leg, and emits the owning agent's FINAL aggregated rows — bounded by
    // #groups, delivered in ONE reply, so there is no cursor and the raw-scan
    // protocol stays a pure scan. Sits under an operator_group_merge_t (the
    // coordinator-side terminal that keeps the plan shape truthful and owns the
    // empty-input scalar row).
    class pushed_reduce_scan final : public read_only_operator_t {
    public:
        pushed_reduce_scan(std::pmr::memory_resource* resource,
                           log_t log,
                           components::catalog::oid_t table_oid,
                           const expressions::compare_expression_ptr& expression,
                           std::vector<size_t> projected_cols,
                           pushed_aggregate_spec_t spec);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // role()==source drives the streaming push/finalize pipeline. The FIRST
        // source_next call lowers the WHERE (a storage_types await only when a real
        // predicate needs typing), sends ONE storage_reduce, and stashes the reply;
        // each subsequent call emits one stashed chunk, then the 0-column drain
        // sentinel. The (at most two) sequential cross-actor awaits live in this
        // NESTED operator coroutine, so the single-slot awaited continuation is
        // republished+cleared between them — no lost wakeup.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind for a re-driven sub-plan (recursive-CTE fixpoint reset): the next
        // source_next re-runs the WHOLE reduce. spec_ is untouched — every OPEN ships
        // open_spec(), a copy.
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            emit_idx_ = 0;
            reduced_.clear();
        }

        // The spec instance each storage_reduce send ships: a DEEP COPY onto the
        // operator's resource (field-by-field — the pmr members' plain copy ctors
        // would SOCCC onto the default resource). The armed spec_ survives every
        // send, so a re-driven scan re-runs the SAME reduce.
        [[nodiscard]] pushed_aggregate_spec_t open_spec();

    private:
        components::catalog::oid_t table_oid_;
        expressions::compare_expression_ptr expression_;
        std::vector<size_t> projected_cols_;
        pushed_aggregate_spec_t spec_;

        // One-reply reduce state: reduced_ holds the agent's final rows after the
        // first source_next; emit_idx_ walks them (moved out one per call).
        bool opened_{false};
        std::size_t emit_idx_{0};
        std::pmr::vector<vector::data_chunk_t> reduced_{resource_};
    };

} // namespace components::operators
