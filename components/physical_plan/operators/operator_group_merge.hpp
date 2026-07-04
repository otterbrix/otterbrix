#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    // Coordinator-side terminal of the aggregate pushdown (SEAM B). Sits where the
    // coordinator operator_group used to sit, so the physical plan keeps a truthful
    // aggregate-shaped node above the pushed_reduce_scan (EXPLAIN / costing / the
    // recursive-CTE reset walk see an aggregate, not a scan impersonating one).
    //
    // TODAY it is an IDENTITY PASSTHROUGH — under the single-owner invariant (see
    // disk_contract.hpp: one agent owns the whole table) the agent already returns
    // FINAL rows — plus the OWNER of the empty-input invariant: a scalar aggregate
    // whose input produced no rows still emits its mandatory single row (COUNT -> 0,
    // SUM/MIN/MAX/AVG -> NULL, typed via the spec's output_types). The agent's own
    // empty-slice finalize normally emits that row already; this finalize is the
    // single coordinator-side owner for any path where no agent row arrives.
    //
    // TOMORROW (table slices sharded across agents) this is the socket where
    // per-slice PARTIAL aggregate states get a real kernel merge — the shape of the
    // plan does not change, only this operator's body.
    class operator_group_merge_t final : public read_only_operator_t {
    public:
        // aggs: (output alias, builtin function name) per aggregate column, in spec
        // order (aggregate columns follow the group-key columns; for a scalar
        // aggregate there are no key columns). scalar == the spec has no group keys.
        operator_group_merge_t(std::pmr::memory_resource* resource,
                               log_t log,
                               bool scalar,
                               std::pmr::vector<types::complex_logical_type> output_types,
                               std::vector<std::pair<std::string, std::string>> aggs);

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

        // A re-driven sub-plan (recursive-CTE fixpoint reset) must re-arm the
        // empty-input synthesis for the next pass.
        void reset_pipeline_state() noexcept override { saw_rows_ = false; }

    private:
        bool scalar_;
        std::pmr::vector<types::complex_logical_type> output_types_;
        std::vector<std::pair<std::string, std::string>> aggs_; // (alias, function name)
        bool saw_rows_{false};
    };

} // namespace components::operators
