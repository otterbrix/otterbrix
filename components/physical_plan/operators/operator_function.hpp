#pragma once

#include "operator.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/expression.hpp>

namespace components::operators {

    // FROM-clause table (set-returning) function, e.g. generate_series(1, 10).
    //
    // A SOURCE that materializes its relation lazily on the first source_next (it
    // needs the pipeline context for the parameter map + function registry, which
    // are absent at construction). It resolves the argument values, invokes the
    // expand_kernel, and hands the produced chunks out one at a time by a cursor —
    // exactly like operator_raw_data_t, but generated rather than literal.
    //
    // Correlated arguments (a column ref into an outer row) are NOT resolved here;
    // that is the LATERAL-join path, which rescans this source per outer row.
    class operator_function_t final : public read_only_operator_t {
    public:
        operator_function_t(std::pmr::memory_resource* resource,
                            log_t log,
                            compute::function_uid uid,
                            std::pmr::vector<expressions::param_storage> args,
                            std::string result_alias);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Re-runnable for a future LATERAL rescan: drop the materialized relation and
        // rewind the cursor so the next source_next rebuilds it (with rebound args).
        void reset_pipeline_state() noexcept override;

    private:
        // Resolve args, run the expand kernel, and materialize the produced chunks
        // into output_. Sets error state on failure.
        core::error_t materialize_(pipeline::context_t* ctx);
        vector::data_chunk_t make_drain_chunk();

        compute::function_uid uid_;
        std::pmr::vector<expressions::param_storage> args_;
        std::string result_alias_;

        bool materialized_{false};
        std::size_t cursor_{0};
    };

} // namespace components::operators
