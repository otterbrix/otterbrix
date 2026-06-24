#pragma once

#include <components/compute/compute_kernel.hpp>
#include <components/physical_plan/operators/aggregate/grouped_aggregate.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        void set_value(std::pmr::vector<types::logical_value_t>& row, std::string_view key) const;
        types::logical_value_t value() const;
        compute::datum_t take_batch_values();

        // Synchronous per-group aggregation entry point for operator_group_t.
        // operator_func_t aggregation is pure-CPU (no async_wait / await / cross-actor
        // send), so the legacy on_execute() drive is unnecessary: this clears prior
        // state, wires `source_batch` as the input child, and runs the SAME
        // run_aggregation core on_execute_impl reaches — minus the on_execute
        // recursion/state machinery (left_ is an already-executed operator_batch_t,
        // so on_execute's child-drive is a no-op anyway). Read has_error() /
        // take_batch_values() afterwards exactly as before.
        void compute(pipeline::context_t* ctx, const operator_ptr& source_batch);

        // --- Push-based streaming pipeline (STEP 3 / phase C) ---
        // A scalar aggregate (no GROUP BY) folds an unbounded input into a single
        // bounded result, so it is a SINK that needs only O(1) running state.
        //
        // push() folds each input batch DIRECTLY into a typed running accumulator
        // (raw_agg_state_t, the same primitive operator_group uses for its
        // vectorized fast path) — SUM/MIN/MAX/COUNT/AVG advance per batch with no
        // per-cell logical_value_t and no full-input buffering. finalize() converts
        // the running state to the single result row via finalize_state().
        //
        // Aggregates the typed fast path cannot reproduce identically (DISTINCT,
        // expression args, count(*), HUGEINT/DECIMAL/BOOLEAN/string MIN-MAX, or any
        // non-builtin func) fall back to running the legacy scalar aggregate_impl
        // over a buffered input — O(rows) only for those inputs, never for the
        // common SUM/MIN/MAX/COUNT/AVG-over-a-numeric-column case the fast path
        // owns. The legacy on_execute_impl materialize path stays intact (it is
        // still driven by operator_group's per-group aggregator calls).
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

        // Drop the lazily-classified streaming sink state so a re-driven sub-plan
        // (recursive-CTE recursive term, re-run per fixpoint iteration) re-accumulates
        // from scratch instead of folding into a stale running state. reset_for_reuse()
        // clears state_/output_ but not this push/finalize bookkeeping.
        void reset_pipeline_state() noexcept override {
            sink_mode_ = sink_mode_t::unclassified;
            running_state_ = raw_agg_state_t{};
            fast_kind_ = builtin_agg::UNKNOWN;
            fast_is_count_star_ = false;
            fast_col_path_.clear();
            fast_col_type_ = types::logical_type::NA;
            rows_seen_ = 0;
            schema_carrier_.clear();
            buffered_input_.clear();
        }

    protected:
        operator_aggregate_t(std::pmr::memory_resource* resource, log_t log);

        types::logical_value_t aggregate_result_;
        compute::datum_t batch_results_{std::pmr::vector<types::logical_value_t>{resource_}};

        virtual core::result_wrapper_t<types::logical_value_t>
        aggregate_impl(pipeline::context_t* pipeline_context) = 0;
        virtual core::result_wrapper_t<compute::datum_t> aggregate_batch_impl(pipeline::context_t* pipeline_context);
        virtual std::string key_impl() const = 0;

    private:
        // --- O(1) streaming sink state (push/finalize path) ---
        // Classified lazily on the first push(). When fast_path_ holds, the running
        // accumulator below is folded in place per batch and buffered_input_ stays
        // empty. Otherwise batches are buffered and the legacy scalar aggregate_impl
        // runs once in finalize().
        enum class sink_mode_t
        {
            unclassified,
            fast,
            fallback
        };
        sink_mode_t sink_mode_{sink_mode_t::unclassified};

        // Typed running accumulator for the fast path (SUM/MIN/MAX/COUNT/AVG).
        raw_agg_state_t running_state_{};
        builtin_agg fast_kind_{builtin_agg::UNKNOWN};
        bool fast_is_count_star_{false};
        std::pmr::vector<size_t> fast_col_path_{resource_};
        types::logical_type fast_col_type_{types::logical_type::NA};

        // Number of input rows folded so far (across all batches). Drives the
        // empty-table guard: 0 rows must take the legacy aggregate_impl path so a
        // scalar COUNT yields 0 and SUM/MIN/MAX/AVG yield NULL, identical to the
        // materialize path.
        uint64_t rows_seen_{0};
        // One schema'd (possibly 0-row) batch retained so the empty-table guard can
        // run aggregate_impl over a correctly-typed 0-row input.
        chunks_vector_t schema_carrier_{resource_};

        // Buffered input batches — populated only on the fallback path; drained in
        // finalize().
        chunks_vector_t buffered_input_{resource_};

        // Decide fast vs fallback from the concrete operator_func_t (mirrors the
        // classification operator_group performs for its vectorized path).
        void classify_sink(const vector::data_chunk_t& schema_chunk);
        // Run the legacy scalar aggregate_impl over `in_chunks` and emit the single
        // result row into `out` (fallback + empty-table guard).
        core::error_t finalize_via_legacy(pipeline::context_t* ctx, chunks_vector_t&& in_chunks, chunks_vector_t& out);

        // Shared dispatch between the on_execute_impl materialized path and
        // finalize(): selects the batch vs scalar aggregation depending on the
        // source child's type and stamps batch_results_/aggregate_result_. The
        // source child is already wired as left_ by the caller. finalize_via_legacy
        // routes the fallback/empty-table cases here too, so both entry points
        // reach the same aggregate_impl/aggregate_batch_impl core.
        void run_aggregation(pipeline::context_t* pipeline_context);

        // Design intent (NOT an R6 transitional fallback): push()/finalize() =
        // streaming path (sourced pipelines); on_execute_impl = materialized path
        // for sourceless sub-plans (raw_data / recursive-CTE working sets / no-FROM
        // SELECT) AND for operator_group's per-group aggregator calls. Both route
        // through the SAME run_aggregation core — two entry points to one
        // implementation for two plan shapes.
        void on_execute_impl(pipeline::context_t* pipeline_context) final;
    };

    using operator_aggregate_ptr = boost::intrusive_ptr<operator_aggregate_t>;

} // namespace components::operators::aggregate