#include "operator_aggregate.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/key.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>

namespace components::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::aggregate)
        , aggregate_result_(resource, types::complex_logical_type{types::logical_type::NA}) {}

    void operator_aggregate_t::run_aggregation(pipeline::context_t* pipeline_context) {
        if (left_ && left_->type() == operator_type::batch) {
            if (auto res = aggregate_batch_impl(pipeline_context); res.has_error()) {
                set_error(res.error());
            } else {
                batch_results_ = std::move(res.value());
            }
        } else {
            if (auto res = aggregate_impl(pipeline_context); res.has_error()) {
                set_error(res.error());
            } else {
                aggregate_result_ = std::move(res.value());
            }
        }
    }

    void operator_aggregate_t::compute(pipeline::context_t* ctx, const operator_ptr& source_batch) {
        clear();
        set_children(source_batch);
        run_aggregation(ctx);
    }

    // Mirror the classification operator_group performs for its vectorized fast
    // path: a single builtin aggregate (SUM/MIN/MAX/COUNT/AVG), no DISTINCT, over a
    // single plain column key whose type is supported by raw_agg_state_t — or
    // count(*) (zero args). Anything else stays on the buffered legacy path so the
    // result is byte-for-byte the materialize path (R17).
    void operator_aggregate_t::classify_sink(const vector::data_chunk_t& schema_chunk) {
        sink_mode_ = sink_mode_t::fallback;

        auto* func_op = dynamic_cast<operator_func_t*>(this);
        if (!func_op || !func_op->func()) {
            return;
        }
        auto kind = classify(func_op->func()->name());
        if (kind == builtin_agg::UNKNOWN || func_op->distinct()) {
            return;
        }

        const bool count_star = (kind == builtin_agg::COUNT && func_op->args().empty());
        if (count_star) {
            fast_kind_ = kind;
            fast_is_count_star_ = true;
            fast_col_type_ = types::logical_type::UBIGINT;
            sink_mode_ = sink_mode_t::fast;
            return;
        }

        if (func_op->args().size() != 1 || !std::holds_alternative<expressions::key_t>(func_op->args()[0])) {
            return;
        }
        const auto& key = std::get<expressions::key_t>(func_op->args()[0]);
        if (key.path().empty() || key.path().front() == SIZE_MAX) {
            return;
        }
        fast_col_path_.assign(key.path().begin(), key.path().end());
        const auto* col = schema_chunk.at(fast_col_path_);
        if (!col) {
            return;
        }
        fast_col_type_ = col->type().type();
        if (!types::is_numeric(fast_col_type_)) {
            return;
        }
        fast_kind_ = kind;
        fast_is_count_star_ = false;
        sink_mode_ = sink_mode_t::fast;
    }

    core::error_t
    operator_aggregate_t::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // SINK: fold the batch into bounded state. A standalone scalar aggregate (no
        // GROUP BY) produces one scalar over all rows; emit nothing here.
        if (sink_mode_ == sink_mode_t::unclassified) {
            classify_sink(input);
            // Keep one schema'd (possibly 0-row) batch so the empty-table guard can
            // run aggregate_impl over a correctly-typed 0-row input.
            schema_carrier_.clear();
            schema_carrier_.emplace_back(input.partial_copy(resource_, 0, 0));
        }

        const uint64_t n = input.size();
        rows_seen_ += n;

        if (sink_mode_ == sink_mode_t::fallback) {
            buffered_input_.emplace_back(std::move(input));
            return core::error_t::no_error();
        }

        // Fast path: fold the batch into the single running accumulator. Every row
        // is part of the one global group, so group_ids are all 0.
        if (n != 0) {
            if (fast_is_count_star_) {
                running_state_.u64 += n;
                running_state_.initialized = true;
                running_state_.count += n;
            } else {
                std::pmr::vector<uint32_t> group_ids(resource_);
                group_ids.assign(n, 0u);
                std::pmr::vector<raw_agg_state_t> states(resource_);
                states.assign(1, running_state_);
                update_all(fast_kind_, *input.at(fast_col_path_), group_ids.data(), n, states);
                running_state_ = states.front();
            }
        }
        return core::error_t::no_error();
    }

    core::error_t operator_aggregate_t::finalize_via_legacy(pipeline::context_t* ctx,
                                                            chunks_vector_t&& in_chunks,
                                                            chunks_vector_t& out) {
        // Run the EXACT scalar aggregation, sourcing rows from the buffered batches.
        // operator_empty_t (not operator_type::batch)
        // drives run_aggregation down the scalar aggregate_impl path, which reads
        // left_->output()->data_chunk() — the concatenation of every chunk — so the
        // result is identical to materializing the whole input first.
        auto data = make_operator_data(resource_, std::move(in_chunks));
        set_children(boost::intrusive_ptr<operator_t>(new operator_empty_t(resource_, std::move(data))));
        run_aggregation(ctx);
        if (has_error()) {
            return get_error();
        }

        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.emplace_back(aggregate_result_.type());
        vector::data_chunk_t result(resource_, out_types, 1);
        result.set_value(0, 0, aggregate_result_);
        result.set_cardinality(1);
        out.emplace_back(std::move(result));
        return core::error_t::no_error();
    }

    core::error_t operator_aggregate_t::finalize(pipeline::context_t* ctx, chunks_vector_t& out) {
        // Empty-table guard: a 0-row scalar aggregate must yield COUNT=0 and
        // SUM/MIN/MAX/AVG=NULL — exactly the legacy materialize result over a
        // schema'd 0-row input. raw_agg_state_t::finalize_state returns NA for
        // COUNT-of-empty too, so always take the legacy path when no rows were seen.
        if (rows_seen_ == 0 || sink_mode_ != sink_mode_t::fast) {
            // Source the schema'd 0-row carrier when nothing was buffered (fast-path
            // classified but drained empty), else the buffered fallback input.
            chunks_vector_t src(resource_);
            if (!buffered_input_.empty()) {
                src = std::move(buffered_input_);
            } else {
                src = std::move(schema_carrier_);
            }
            return finalize_via_legacy(ctx, std::move(src), out);
        }

        // Fast path: convert the typed running state to the single result row.
        auto result_value = finalize_state(resource_, fast_kind_, running_state_, fast_col_type_);
        result_value.set_alias(key_impl());
        aggregate_result_ = result_value;

        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.emplace_back(result_value.type());
        vector::data_chunk_t result(resource_, out_types, 1);
        result.set_value(0, 0, result_value);
        result.set_cardinality(1);
        out.emplace_back(std::move(result));
        return core::error_t::no_error();
    }

    core::result_wrapper_t<compute::datum_t>
    operator_aggregate_t::aggregate_batch_impl(pipeline::context_t* pipeline_context) {
        auto& chunks = left_->output()->chunks();
        std::pmr::vector<types::logical_value_t> results(resource_);
        results.reserve(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            auto data = make_operator_data(resource_, std::move(chunks[i]));
            set_children(boost::intrusive_ptr<operator_t>(new operator_empty_t(resource_, std::move(data))));
            auto res = aggregate_impl(pipeline_context);
            if (res.has_error()) {
                return res.convert_error<compute::datum_t>();
            }
            aggregate_result_ = std::move(res.value());
            results.push_back(aggregate_result_);
        }
        return compute::datum_t{std::move(results)};
    }

    compute::datum_t operator_aggregate_t::take_batch_values() { return std::move(batch_results_); }
} // namespace components::operators::aggregate
