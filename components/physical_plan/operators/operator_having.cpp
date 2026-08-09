#include "operator_having.hpp"

namespace components::operators {

    namespace {
        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary; they
        // must be skipped when reading values. operator_group output is dense, so this is defensive.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }
    } // namespace

    operator_having_t::operator_having_t(std::pmr::memory_resource* resource,
                                         log_t log,
                                         const expressions::expression_ptr& expression)
        : read_only_operator_t(resource, log, operator_type::having)
        , expression_(expression) {}

    core::error_t
    operator_having_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Empty-batch guard: defensive symmetry with operator_match (and the schema'd-0-row source
        // contract). operator_group does not actually hand HAVING a 0-row chunk in current code.
        if (input.size() == 0) {
            return core::error_t::no_error();
        }

        // Build + cache the predicate on the first batch. The group-output schema is stable across
        // every finalize chunk (operator_group fixes out_types once), and resource_ (context.resource)
        // outlives them all, so the predicate's value-getter closures stay valid across all chunks.
        if (!stream_ready_) {
            stream_types_.clear();
            stream_types_.reserve(input.column_count());
            for (size_t j = 0; j < input.column_count(); j++) {
                stream_types_.push_back(input.data[j].type());
            }
            // Only slots with real data flow downstream; a projected scan leaves un-projected slots
            // as placeholders (no buffer) so column indices stay stable.
            stream_populated_cols_.clear();
            stream_populated_cols_.reserve(input.column_count());
            for (size_t j = 0; j < input.column_count(); j++) {
                if (!is_placeholder(input.data[j])) {
                    stream_populated_cols_.push_back(j);
                }
            }
            stream_sparse_ = stream_populated_cols_.size() != input.column_count();
            condition_ = expressions::classify_condition(expression_);
            if (condition_ == expressions::condition_kind::computed) {
                auto graph = expressions::build_condition_graph(resource_,
                                                                ctx->parameters.parameters,
                                                                expression_.get(),
                                                                stream_types_);
                if (graph.has_error()) {
                    return graph.error();
                }
                graph_ = std::move(graph.value());
            }
            stream_ready_ = true;
        }

        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }

        // Compute the condition over the whole batch: the graph is a pure N->N computation and
        // THIS operator does the row elimination below.
        std::optional<vector::data_chunk_t> produced;
        if (graph_) {
            auto computed =
                expressions::run_graph(graph_.get(), ctx->parameters.parameters, input, ctx->execution_context);
            if (computed.has_error()) {
                return computed.error();
            }
            produced = std::move(computed.value());
        }
        const vector::vector_t* decisions = produced.has_value() ? &produced->data.front() : nullptr;

        // Build the selection of surviving rows. The selection MUST be sized to the full input
        // length (set_index is unchecked); only the first out_count slots are filled/read.
        vector::indexing_vector_t sel(resource_);
        sel.reset(input.size());
        uint64_t out_count = 0;
        for (uint64_t i = 0; i < input.size(); i++) {
            if (decisions == nullptr || (!decisions->is_null(i) && decisions->get_value<bool>(i))) {
                sel.set_index(out_count, i);
                out_count++;
            }
        }
        if (out_count == 0) {
            return core::error_t::no_error(); // nothing survived — emit no chunk (matches operator_match)
        }

        // TYPED, no-box gather: data_chunk_t::copy routes each column through vector_ops::copy
        // (no per-cell logical_value_t), skips placeholder columns, gathers row_ids (all-zero over a
        // group sink — identical to match's zero sentinel), and sets the target cardinality itself.
        vector::data_chunk_t out_chunk =
            stream_sparse_ ? vector::data_chunk_t(resource_, stream_types_, stream_populated_cols_, out_count)
                           : vector::data_chunk_t(resource_, stream_types_, out_count);
        input.copy(out_chunk, sel, out_count);
        out.emplace_back(std::move(out_chunk));
        return core::error_t::no_error();
    }

} // namespace components::operators
