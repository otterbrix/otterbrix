#include "operator_having.hpp"

#include "predicates/predicate.hpp"

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
            // expression_ is ALWAYS non-null (create_plan_having returns nullptr on empty expressions),
            // so there is no create_all_true_predicate branch.
            stream_predicate_ = predicates::create_predicate(resource_,
                                                             ctx->function_registry,
                                                             expression_,
                                                             stream_types_,
                                                             stream_types_,
                                                             &ctx->parameters,
                                                             ctx->session_tz);
            stream_ready_ = true;
        }

        // Evaluate the predicate over the whole batch (single-input filter: chunk compared to itself).
        vector::indexing_vector_t all_indices(nullptr, nullptr);
        auto results = stream_predicate_->batch_check(input, input, all_indices, all_indices, input.size());
        if (results.has_error()) {
            return results.error();
        }
        const std::vector<types::tri_bool_t>& mask = results.value();

        // Build the selection of surviving (predicate-true) rows. The selection MUST be sized to the
        // full input length (set_index is unchecked); only the first out_count slots are filled/read.
        // HAVING keeps a group only when the predicate is definitely TRUE -- a NULL operand (e.g. an
        // aggregate over an all-NULL group) yields UNKNOWN, which drops the group, exactly as WHERE
        // drops an UNKNOWN row.
        vector::indexing_vector_t sel(resource_);
        sel.reset(input.size());
        uint64_t out_count = 0;
        for (uint64_t i = 0; i < input.size(); i++) {
            if (types::selects(mask[i])) {
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
