#include "operator_having.hpp"

#include "predicate_executor.hpp"

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
            stream_schema_ = vector::clone_schema(resource_, input.schema());
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
            // expression_ is ALWAYS non-null (create_plan_having returns nullptr on empty
            // expressions), so the null-expression ("every row matches") shape is unused here.
            // A HAVING is a single-input filter: the chunk is compared against itself, so the same
            // schema is both sides.
            auto executor = predicate_executor_t::create(resource_,
                                                         expression_,
                                                         stream_schema_,
                                                         stream_schema_,
                                                         ctx->function_registry,
                                                         &ctx->parameters,
                                                         ctx->session_tz);
            if (executor.has_error()) {
                return executor.error();
            }
            stream_predicate_.emplace(std::move(executor.value()));
            // Sized ONCE to the widest batch: set_index is unchecked, so the selection must be as
            // long as the input can be, not as long as the first input happened to be.
            stream_selection_ = vector::indexing_vector_t{resource_, vector::DEFAULT_VECTOR_CAPACITY};
            stream_ready_ = true;
        }

        // ONE evaluation for the whole batch. select() applies the rule itself: a group survives only
        // when the predicate is definitely TRUE, so a NULL operand (an aggregate over an all-NULL
        // group) yields UNKNOWN and drops the group, exactly as a WHERE drops an UNKNOWN row.
        stream_selection_.reset(input.size());
        auto selected =
            stream_predicate_->select(input, input.size(), ctx->parameters, ctx->session_tz, stream_selection_);
        if (selected.has_error()) {
            return selected.error();
        }
        const uint64_t out_count = selected.value();
        auto& sel = stream_selection_;
        if (out_count == 0) {
            return core::error_t::no_error(); // nothing survived — emit no chunk (matches operator_match)
        }

        // TYPED, no-box gather: data_chunk_t::copy routes each column through vector_ops::copy
        // (no per-cell logical_value_t), skips placeholder columns, gathers row_ids (all-zero over a
        // group sink — identical to match's zero sentinel), and sets the target cardinality itself.
        vector::data_chunk_t out_chunk =
            stream_sparse_ ? vector::make_chunk(resource_, stream_schema_, stream_populated_cols_, out_count)
                           : vector::make_chunk(resource_, stream_schema_, out_count);
        input.copy(out_chunk, sel, out_count);
        out.emplace_back(std::move(out_chunk));
        return core::error_t::no_error();
    }

} // namespace components::operators
