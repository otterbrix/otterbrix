#include "operator_match.hpp"

#include "predicates/predicate.hpp"
#include <components/expressions/function_expression.hpp>

namespace components::operators {

    namespace {
        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
        // They must be skipped when reading values — vector_t::value() would crash otherwise.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }
    } // namespace

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    // Build the predicate + the populated-column projection metadata for one input
    // chunk schema. Shared one-time setup for both the materialized and the streaming
    // entry point: the predicate depends only on the (stable) chunk SCHEMA, not its
    // arena, so each entry point builds it once.
    //
    // DEFECT FIX (b): the predicate and the `types` working copy are allocated on the
    // caller-chosen STABLE `resource`, NOT on the per-batch sample.resource(). The
    // caller passes effective_resource_(sample) — the operator's own resource_ when it
    // has one, else the input chunk's resource captured once for the whole streaming
    // run (see push()). Over a SCAN source this is the operator's resource exactly as
    // before; over a SINK (a group/join output) it is the sink operator's stable
    // resource, which outlives every batch — so a predicate whose value-getter closures
    // were allocated here does NOT dangle on the second finalize chunk (the prior code
    // rebuilt against each batch's arena). The types are COPIED element-wise from the
    // sample's column types (never move-assigned from sample.types(), whose vector is
    // allocated on the foreign/null sink arena — that move-assign compares allocators
    // and dereferences the dangling sink resource).
    core::error_t operator_match_t::build_predicate_(pipeline::context_t* ctx,
                                                     const vector::data_chunk_t& sample,
                                                     std::pmr::memory_resource* resource,
                                                     std::pmr::vector<types::complex_logical_type>& types,
                                                     std::vector<size_t>& populated_cols,
                                                     bool& sparse,
                                                     predicates::predicate_ptr& predicate) {
        // `types` is already constructed on `resource` by the caller (so its allocator
        // is the stable resource, never null), so an in-place fill is safe.
        types.clear();
        types.reserve(sample.column_count());
        for (size_t j = 0; j < sample.column_count(); j++) {
            types.push_back(sample.data[j].type());
        }

        // populated_cols: only slots with real data flow downstream. A projected scan
        // leaves the un-projected slots as placeholders (no buffer) so column indices
        // stay stable; vector_t::value() would crash on those, so they are skipped.
        populated_cols.clear();
        populated_cols.reserve(sample.column_count());
        for (size_t j = 0; j < sample.column_count(); j++) {
            if (!is_placeholder(sample.data[j])) {
                populated_cols.push_back(j);
            }
        }
        sparse = populated_cols.size() != sample.column_count();

        predicate = expression_ ? predicates::create_predicate(resource,
                                                               ctx->function_registry,
                                                               expression_,
                                                               types,
                                                               types,
                                                               &ctx->parameters,
                                                               ctx->session_tz)
                                : predicates::create_all_true_predicate(resource);
        return core::error_t::no_error();
    }

    // Shared filter core (R6): filter ONE chunk through the predicate + projection,
    // advancing the caller-owned LIMIT/OFFSET running counter `limit_total` across
    // batches, and append the surviving-rows chunk to `out`. Called by push() (per
    // streamed batch, MEMBER counter). The predicate compares chunk against itself
    // (single-input filter), surviving rows carry their absolute row_id, and only
    // populated (non-placeholder) columns are copied — exactly as the prior single-loop
    // implementation did.
    core::error_t operator_match_t::filter_batch_(std::pmr::memory_resource* resource,
                                                  predicates::predicate_ptr& predicate,
                                                  const std::vector<size_t>& populated_cols,
                                                  bool sparse,
                                                  bool row_ids_meaningful,
                                                  const std::pmr::vector<types::complex_logical_type>& types,
                                                  const vector::data_chunk_t& chunk,
                                                  int64_t& limit_total,
                                                  chunks_vector_t& out) {
        // Already at the LIMIT, or an empty input batch: nothing to emit.
        if (!limit_.check(limit_total) || chunk.size() == 0) {
            return core::error_t::no_error();
        }

        vector::data_chunk_t out_chunk = sparse ? vector::data_chunk_t(resource, types, populated_cols, chunk.size())
                                                : vector::data_chunk_t(resource, types, chunk.size());
        vector::indexing_vector_t all_indices(nullptr, nullptr);
        auto results = predicate->batch_check(chunk, chunk, all_indices, all_indices, chunk.size());
        if (results.has_error()) {
            return results.error();
        }
        int64_t out_count = 0;
        for (size_t i = 0; i < chunk.size(); i++) {
            if (results.value()[i]) {
                if (!limit_.is_skipping(limit_total)) {
                    for (size_t j : populated_cols) {
                        out_chunk.set_value(j, static_cast<uint64_t>(out_count), chunk.data[j].value(i));
                    }
                    // DEFECT FIX (a): only propagate the input row_id when it is a REAL
                    // absolute id (input is a scan source's batch). Over a SINK
                    // (group/join) the input's row_ids are zero-filled placeholders; the
                    // out_chunk's row_ids are likewise zero-initialized, so leaving them
                    // (no copy) reproduces exactly what the materialized path produced
                    // over a sink — and crucially never hands a downstream DML/index
                    // consumer the bogus absolute id 0.
                    if (row_ids_meaningful) {
                        out_chunk.row_ids.data<int64_t>()[out_count] = chunk.row_ids.data<int64_t>()[i];
                    }
                    ++out_count;
                }
                ++limit_total;
                if (!limit_.check(limit_total)) {
                    break;
                }
            }
        }
        out_chunk.set_cardinality(static_cast<uint64_t>(out_count));
        if (out_count > 0) {
            out.emplace_back(std::move(out_chunk));
        }
        return core::error_t::no_error();
    }

    core::error_t operator_match_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Streaming filter: run the per-chunk filter+projection on the single batch
        // handed in via `input`. The predicate + projection metadata depend only on the
        // (stable) chunk schema, so they are built once on the first batch and reused.
        // The LIMIT/OFFSET counter (limit_total_) persists across calls so a LIMIT caps
        // the total emitted across ALL batches and an OFFSET skips the head of the
        // stream.
        if (!stream_ready_) {
            // Stable resource for the whole streaming run: the operator's own resource_
            // when it has one (scan-source matches), else the input chunk's resource —
            // captured ONCE here. Over a sink (group/join) the match is built with a
            // null resource_ (the create_plan "no table_oid" fallback), so it must fall
            // back to the sink output's resource, which is the sink operator's stable
            // resource and outlives every batch. The cached predicate is allocated on
            // it, so it stays valid across all batches (defect fix (b)).
            stream_resource_ = resource_ ? resource_ : input.resource();
            // Rebind stream_types_ to the stable resource (it was member-initialized
            // with the possibly-null resource_). Destroy + placement-construct so its
            // allocator is the chosen resource — a plain assignment would compare the
            // old (null) allocator and crash.
            stream_types_.~vector();
            new (&stream_types_) std::pmr::vector<types::complex_logical_type>(stream_resource_);
            auto err = build_predicate_(ctx,
                                        input,
                                        stream_resource_,
                                        stream_types_,
                                        stream_populated_cols_,
                                        stream_sparse_,
                                        stream_predicate_);
            if (err.contains_error()) {
                return err;
            }
            stream_ready_ = true;
        }
        return filter_batch_(stream_resource_,
                             stream_predicate_,
                             stream_populated_cols_,
                             stream_sparse_,
                             row_ids_meaningful_(),
                             stream_types_,
                             input,
                             stream_limit_total_,
                             out);
    }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_match_t::source_next(pipeline::context_t* /*ctx*/) {
        // Sourceless no-table match (left_ == nullptr): no input to filter, so drain
        // immediately with the 0-column sentinel — an empty result.
        co_return core::result_wrapper_t<vector::data_chunk_t>(
            vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0});
    }

} // namespace components::operators
