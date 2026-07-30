#include "operator_match.hpp"

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

    // Bind the filter expression and build its executor, plus the populated-column projection
    // metadata, for one input chunk schema. Shared one-time setup: the bound tree depends only on
    // the (stable) chunk SCHEMA, not its arena, so this runs on the first batch and never again.
    //
    // DEFECT FIX (b): the bound tree and the executor's intermediates are allocated on the
    // caller-chosen STABLE `resource`, NOT on the per-batch sample.resource(). The caller passes the
    // operator's own resource_ when it has one, else the input chunk's resource captured once for
    // the whole streaming run (see push()). Over a SCAN source this is the operator's resource
    // exactly as before; over a SINK (a group/join output) it is the sink operator's stable
    // resource, which outlives every batch — so an executor whose result slots were allocated here
    // does NOT dangle on the second finalize chunk. The types are COPIED element-wise from the
    // sample's column types (never move-assigned from sample.types(), whose vector is allocated on
    // the foreign/null sink arena — that move-assign compares allocators and dereferences the
    // dangling sink resource).
    core::error_t operator_match_t::build_executor_(pipeline::context_t* ctx,
                                                    const vector::data_chunk_t& sample,
                                                    std::pmr::memory_resource* resource,
                                                    vector::schema_t& schema,
                                                    std::vector<size_t>& populated_cols,
                                                    bool& sparse) {
        // `schema` is already constructed on `resource` by the caller (so its allocator
        // is the stable resource, never null), so an in-place fill is safe.
        schema.clear();
        schema.reserve(sample.column_count());
        for (const auto& record : sample.schema()) {
            schema.push_back(record.clone(resource));
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

        // The input schema the tree binds against, restated in the binder's own carrier. Names are
        // a FALLBACK for binding, because every key that reaches an operator already carries the
        // ordinals validate_logical_plan resolved for it and binder_t::bind_key uses those first.
        expressions::bind_schema_t bind_schema{resource};
        for (const auto& record : schema) {
            bind_schema.add(std::string_view{record.name}, record.type);
        }

        expressions::binder_context_t bind_context{};
        bind_context.left = &bind_schema;
        bind_context.right = &bind_schema; // a single-input filter compares the chunk against itself
        bind_context.functions = ctx->function_registry;
        bind_context.parameters = &ctx->parameters;
        bind_context.session_tz = ctx->session_tz;

        expressions::binder_t binder{resource};
        expressions::bound_expression_ptr root;
        if (expression_) {
            auto bound = binder.bind(expression_, bind_context);
            if (bound.has_error()) {
                return bound.error();
            }
            root = std::move(bound.value());
        } else {
            // No filter expression: every row survives. Bound as a CONSTANT rather than branched
            // around, so there is exactly ONE evaluation path below (rule 6) — and, being foldable,
            // the executor evaluates it once in create() and never per chunk.
            root = expressions::bound_expression_ptr{
                expressions::make_bound_constant(resource, types::logical_value_t{resource, true})};
        }

        auto executor = expressions::expression_executor_t::create(resource, std::move(root));
        if (executor.has_error()) {
            return executor.error();
        }
        // emplace, not assignment: the executor hands out pointers into its own slots, so it is
        // move-CONSTRUCTIBLE but deliberately not assignable.
        stream_executor_.emplace(std::move(executor.value()));

        // Allocated ONCE, for the widest chunk this operator can be handed. reset() below is on the
        // full input length per batch because indexing_vector_t::set_index is unchecked.
        stream_selection_ = vector::indexing_vector_t{resource, vector::DEFAULT_VECTOR_CAPACITY};
        return core::error_t::no_error();
    }

    // Shared filter core (R6): filter ONE chunk through the bound expression + projection,
    // advancing the caller-owned LIMIT running counter `limit_total` across batches, and append the
    // surviving-rows chunk to `out`. Called by push() (per streamed batch, MEMBER counter). The
    // expression is evaluated over the WHOLE batch at once and answers a selection vector; the
    // surviving rows are then gathered TYPED, with no per-cell logical_value_t.
    core::error_t operator_match_t::filter_batch_(std::pmr::memory_resource* resource,
                                                  const std::vector<size_t>& populated_cols,
                                                  bool sparse,
                                                  bool row_ids_meaningful,
                                                  const vector::schema_t& schema,
                                                  const vector::data_chunk_t& chunk,
                                                  int64_t& limit_total,
                                                  chunks_vector_t& out) {
        // Already at the LIMIT, or an empty input batch: nothing to emit.
        if (!limit_.check(limit_total) || chunk.size() == 0) {
            return core::error_t::no_error();
        }

        expressions::expression_executor_t::context_t execution{};
        execution.parameters = current_parameters_;
        execution.session_tz = current_session_tz_;

        // ONE evaluation for the whole batch. select() applies the WHERE rule itself: a row is
        // selected only when the predicate is definitely TRUE, so a NULL operand (UNKNOWN) drops the
        // row and a NOT above it cannot turn that into TRUE.
        stream_selection_.reset(chunk.size());
        auto selected = stream_executor_->select(chunk, chunk.size(), execution, stream_selection_);
        if (selected.has_error()) {
            return selected.error();
        }

        // Truncate the selection to the LIMIT in place. The `break` IS the cap: out_count ends up as
        // min(survivors, remaining budget) without anyone computing a remainder — recomputing it as
        // "limit - total" would duplicate limit_t's unlimit_ sentinel outside limit_t, where -1
        // means "no limit" and would silently read as a budget of minus one.
        uint64_t out_count = 0;
        for (uint64_t k = 0; k < selected.value(); ++k) {
            ++out_count;
            // Count-cap: the AUTHORITATIVE affected-row bound for DML …WHERE f(x) LIMIT n
            // (no operator_limit over a DML root), and an advisory read-cap under
            // operator_limit for SELECT. OFFSET is applied by operator_limit (SELECT) and
            // does not exist for DML, so this stream is never skipped, only capped.
            ++limit_total;
            if (!limit_.check(limit_total)) {
                break;
            }
        }
        if (out_count == 0) {
            return core::error_t::no_error(); // nothing survived — emit no chunk
        }

        vector::data_chunk_t out_chunk = sparse ? vector::make_chunk(resource, schema, populated_cols, out_count)
                                                : vector::make_chunk(resource, schema, out_count);
        // TYPED, no-box gather: data_chunk_t::copy routes each column through vector_ops::copy
        // (no per-cell logical_value_t), skips placeholder columns, and sets the cardinality itself.
        // It is the same primitive operator_having uses over a group sink.
        chunk.copy(out_chunk, stream_selection_, out_count);

        // Only propagate the input row_id when it is a REAL absolute id (the input is a scan
        // source's batch). Over a SINK (group/join) — or the sourceless no-table shape — the input's
        // row_ids are placeholders, so the gathered ones are OVERWRITTEN with the zero sentinel the
        // out chunk was born with. Today's sinks already answer all-zero row_ids, but that is a
        // property of the PLAN BUILDER, not of sinks — operator_sort::finalize copies its input's
        // row_ids forward (operator_sort.cpp), so a plan shape that put a sort under a filter would
        // break the premise silently, and a downstream DML/index consumer would be handed a foreign
        // absolute id. One pass over the surviving rows buys that away.
        if (!row_ids_meaningful) {
            auto* ids = out_chunk.row_ids.data<int64_t>();
            for (uint64_t k = 0; k < out_count; ++k) {
                ids[k] = 0;
            }
        }
        out.emplace_back(std::move(out_chunk));
        return core::error_t::no_error();
    }

    core::error_t operator_match_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Streaming filter: run the per-chunk filter+projection on the single batch
        // handed in via `input`. The bound tree + projection metadata depend only on the
        // (stable) chunk schema, so they are built once on the first batch and reused.
        // The LIMIT counter (limit_total_) persists across calls so a LIMIT caps the
        // total emitted across ALL batches. (OFFSET is applied by operator_limit for
        // SELECT and does not exist for DML, so the stream is capped, never skipped.)
        //
        // The parameter map is re-read from the context on EVERY batch, never captured: a
        // correlated (LATERAL) sub-query rebinds its correlation slots between two runs of this
        // same operator, and the bound tree reads those slots live.
        current_parameters_ = &ctx->parameters;
        current_session_tz_ = ctx->session_tz;
        if (!stream_ready_) {
            // Stable resource for the whole streaming run: the operator's own resource_
            // when it has one (scan-source matches), else the input chunk's resource —
            // captured ONCE here. Over a sink (group/join) the match is built with a
            // null resource_ (the create_plan "no table_oid" fallback), so it must fall
            // back to the sink output's resource, which is the sink operator's stable
            // resource and outlives every batch. The bound tree and the executor's slots
            // are allocated on it, so they stay valid across all batches (defect fix (b)).
            stream_resource_ = resource_ ? resource_ : input.resource();
            // Rebind stream_schema_ to the stable resource (it was member-initialized
            // with the possibly-null resource_). Destroy + placement-construct so its
            // allocator is the chosen resource — a plain assignment would compare the
            // old (null) allocator and crash.
            stream_schema_.~vector();
            new (&stream_schema_) vector::schema_t(stream_resource_);
            auto err = build_executor_(ctx,
                                       input,
                                       stream_resource_,
                                       stream_schema_,
                                       stream_populated_cols_,
                                       stream_sparse_);
            if (err.contains_error()) {
                return err;
            }
            stream_ready_ = true;
        }
        return filter_batch_(stream_resource_,
                             stream_populated_cols_,
                             stream_sparse_,
                             row_ids_meaningful_(),
                             stream_schema_,
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
