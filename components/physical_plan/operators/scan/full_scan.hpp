#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/scan/scan_identity_projection.hpp>
#include <components/storage/storage.hpp>
#include <components/table/column_state.hpp>
#include <core/result_wrapper.hpp>

namespace components::operators {

    // Build a table_filter_t from a compare-expression predicate + bound parameters, resolving
    // parameter ids against `parameters` and coercing literals in `session_tz`. Defined in
    // full_scan.cpp; used by the full_scan disk-send OPEN to lower the WHERE once.
    // Returns a null unique_ptr for an all-true / absent predicate, or a physical_plan_error /
    // invalid_parameter on a malformed expression — never throws (R2).
    //
    // `schema` is the relation's storage schema as it came back from the disk actor. The lowering
    // itself is positional — a table_filter_t addresses storage columns by ordinal, and every key
    // reaching here already carries the ordinals validation resolved — so what it reads out of the
    // schema is the column's TYPE. It takes the schema rather than a projection of it because that
    // is what its callers hold; extracting a type list to pass here would be a per-scan-open
    // rebuild for nothing.
    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const vector::schema_t& schema,
                        const logical_plan::storage_parameters* parameters,
                        core::date::timezone_offset_t session_tz);

    class full_scan final : public read_only_operator_t {
    public:
        // `table_md` is the relation's resolved catalog metadata (null when the plan has none: the
        // no-table sentinel scan, a unit-test construction). Read in the ctor, not retained: all it
        // decides is whether the identity projection engages. When it does, `projected_cols` is
        // dropped (a pruning index is a LOGICAL ordinal and no longer names the storage column it
        // meant), and a non-null `expression` is REFUSED at open time — a table_filter_t addresses
        // storage columns by the same logical ordinal, so pushing one down a displaced relation
        // would filter on the wrong column. create_plan_match keeps such a predicate above the scan
        // in an operator_match_t instead; this is the guard that makes forgetting that loud.
        full_scan(std::pmr::memory_resource* resource,
                  log_t log,
                  components::catalog::oid_t table_oid,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit,
                  std::vector<size_t> projected_cols = {},
                  const components::logical_plan::resolved_table_metadata_t* table_md = nullptr);

        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::limit_t& limit() const { return limit_; }
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }

        // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
        // role()==source drives the streaming push/finalize pipeline. The FIRST source_next call
        // OPENs a position-only fetch-next cursor on the owning agent (storage_fetch_next_batch,
        // cursor_id==0), passing the filter + projection + the (offset+limit) head cap; each
        // subsequent call ADVANCEs the SAME cursor (cursor_id!=0, filter dropped) and reads exactly
        // ONE batch — zero pins survive the round-trip, so peak scan memory is one batch regardless
        // of table size. A drained cursor (cardinality-0 reply) yields the 0-column sentinel so
        // execute_pipeline stops. The N sequential cross-actor co_awaits live in this NESTED
        // operator coroutine (driven by co_await from execute_pipeline), not in a behavior() handler,
        // so the actor-zeta single-slot awaited continuation is republished+cleared between each
        // sequential await — no lost-wakeup.
        // A no-table sentinel scan (INVALID_OID, e.g. a no-FROM `SELECT 2+3`) is ALSO a
        // source: source_next emits ONE synthetic single-row batch with one placeholder
        // column (not the 0-column drain sentinel), then drains, so the downstream
        // operator_select_t projects its constant/arithmetic columns over that one row —
        // matching the legacy virtual-row path. role() is therefore unconditionally source.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind the per-batch fetch-next cursor so a re-driven sub-plan (recursive-CTE
        // recursive term, re-run once per fixpoint iteration) re-OPENs the scan from the
        // head of the stream instead of seeing a drained cursor. Clears only the streaming
        // cursor bookkeeping; reset_for_reuse() handles the generic state_/output_.
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            cursor_id_ = 0;
            guard_schema_.clear();
        }

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        // Projected empty chunk (drained / short-circuit sentinel) carrying the table schema, so a
        // downstream OUTER join can NULL-pad and a scalar aggregate can emit COUNT=0.
        //
        // This is a USER-VISIBLE result: a query that matches nothing returns exactly this chunk,
        // and the names its columns answer with are the names the cursor reports. It is built from
        // the schema, not from a list of types, so those names are stamped on purpose rather than
        // inherited from wherever a type happens to keep one — and each column carries the identity
        // of the table column it stands for, exactly as a non-empty result does.
        vector::data_chunk_t make_drain_chunk(const vector::schema_t& schema);

        // Apply the drained empty-guard to one fetched batch. Returns the batch to emit, the schema'd
        // empty-guard, or the 0-column drain sentinel. (OFFSET is applied by operator_limit above.)
        actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;
        // Disengaged unless the relation's storage layout has outlived its logical schema
        // (ALTER TABLE ... DROP COLUMN before the next VACUUM). See scan_identity_projection.hpp.
        scan_identity_projection_t identity_projection_{resource_};

        // Per-batch fetch-next cursor state.
        //   opened_   : false until the first source_next runs the one-time setup (short-circuits,
        //               filter build, the storage_types await for the empty-guard schema).
        //   cursor_id_: 0 ⇒ next fetch is the OPEN; non-zero ⇒ the agent-minted id to ADVANCE.
        //   drained_  : the agent returned a cardinality-0 batch ⇒ source exhausted.
        //   emitted_any_ / guard_schema_: if the scan drains having produced zero real rows, emit ONE
        //               schema'd 0-row guard chunk (so a scalar aggregate emits COUNT=0 and an OUTER
        //               join NULL-pads), then the 0-column sentinel.
        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        uint64_t cursor_id_{0};
        vector::schema_t guard_schema_{resource_};
    };

} // namespace components::operators
