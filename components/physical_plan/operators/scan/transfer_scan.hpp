#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/scan/scan_identity_projection.hpp>

#include <vector>

namespace components::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        // Unified ctor: OID-routed (our PR direction) + index-based projection
        // (main's column_pruning output). Empty projected_cols means
        // "pass-through, read all columns".
        //
        // Column indices reference the canonical table schema. This is stable
        // within a single query for both relkind='r' (regular) and relkind='g'
        // (computed/dynamic-schema) tables — chunks are materialized from the
        // table's shared `types_`, and tombstones are filtered at resolve time.
        //
        // `table_md` is the relation's resolved catalog metadata (null when the plan has none:
        // the no-table sentinel scan, a unit-test construction). It is read in the ctor and NOT
        // retained — all it decides is whether the identity projection engages, and with which
        // logical columns. When it does, `projected_cols` is dropped: a pruning index is a
        // LOGICAL ordinal, and once the two layouts disagree it no longer names the storage
        // column it meant to. Reading the relation whole and projecting by identity is the
        // correct answer; VACUUM is what makes pruning meaningful again.
        transfer_scan(std::pmr::memory_resource* resource,
                      components::catalog::oid_t table_oid,
                      logical_plan::limit_t limit,
                      std::vector<size_t> projected_cols = {},
                      const components::logical_plan::resolved_table_metadata_t* table_md = nullptr);

        const logical_plan::limit_t& limit() const { return limit_; }
        // Storage-chunk column indices the scan projects (empty ⇒ all columns), so a downstream
        // group/select sees the SAME projected column layout it expects.
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }

        // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
        // role()==source drives the streaming push/finalize pipeline. The FIRST source_next OPENs a
        // position-only fetch-next cursor (storage_fetch_next_batch, cursor_id==0, no filter —
        // transfer_scan is the unfiltered scan, offset+limit pushed as the head cap); each subsequent
        // call ADVANCEs the same cursor and reads exactly ONE batch — zero pins survive a round-trip,
        // so peak scan memory is one batch. The N sequential cross-actor awaits live in this nested
        // operator coroutine (driven by execute_pipeline), not a behavior() handler — no lost-wakeup.
        // A no-table sentinel scan (INVALID_OID, e.g. a no-FROM `SELECT 2+3`) is ALSO a
        // source: source_next emits ONE synthetic single-row batch carrying one
        // placeholder column (so it is not the 0-column drain sentinel), then drains.
        // The downstream operator_select_t projects its constant/arithmetic columns over
        // that one row — those columns ignore input columns, so the placeholder is inert —
        // yielding exactly the single constants row the legacy virtual-row path produced.
        // role() is therefore unconditionally source.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind the per-batch fetch-next cursor so a re-driven sub-plan re-OPENs from the
        // head of the stream (recursive-CTE recursive term, re-run per fixpoint iteration).
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            guard_schema_loaded_ = false;
            cursor_id_ = 0;
            guard_schema_.clear();
        }

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        // Projected empty chunk (drained sentinel) carrying the table schema, so a downstream OUTER
        // join can NULL-pad and a scalar aggregate can emit COUNT=0.
        //
        // Also the USER's result for a query over an empty table: the names its columns answer with
        // are the names the cursor reports, so they are stamped from the schema records rather than
        // inherited from whatever a bare type happens to carry.
        vector::data_chunk_t make_drain_chunk(const vector::schema_t& schema);

        // Apply the drained empty-guard to one fetched batch. (OFFSET is applied by operator_limit
        // above; every scan receives offset()==0, so there is no per-batch skip.)
        actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;
        // Disengaged unless the relation's storage layout has outlived its logical schema
        // (ALTER TABLE ... DROP COLUMN before the next VACUUM). See scan_identity_projection.hpp.
        scan_identity_projection_t identity_projection_{resource_};

        // Per-batch fetch-next cursor state (see full_scan.hpp for the field semantics; transfer_scan
        // has no filter — the empty-guard schema comes from a lazy storage_types await on the
        // drained-with-zero-rows path).
        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        bool guard_schema_loaded_{false};
        uint64_t cursor_id_{0};
        vector::schema_t guard_schema_{resource_};
    };

} // namespace components::operators