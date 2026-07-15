#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // Test-observable counter of storage_update sends the UPDATE sink issued (one
    // bump per flushed batch that reached the disk agent). Pins the clean-failure
    // contract: a statement that fails BEFORE any storage mutation (e.g. a
    // RETURNING projection error) must leave this counter untouched. Process-global
    // + relaxed: coarse instrumentation, mirroring create_index_backfill_batches().
    uint64_t update_storage_update_sends() noexcept;
#endif

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(std::pmr::memory_resource* resource,
                        log_t log,
                        components::catalog::oid_t table_oid,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        std::pmr::vector<select_column_t> returning,
                        expressions::expression_ptr expr = nullptr,
                        // Matched-row bound for the UPDATE ... FROM source path
                        // (UPDATE ... LIMIT n). -1 = unbounded. The no-source path
                        // leaves this -1 (bound applied upstream by the scan /
                        // operator_match count-cap); the source (semi-join) path reads
                        // ALL left rows and stops here at exactly n MATCHED rows (MySQL
                        // "rows-matched restriction" — matched, not value-changed),
                        // across mid-pump flushes.
                        std::int64_t affected_bound = -1);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // STREAMING DML (STEP 3b). Both UPDATE shapes are SINKs on the LEFT (target)
        // scan input:
        //   - SIMPLE predicate-scan UPDATE (no FROM): push() folds each scan batch
        //     via consume_batch_ — matching, applying the SET expressions into
        //     out_chunks accumulated in output_, modified_, and staging
        //     the matched OLD scan rows for the index mirror.
        //   - UPDATE ... FROM (right_ = the materialized FROM scan): push() probes
        //     each LEFT batch against right_->output() via consume_join_batch_ —
        //     same semi-join match, SET application, modified_, index-old
        //     staging and lockstep FROM rows for joined RETURNING.
        // The LEFT scan streams; the RIGHT (FROM) build side is fully materialized
        // before the first push (the executor materializes join build sides —
        // traverse_plan_ split / materialize_build_sides_). needs_async_finalize
        // drives the async commit after the pump.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // Self-contained DML side-effects. Performs storage_update +
        // WAL physical_update + index::update_rows, populates ctx->dml_*
        // swap-info, then mark_executed. Driven INCREMENTALLY: once per
        // mid-pump flush (dml_flush_is_final==false) and once at finalize (==true).
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Bounded-sink hook: rows folded into output_ but not yet flushed. The
        // executor's mid-pump gate compares this to dml_flush_row_threshold; each
        // flush clears output_->chunks(), so it drops back to 0 until push() refills.
        uint64_t buffered_rows() const noexcept override { return output_ ? output_->size() : 0; }

    private:
        // Shared SIMPLE-path core. Matches expr_ (all-true when null — the scan
        // already filtered) over ONE scan chunk; builds the updated out_chunk
        // (matched rows, SET applied), appends it to output_, accumulates
        // modified_, and stages the matched OLD scan rows for the
        // index mirror. push() calls it per batch.
        core::error_t consume_batch_(pipeline::context_t* ctx, const vector::data_chunk_t& chunk);
        // Shared UPDATE...FROM core. Probes ONE LEFT (target) scan chunk against
        // the fully-materialized RIGHT (FROM) build chunks as a semi-join, and
        // stages the SAME bounded state consume_batch_ does — the updated out_chunk
        // (matched columns, DICTIONARY row-id fallback, SET applied) appended to
        // output_, modified_, the matched OLD rows for the index
        // mirror, and (for RETURNING) the matched FROM rows in lockstep. push()
        // calls it per LEFT batch. await_async_and_resume drains it.
        core::error_t consume_join_batch_(pipeline::context_t* ctx,
                                          const vector::data_chunk_t& chunk_left,
                                          const chunks_vector_t& right_chunks);
        // Lazily create modified_/output_ accumulator + staging for
        // the per-operator init.
        void ensure_simple_init_();

        components::catalog::oid_t table_oid_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::expression_ptr expr_;
        bool upsert_;
        std::pmr::vector<select_column_t> returning_;
        // UPDATE ... FROM RETURNING: the matched FROM rows, gathered in lockstep
        // with the updated rows so a joined RETURNING column reads the right chunk.
        chunks_vector_t returning_from_chunks_;
        // SIMPLE-path index-mirror staging (filled by consume_batch_): the matched
        // OLD scan rows, aligned row-for-row with the NEW updated rows accumulated
        // in output_, so update_rows gets old/new/row_id triples without
        // left_->output() (empty when streaming).
        chunks_vector_t index_old_chunks_{resource_};
        bool simple_init_done_{false};
        // Bounded-sink accumulators — persist ACROSS incremental flushes (each
        // flush clears output_/index_old_chunks_/returning_from_chunks_, but these
        // must span the whole statement). returning_accum_ gathers the projected
        // RETURNING chunks from every flush; affected_rows_ totals the storage_update
        // counts when there is NO RETURNING (output_ is cleared per flush, so it
        // cannot double as the affected-count carrier); delete_marker_recorded_
        // guards the single MVCC delete tombstone (one per txn/table, not per flush).
        chunks_vector_t returning_accum_{resource_};
        uint64_t affected_rows_{0};
        bool delete_marker_recorded_{false};
        // UPDATE ... FROM matched-row bound (UPDATE ... LIMIT n); -1 = unbounded.
        // matched_total_ counts MATCHED left rows at MATCH time (in consume_join_batch_)
        // and persists across mid-pump flushes — output_/affected_rows_ are cleared/lag
        // per flush, so a flush-derived count would miss already-flushed matches.
        std::int64_t affected_bound_{-1};
        uint64_t matched_total_{0};
    };

} // namespace components::operators
