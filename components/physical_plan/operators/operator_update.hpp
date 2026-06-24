#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/physical_plan/operators/resolved_table_metadata.hpp>

#include <optional>

namespace components::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(std::pmr::memory_resource* resource,
                        log_t log,
                        components::catalog::oid_t table_oid,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        std::pmr::vector<select_column_t> returning,
                        expressions::expression_ptr expr = nullptr);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // STREAMING DML (STEP 3b). Both UPDATE shapes are SINKs on the LEFT (target)
        // scan input:
        //   - SIMPLE predicate-scan UPDATE (no FROM): push() folds each scan batch
        //     via consume_batch_ — matching, applying the SET expressions into
        //     out_chunks accumulated in output_, modified_/no_modified_, and staging
        //     the matched OLD scan rows for the index mirror.
        //   - UPDATE ... FROM (right_ = the materialized FROM scan): push() probes
        //     each LEFT batch against right_->output() via consume_join_batch_ —
        //     same semi-join match, SET application, modified_/no_modified_, index-old
        //     staging and lockstep FROM rows for joined RETURNING.
        // The LEFT scan streams; the RIGHT (FROM) build side is fully materialized
        // before the first push (the executor materializes join build sides —
        // traverse_plan_ split / materialize_build_sides_). needs_async_finalize
        // drives the async commit after the pump.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // Self-contained DML side-effects. Performs storage_update +
        // WAL physical_update + index::update_rows, populates ctx->dml_*
        // swap-info, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Accept pre-resolved table metadata from an upstream resolver
        // sibling. See operator_insert::accept_resolved_metadata.
        void accept_resolved_metadata(resolved_table_metadata_t metadata) override;
        bool wants_resolved_metadata() const noexcept override { return true; }
        bool has_resolved_metadata() const noexcept { return resolved_metadata_.has_value(); }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // Shared SIMPLE-path core (R6: one implementation, two entry points).
        // Matches expr_ (all-true when null — the scan already filtered) over ONE
        // scan chunk; builds the updated out_chunk (matched rows, SET applied),
        // appends it to output_, accumulates modified_/no_modified_, and stages the
        // matched OLD scan rows for the index mirror. push() calls it per batch;
        // on_execute_impl's simple path loops left_->output()->chunks() through it.
        core::error_t consume_batch_(pipeline::context_t* ctx, const vector::data_chunk_t& chunk);
        // Shared UPDATE...FROM core (R6: one implementation, two entry points).
        // Probes ONE LEFT (target) scan chunk against the fully-materialized RIGHT
        // (FROM) build chunks as a semi-join, and stages the SAME bounded state
        // consume_batch_ does — the updated out_chunk (matched columns, DICTIONARY
        // row-id fallback, SET applied) appended to output_, modified_/no_modified_,
        // the matched OLD rows for the index mirror, and (for RETURNING) the matched
        // FROM rows in lockstep. push() calls it per LEFT batch; on_execute_impl loops
        // the materialized left chunks through it. await_async_and_resume drains it.
        core::error_t consume_join_batch_(pipeline::context_t* ctx,
                                          const vector::data_chunk_t& chunk_left,
                                          const chunks_vector_t& right_chunks);
        // Lazily create modified_/no_modified_/output_ accumulator + staging so
        // push() and on_execute_impl share the same per-operator init.
        void ensure_simple_init_();

        components::catalog::oid_t table_oid_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::expression_ptr expr_;
        bool upsert_;
        std::optional<resolved_table_metadata_t> resolved_metadata_;
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
    };

} // namespace components::operators
