#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>
#include <components/physical_plan/operators/resolved_table_metadata.hpp>

#include <optional>

namespace components::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        operator_delete(std::pmr::memory_resource* resource,
                        log_t log,
                        components::catalog::oid_t table_oid,
                        std::pmr::vector<select_column_t> returning,
                        expressions::expression_ptr expr = nullptr);

        // Catalog-table delete (DDL pg_catalog row scrub): deletes every row in
        // `catalog_table_oid` where column[oid_col_idx] == target_oid via the
        // WAL-first delete_pg_catalog_rows path. No predicate scan, no children.
        operator_delete(std::pmr::memory_resource* resource,
                        log_t log,
                        components::catalog::oid_t catalog_table_oid,
                        std::int64_t oid_col_idx,
                        components::catalog::oid_t target_oid);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // STREAMING DML (STEP 3b). Both DELETE shapes that have a scan source are
        // SINKs on the LEFT (target) scan input:
        //   - SIMPLE predicate-scan DELETE (no USING): push() folds each scan batch
        //     via consume_batch_ — matched absolute row-ids into modified_, matched
        //     RETURNING rows, and the matched OLD scan rows (index mirror).
        //   - DELETE ... USING (right_ = the materialized USING scan): push() probes
        //     each LEFT batch against right_->output() via consume_join_batch_ —
        //     same semi-join match, modified_, index-old staging and per-batch joined
        //     RETURNING.
        // The catalog form (oid_col_idx_>=0) is a SOURCELESS sink: it has no children
        // and no scan input — its entire effect is the WAL-first delete_pg_catalog_rows
        // commit in await_async_and_resume, which the executor drives via the bottom-up
        // needs_async_finalize pass (the sourceless-sink-root shape; push()/finalize()
        // are never reached because there is no source to pump). The scan-sourced forms
        // are sinks on the LEFT scan input. The RIGHT (USING) build side is fully
        // materialized before the first push (the executor materializes join build
        // sides — traverse_plan_ split / materialize_build_sides_). needs_async_finalize
        // drives the async commit after the pump (or, for the catalog form, directly).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // Self-contained DML side-effects. Performs storage_delete_rows +
        // WAL physical_delete + index::delete_rows, populates ctx->dml_*
        // swap-info, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Accept pre-resolved table metadata from an upstream resolver
        // sibling. See operator_insert::accept_resolved_metadata.
        void accept_resolved_metadata(resolved_table_metadata_t metadata) override;
        bool wants_resolved_metadata() const noexcept override { return true; }
        bool has_resolved_metadata() const noexcept { return resolved_metadata_.has_value(); }

    private:
        // Shared SIMPLE-path core. Matches expression_ (all-true when null — the
        // scan already filtered) over ONE scan chunk; appends matched ABSOLUTE
        // row-ids to modified_ (dictionary-index branch + chunk.row_ids), stages
        // matched RETURNING rows, and stages the matched OLD scan rows + their
        // absolute row-ids for the index mirror. push() calls it per batch.
        core::error_t consume_batch_(pipeline::context_t* ctx, const vector::data_chunk_t& chunk);
        // Shared DELETE...USING core. Probes ONE LEFT (target) scan chunk against
        // the fully-materialized RIGHT (USING) build chunk as a semi-join, and
        // stages the SAME bounded state consume_batch_ does — matched ABSOLUTE
        // row-ids (DICTIONARY fallback) into modified_, the matched OLD left rows +
        // their ids for the index mirror, and the per-batch joined RETURNING
        // projection (matched left+right pair gathered in lockstep). push() calls
        // it per LEFT batch. await_async_and_resume drains it.
        core::error_t consume_join_batch_(pipeline::context_t* ctx,
                                          const vector::data_chunk_t& chunk_left,
                                          const chunks_vector_t& right_chunks);
        // Lazily create modified_ + the staging buffers for the per-operator init.
        void ensure_simple_init_();

        components::catalog::oid_t table_oid_;
        expressions::expression_ptr expression_;
        std::optional<resolved_table_metadata_t> resolved_metadata_;
        std::pmr::vector<select_column_t> returning_;
        // SIMPLE-path staging (filled by consume_batch_, drained in
        // await_async_and_resume). returning_staged_ holds the projected RETURNING
        // chunks; index_old_chunks_ + index_old_row_ids_ hold the matched OLD scan
        // rows (aligned: index_old_chunks_ merged row i pairs with
        // index_old_row_ids_[i]) so the index mirror does not need left_->output().
        chunks_vector_t returning_staged_{resource_};
        chunks_vector_t index_old_chunks_{resource_};
        std::pmr::vector<int64_t> index_old_row_ids_{resource_};
        bool simple_init_done_{false};
        // Catalog-delete spec (set only by the catalog constructor). oid_col_idx_
        // < 0 marks "not a catalog delete" → the predicate-scan path runs.
        std::int64_t oid_col_idx_{-1};
        components::catalog::oid_t target_oid_{components::catalog::INVALID_OID};
    };

} // namespace components::operators
