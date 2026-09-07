#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace components::operators {

#ifdef DEV_MODE
    uint64_t delete_scanned_columns() noexcept;

    // Deterministic hold right after a DELETE's WAL record becomes durable — the interleaving seam
    // of test_delete_floor_resurrection.
    struct delete_wal_apply_gate_t {
        virtual ~delete_wal_apply_gate_t() = default;
        virtual bool hold(components::catalog::oid_t table_oid) = 0;
    };
    void dev_set_delete_wal_apply_gate(delete_wal_apply_gate_t* gate); // nullptr = off
    delete_wal_apply_gate_t* dev_delete_wal_apply_gate();
#endif

    class operator_delete final : public read_write_operator_t {
    public:
        operator_delete(std::pmr::memory_resource* resource,
                        log_t log,
                        components::catalog::oid_t table_oid,
                        std::pmr::vector<projected_column_t> returning,
                        expressions::expression_ptr expr = nullptr,
                        // DELETE...LIMIT n bound for the USING path; -1 = unbounded (no-source is capped upstream).
                        std::int64_t affected_bound = -1);

        // Catalog-table DDL scrub: deletes rows via the WAL-first delete_pg_catalog_rows path.
        operator_delete(std::pmr::memory_resource* resource,
                        log_t log,
                        components::catalog::oid_t catalog_table_oid,
                        std::int64_t oid_col_idx,
                        components::catalog::oid_t target_oid);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // Defaults to true: an unstamped plan must not guess "no index" and skip a real one.
        void set_table_has_indexes(bool value) noexcept { table_has_indexes_ = value; }

        // The catalog form is a SOURCELESS sink (no children, no scan input): its entire effect is
        // the WAL-first commit in await_async_and_resume, driven directly by needs_async_finalize.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // storage_delete_rows + WAL physical_delete + index::delete_rows, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // The catalog form buffers nothing, so it is never mid-flushed.
        [[nodiscard]] uint64_t buffered_rows() const noexcept override { return modified_ ? modified_->size() : 0; }

    private:
        // Matches expression_ over one scan chunk, staging matched rows/ids for RETURNING and the index mirror.
        core::error_t consume_batch_(pipeline::context_t* ctx, const vector::data_chunk_t& chunk);
        // Same staging as consume_batch_, but as a semi-join probe against the materialized RIGHT (USING) side.
        core::error_t consume_join_batch_(pipeline::context_t* ctx,
                                          const vector::data_chunk_t& chunk_left,
                                          const chunks_vector_t& right_chunks);
        // Lazily create modified_ + the staging buffers for the per-operator init.
        void ensure_simple_init_();

        components::catalog::oid_t table_oid_;
        expressions::expression_ptr expression_;
        expressions::condition_kind condition_{expressions::condition_kind::always};
        std::unique_ptr<execution_dag::execution_dag_t> graph_;
        std::pmr::vector<projected_column_t> returning_;
        bool table_has_indexes_{true};
        std::unique_ptr<execution_dag::execution_dag_t> returning_graph_;
        // index_old_chunks_ merged row i pairs with index_old_row_ids_[i] for the index mirror.
        chunks_vector_t returning_staged_{resource_};
        chunks_vector_t index_old_chunks_{resource_};
        std::pmr::vector<int64_t> index_old_row_ids_{resource_};
        bool simple_init_done_{false};
        // delete_marker_recorded_ guards ctx->dml_deletes so repeated mid-flushes push it only once.
        uint64_t affected_rows_{0};
        bool delete_marker_recorded_{false};
        // matched_total_ persists across mid-pump flushes, since modified_ clears on every flush.
        std::int64_t affected_bound_{-1};
        uint64_t matched_total_{0};
        // < 0 marks "not a catalog delete" — the predicate-scan path runs instead.
        std::int64_t oid_col_idx_{-1};
        components::catalog::oid_t target_oid_{components::catalog::INVALID_OID};
    };

} // namespace components::operators
