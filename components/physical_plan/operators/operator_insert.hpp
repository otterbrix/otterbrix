#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace components::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        // `returning` holds the RETURNING projection columns (empty when the
        // statement has no RETURNING clause). When non-empty, the operator reads
        // the appended segment back from storage (so DB-applied DEFAULTs and
        // generated columns are present) and projects these columns into its
        // output instead of an empty result chunk.
        operator_insert(std::pmr::memory_resource* resource,
                        log_t log,
                        catalog::oid_t table_oid,
                        std::pmr::vector<select_column_t> returning);

        catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // INSERT ... SELECT only: the target column names, in target order. The
        // streamed SELECT columns are renamed to these before the (name-based)
        // append, so 'INSERT INTO t (id, a.b) SELECT 5, 55' lands 5,55 in id,a/b
        // rather than in projection-named columns that leave id/a.b null. Left
        // empty for INSERT ... VALUES, whose raw-data columns are already named.
        // Setting this also makes the write set identity-free — see push(), where
        // the rename drops the source attoid it renames over.
        void set_rename_targets(std::pmr::vector<std::pmr::string> targets) { rename_targets_ = std::move(targets); }

        // STREAMING DML (STEP 3b). The insert is a SINK on its input: push() folds
        // each input batch into a bounded accumulator and emits nothing; the executor
        // then drives the async WAL->storage->index commit via await_async_and_resume
        // after the pump (needs_async_finalize()==true). This streams over BOTH a scan
        // source (INSERT...SELECT) and a raw_data source (INSERT...VALUES, now that
        // operator_raw_data_t is role()==source) — the VALUES rows are folded one
        // chunk at a time instead of adopting left_->output() wholesale.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // Self-contained DML side-effects. Performs storage_append +
        // WAL physical_insert + index::insert_rows, populates ctx->dml_*
        // swap-info fields, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Rows folded into output_ but not yet flushed to storage. The executor's
        // mid-pump flush gate compares this to the flush threshold. Catalog
        // inserts stay single-shot (return 0) so the gate never mid-flushes them.
        [[nodiscard]] uint64_t buffered_rows() const noexcept override {
            return (output_ && !components::catalog::is_catalog_table(table_oid_)) ? output_->size() : 0;
        }

    private:
        catalog::oid_t table_oid_;
        std::pmr::vector<select_column_t> returning_;
        // Cross-flush accumulators for the incremental drive. RETURNING rows are
        // projected into returning_accum_ as each slice is read back; when the
        // statement has no RETURNING, affected_rows_ tallies the appended count.
        // Both are materialized into output_ only on the final (is_final) drive.
        chunks_vector_t returning_accum_{resource_};
        uint64_t affected_rows_{0};
        // Target column names for an INSERT ... SELECT rename (see set_rename_targets).
        std::pmr::vector<std::pmr::string> rename_targets_{resource_};
    };

} // namespace components::operators
