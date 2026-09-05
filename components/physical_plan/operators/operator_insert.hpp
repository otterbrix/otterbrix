#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // Test-observable count of index-mirror sends an INSERT issues. Every send carries a DEEP
    // COPY of the inserted chunk across a mailbox, and the index manager then walks the rows.
    // On a table with NO indexes all of that produces nothing, so this must read zero there.
    uint64_t insert_index_mirror_sends() noexcept;
    void reset_insert_index_mirror_sends() noexcept;
#endif

    class operator_insert final : public read_write_operator_t {
    public:
        // `returning` holds the RETURNING projection columns (empty when the
        // statement has no RETURNING clause). When non-empty, the operator reads
        // the appended segment back from storage (so anything the storage layer
        // derives for itself is present) and projects these columns into its
        // output instead of an empty result chunk. DEFAULTs are already in the
        // submitted chunk — push() expands them — so they do not depend on it.
        operator_insert(std::pmr::memory_resource* resource,
                        log_t log,
                        catalog::oid_t table_oid,
                        std::pmr::vector<select_column_t> returning);

        catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // One entry per incoming chunk column, resolved by the validator: the name the
        // (name-based) append routes on, and the cast that converts the column to its
        // stored type. Renaming matters for 'INSERT INTO t (id, a.b) SELECT 5, 55',
        // whose projection columns would otherwise leave id/a.b null.
        void set_column_bindings(logical_plan::insert_column_bindings_t bindings) {
            column_bindings_ = std::move(bindings);
        }

        // The table columns this statement did NOT write, with the value each must be
        // filled with — resolved by the enrich pass from pg_attribute.attdefspec, which
        // is the ONLY place a default is read on the write path. push() materialises
        // them, so the chunk that reaches storage, the WAL record and the constraint
        // operators is FULL WIDTH. That is what removes the second source of truth: the
        // storage layer no longer substitutes anything, and there is nothing left to
        // diverge from after a restart. Empty for a dynamic-schema target and for the
        // paths that hand storage ready-made rows (see push()).
        void set_fill_list(logical_plan::insert_fill_list_t fill) { fill_list_ = std::move(fill); }

        // Whether the target table has any index (stamped by enrich onto the plan node).
        // False skips the index mirror entirely. Defaults to true so an unstamped plan
        // behaves exactly as before.
        void set_table_has_indexes(bool value) noexcept { table_has_indexes_ = value; }

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
        std::unique_ptr<execution_dag::execution_dag_t> returning_graph_;
        // Cross-flush accumulators for the incremental drive. RETURNING rows are
        // projected into returning_accum_ as each slice is read back; when the
        // statement has no RETURNING, affected_rows_ tallies the appended count.
        // Both are materialized into output_ only on the final (is_final) drive.
        chunks_vector_t returning_accum_{resource_};
        uint64_t affected_rows_{0};
        logical_plan::insert_column_bindings_t column_bindings_{resource_};
        logical_plan::insert_fill_list_t fill_list_{resource_};
        bool table_has_indexes_{true};
    };

} // namespace components::operators
