#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/physical_plan/operators/resolved_table_metadata.hpp>

#include <optional>

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

        // STREAMING DML (STEP 3b). The insert is a SINK on its scan input
        // (INSERT...SELECT): push() folds each scan batch into a bounded
        // accumulator and emits nothing; the executor then drives the async
        // WAL->storage->index commit via await_async_and_resume after the pump
        // (needs_async_finalize()==true). The VALUES / catalog-row form has a
        // raw_data input (role()==none), so is_streaming_pipeline routes it to
        // the legacy on_execute path automatically — only INSERT...SELECT streams.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // Self-contained DML side-effects. Performs storage_append +
        // WAL physical_insert + index::insert_rows, populates ctx->dml_*
        // swap-info fields, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Accept pre-resolved table metadata from an upstream
        // operator_resolve_table_t sibling (delivered by operator_sequence_t
        // post-resolve). When present, the insert operator will compute a
        // chunk_position -> table_position translation via alias matching
        // just before storage_append.
        void accept_resolved_metadata(resolved_table_metadata_t metadata) override;
        bool wants_resolved_metadata() const noexcept override { return true; }
        bool has_resolved_metadata() const noexcept { return resolved_metadata_.has_value(); }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        catalog::oid_t table_oid_;
        std::optional<resolved_table_metadata_t> resolved_metadata_;
        std::pmr::vector<select_column_t> returning_;
    };

} // namespace components::operators
