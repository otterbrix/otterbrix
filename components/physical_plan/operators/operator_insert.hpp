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
