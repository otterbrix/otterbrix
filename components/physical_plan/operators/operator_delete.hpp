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
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        components::catalog::oid_t table_oid_;
        expressions::expression_ptr expression_;
        std::optional<resolved_table_metadata_t> resolved_metadata_;
        std::pmr::vector<select_column_t> returning_;
        // Catalog-delete spec (set only by the catalog constructor). oid_col_idx_
        // < 0 marks "not a catalog delete" → the predicate-scan path runs.
        std::int64_t oid_col_idx_{-1};
        components::catalog::oid_t target_oid_{components::catalog::INVALID_OID};
    };

} // namespace components::operators
