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

        components::catalog::oid_t table_oid_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::expression_ptr expr_;
        bool upsert_;
        std::optional<resolved_table_metadata_t> resolved_metadata_;
        std::pmr::vector<select_column_t> returning_;
        // UPDATE ... FROM RETURNING: the matched FROM rows, gathered in lockstep
        // with the updated rows so a joined RETURNING column reads the right chunk.
        chunks_vector_t returning_from_chunks_;
    };

} // namespace components::operators
