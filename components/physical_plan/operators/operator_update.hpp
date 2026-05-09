#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(std::pmr::memory_resource* resource,
                        log_t log,
                        collection_full_name_t name,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        expressions::expression_ptr expr = nullptr);

        const collection_full_name_t& collection_name() const noexcept { return name_; }

        // Phase 5: self-contained DML side-effects. Performs storage_update +
        // WAL physical_update + index::update_rows, populates ctx->dml_*
        // swap-info, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::expression_ptr expr_;
        bool upsert_;
    };

} // namespace components::operators
