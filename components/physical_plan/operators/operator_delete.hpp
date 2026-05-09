#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>

namespace components::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        explicit operator_delete(std::pmr::memory_resource* resource,
                                 log_t log,
                                 collection_full_name_t name,
                                 expressions::expression_ptr expr = nullptr);

        const collection_full_name_t& collection_name() const noexcept { return name_; }

        // Phase 5: self-contained DML side-effects. Performs
        // storage_delete_rows + WAL physical_delete + index::delete_rows,
        // populates ctx->dml_* swap-info, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        expressions::expression_ptr expression_;
    };

} // namespace components::operators