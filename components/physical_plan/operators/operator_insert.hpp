#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(std::pmr::memory_resource* resource, log_t log, collection_full_name_t name);

        const collection_full_name_t& collection_name() const noexcept { return name_; }

        // Phase 5: self-contained DML side-effects. Performs storage_append +
        // WAL physical_insert + index::insert_rows, populates ctx->dml_*
        // swap-info fields, then mark_executed.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
    };

} // namespace components::operators
