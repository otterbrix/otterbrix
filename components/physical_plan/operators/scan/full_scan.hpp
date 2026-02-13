#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_state.hpp>
#include <expressions/compare_expression.hpp>

namespace components::operators {

    std::unique_ptr<table::table_filter_t>
    transform_predicate(const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit);

        const collection_full_name_t& collection_name() const noexcept { return name_; }
        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::operators
