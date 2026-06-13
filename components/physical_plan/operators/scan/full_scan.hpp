#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_state.hpp>
#include <core/result_wrapper.hpp>
#include <components/expressions/compare_expression.hpp>

namespace components::operators {

    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(std::pmr::memory_resource* resource,
                  log_t log,
                  components::catalog::oid_t table_oid,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit,
                  std::vector<size_t> projected_cols = {});

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        components::catalog::oid_t table_oid_;
        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;
    };

} // namespace components::operators
