#pragma once

#include <components/expressions/compare_expression.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(std::pmr::memory_resource* resource, log_t* log,
                   collection_full_name_t name,
                   const expressions::compare_expression_ptr& expr,
                   logical_plan::limit_t limit);

        const collection_full_name_t& collection_name() const noexcept { return name_; }
        const expressions::compare_expression_ptr& expr() const { return expr_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        const expressions::compare_expression_ptr expr_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::operators
