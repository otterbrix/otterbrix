#pragma once

#include <components/expressions/compare_expression.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <actor-zeta/detail/future.hpp>
#include <services/disk/index_disk.hpp>
#include <memory>

namespace components::collection::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(services::collection::context_collection_t* collection,
                   expressions::compare_expression_ptr expr,
                   logical_plan::limit_t limit);

        bool has_disk_future() const { return disk_future_ != nullptr; }
        bool disk_future_ready() const { return disk_future_ready_; }
        auto& disk_future() { return *disk_future_; }
        void set_disk_result(services::disk::index_disk_t::result result) { disk_result_ = std::move(result); }

        const expressions::compare_expression_ptr& expr() const { return expr_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;
        void on_resume_impl(pipeline::context_t* pipeline_context) override;

        const expressions::compare_expression_ptr expr_;
        const logical_plan::limit_t limit_;

        bool disk_future_ready_{false};
        std::unique_ptr<actor_zeta::unique_future<services::disk::index_disk_t::result>> disk_future_;
        services::disk::index_disk_t::result disk_result_;
    };

} // namespace components::collection::operators
