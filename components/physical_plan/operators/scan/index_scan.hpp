#pragma once

#include <components/expressions/compare_expression.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/index/index_engine.hpp>
#include <components/index/index.hpp>
#include <memory>

namespace components::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(std::pmr::memory_resource* resource, log_t* log,
                   collection_full_name_t name,
                   index::index_engine_ptr& index_engine,
                   const expressions::compare_expression_ptr& expr,
                   logical_plan::limit_t limit);

        const expressions::compare_expression_ptr& expr() const { return expr_; }
        const logical_plan::limit_t& limit() const { return limit_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        index::index_engine_ptr* index_engine_{nullptr};
        const expressions::compare_expression_ptr expr_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::operators
