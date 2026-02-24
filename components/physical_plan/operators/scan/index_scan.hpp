#pragma once

#include <components/expressions/compare_expression.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(std::pmr::memory_resource* resource,
                   log_t log,
                   collection_full_name_t name,
                   const expressions::key_t& key,
                   const types::logical_value_t& value,
                   expressions::compare_type compare_type,
                   logical_plan::limit_t limit);

        const collection_full_name_t& collection_name() const noexcept { return name_; }
        const expressions::key_t& key() const { return key_; }
        const types::logical_value_t& value() const { return value_; }
        expressions::compare_type compare_type() const { return compare_type_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        const expressions::key_t key_;
        const types::logical_value_t value_;
        const expressions::compare_type compare_type_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::operators
