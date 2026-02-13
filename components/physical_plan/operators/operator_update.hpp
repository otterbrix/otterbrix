#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        expressions::compare_expression_ptr comp_expr = nullptr);

        const collection_full_name_t& collection_name() const noexcept { return name_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::compare_expression_ptr comp_expr_;
        bool upsert_;
    };

} // namespace components::operators
