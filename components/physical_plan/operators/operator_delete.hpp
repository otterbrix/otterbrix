#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>

namespace components::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        explicit operator_delete(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name,
                                 expressions::compare_expression_ptr expr = nullptr);

        const collection_full_name_t& collection_name() const noexcept { return name_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        collection_full_name_t name_;
        expressions::compare_expression_ptr compare_expression_;
    };

} // namespace components::operators