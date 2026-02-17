#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class aggregation final : public read_only_operator_t {
    public:
        aggregation(std::pmr::memory_resource* resource, log_t log, collection_full_name_t name);

        void set_match(operator_ptr&& match);
        void set_group(operator_ptr&& group);
        void set_sort(operator_ptr&& sort);
        void set_limit(logical_plan::limit_t limit);

    private:
        collection_full_name_t name_;
        operator_ptr match_{nullptr};
        operator_ptr group_{nullptr};
        operator_ptr sort_{nullptr};
        logical_plan::limit_t limit_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
        void on_prepare_impl() override;
    };

} // namespace components::operators
