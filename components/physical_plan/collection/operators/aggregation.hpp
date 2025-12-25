#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators {

    class aggregation final : public read_only_operator_t {
    public:
        explicit aggregation(services::collection::context_collection_t* context);

        void set_match(operator_ptr&& match);
        void set_group(operator_ptr&& group);
        void set_sort(operator_ptr&& sort);

    private:
        operator_ptr match_{nullptr};
        operator_ptr group_{nullptr};
        operator_ptr sort_{nullptr};

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
        void on_prepare_impl() override;
    };

} // namespace components::collection::operators
