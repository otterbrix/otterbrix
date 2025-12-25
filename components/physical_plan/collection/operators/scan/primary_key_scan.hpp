#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(services::collection::context_collection_t* context);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::collection::operators