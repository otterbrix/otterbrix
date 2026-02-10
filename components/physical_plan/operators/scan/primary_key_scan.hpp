#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(services::collection::context_collection_t* context);

        void append(size_t id);

    private:
        vector::vector_t rows_;
        size_t size_{0};

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators