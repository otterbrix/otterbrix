#pragma once

#include "operator_merge.hpp"

namespace components::collection::operators::merge {

    class operator_or_t final : public operator_merge_t {
    public:
        explicit operator_or_t(services::collection::context_collection_t* context, logical_plan::limit_t limit);

    private:
        void on_merge_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::collection::operators::merge
