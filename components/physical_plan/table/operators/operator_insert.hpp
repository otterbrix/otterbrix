#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

namespace components::table::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(services::collection::context_collection_t* collection);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::table::operators
