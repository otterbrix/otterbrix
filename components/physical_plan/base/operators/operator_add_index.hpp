#pragma once
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::base::operators {

    class operator_add_index final : public read_write_operator_t {
    public:
        operator_add_index(services::collection::context_collection_t* context,
                           logical_plan::node_create_index_ptr node);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        logical_plan::node_create_index_ptr index_node_;
    };

} // namespace components::base::operators
