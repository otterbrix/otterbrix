#pragma once
#include <components/physical_plan/base/operators/operator.hpp>

#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_drop_index.hpp>

namespace components::base::operators {

    class operator_drop_index final : public read_write_operator_t {
    public:
        operator_drop_index(services::collection::context_collection_t* context,
                            logical_plan::node_drop_index_ptr node);

        const cursor::cursor_t_ptr& error_cursor() const { return error_cursor_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        logical_plan::node_drop_index_ptr node_;
        cursor::cursor_t_ptr error_cursor_{nullptr};
    };

} // namespace components::base::operators
