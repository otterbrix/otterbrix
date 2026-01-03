#pragma once
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <actor-zeta/detail/future.hpp>
#include <memory>

namespace components::base::operators {

    class operator_add_index final : public read_write_operator_t {
    public:
        operator_add_index(services::collection::context_collection_t* context,
                           logical_plan::node_create_index_ptr node);

        const std::string& index_name() const { return index_name_; }
        bool disk_future_ready() const { return disk_future_ready_; }
        auto& disk_future() { return *disk_future_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        logical_plan::node_create_index_ptr index_node_;
        std::string index_name_;  // cached before index_node_ is moved
        bool disk_future_ready_{false};
        std::unique_ptr<actor_zeta::unique_future<actor_zeta::address_t>> disk_future_;
    };

} // namespace components::base::operators
