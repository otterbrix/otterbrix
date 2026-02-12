#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        transfer_scan(std::pmr::memory_resource* resource, collection_full_name_t name,
                      logical_plan::limit_t limit);

        const logical_plan::limit_t& limit() const { return limit_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        const logical_plan::limit_t limit_;
    };

} // namespace components::operators
