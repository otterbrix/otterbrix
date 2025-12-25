#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/collection/operators/sort/sort.hpp>

namespace components::collection::operators {

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;

        explicit operator_sort_t(services::collection::context_collection_t* context);

        void add(const std::string& key, order order_ = order::ascending);
        void add(const std::vector<std::string>& keys, order order_ = order::ascending);

    private:
        sort::sorter_t sorter_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::collection::operators
