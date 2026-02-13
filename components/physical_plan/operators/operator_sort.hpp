#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

namespace components::operators {

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;

        operator_sort_t(std::pmr::memory_resource* resource, log_t log);

        void add(size_t index, order order_ = order::ascending);
        // TODO: remove this method, calculate index via schema
        void add(const std::string& key, order order_ = order::ascending);
        void add(const std::vector<size_t>& indices, order order_ = order::ascending);

    private:
        sort::sorter_t sorter_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
