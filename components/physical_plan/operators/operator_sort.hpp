#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

namespace components::operators {

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;

        operator_sort_t(std::pmr::memory_resource* resource, log_t log);

        // TODO: search-by-key — resolve sort keys by name during plan building
        void add(size_t index, order order_ = order::ascending);
        void add(const std::pmr::vector<size_t>& col_path, order order_ = order::ascending);

    private:
        sort::columnar_sorter_t sorter_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
