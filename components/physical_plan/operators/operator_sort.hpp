#pragma once

#include <components/expressions/expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

namespace components::operators {

    // A sort key that must be computed via an arithmetic expression.
    // Used when ORDER BY references a SELECT alias like "ORDER BY a + b" or "ORDER BY c"
    // where c is defined as "a + b AS c" in the SELECT list.
    struct computed_sort_key_t {
        explicit computed_sort_key_t(std::pmr::memory_resource* r)
            : operands(r) {}
        expressions::scalar_type op{expressions::scalar_type::invalid};
        std::pmr::vector<expressions::param_storage> operands;
        sort::order order_{sort::order::ascending};
    };

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;

        operator_sort_t(std::pmr::memory_resource* resource, log_t log);

        void add(size_t index, order order_ = order::ascending);
        void add(const std::pmr::vector<size_t>& col_path, order order_ = order::ascending);
        void add_computed(computed_sort_key_t&& key);

        void set_expected_output_count(size_t n) { expected_output_count_ = n; }

    private:
        sort::columnar_sorter_t sorter_;
        std::pmr::vector<computed_sort_key_t> computed_keys_;
        size_t expected_output_count_{0};

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
