#pragma once

#include <components/physical_plan/operators/aggregate/operator_aggregate.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_group.hpp>

namespace components::operators {

    // One column in the SELECT output list.
    struct select_column_t {
        enum class kind
        {
            field_ref,  // simple column reference (get_field) — uses group_key_t::kind::column
            coalesce,   // COALESCE(...)                       — uses group_key_t::kind::coalesce
            case_when,  // CASE WHEN ... END                   — uses group_key_t::kind::case_when
            arithmetic, // add/subtract/multiply/divide/...    — uses arith_op + operands
            constant,   // literal constant                    — uses constant_value
            aggregate,  // COUNT/SUM/... (global mode only)    — uses aggregator
            star_expand // SELECT * — copy all columns from input chunk as-is
        };

        kind type{kind::field_ref};

        // Used for field_ref, coalesce, case_when.
        // For field_ref: group_key_t::kind::column with full_path set.
        // Alias is always read from key.name.
        group_key_t key;

        // Used for arithmetic.
        expressions::scalar_type arith_op{expressions::scalar_type::invalid};
        std::pmr::vector<expressions::param_storage> operands;

        // Used for constant.
        types::logical_value_t constant_value;

        // Used for aggregate (global_aggregate mode only).
        aggregate::operator_aggregate_ptr aggregator;

        explicit select_column_t(std::pmr::memory_resource* r)
            : key(r)
            , operands(r)
            , constant_value(r, nullptr) {}
    };

    // operator_select_t — always the last operator before DISTINCT.
    //
    // Two modes:
    //   evaluation        — processes rows one-by-one (no GROUP BY, no aggregates).
    //                       Output row count = input row count.
    //   global_aggregate  — treats all input rows as a single group and runs aggregators.
    //                       Output row count = 1.
    //
    // After GROUP BY the operator runs in evaluation mode over the group output chunk.
    class operator_select_t final : public read_write_operator_t {
    public:
        enum class mode
        {
            evaluation,
            global_aggregate
        };

        operator_select_t(std::pmr::memory_resource* resource, log_t log, mode m = mode::evaluation);

        void add_column(select_column_t&& col);
        void set_internal_aggregate_count(size_t n) { internal_aggregate_count_ = n; }

    private:
        std::pmr::vector<select_column_t> columns_;
        mode mode_;
        size_t internal_aggregate_count_{0};

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // Build result chunk row-by-row (evaluation mode).
        vector::data_chunk_t evaluate(pipeline::context_t* pipeline_context, vector::data_chunk_t& input);

        // Build result chunk by accumulating all rows into aggregators (global_aggregate mode).
        vector::data_chunk_t accumulate(pipeline::context_t* pipeline_context, vector::data_chunk_t& input);
    };

} // namespace components::operators
