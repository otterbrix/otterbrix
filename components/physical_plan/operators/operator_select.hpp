#pragma once

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

        explicit select_column_t(std::pmr::memory_resource* r)
            : key(r)
            , operands(r)
            , constant_value(r, nullptr) {}
    };

    // operator_select_t — always the last operator before DISTINCT.
    // Processes rows one-by-one (evaluation mode): output row count equals input row count.
    // Aggregation is always handled upstream by operator_group_t.
    class operator_select_t final : public read_write_operator_t {
    public:
        operator_select_t(std::pmr::memory_resource* resource, log_t log);

        void add_column(select_column_t&& col);

    private:
        std::pmr::vector<select_column_t> columns_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // Build result chunk row-by-row.
        vector::data_chunk_t evaluate(pipeline::context_t* pipeline_context, vector::data_chunk_t& input);
    };

} // namespace components::operators
