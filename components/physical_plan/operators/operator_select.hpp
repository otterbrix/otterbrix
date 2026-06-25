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
        // Used for arithmetic.
        expressions::scalar_type arith_op{expressions::scalar_type::invalid};

        // Used for field_ref, coalesce, case_when.
        // For field_ref: group_key_t::kind::column with full_path set.
        // Alias is always read from key.name.
        group_key_t key;

        std::pmr::vector<expressions::param_storage> operands;

        // Used for constant.
        types::logical_value_t constant_value;

        // Plan-time resolved output type (variant 1). The column type IS this type,
        // authoritatively — the validator resolved it data-independently and physgen
        // forwarded it, so a projection over zero rows still produces a correctly-typed
        // column (the per-row type-from-first-value seed cannot run over zero rows).
        types::complex_logical_type result_type;

        explicit select_column_t(std::pmr::memory_resource* r)
            : key(r)
            , operands(r)
            , constant_value(r, nullptr) {}
    };

    // Evaluate a projection column list against ONE input chunk, producing an
    // output chunk with one column per select_column_t (row count == input row
    // count). Because the projection is a 1:1 row mapping, a <=1024-row input
    // yields a <=1024-row output, so callers stay within DEFAULT_VECTOR_CAPACITY
    // by feeding one chunk at a time and accumulating a chunks_vector_t.
    // Shared by operator_select_t and the DML operators' RETURNING path.
    core::result_wrapper_t<vector::data_chunk_t> evaluate_projection(std::pmr::memory_resource* resource,
                                                                     const std::pmr::vector<select_column_t>& columns,
                                                                     vector::data_chunk_t* left_input,
                                                                     const logical_plan::storage_parameters& parameters,
                                                                     core::date::timezone_offset_t session_tz,
                                                                     vector::data_chunk_t* right_input = nullptr);

    // operator_select_t — always the last operator before DISTINCT.
    // Processes rows one-by-one (evaluation mode): output row count equals input row count.
    // Aggregation is always handled upstream by operator_group_t.
    class operator_select_t final : public read_write_operator_t {
    public:
        operator_select_t(std::pmr::memory_resource* resource, log_t log);

        void add_column(select_column_t&& col);

        // Forward the plan-resolved output column types (one per output column, in
        // projection order) onto the columns, so each column is correctly typed even when
        // the projection produces zero rows.
        void set_output_types(const std::pmr::vector<types::complex_logical_type>& types) override;

        // --- Push-based streaming pipeline (STEP 3 / phase C) ---
        // A SELECT is a pure 1-batch-in -> 1-batch-out projection: each input
        // chunk maps to exactly one output chunk via evaluate_projection(), with
        // no cross-batch accumulation, so it is a streaming operator. finalize()
        // keeps the default no-op.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::streaming; }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

    private:
        std::pmr::vector<select_column_t> columns_;
    };

} // namespace components::operators
