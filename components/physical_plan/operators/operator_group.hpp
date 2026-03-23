#pragma once

#include <components/expressions/expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <memory_resource>
#include <unordered_map>

#include <components/physical_plan/operators/aggregate/operator_aggregate.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    struct group_key_t {
        explicit group_key_t(std::pmr::memory_resource* r)
            : name(r)
            , full_path(r)
            , coalesce_entries(r)
            , case_clauses(r)
            , else_constant(r, nullptr) {}

        std::pmr::string name;
        enum class kind
        {
            column,
            coalesce,
            case_when
        } type = kind::column;
        std::pmr::vector<size_t> full_path;

        // for coalesce: ordered list of sources (col index or constant)
        struct coalesce_entry {
            explicit coalesce_entry(std::pmr::memory_resource* r)
                : constant(r, nullptr) {}
            enum class source
            {
                column,
                constant
            } type = source::column;
            size_t col_index = 0;
            types::logical_value_t constant;
        };
        std::pmr::vector<coalesce_entry> coalesce_entries;

        // for case_when: list of when-clauses
        struct case_clause {
            explicit case_clause(std::pmr::memory_resource* r)
                : condition_value(r, nullptr)
                , res_constant(r, nullptr) {}
            size_t condition_col = 0;
            expressions::compare_type cmp = expressions::compare_type::eq;
            types::logical_value_t condition_value;
            enum class result_source
            {
                column,
                constant
            } res_type = result_source::column;
            size_t res_col = 0;
            types::logical_value_t res_constant;
        };
        std::pmr::vector<case_clause> case_clauses;

        // else result for case_when
        enum class else_source
        {
            column,
            constant,
            null_value
        } else_type = else_source::null_value;
        size_t else_col = 0;
        types::logical_value_t else_constant;
    };

    struct group_value_t {
        std::pmr::string name;
        aggregate::operator_aggregate_ptr aggregator;
    };

    // Pre-group computed column (arithmetic on raw data before grouping)
    struct computed_column_t {
        std::pmr::string alias;
        expressions::scalar_type op;
        std::pmr::vector<expressions::param_storage> operands;
        size_t resolved_key_index = SIZE_MAX; // index into keys_ for this computed column
    };

    // Post-aggregate computed column (arithmetic on aggregate results)
    struct post_aggregate_column_t {
        std::pmr::string alias;
        expressions::scalar_type op;
        std::pmr::vector<expressions::param_storage> operands;
    };

    class operator_group_t final : public read_write_operator_t {
    public:
        operator_group_t(std::pmr::memory_resource* resource,
                         log_t log,
                         expressions::expression_ptr having = nullptr,
                         size_t internal_aggregate_count = 0);

        void add_key(group_key_t&& key);
        void add_key(const std::pmr::string& name);
        void add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator);
        void add_computed_column(computed_column_t&& col);
        void add_post_aggregate(post_aggregate_column_t&& col);
        void set_select_order(std::pmr::vector<size_t>&& order);

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<computed_column_t> computed_columns_;
        std::pmr::vector<post_aggregate_column_t> post_aggregates_;
        expressions::expression_ptr having_;
        size_t internal_aggregate_count_;
        std::pmr::vector<size_t> select_order_;

        std::pmr::vector<std::pmr::vector<size_t>> row_ids_per_group_;
        std::pmr::vector<uint32_t> fast_group_ids_;  // row → group_id, built in fast path
        bool fast_group_ids_valid_ = false;
        std::pmr::vector<uint64_t> first_row_per_group_;  // representative row index per group (fast path only)
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> group_keys_;
        std::pmr::unordered_map<size_t, std::pmr::vector<size_t>> group_index_;
        std::pmr::vector<types::complex_logical_type> key_col_types_; // source column types for key columns (fast path)
        std::pmr::vector<size_t> key_col_indices_;                    // source column indices for key columns (fast path)

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        void create_list_rows();
        vector::data_chunk_t calc_aggregate_values(pipeline::context_t* pipeline_context);
        vector::data_chunk_t calc_aggregate_values_fallback(pipeline::context_t* pipeline_context);
        vector::data_chunk_t
        build_result_chunk(size_t num_groups,
                           size_t key_count,
                           std::pmr::vector<std::pmr::vector<types::logical_value_t>>& agg_results);
        void calc_post_aggregates(pipeline::context_t* pipeline_context, vector::data_chunk_t& result);
        void filter_having(pipeline::context_t* pipeline_context, vector::data_chunk_t& result);
    };

} // namespace components::operators
