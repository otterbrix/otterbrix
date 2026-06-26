#pragma once

#include <components/expressions/expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <memory_resource>
#include <unordered_map>

#include <components/physical_plan/operators/aggregate/grouped_aggregate.hpp>
#include <components/physical_plan/operators/aggregate/operator_aggregate.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>

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

        expressions::side_t side = expressions::side_t::undefined;

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

        // Plan-time resolved output column types, by FINAL output position (keys first,
        // then aggregate values), forwarded from the logical aggregate node's
        // output_schema(). Used to build correctly-typed results over ZERO input rows
        // instead of falling back to the 0-byte logical_type::NA sentinel (which crashes
        // downstream under gcc -O3). Empty when not forwarded -> data-derived fallback.
        void set_output_types(const std::pmr::vector<types::complex_logical_type>& types) override;

        // --- Push-based streaming pipeline (STEP 3) ---
        // GROUP BY / aggregation folds an unbounded input into a bounded set of group
        // rows, so it is a SINK. Unlike a buffer-everything sink, this one accumulates
        // INCREMENTALLY: push() folds each input batch into the running group table
        // (typed per-group aggregate accumulators), appending nothing to out;
        // finalize() materializes the accumulated groups into the result chunk(s).
        // State is bounded by the number of GROUPS, not by the input row count, so a
        // 4-table-join + GROUP BY no longer materializes every intermediate row.
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<computed_column_t> computed_columns_;
        std::pmr::vector<post_aggregate_column_t> post_aggregates_;
        // Plan-time resolved output types by final output position (see set_output_types).
        std::pmr::vector<types::complex_logical_type> output_types_;
        expressions::expression_ptr having_;
        size_t internal_aggregate_count_;

        // --- Incremental hash-aggregate group table (R1-b: typed HASH+VERIFY) ---
        // A group is identified by a uint64 TYPED hash of its key cells; collisions
        // resolve by storing the candidate key cells (one row per group in
        // group_key_chunk_) and VERIFYING with a typed cell-by-cell comparison. No
        // logical_value_t in the per-row accumulate hot path.
        bool plan_built_ = false;           // first-push lazy init of the agg plan + key chunk
        bool any_input_ = false;            // at least one input batch was pushed
        size_t group_count_ = 0;            // number of live groups
        size_t key_count_ = 0;              // number of group-key columns
        bool need_row_gather_ = false;      // any non-vectorizable aggregate present

        // Per-aggregate plan, resolved once on the first non-empty push from the schema.
        struct agg_plan_t {
            bool vectorizable = false;
            aggregate::builtin_agg kind = aggregate::builtin_agg::UNKNOWN;
            std::pmr::vector<size_t> arg_path;            // argument column path (vectorizable)
            types::logical_type col_type = types::logical_type::NA;
            bool is_count_star = false;
            explicit agg_plan_t(std::pmr::memory_resource* r)
                : arg_path(r) {}
        };
        std::pmr::vector<agg_plan_t> agg_plan_;

        // Candidate key cells, one row per group. Column types come from the first
        // probe chunk's key columns (stable, never NA), so output key types are read
        // straight off this chunk — no group_keys_[0][k].type() NA hazard.
        // Held by pointer because data_chunk_t has no default ctor and its schema is
        // only known on first push.
        std::pmr::vector<vector::data_chunk_t> group_key_chunk_storage_; // 0 or 1 element
        std::pmr::unordered_map<uint64_t, std::pmr::vector<uint32_t>> group_hash_index_;

        // Vectorizable path: running typed accumulators, [agg_idx][group_id].
        std::pmr::vector<std::pmr::vector<aggregate::raw_agg_state_t>> agg_states_;

        // Non-vectorizable path (DISTINCT / custom funcs / non-numeric args): the
        // contributing source rows gathered per group, fused + aggregated once in
        // finalize() via the general operator_func_t batch path. Only populated when
        // need_row_gather_ is true; otherwise the table stays bounded by #groups.
        std::pmr::vector<chunks_vector_t> gathered_rows_per_group_;

        // Folds one input batch into the running group table. Returns an error_t
        // (no throw / no set_error on the streaming path). Computed-key columns are
        // appended to `input` for the duration of the call.
        core::error_t accumulate(pipeline::context_t* pipeline_context, vector::data_chunk_t& input);

        // First-push lazy setup: resolve the per-aggregate plan + key column schema.
        core::error_t build_plan(const vector::data_chunk_t& probe);

        // Builds the per-input "probe" key chunk: column key -> referenced source
        // column (zero copy); coalesce / case_when / multi-part path -> a derived
        // column materialized via extract_key_value. One uniform chunk feeds the
        // typed hash + typed verify for single- AND multi-column keys.
        vector::data_chunk_t make_key_probe(const vector::data_chunk_t& input);

        // Materializes the accumulated group table into <=DEFAULT_VECTOR_CAPACITY-group
        // result chunks appended to `out` (key columns + finalized aggregates), running
        // post-aggregate arithmetic + HAVING per slice. Slicing here (never building a
        // chunk with capacity > 1024) is what keeps the data_chunk_t ctor invariant for
        // an unbounded number of groups. Errors are reported via set_error()/has_error().
        void materialize_groups(pipeline::context_t* pipeline_context, chunks_vector_t& out);

        // Builds the single-row result for a global aggregate (no GROUP BY keys) over
        // an EMPTY input — e.g. SELECT COUNT(*) FROM empty_table.
        vector::data_chunk_t empty_aggregate_result(pipeline::context_t* pipeline_context);

        void calc_post_aggregates(pipeline::context_t* pipeline_context, vector::data_chunk_t& result);
        void filter_having(pipeline::context_t* pipeline_context, vector::data_chunk_t& result);
    };

} // namespace components::operators
