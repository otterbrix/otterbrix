#pragma once

#include <components/expressions/expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <memory_resource>

#include <components/expressions/execution_dag_builder.hpp>
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
        types::complex_logical_type result_type;
    };

    class operator_hash_group_t final : public read_write_operator_t {
    public:
        operator_hash_group_t(std::pmr::memory_resource* resource, log_t log);

        void add_key(group_key_t&& key);
        void add_key(const std::pmr::string& name);
        void add_value(const std::pmr::string& name, const types::complex_logical_type& result_type);
        void add_output(const expressions::expression_ptr& output);

        // Plan-time resolved output column types, by FINAL output position (keys first,
        // then aggregate values), forwarded from the logical aggregate node's
        // output_schema(). Used to build correctly-typed results over ZERO input rows
        // instead of falling back to the 0-byte logical_type::NA sentinel (which crashes
        // downstream under gcc -O3). Empty when not forwarded -> data-derived fallback.
        void set_output_types(const std::pmr::vector<types::complex_logical_type>& types) override;

        void set_input_types(const std::pmr::vector<types::complex_logical_type>& types);

        // GROUP BY folds an unbounded input into a bounded set of group rows, so it is a SINK:
        // push() folds each batch into the group table and appends nothing, finalize() emits the
        // groups.
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<expressions::expression_ptr> outputs_;
        // Plan-time resolved output types by final output position (see set_output_types).
        std::pmr::vector<types::complex_logical_type> output_types_;
        std::pmr::vector<types::complex_logical_type> input_types_{resource_};

        std::unique_ptr<execution_dag::execution_dag_t> graph_;
        // Slots holding the grouping keys, in key order. The graph writes them per chunk (that is
        // what gets hashed) and reads them back per group at finalize.
        execution_dag::slot_list_t key_slots_{resource_};

        // The group table. Keys live in fixed DEFAULT_VECTOR_CAPACITY-row blocks, so a group id
        // splits into (block, row), growth appends a block, and no chunk ever exceeds the vector
        // capacity. Group ids are handed out in discovery order.
        std::pmr::vector<vector::data_chunk_t> key_blocks_;
        uint64_t group_count_{0};

        // Flat open-addressed index over the key blocks: power-of-two capacity, linear probing,
        // the full hash stored beside the id. The hash only FILTERS — every candidate is verified
        // cell by cell, because two different keys can hash alike and a key may be a struct or an
        // array.
        struct hash_entry_t {
            uint64_t hash{0};
            uint32_t group{empty_group};
        };
        static constexpr uint32_t empty_group{std::numeric_limits<uint32_t>::max()};
        std::pmr::vector<hash_entry_t> index_;
        uint64_t index_mask_{0};

        // Per-chunk scratch, allocated once at plan time and refilled: the key columns the graph
        // wrote (referenced, never copied), one hash per row, and each row's group id.
        std::pmr::vector<vector::data_chunk_t> key_probe_;
        std::pmr::vector<vector::vector_t> hashes_;
        std::vector<uint64_t> key_columns_;
        std::pmr::vector<uint32_t> row_groups_{resource_};

        bool plan_built_{false};
        bool any_input_{false};

        // First-push setup: builds the graph over the input schema, declaring a slot per key.
        core::error_t build_plan(pipeline::context_t* pipeline_context, const vector::data_chunk_t& probe);
        // Finds or creates the group of every row of the current key columns, filling row_groups_.
        core::error_t resolve_groups(vector::data_chunk_t& keys);
        // Appends one group's key cells to the key blocks and indexes it.
        void append_group(const vector::data_chunk_t& keys, uint64_t row, uint64_t hash);
        void grow_index();
        [[nodiscard]] bool keys_equal(const vector::data_chunk_t& keys, uint64_t row, uint32_t group) const;
    };

} // namespace components::operators
