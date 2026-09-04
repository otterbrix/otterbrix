#pragma once

#include <components/expressions/expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <memory_resource>

#include <components/expressions/execution_dag_builder.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace components::operators {

    struct group_value_t {
        std::pmr::string name;
        types::complex_logical_type result_type;
    };

    class operator_hash_group_t final : public read_write_operator_t {
    public:
        operator_hash_group_t(std::pmr::memory_resource* resource, log_t log);

        // A grouping key is a named expression exactly like a projected column: a plain column
        // reference, or anything else the engine can compute.
        void add_key(projected_column_t&& key);
        void add_value(const std::pmr::string& name, const types::complex_logical_type& result_type);
        void add_output(const expressions::expression_ptr& output);

        // Plan-time resolved output column types, by FINAL output position
        void set_output_types(const std::pmr::vector<types::complex_logical_type>& types) override;

        void set_input_types(const std::pmr::vector<types::complex_logical_type>& types);

        // GROUP BY folds an unbounded input into a bounded set of group rows, so it is a SINK:
        // push() folds each batch into the group table and appends nothing, finalize() emits the
        // groups.
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        std::pmr::vector<projected_column_t> keys_;
        std::pmr::vector<projected_column_t> computed_keys_;
        std::unique_ptr<execution_dag::execution_dag_t> computed_keys_graph_;
        // What the keys graph writes into
        vector::data_chunk_t computed_keys_chunk_;
        // Ordinal of the first appended key column: the width of the chunk the group is fed.
        size_t computed_key_base_{0};
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

        // Evaluates the computed keys and appends one column per key to `chunk`. A no-op when
        // every key is a plain column reference.
        core::error_t append_computed_keys(pipeline::context_t* pipeline_context, vector::data_chunk_t* chunk);
        // First-push setup: builds the graph over the (already extended) input schema, declaring
        // a slot per key.
        core::error_t build_plan(pipeline::context_t* pipeline_context, const vector::data_chunk_t& probe);
        // Finds or creates the group of every row of the current key columns, filling row_groups_.
        core::error_t resolve_groups(vector::data_chunk_t& keys);
        // Appends one group's key cells to the key blocks and indexes it.
        void append_group(const vector::data_chunk_t& keys, uint64_t row, uint64_t hash);
        void grow_index();
        [[nodiscard]] bool keys_equal(const vector::data_chunk_t& keys, uint64_t row, uint32_t group) const;
    };

} // namespace components::operators
