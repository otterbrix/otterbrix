#pragma once

#include <components/expressions/expression.hpp>
#include <components/logical_plan/node_limit.hpp>
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
        void set_limit(logical_plan::limit_t limit) { limit_ = limit; }

        // --- Push-based streaming pipeline (STEP 3 / phase C) ---
        // A sort is a blocking SINK: it must see the whole input before it can
        // emit a single sorted row. push() folds each input batch into
        // buffered_input_ and emits nothing; finalize() runs the per-chunk
        // key-eval + local sort and the k-way merge over the buffer, emitting the
        // sorted result into `out`. The legacy on_execute_impl materialize path
        // stays intact until phase E; both paths share sort_merge().
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        sort::columnar_sorter_t sorter_;
        std::pmr::vector<computed_sort_key_t> computed_keys_;
        size_t expected_output_count_{0};
        logical_plan::limit_t limit_;
        chunks_vector_t buffered_input_{resource_};

        // Core sort+merge. Sources chunks from `source_chunks` (mutated in place:
        // temporary computed-key columns are appended then stripped) and appends
        // the sorted, limit/offset-applied output chunks to `out`. Used by both
        // on_execute_impl (legacy materialize) and finalize (streaming sink).
        [[nodiscard]] core::error_t
        sort_merge(pipeline::context_t* pipeline_context, chunks_vector_t& source_chunks, chunks_vector_t& out);

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
