#pragma once

#include <components/expressions/execution_graph_builder.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

#include <memory>

namespace components::operators {

    // One ORDER BY key, held at its ORDER BY position — the position in the spec list IS
    // the key's priority. A plain key references an input column (expression == nullptr,
    // col_path set); a computed key — "ORDER BY a + b", or a SELECT alias like "a + b AS c" —
    // carries the validated expression, which the sort's graph evaluates into a temporary
    // column at sort time, exactly as every other computation in the engine is evaluated.
    struct sort_key_spec_t {
        explicit sort_key_spec_t(std::pmr::memory_resource* r)
            : col_path(r) {}
        expressions::expression_ptr expression;
        std::pmr::vector<size_t> col_path;
        sort::order order_{sort::order::ascending};
        sort::null_order null_order_{sort::null_order::last};
    };

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;
        using null_order = sort::null_order;

        operator_sort_t(std::pmr::memory_resource* resource, log_t log);

        void add(size_t index, order order_ = order::ascending, null_order null_order_ = null_order::last);
        void add(const std::pmr::vector<size_t>& col_path,
                 order order_ = order::ascending,
                 null_order null_order_ = null_order::last);
        void add_computed(sort_key_spec_t&& key);

        void set_expected_output_count(size_t n) { expected_output_count_ = n; }
        void set_limit(logical_plan::limit_t limit) { limit_ = limit; }

        // --- Push-based streaming pipeline (STEP 3 / phase C) ---
        // A sort is a blocking SINK: it must see the whole input before it can
        // emit a single sorted row. push() folds each input batch into
        // buffered_input_ and emits nothing; finalize() runs the per-chunk
        // key-eval + local sort and the k-way merge over the buffer, emitting the
        // sorted result into `out` via sort_merge().
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        // The sorter is populated once, inside sort_merge, by walking key_specs_ in order —
        // a computed key's temporary-column index is unknowable before the first chunk's
        // width is seen, and registering plain keys any earlier would demote every computed
        // key to a trailing tie-breaker.
        sort::columnar_sorter_t sorter_;
        std::pmr::vector<sort_key_spec_t> key_specs_;
        // ONE graph computing every computed key, in key order, over an input chunk. All the
        // buffered chunks share a schema, so the first non-empty one builds it and the rest reuse
        // it; null until then.
        std::unique_ptr<execution_graph::execution_graph_t> computed_graph_;
        size_t expected_output_count_{0};
        logical_plan::limit_t limit_;
        chunks_vector_t buffered_input_{resource_};

        // Core sort+merge. Sources chunks from `source_chunks` (mutated in place:
        // temporary computed-key columns are appended then stripped) and appends
        // the sorted, limit/offset-applied output chunks to `out`. Used by
        // finalize (streaming sink).
        [[nodiscard]] core::error_t
        sort_merge(pipeline::context_t* pipeline_context, chunks_vector_t& source_chunks, chunks_vector_t& out);

        // Types the input from `probe` and builds computed_graph_ over it. Idempotent.
        [[nodiscard]] core::error_t build_computed_graph(pipeline::context_t* pipeline_context,
                                                         const vector::data_chunk_t& probe);
    };

} // namespace components::operators
