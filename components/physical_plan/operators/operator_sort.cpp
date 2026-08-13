#include "operator_sort.hpp"

#include <algorithm>
#include <components/vector/vector_operations.hpp>
#include <numeric>
#include <optional>
#include <queue>

namespace components::operators {

    operator_sort_t::operator_sort_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::sort)
        , key_specs_(resource) {}

    void operator_sort_t::add(size_t index, operator_sort_t::order order_, operator_sort_t::null_order null_order_) {
        sort_key_spec_t spec(resource_);
        spec.col_path.push_back(index);
        spec.order_ = order_;
        spec.null_order_ = null_order_;
        key_specs_.push_back(std::move(spec));
    }

    void operator_sort_t::add(const std::pmr::vector<size_t>& col_path,
                              order order_,
                              operator_sort_t::null_order null_order_) {
        sort_key_spec_t spec(resource_);
        spec.col_path.assign(col_path.begin(), col_path.end());
        spec.order_ = order_;
        spec.null_order_ = null_order_;
        key_specs_.push_back(std::move(spec));
    }

    void operator_sort_t::add_computed(sort_key_spec_t&& key) { key_specs_.push_back(std::move(key)); }

    core::error_t operator_sort_t::build_computed_graph(pipeline::context_t* pipeline_context,
                                                        const vector::data_chunk_t& probe) {
        if (computed_graph_) {
            return core::error_t::no_error();
        }
        std::pmr::vector<const expressions::expression_i*> key_expressions(resource_);
        key_expressions.reserve(key_specs_.size());
        for (const auto& spec : key_specs_) {
            if (spec.expression) {
                key_expressions.push_back(spec.expression.get());
            }
        }

        auto built = expressions::build_graph(resource_,
                                              pipeline_context->parameters.parameters,
                                              key_expressions,
                                              probe.types());
        if (built.has_error()) {
            return built.error();
        }
        computed_graph_ = std::move(built.value());
        return core::error_t::no_error();
    }

    core::error_t
    operator_sort_t::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // Blocking sink: accumulate the batch, emit nothing. The sort cannot
        // produce any ordered output until finalize() has seen every input chunk.
        buffered_input_.emplace_back(std::move(input));
        return core::error_t::no_error();
    }

    core::error_t operator_sort_t::finalize(pipeline::context_t* ctx, chunks_vector_t& out) {
        // Upstream is drained; run the Phase 1 / Phase 2 logic over the accumulated
        // buffer, writing into the pipeline sink `out`.
        return sort_merge(ctx, buffered_input_, out);
    }

    core::error_t operator_sort_t::sort_merge(pipeline::context_t* pipeline_context,
                                              chunks_vector_t& in_chunks,
                                              chunks_vector_t& out_chunks) {
        // All input chunks share the same schema. Capture original (pre-computed) column count
        // and types from the first chunk.
        size_t first_computed_col = 0;
        std::pmr::vector<types::complex_logical_type> out_types{resource_};
        if (!in_chunks.empty()) {
            first_computed_col = in_chunks.front().data.size();
            out_types = in_chunks.front().types();
        }

        // Phase 1: per-chunk evaluate computed keys (mutating chunk) + local sort.
        std::vector<std::vector<uint32_t>> sorted_indices;
        sorted_indices.reserve(in_chunks.size());

        bool keys_registered = false;
        bool has_computed = false;
        for (auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                sorted_indices.emplace_back();
                continue;
            }
            // Every computed key of this chunk comes out of ONE graph run, in key order.
            std::optional<vector::data_chunk_t> computed;
            if (std::any_of(key_specs_.begin(), key_specs_.end(), [](const sort_key_spec_t& spec) {
                    return spec.expression != nullptr;
                })) {
                if (auto error = build_computed_graph(pipeline_context, chunk); error.contains_error()) {
                    return error;
                }
                auto produced = expressions::run_graph(computed_graph_.get(),
                                                       pipeline_context->parameters.parameters,
                                                       chunk,
                                                       pipeline_context->execution_context);
                if (produced.has_error()) {
                    return produced.error();
                }
                computed = std::move(produced.value());
            }

            // Walk the specs in ORDER BY position: a computed key materializes into a temp
            // column, a plain key references its input column — and each is registered with
            // the sorter at exactly its spec position, so priority follows the ORDER BY
            // list, never the plain-before-computed registration order.
            size_t computed_column = 0;
            for (const auto& spec : key_specs_) {
                if (!spec.expression) {
                    if (!keys_registered) {
                        sorter_.add(spec.col_path, spec.order_, spec.null_order_);
                    }
                    continue;
                }
                has_computed = true;
                // The graph's columns reference its slots, which the next chunk overwrites, and
                // every chunk stays live until the merge below — so each key is copied out.
                const vector::vector_t& source_vec = computed.value().data[computed_column++];
                vector::vector_t vec(resource_, source_vec.type(), chunk.size());
                vector::vector_ops::copy(source_vec, vec, chunk.size(), 0, 0);
                if (!keys_registered) {
                    sorter_.add(chunk.data.size(), spec.order_, spec.null_order_);
                }
                chunk.data.emplace_back(std::move(vec));
            }
            keys_registered = true;

            std::vector<uint32_t> idx(chunk.size());
            std::iota(idx.begin(), idx.end(), uint32_t{0});
            sorter_.set_chunk(chunk);
            std::sort(idx.begin(), idx.end(), std::ref(sorter_));
            sorted_indices.emplace_back(std::move(idx));
        }

        // Output column count (drop computed sort-key columns).
        size_t out_cols_effective = expected_output_count_ > 0 ? expected_output_count_ : first_computed_col;
        if (has_computed && out_cols_effective > first_computed_col) {
            out_cols_effective = first_computed_col;
        }
        if (out_types.size() > out_cols_effective) {
            out_types.erase(out_types.begin() + static_cast<ptrdiff_t>(out_cols_effective), out_types.end());
        }

        // Phase 2: k-way merge via min-heap.
        struct cursor_t {
            uint32_t chunk_idx;
            uint32_t cursor;
        };
        auto cmp = [&](const cursor_t& a, const cursor_t& b) {
            size_t ra = sorted_indices[a.chunk_idx][a.cursor];
            size_t rb = sorted_indices[b.chunk_idx][b.cursor];
            int c = sorter_.compare_cross(in_chunks[a.chunk_idx], ra, in_chunks[b.chunk_idx], rb);
            // std::priority_queue is a max-heap; reverse for min-heap behaviour.
            // Tie-break on chunk_idx then cursor for deterministic order.
            if (c != 0)
                return c > 0;
            if (a.chunk_idx != b.chunk_idx)
                return a.chunk_idx > b.chunk_idx;
            return a.cursor > b.cursor;
        };
        std::priority_queue<cursor_t, std::vector<cursor_t>, decltype(cmp)> heap(cmp);
        for (uint32_t ci = 0; ci < in_chunks.size(); ++ci) {
            if (!sorted_indices[ci].empty()) {
                heap.push({ci, uint32_t{0}});
            }
        }

        // The full blocking sort truncates its OUTPUT to `take` rows. OFFSET is applied by
        // operator_limit above (a full sort has no top-N heap to skip into), so only the LIMIT
        // count caps the emitted output here.
        int64_t limit_val = limit_.limit();
        uint64_t take = (limit_val >= 0) ? static_cast<uint64_t>(limit_val) : std::numeric_limits<uint64_t>::max();

        vector::data_chunk_t cur(resource_, out_types, vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t cur_filled = 0;
        uint64_t produced = 0;

        auto flush_cur = [&]() {
            if (cur_filled == 0) {
                return;
            }
            cur.set_cardinality(cur_filled);
            out_chunks.emplace_back(std::move(cur));
            cur = vector::data_chunk_t(resource_, out_types, vector::DEFAULT_VECTOR_CAPACITY);
            cur_filled = 0;
        };

        while (!heap.empty() && produced < take) {
            auto top = heap.top();
            heap.pop();
            auto& src_chunk = in_chunks[top.chunk_idx];
            size_t row = sorted_indices[top.chunk_idx][top.cursor];

            if (cur_filled == vector::DEFAULT_VECTOR_CAPACITY) {
                flush_cur();
            }
            // vector_ops::copy arg 3 is the END index (exclusive), arg 4 is the start
            // offset in the source, arg 5 is the target offset. Copy count = end - offset.
            for (size_t c = 0; c < out_cols_effective; ++c) {
                vector::vector_ops::copy(src_chunk.data[c], cur.data[c], row + 1, row, cur_filled);
            }
            vector::vector_ops::copy(src_chunk.row_ids, cur.row_ids, row + 1, row, cur_filled);
            ++cur_filled;
            ++produced;

            ++top.cursor;
            if (top.cursor < sorted_indices[top.chunk_idx].size()) {
                heap.push(top);
            }
        }

        flush_cur();

        // Restore input chunks: strip the temporary computed-key columns.
        if (has_computed) {
            for (auto& chunk : in_chunks) {
                if (chunk.data.size() > first_computed_col) {
                    chunk.data.erase(chunk.data.begin() + static_cast<ptrdiff_t>(first_computed_col), chunk.data.end());
                }
            }
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(resource_, out_types, 0);
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
