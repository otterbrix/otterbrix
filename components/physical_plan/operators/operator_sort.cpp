#include "operator_sort.hpp"

#include "projection_executor.hpp"

#include <algorithm>
#include <components/vector/vector_operations.hpp>
#include <numeric>
#include <queue>

namespace components::operators {

    operator_sort_t::operator_sort_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::sort)
        , computed_keys_(resource) {}

    void operator_sort_t::add(size_t index, operator_sort_t::order order_, operator_sort_t::null_order null_order_) {
        sorter_.add(index, order_, null_order_);
    }

    void operator_sort_t::add(const std::pmr::vector<size_t>& col_path,
                              order order_,
                              operator_sort_t::null_order null_order_) {
        sorter_.add(col_path, order_, null_order_);
    }

    void operator_sort_t::add_computed(computed_sort_key_t&& key) { computed_keys_.push_back(std::move(key)); }

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
        // All input chunks share the same schema. Capture the original (pre-computed) column
        // count and the input's SCHEMA — not its types: the output chunks below are the ones a
        // user reads, and a type list has carried no column name since M3-B5.
        size_t first_computed_col = 0;
        vector::schema_t out_schema{resource_};
        if (!in_chunks.empty()) {
            first_computed_col = in_chunks.front().data.size();
            out_schema = vector::clone_schema(resource_, in_chunks.front().schema());
        }

        // Phase 1: per-chunk evaluate computed keys (mutating chunk) + local sort.
        std::vector<std::vector<uint32_t>> sorted_indices;
        sorted_indices.reserve(in_chunks.size());

        bool computed_added = false;
        for (auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                sorted_indices.emplace_back();
                continue;
            }
            for (const auto& ck : computed_keys_) {
                auto result_vec = evaluate_scalar(resource_,
                                                  ck.op,
                                                  ck.operands,
                                                  chunk,
                                                  pipeline_context->function_registry,
                                                  pipeline_context->parameters,
                                                  pipeline_context->session_tz);
                if (result_vec.has_error()) {
                    return result_vec.error();
                }
                if (!computed_added) {
                    sorter_.add(chunk.data.size(), ck.order_, ck.null_order_);
                }
                chunk.data.emplace_back(std::move(result_vec.value()));
            }
            computed_added = true;

            std::vector<uint32_t> idx(chunk.size());
            std::iota(idx.begin(), idx.end(), uint32_t{0});
            sorter_.set_chunk(chunk);
            std::sort(idx.begin(), idx.end(), std::ref(sorter_));
            sorted_indices.emplace_back(std::move(idx));
        }

        // Output column count (drop computed sort-key columns).
        size_t out_cols_effective = expected_output_count_ > 0 ? expected_output_count_ : first_computed_col;
        if (!computed_keys_.empty() && out_cols_effective > first_computed_col) {
            out_cols_effective = first_computed_col;
        }
        if (out_schema.size() > out_cols_effective) {
            out_schema.erase(out_schema.begin() + static_cast<ptrdiff_t>(out_cols_effective), out_schema.end());
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

        auto cur = vector::make_chunk(resource_, out_schema, vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t cur_filled = 0;
        uint64_t produced = 0;

        auto flush_cur = [&]() {
            if (cur_filled == 0) {
                return;
            }
            cur.set_cardinality(cur_filled);
            out_chunks.emplace_back(std::move(cur));
            cur = vector::make_chunk(resource_, out_schema, vector::DEFAULT_VECTOR_CAPACITY);
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
        if (!computed_keys_.empty()) {
            for (auto& chunk : in_chunks) {
                if (chunk.data.size() > first_computed_col) {
                    chunk.data.erase(chunk.data.begin() + static_cast<ptrdiff_t>(first_computed_col), chunk.data.end());
                }
            }
        }

        if (out_chunks.empty()) {
            // An empty result still describes its columns: a zero-row answer must name and
            // type them for the user.
            out_chunks.emplace_back(vector::make_chunk(resource_, out_schema, 0));
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
