#include "operator_sort.hpp"

#include "arithmetic_eval.hpp"

#include <algorithm>
#include <components/vector/vector_operations.hpp>
#include <numeric>
#include <queue>

namespace components::operators {

    operator_sort_t::operator_sort_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::sort)
        , computed_keys_(resource) {}

    void operator_sort_t::add(size_t index, operator_sort_t::order order_) { sorter_.add(index, order_); }

    void operator_sort_t::add(const std::pmr::vector<size_t>& col_path, order order_) { sorter_.add(col_path, order_); }

    void operator_sort_t::add_computed(computed_sort_key_t&& key) { computed_keys_.push_back(std::move(key)); }

    void operator_sort_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            return;
        }
        auto in = left_->output();
        auto& in_chunks = in->chunks();

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

        bool computed_added = false;
        for (auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                sorted_indices.emplace_back();
                continue;
            }
            for (const auto& ck : computed_keys_) {
                auto result_vec =
                    evaluate_arithmetic(resource_, ck.op, ck.operands, chunk, pipeline_context->parameters);
                if (result_vec.has_error()) {
                    set_error(result_vec.error());
                    return;
                }
                if (!computed_added) {
                    sorter_.add(chunk.data.size(), ck.order_);
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

        int64_t offset_val = limit_.offset();
        int64_t limit_val = limit_.limit();
        uint64_t skip = offset_val > 0 ? static_cast<uint64_t>(offset_val) : 0;
        uint64_t take = (limit_val >= 0) ? static_cast<uint64_t>(limit_val)
                                         : std::numeric_limits<uint64_t>::max();

        std::vector<vector::data_chunk_t> out_chunks;
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

            if (skip > 0) {
                --skip;
            } else {
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
            }

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
                    chunk.data.erase(chunk.data.begin() + static_cast<ptrdiff_t>(first_computed_col),
                                     chunk.data.end());
                }
            }
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(resource_, out_types, 0);
        }
        output_ = operators::make_operator_data(in->resource(), std::move(out_chunks));
    }

} // namespace components::operators
