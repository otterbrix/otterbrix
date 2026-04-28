#include "operator_sort.hpp"

#include "arithmetic_eval.hpp"

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
        auto& chunk = left_->output()->data_chunk();
        auto num_rows = chunk.size();

        if (num_rows == 0) {
            output_ = operators::make_operator_data(left_->output()->resource(),
                                                    vector::data_chunk_t(resource_, chunk.types(), 0));
            return;
        }

        // If there are computed sort keys, evaluate each one and append the result
        // as a temporary extra column so the sorter can sort by column index.
        size_t first_computed_col = chunk.data.size();
        for (const auto& ck : computed_keys_) {
            auto result_vec = evaluate_arithmetic(resource_, ck.op, ck.operands, chunk, pipeline_context->parameters);
            if (result_vec.has_error()) {
                set_error(result_vec.error());
                return;
            }
            sorter_.add(chunk.data.size(), ck.order_);
            chunk.data.emplace_back(std::move(result_vec.value()));
        }

        // Sort by index array.
        vector::indexing_vector_t indexing(resource_, uint64_t(0), num_rows);
        sorter_.set_chunk(chunk);
        std::sort(indexing.data(), indexing.data() + num_rows, std::ref(sorter_));

        // Determine the output column count (drop computed sort-key columns).
        size_t out_cols = expected_output_count_ > 0 ? expected_output_count_ : first_computed_col;
        if (!computed_keys_.empty() && out_cols > first_computed_col) {
            out_cols = first_computed_col;
        }

        // Copy result with indexing.
        vector::data_chunk_t result(resource_, chunk.types(), num_rows);
        chunk.copy(result, indexing, num_rows, 0);

        // Truncate extra columns (computed sort-key temporaries + any legacy expected_output_count_).
        if (result.data.size() > out_cols) {
            result.data.erase(result.data.begin() + static_cast<ptrdiff_t>(out_cols), result.data.end());
        }

        // Remove the temporary columns we added to the input chunk.
        if (!computed_keys_.empty()) {
            chunk.data.erase(chunk.data.begin() + static_cast<ptrdiff_t>(first_computed_col), chunk.data.end());
        }

        // Apply offset and limit to sorted result
        int64_t offset_val = limit_.offset();
        int64_t limit_val = limit_.limit();
        if (offset_val > 0 || limit_val >= 0) {
            uint64_t total = result.size();
            uint64_t start = static_cast<uint64_t>(std::min(static_cast<int64_t>(total), offset_val));
            uint64_t remaining = total - start;
            uint64_t take = (limit_val >= 0) ? std::min(remaining, static_cast<uint64_t>(limit_val)) : remaining;
            if (start > 0 || take < total) {
                result = result.partial_copy(resource_, start, take);
            }
        }

        output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));
    }

} // namespace components::operators
