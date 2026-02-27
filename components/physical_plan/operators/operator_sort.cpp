#include "operator_sort.hpp"

#include <numeric>

namespace components::operators {

    operator_sort_t::operator_sort_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::sort) {}

    void operator_sort_t::add(size_t index, operator_sort_t::order order_) { sorter_.add(index, order_); }

    void operator_sort_t::add(const std::string& key, operator_sort_t::order order_) { sorter_.add(key, order_); }

    void operator_sort_t::add(const std::vector<size_t>& col_path, order order_) {
        sorter_.add(col_path, order_);
    }

    void operator_sort_t::add(const std::vector<size_t>& col_path, const std::string& key, order order_) {
        sorter_.add(col_path, key, order_);
    }

    void operator_sort_t::on_execute_impl(pipeline::context_t*) {
        if (left_ && left_->output()) {
            auto& chunk = left_->output()->data_chunk();
            auto num_rows = chunk.size();

            if (num_rows == 0) {
                output_ = operators::make_operator_data(left_->output()->resource(),
                                                        vector::data_chunk_t(resource_, chunk.types(), 0));
                return;
            }

            // 1. Create index array [0, 1, 2, ..., N-1]
            std::vector<size_t> indices(num_rows);
            std::iota(indices.begin(), indices.end(), size_t(0));

            // 2. Sort indices using columnar comparator
            sorter_.set_chunk(chunk);
            std::sort(indices.begin(), indices.end(), std::ref(sorter_));

            // 3. Build indexing vector from sorted indices
            vector::indexing_vector_t indexing(resource_, num_rows);
            for (size_t i = 0; i < num_rows; i++) {
                indexing.set_index(i, indices[i]);
            }

            // 4. Create result via copy with indexing (no transpose needed)
            vector::data_chunk_t result(resource_, chunk.types(), num_rows);
            chunk.copy(result, indexing, num_rows, 0);

            output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));
        }
    }

} // namespace components::operators
