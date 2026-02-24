#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_) { add(index, order_); }
    columnar_sorter_t::columnar_sorter_t(const std::string& key, order order_) { add(key, order_); }

    void columnar_sorter_t::add(size_t index, order order_) {
        keys_.push_back({index, {}, order_, false});
    }

    void columnar_sorter_t::add(const std::string& key, order order_) {
        keys_.push_back({0, key, order_, true});
    }

    void columnar_sorter_t::set_chunk(const vector::data_chunk_t& chunk) {
        chunk_ = &chunk;
        // Resolve name-based keys to column indices
        for (auto& k : keys_) {
            if (k.by_name) {
                for (size_t c = 0; c < chunk.column_count(); c++) {
                    if (chunk.data[c].type().has_alias() && chunk.data[c].type().alias() == k.col_name) {
                        k.col_idx = c;
                        break;
                    }
                }
            }
        }
    }

} // namespace components::sort
