#pragma once
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <functional>

namespace components::sort {

    using types::compare_t;

    enum class order
    {
        descending = -1,
        ascending = 1
    };

    class columnar_sorter_t {
        struct sort_key {
            std::pmr::vector<size_t> col_path;
            order order_ = order::ascending;
            const vector::vector_t* vec = nullptr; // cached pointer set in set_chunk()
        };

    public:
        explicit columnar_sorter_t() = default;
        explicit columnar_sorter_t(size_t index, order order_ = order::ascending);

        void add(size_t index, order order_ = order::ascending);
        void add(const std::pmr::vector<size_t>& col_path, order order_ = order::ascending);

        void set_chunk(const vector::data_chunk_t& chunk);

        bool operator()(size_t row_a, size_t row_b) const {
            for (const auto& k : keys_) {
                if (!k.vec)
                    continue;
                int cmp = compare_raw(*k.vec, row_a, row_b);
                if (cmp == 0)
                    continue;
                return (k.order_ == order::ascending) ? (cmp < 0) : (cmp > 0);
            }
            return false;
        }

    private:
        static int compare_raw(const vector::vector_t& vec, size_t a, size_t b);

        std::vector<sort_key> keys_;
        const vector::data_chunk_t* chunk_ = nullptr;
    };

} // namespace components::sort