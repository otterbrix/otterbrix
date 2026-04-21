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
                int cmp = compare_raw(*k.vec, row_a, *k.vec, row_b);
                if (cmp == 0)
                    continue;
                return (k.order_ == order::ascending) ? (cmp < 0) : (cmp > 0);
            }
            return false;
        }

        // Compare a row from one chunk against a row from a (possibly different) chunk.
        // Does not use cached k.vec pointers; resolves col_path on each side per comparison.
        // Returns <0 if (a,ra) should sort before (b,rb), >0 if after, 0 if equal under the
        // configured sort keys and orders.
        int compare_cross(const vector::data_chunk_t& a, size_t row_a,
                          const vector::data_chunk_t& b, size_t row_b) const;

    private:
        static int compare_raw(const vector::vector_t& va, size_t a,
                               const vector::vector_t& vb, size_t b);

        std::vector<sort_key> keys_;
        const vector::data_chunk_t* chunk_ = nullptr;
    };

} // namespace components::sort
