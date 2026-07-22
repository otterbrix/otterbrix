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

    // NULL placement, independent of ascending/descending: `first` always before non-NULLs, `last` after.
    enum class null_order
    {
        first,
        last
    };

    class columnar_sorter_t {
        struct sort_key {
            std::pmr::vector<size_t> col_path;
            order order_ = order::ascending;
            null_order null_order_ = null_order::last;
        };

    public:
        explicit columnar_sorter_t() = default;
        explicit columnar_sorter_t(size_t index, order order_ = order::ascending, null_order null_order_ = null_order::last);

        void add(size_t index, order order_ = order::ascending, null_order null_order_ = null_order::last);
        void
        add(const std::pmr::vector<size_t>& col_path, order order_ = order::ascending, null_order null_order_ = null_order::last);

        void set_chunk(const vector::data_chunk_t& chunk);

        bool operator()(size_t row_a, size_t row_b) const {
            for (const auto& k : keys_) {
                int cmp = compare_key(*chunk_, row_a, *chunk_, row_b, k);
                if (cmp == 0)
                    continue;
                return cmp < 0;
            }
            return false;
        }

        // Compare a row from one chunk against a row from a (possibly different) chunk under the configured
        // sort keys. Returns <0 if (a,ra) sorts before (b,rb), >0 if after, 0 if equal.
        int
        compare_cross(const vector::data_chunk_t& a, size_t row_a, const vector::data_chunk_t& b, size_t row_b) const;

    private:
        // One sort key between (a,row_a) and (b,row_b). Resolves each side through col_path — so a plain
        // column, a struct field and an ARRAY/LIST element subscript (v[i]) all read the correct leaf — and
        // compares raw. NULL placement (null_order_) is independent of the ascending/descending sign.
        static int compare_key(const vector::data_chunk_t& a,
                               size_t row_a,
                               const vector::data_chunk_t& b,
                               size_t row_b,
                               const sort_key& key);

        std::vector<sort_key> keys_;
        const vector::data_chunk_t* chunk_ = nullptr;
    };

} // namespace components::sort