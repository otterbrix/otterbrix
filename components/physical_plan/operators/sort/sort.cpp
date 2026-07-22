#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_, null_order null_order_) {
        add(index, order_, null_order_);
    }

    void columnar_sorter_t::add(size_t index, order order_, null_order null_order_) {
        std::pmr::vector<size_t> path;
        path.push_back(index);
        keys_.push_back({std::move(path), order_, null_order_, nullptr});
    }

    void columnar_sorter_t::add(const std::pmr::vector<size_t>& col_path, order order_, null_order null_order_) {
        assert(!col_path.empty());
        keys_.push_back({col_path, order_, null_order_, nullptr});
    }

    void columnar_sorter_t::set_chunk(const vector::data_chunk_t& chunk) {
        chunk_ = &chunk;
        for (auto& k : keys_) {
            assert(!k.col_path.empty());
            k.vec = chunk.at(k.col_path);
        }
    }

    namespace {

        template<typename T>
        int compare_typed(const vector::vector_t& va, size_t a, const vector::vector_t& vb, size_t b) {
            auto* da = va.data<T>();
            auto* db = vb.data<T>();
            return (da[a] < db[b]) ? -1 : (da[a] > db[b]) ? 1 : 0;
        }

    } // anonymous namespace

    namespace {

        int compare_values(const vector::vector_t& va, size_t a, const vector::vector_t& vb, size_t b) {
            switch (va.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return compare_typed<int8_t>(va, a, vb, b);
            case types::physical_type::INT16:
                return compare_typed<int16_t>(va, a, vb, b);
            case types::physical_type::INT32:
                return compare_typed<int32_t>(va, a, vb, b);
            case types::physical_type::INT64:
                return compare_typed<int64_t>(va, a, vb, b);
            case types::physical_type::UINT8:
                return compare_typed<uint8_t>(va, a, vb, b);
            case types::physical_type::UINT16:
                return compare_typed<uint16_t>(va, a, vb, b);
            case types::physical_type::UINT32:
                return compare_typed<uint32_t>(va, a, vb, b);
            case types::physical_type::UINT64:
                return compare_typed<uint64_t>(va, a, vb, b);
            case types::physical_type::INT128:
                return compare_typed<types::int128_t>(va, a, vb, b);
            case types::physical_type::UINT128:
                return compare_typed<types::uint128_t>(va, a, vb, b);
            case types::physical_type::FLOAT:
                return compare_typed<float>(va, a, vb, b);
            case types::physical_type::DOUBLE:
                return compare_typed<double>(va, a, vb, b);
            case types::physical_type::STRING:
                return compare_typed<std::string_view>(va, a, vb, b);
            default: {
                // Fallback for composite types (STRUCT, LIST, etc.) — use logical_value_t
                if (!va.resource())
                    return 0;
                auto lva = va.value(a);
                auto lvb = vb.value(b);
                auto cmp = lva.compare(lvb);
                return (cmp == types::compare_t::less) ? -1 : (cmp == types::compare_t::more) ? 1 : 0;
            }
            }
        }

    } // anonymous namespace

    int columnar_sorter_t::compare_raw(const vector::vector_t& va,
                                       size_t a,
                                       const vector::vector_t& vb,
                                       size_t b,
                                       order ord,
                                       null_order nord) {
        const bool a_null = va.is_null(a);
        const bool b_null = vb.is_null(b);
        if (a_null || b_null) {
            // NULL placement is fixed by nord and is NOT flipped by the ascending/descending sign.
            if (a_null && b_null) {
                return 0;
            }
            const bool nulls_first = nord == null_order::first;
            if (a_null) {
                return nulls_first ? -1 : 1;
            }
            return nulls_first ? 1 : -1;
        }
        const int cmp = compare_values(va, a, vb, b);
        return (ord == order::ascending) ? cmp : -cmp;
    }

    int columnar_sorter_t::compare_cross(const vector::data_chunk_t& a,
                                         size_t row_a,
                                         const vector::data_chunk_t& b,
                                         size_t row_b) const {
        for (const auto& k : keys_) {
            assert(!k.col_path.empty());
            const auto* va = a.at(k.col_path);
            const auto* vb = b.at(k.col_path);
            if (!va || !vb)
                continue;
            int cmp = compare_raw(*va, row_a, *vb, row_b, k.order_, k.null_order_);
            if (cmp == 0)
                continue;
            return cmp;
        }
        return 0;
    }

} // namespace components::sort
