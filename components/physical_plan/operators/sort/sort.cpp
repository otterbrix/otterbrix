#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_) { add(index, order_); }

    void columnar_sorter_t::add(size_t index, order order_) {
        std::pmr::vector<size_t> path;
        path.push_back(index);
        keys_.push_back({std::move(path), order_, nullptr});
    }

    void columnar_sorter_t::add(const std::pmr::vector<size_t>& col_path, order order_) {
        assert(!col_path.empty());
        keys_.push_back({col_path, order_, nullptr});
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
        int compare_typed(const vector::vector_t& vec, size_t a, size_t b) {
            auto* d = vec.data<T>();
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }

    } // anonymous namespace

    int columnar_sorter_t::compare_raw(const vector::vector_t& vec, size_t a, size_t b) {
        // Handle NULLs: NULLs sort last
        bool a_null = vec.is_null(a);
        bool b_null = vec.is_null(b);
        if (a_null && b_null)
            return 0;
        if (a_null)
            return 1;
        if (b_null)
            return -1;

        switch (vec.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return compare_typed<int8_t>(vec, a, b);
            case types::physical_type::INT16:
                return compare_typed<int16_t>(vec, a, b);
            case types::physical_type::INT32:
                return compare_typed<int32_t>(vec, a, b);
            case types::physical_type::INT64:
                return compare_typed<int64_t>(vec, a, b);
            case types::physical_type::UINT8:
                return compare_typed<uint8_t>(vec, a, b);
            case types::physical_type::UINT16:
                return compare_typed<uint16_t>(vec, a, b);
            case types::physical_type::UINT32:
                return compare_typed<uint32_t>(vec, a, b);
            case types::physical_type::UINT64:
                return compare_typed<uint64_t>(vec, a, b);
            case types::physical_type::INT128:
                return compare_typed<types::int128_t>(vec, a, b);
            case types::physical_type::UINT128:
                return compare_typed<types::uint128_t>(vec, a, b);
            case types::physical_type::FLOAT:
                return compare_typed<float>(vec, a, b);
            case types::physical_type::DOUBLE:
                return compare_typed<double>(vec, a, b);
            case types::physical_type::STRING:
                return compare_typed<std::string_view>(vec, a, b);
            default: {
                // Fallback for composite types (STRUCT, LIST, etc.) — use logical_value_t
                if (!vec.resource())
                    return 0;
                auto va = vec.value(a);
                auto vb = vec.value(b);
                auto cmp = va.compare(vb);
                return (cmp == types::compare_t::less) ? -1 : (cmp == types::compare_t::more) ? 1 : 0;
            }
        }
    }

} // namespace components::sort
