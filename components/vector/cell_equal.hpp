#pragma once

#include <cstddef>
#include <string_view>
#include <type_traits>

#include <components/types/logical_value.hpp>
#include <components/vector/vector.hpp>
#include <core/operations_helper.hpp>

namespace components::vector {

    // Typed cell equality (the per-physical-type leg of cells_equal). Floats compare
    // EXACTLY, with NaN==NaN true: this function is the verify half of a hash+verify
    // pair (data_chunk_t::hash buckets by std::hash over the raw value), so it must
    // agree with the hash — an epsilon compare breaks that contract exactly where it
    // differs from ==: is_equals(NaN,NaN) is false while identical NaN bit patterns
    // share a bucket (a UNIQUE/PK dup slips through; an FK NaN never matches its own
    // parent), and epsilon-close-but-unequal values land in different buckets anyway
    // so the tolerance is unreachable. -0.0 == 0.0 stays true, matching std::hash's
    // ±0 special case.
    template<typename T>
    inline bool cells_equal_typed(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb) {
        if constexpr (std::is_floating_point_v<T>) {
            const T x = a.data<T>()[ra];
            const T y = b.data<T>()[rb];
            return x == y || (x != x && y != y);
        } else {
            return a.data<T>()[ra] == b.data<T>()[rb];
        }
    }

    // NULL-aware typed cell-by-cell equality between two vector cells (R1: NO logical_value_t
    // round-trip on the hot path — dispatch on physical_type and compare raw cells; only
    // nested/unsupported physical types fall back to value()). Two NULLs compare EQUAL, one
    // NULL UNEQUAL. Single source of the dedup/verify semantics shared by GROUP BY / HASH JOIN,
    // the UNIQUE/PK within-batch dedup and the FK single-pass hash semi-join.
    inline bool cells_equal(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb) {
        const bool a_null = a.is_null(ra);
        const bool b_null = b.is_null(rb);
        if (a_null || b_null) {
            return a_null == b_null;
        }
        switch (a.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return cells_equal_typed<int8_t>(a, ra, b, rb);
            case types::physical_type::INT16:
                return cells_equal_typed<int16_t>(a, ra, b, rb);
            case types::physical_type::INT32:
                return cells_equal_typed<int32_t>(a, ra, b, rb);
            case types::physical_type::INT64:
                return cells_equal_typed<int64_t>(a, ra, b, rb);
            case types::physical_type::UINT8:
                return cells_equal_typed<uint8_t>(a, ra, b, rb);
            case types::physical_type::UINT16:
                return cells_equal_typed<uint16_t>(a, ra, b, rb);
            case types::physical_type::UINT32:
                return cells_equal_typed<uint32_t>(a, ra, b, rb);
            case types::physical_type::UINT64:
                return cells_equal_typed<uint64_t>(a, ra, b, rb);
            case types::physical_type::INT128:
                return cells_equal_typed<types::int128_t>(a, ra, b, rb);
            case types::physical_type::UINT128:
                return cells_equal_typed<types::uint128_t>(a, ra, b, rb);
            case types::physical_type::FLOAT:
                return cells_equal_typed<float>(a, ra, b, rb);
            case types::physical_type::DOUBLE:
                return cells_equal_typed<double>(a, ra, b, rb);
            case types::physical_type::STRING:
                return a.data<std::string_view>()[ra] == b.data<std::string_view>()[rb];
            default:
                return a.value(ra) == b.value(rb);
        }
    }

} // namespace components::vector
