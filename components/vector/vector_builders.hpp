#pragma once

#include <cassert>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include "vector.hpp"

namespace components::vector {

    // Builders for ONE-row CONSTANT vectors. Every nested builder takes its elements as one-row
    // vectors (any vector_type, row 0 is read) whose type equals the declared element type.

    namespace detail {
        vector_t make_constant(std::pmr::memory_resource* resource, const types::complex_logical_type& type);

        template<typename T>
        inline constexpr bool is_string_like_v =
            std::is_same_v<T, std::string> || std::is_same_v<T, std::pmr::string> ||
            std::is_same_v<T, std::string_view> || std::is_same_v<T, const char*> || std::is_same_v<T, char*>;
    } // namespace detail

    // `type` must have the physical storage of T (DATE <- date_t or int32_t, UUID <- int128_t, ...;
    // string-like T needs STRING_LITERAL storage). A mismatch aborts in every build.
    template<typename T>
    vector_t make_scalar(std::pmr::memory_resource* resource, const types::complex_logical_type& type, T value) {
        const auto expected = detail::is_string_like_v<T> ? types::physical_type::STRING
                                                          : types::to_physical_type(types::to_logical_type<T>());
        if (type.to_physical_type() != expected) {
            assert(false && "make_scalar: the value's physical type does not match the vector type");
            std::abort();
        }
        auto result = detail::make_constant(resource, type);
        if constexpr (detail::is_string_like_v<T>) {
            result.set_value(0, std::string_view{value});
        } else {
            result.set_value(0, std::move(value));
        }
        return result;
    }

    // The type is to_logical_type<T>(), and STRING_LITERAL for the
    // string-like T (to_logical_type<const char*> alone would answer POINTER).
    template<typename T>
    vector_t make_scalar(std::pmr::memory_resource* resource, T value) {
        if constexpr (detail::is_string_like_v<T>) {
            return make_scalar(resource,
                               types::complex_logical_type{types::logical_type::STRING_LITERAL},
                               std::move(value));
        } else {
            static_assert(types::to_logical_type<T>() != types::logical_type::INVALID,
                          "make_scalar<T>: no logical type for T; pass the type explicitly");
            return make_scalar(resource, types::complex_logical_type{types::to_logical_type<T>()}, std::move(value));
        }
    }

    // A deep one-row CONSTANT copy of row `row` of `source`: it shares no buffer with `source`, so it may cross
    // an actor boundary or be flattened without touching the original.
    vector_t make_constant_copy(std::pmr::memory_resource* resource, const vector_t& source, uint64_t row = 0);

    // A FLAT copy of the first `count` rows of `source`, for the kernels that read FLAT vectors only
    // (cast_vector, binding casts).
    vector_t make_flat_copy(std::pmr::memory_resource* resource, const vector_t& source, uint64_t count);

    // A typed NULL of any type, nested included.
    vector_t make_null(std::pmr::memory_resource* resource, const types::complex_logical_type& type);

    // `value` is the unscaled integer; the storage width (INT16/32/64/128) follows from the type.
    vector_t make_decimal(std::pmr::memory_resource* resource,
                          const types::complex_logical_type& decimal_type,
                          types::int128_t value);
    // An invalid (width, scale) comes back as the error complex_logical_type::create_decimal reports.
    core::result_wrapper_t<vector_t>
    make_decimal(std::pmr::memory_resource* resource, types::int128_t value, uint8_t width, uint8_t scale);

    // LIST<element_type>.
    vector_t make_list(std::pmr::memory_resource* resource,
                       const types::complex_logical_type& element_type,
                       std::span<const vector_t> elements);

    // ARRAY<element_type>[elements.size()].
    vector_t make_array(std::pmr::memory_resource* resource,
                        const types::complex_logical_type& element_type,
                        std::span<const vector_t> elements);

    // fields[i] has the type of struct_type's i-th field.
    vector_t make_struct(std::pmr::memory_resource* resource,
                         const types::complex_logical_type& struct_type,
                         std::span<const vector_t> fields);

    // MAP<key_type, value_type>: a LIST of STRUCT<key, value> (complex_logical_type::create_map).
    vector_t make_map(std::pmr::memory_resource* resource,
                      const types::complex_logical_type& key_type,
                      const types::complex_logical_type& value_type,
                      std::span<const vector_t> keys,
                      std::span<const vector_t> values);

} // namespace components::vector
