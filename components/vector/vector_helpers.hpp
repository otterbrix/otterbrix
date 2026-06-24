#pragma once

#include <cassert>
#include <limits>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace components::vector {

    template<typename T>
    inline constexpr bool is_std_vector_v = false;
    template<typename Element, typename Allocator>
    inline constexpr bool is_std_vector_v<std::vector<Element, Allocator>> = true;

    template<typename T>
    concept is_vector = is_std_vector_v<T>;

    template<typename T>
    inline constexpr bool is_optional_v = false;
    template<typename T>
    inline constexpr bool is_optional_v<std::optional<T>> = true;

    template<typename T>
    inline constexpr bool is_tuple_v = false;
    template<typename... Ts>
    inline constexpr bool is_tuple_v<std::tuple<Ts...>> = true;

    // The stored value type behind a set_value argument: the element of an optional, or the type itself.
    template<typename T>
    struct stored_value_type {
        using type = std::remove_cvref_t<T>;
    };
    template<typename T>
    struct stored_value_type<std::optional<T>> {
        using type = T;
    };
    template<typename T>
    using stored_value_type_t = typename stored_value_type<std::remove_cvref_t<T>>::type;

    template<typename T>
    constexpr decltype(auto) stored_value(T&& value) {
        if constexpr (is_optional_v<std::remove_cvref_t<T>>) {
            return *std::forward<T>(value);
        } else {
            return std::forward<T>(value);
        }
    }

    template<typename T>
    T sequence_entry(int64_t value);

    template<typename T>
    T sequence_entry(int64_t value) {
        assert(value >= std::numeric_limits<T>::min() && value <= std::numeric_limits<T>::max());
        return static_cast<T>(value);
    }

} // namespace components::vector