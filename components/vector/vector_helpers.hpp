#pragma once

#include <cassert>
#include <tuple>
#include <type_traits>
#include <vector>

namespace components::vector {

    template<typename T>
    inline constexpr bool is_std_vector_v = false;
    template<typename Element, typename Allocator>
    inline constexpr bool is_std_vector_v<std::vector<Element, Allocator>> = true;

    template<typename T>
    concept is_vector = is_std_vector_v<T>;

    template<typename T>
    inline constexpr bool is_tuple_v = false;
    template<typename... Ts>
    inline constexpr bool is_tuple_v<std::tuple<Ts...>> = true;

    template<typename T>
    concept non_logical_value_arg = !std::is_same_v<std::remove_cvref_t<T>, types::logical_value_t>;

    template<typename T>
    T sequence_entry(int64_t value);

    template<typename T>
    T sequence_entry(int64_t value) {
        assert(value >= std::numeric_limits<T>::min() && value <= std::numeric_limits<T>::max());
        return static_cast<T>(value);
    }

} // namespace components::vector