#pragma once

#include "types.hpp"

namespace components::types {

    template<typename T>
    struct get_physical_type;

    template<>
    struct get_physical_type<bool> {
        static constexpr physical_type type = physical_type::BOOL;
    };

    template<>
    struct get_physical_type<uint8_t> {
        static constexpr physical_type type = physical_type::UINT8;
    };
    template<>
    struct get_physical_type<uint16_t> {
        static constexpr physical_type type = physical_type::UINT16;
    };
    template<>
    struct get_physical_type<uint32_t> {
        static constexpr physical_type type = physical_type::UINT32;
    };
    template<>
    struct get_physical_type<uint64_t> {
        static constexpr physical_type type = physical_type::UINT64;
    };
    template<>
    struct get_physical_type<uint128_t> {
        static constexpr physical_type type = physical_type::UINT128;
    };
    template<>
    struct get_physical_type<int8_t> {
        static constexpr physical_type type = physical_type::INT8;
    };
    template<>
    struct get_physical_type<int16_t> {
        static constexpr physical_type type = physical_type::INT16;
    };
    template<>
    struct get_physical_type<int32_t> {
        static constexpr physical_type type = physical_type::INT32;
    };
    template<>
    struct get_physical_type<int64_t> {
        static constexpr physical_type type = physical_type::INT64;
    };
    template<>
    struct get_physical_type<int128_t> {
        static constexpr physical_type type = physical_type::INT128;
    };

    template<>
    struct get_physical_type<float> {
        static constexpr physical_type type = physical_type::FLOAT;
    };
    template<>
    struct get_physical_type<double> {
        static constexpr physical_type type = physical_type::DOUBLE;
    };

    template<>
    struct get_physical_type<std::string_view> {
        static constexpr physical_type type = physical_type::STRING;
    };

} // namespace components::types