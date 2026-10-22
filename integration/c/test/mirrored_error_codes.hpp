#pragma once

#include <cstdint>

// core::error_code_t values the C API answers; error_code_pins.cpp holds them to the engine's enum.
inline constexpr int32_t EC_INVALID_PARAMETER = 22;
inline constexpr int32_t EC_SQL_PARSE_ERROR = 30;
inline constexpr int32_t EC_IO_ERROR = 40;
