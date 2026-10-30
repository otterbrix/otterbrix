#pragma once

#include <cstdint>

// core::error_code_t values the C API answers; error_code_pins.cpp holds them to the engine's enum.
inline constexpr int32_t EC_INVALID_PARAMETER = 22;
inline constexpr int32_t EC_SQL_PARSE_ERROR = 30;
inline constexpr int32_t EC_IO_ERROR = 40;
inline constexpr int32_t EC_ACTIVE_SQL_TRANSACTION = 46;
inline constexpr int32_t EC_DEPENDENT_OBJECTS_STILL_EXIST = 47;
inline constexpr int32_t EC_SCHEMA_ALREADY_EXISTS = 48;
inline constexpr int32_t EC_SCHEMA_NOT_EXISTS = 49;
inline constexpr int32_t EC_DUPLICATE_COLUMN = 53;
inline constexpr int32_t EC_LOCK_FILE_EXISTS = 56;
inline constexpr int32_t EC_DIVISION_BY_ZERO = 67;
inline constexpr int32_t EC_UNIQUE_VIOLATION = 83;
inline constexpr int32_t EC_NO_ACTIVE_SQL_TRANSACTION = 91;
inline constexpr int32_t EC_UNDEFINED_PREPARED_STATEMENT = 94;
inline constexpr int32_t EC_DUPLICATE_PREPARED_STATEMENT = 95;
inline constexpr int32_t EC_UNDEFINED_PARAMETER = 96;
inline constexpr int32_t EC_WINDOWING_ERROR = 97;
inline constexpr int32_t EC_UNDEFINED_CURSOR = 98;
inline constexpr int32_t EC_DUPLICATE_CURSOR = 99;
inline constexpr int32_t EC_OBJECT_IN_USE = 100;
