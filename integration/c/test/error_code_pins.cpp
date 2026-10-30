#include "mirrored_error_codes.hpp"

#include <core/result_wrapper.hpp>

static_assert(EC_INVALID_PARAMETER == static_cast<int32_t>(core::error_code_t::invalid_parameter));
static_assert(EC_SQL_PARSE_ERROR == static_cast<int32_t>(core::error_code_t::sql_parse_error));
static_assert(EC_IO_ERROR == static_cast<int32_t>(core::error_code_t::io_error));
static_assert(EC_ACTIVE_SQL_TRANSACTION == static_cast<int32_t>(core::error_code_t::active_sql_transaction));
static_assert(EC_DEPENDENT_OBJECTS_STILL_EXIST ==
              static_cast<int32_t>(core::error_code_t::dependent_objects_still_exist));
static_assert(EC_SCHEMA_ALREADY_EXISTS == static_cast<int32_t>(core::error_code_t::schema_already_exists));
static_assert(EC_SCHEMA_NOT_EXISTS == static_cast<int32_t>(core::error_code_t::schema_not_exists));
static_assert(EC_DUPLICATE_COLUMN == static_cast<int32_t>(core::error_code_t::duplicate_column));
static_assert(EC_LOCK_FILE_EXISTS == static_cast<int32_t>(core::error_code_t::lock_file_exists));
static_assert(EC_DIVISION_BY_ZERO == static_cast<int32_t>(core::error_code_t::division_by_zero));
static_assert(EC_UNIQUE_VIOLATION == static_cast<int32_t>(core::error_code_t::unique_violation));
static_assert(EC_NO_ACTIVE_SQL_TRANSACTION == static_cast<int32_t>(core::error_code_t::no_active_sql_transaction));
static_assert(EC_UNDEFINED_PREPARED_STATEMENT ==
              static_cast<int32_t>(core::error_code_t::undefined_prepared_statement));
static_assert(EC_DUPLICATE_PREPARED_STATEMENT ==
              static_cast<int32_t>(core::error_code_t::duplicate_prepared_statement));
static_assert(EC_UNDEFINED_PARAMETER == static_cast<int32_t>(core::error_code_t::undefined_parameter));
static_assert(EC_WINDOWING_ERROR == static_cast<int32_t>(core::error_code_t::windowing_error));
