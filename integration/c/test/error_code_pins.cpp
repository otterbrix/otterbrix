#include "mirrored_error_codes.hpp"

#include <core/result_wrapper.hpp>

static_assert(EC_INVALID_PARAMETER == static_cast<int32_t>(core::error_code_t::invalid_parameter));
static_assert(EC_SQL_PARSE_ERROR == static_cast<int32_t>(core::error_code_t::sql_parse_error));
static_assert(EC_IO_ERROR == static_cast<int32_t>(core::error_code_t::io_error));
static_assert(EC_ACTIVE_SQL_TRANSACTION == static_cast<int32_t>(core::error_code_t::active_sql_transaction));
static_assert(EC_DEPENDENT_OBJECTS_STILL_EXIST ==
              static_cast<int32_t>(core::error_code_t::dependent_objects_still_exist));
