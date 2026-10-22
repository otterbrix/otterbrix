#include "mirrored_error_codes.hpp"

#include <core/result_wrapper.hpp>

static_assert(EC_INVALID_PARAMETER == static_cast<int32_t>(core::error_code_t::invalid_parameter));
static_assert(EC_SQL_PARSE_ERROR == static_cast<int32_t>(core::error_code_t::sql_parse_error));
static_assert(EC_IO_ERROR == static_cast<int32_t>(core::error_code_t::io_error));
