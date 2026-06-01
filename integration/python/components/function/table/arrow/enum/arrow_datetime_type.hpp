#pragma once

#include <cstdint>

namespace components::function::table {

//===--------------------------------------------------------------------===//
// Arrow Time/Date Types
//===--------------------------------------------------------------------===//
enum class ArrowDateTimeType : uint8_t {
	MILLISECONDS,
	MICROSECONDS,
	NANOSECONDS,
	SECONDS,
	DAYS,
	MONTHS,
	MONTH_DAY_NANO
};

} // namespace components::function::table
