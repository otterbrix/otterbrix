#pragma once

#include <cstdint>

namespace components::function::table {

//===--------------------------------------------------------------------===//
// Arrow Variable Size Types
//===--------------------------------------------------------------------===//
enum class ArrowVariableSizeType : uint8_t { NORMAL, FIXED_SIZE, SUPER_SIZE, VIEW };

} // namespace components::function::table
