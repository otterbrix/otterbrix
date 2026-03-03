#pragma once

#include <cstdint>

namespace components::vector {

    // op_kind: 0=add, 1=sub, 2=mul, 3=div, 4=mod
    enum class arithmetic_op : uint8_t { add = 0, subtract, multiply, divide, mod };

} // namespace components::vector
