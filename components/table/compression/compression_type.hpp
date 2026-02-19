#pragma once

#include <cstdint>

namespace components::table::compression {

    enum class compression_type : uint8_t
    {
        INVALID = 0,
        UNCOMPRESSED = 1,
        CONSTANT = 2,
        RLE = 3,
        BITPACKING = 4,
        DICTIONARY = 5,
        VALIDITY_UNCOMPRESSED = 6
    };

} // namespace components::table::compression
