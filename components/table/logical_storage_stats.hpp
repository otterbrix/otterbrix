#pragma once

#include <cstdint>

namespace components::table {

    struct logical_storage_stats_t {
        uint64_t rows{0};
        uint64_t fixed_value_bytes{0};
        uint64_t varlen_value_bytes{0};
        uint64_t validity_bytes{0};

        uint64_t logical_input_bytes() const { return fixed_value_bytes + varlen_value_bytes + validity_bytes; }

        logical_storage_stats_t& operator+=(const logical_storage_stats_t& other) {
            rows += other.rows;
            fixed_value_bytes += other.fixed_value_bytes;
            varlen_value_bytes += other.varlen_value_bytes;
            validity_bytes += other.validity_bytes;
            return *this;
        }
    };

} // namespace components::table
