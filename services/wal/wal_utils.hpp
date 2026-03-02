#pragma once

#include <cstdint>
#include <string>

namespace services::wal {

    using size_tt = std::uint32_t;
    using crc32_t = std::uint32_t;

    // Read a 4-byte big-endian size from raw char buffer
    inline size_tt read_size_raw(const char* input, size_tt index_start) {
        size_tt size_tmp = 0;
        size_tmp = 0xff000000 & (size_tt(uint8_t(input[index_start])) << 24);
        size_tmp |= 0x00ff0000 & (size_tt(uint8_t(input[index_start + 1])) << 16);
        size_tmp |= 0x0000ff00 & (size_tt(uint8_t(input[index_start + 2])) << 8);
        size_tmp |= 0x000000ff & (size_tt(uint8_t(input[index_start + 3])));
        return size_tmp;
    }

    // Read a 4-byte big-endian size from pmr::string buffer
    inline size_tt read_size_raw(const std::pmr::string& input, size_tt index_start) {
        return read_size_raw(input.data(), index_start);
    }

    // Read a 4-byte big-endian CRC32 from raw char buffer
    inline crc32_t read_crc32_raw(const char* input, size_tt index_start) {
        crc32_t crc32_tmp = 0;
        crc32_tmp = 0xff000000 & (uint32_t(uint8_t(input[index_start])) << 24);
        crc32_tmp |= 0x00ff0000 & (uint32_t(uint8_t(input[index_start + 1])) << 16);
        crc32_tmp |= 0x0000ff00 & (uint32_t(uint8_t(input[index_start + 2])) << 8);
        crc32_tmp |= 0x000000ff & (uint32_t(uint8_t(input[index_start + 3])));
        return crc32_tmp;
    }

    // Read a 4-byte big-endian CRC32 from pmr::string buffer
    inline crc32_t read_crc32_raw(const std::pmr::string& input, size_tt index_start) {
        return read_crc32_raw(input.data(), index_start);
    }

} // namespace services::wal
