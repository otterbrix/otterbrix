#include "operations_helper.hpp"

namespace components::types {

    template<typename T>
    int unsigned_length(T value);

    template<>
    int unsigned_length(uint8_t value) {
        int length = 1;
        length += value >= 10;
        length += value >= 100;
        return length;
    }

    template<>
    int unsigned_length(uint16_t value) {
        int length = 1;
        length += value >= 10;
        length += value >= 100;
        length += value >= 1000;
        length += value >= 10000;
        return length;
    }

    template<>
    int unsigned_length(uint32_t value) {
        if (value >= 10000) {
            int length = 5;
            length += value >= 100000;
            length += value >= 1000000;
            length += value >= 10000000;
            length += value >= 100000000;
            length += value >= 1000000000;
            return length;
        } else {
            int length = 1;
            length += value >= 10;
            length += value >= 100;
            length += value >= 1000;
            return length;
        }
    }

    template<>
    int unsigned_length(uint64_t value) {
        if (value >= 10000000000ULL) {
            if (value >= 1000000000000000ULL) {
                int length = 16;
                length += value >= 10000000000000000ULL;
                length += value >= 100000000000000000ULL;
                length += value >= 1000000000000000000ULL;
                length += value >= 10000000000000000000ULL;
                return length;
            } else {
                int length = 11;
                length += value >= 100000000000ULL;
                length += value >= 1000000000000ULL;
                length += value >= 10000000000000ULL;
                length += value >= 100000000000000ULL;
                return length;
            }
        } else {
            if (value >= 100000ULL) {
                int length = 6;
                length += value >= 1000000ULL;
                length += value >= 10000000ULL;
                length += value >= 100000000ULL;
                length += value >= 1000000000ULL;
                return length;
            } else {
                int length = 1;
                length += value >= 10ULL;
                length += value >= 100ULL;
                length += value >= 1000ULL;
                length += value >= 10000ULL;
                return length;
            }
        }
    }

    template<>
    int unsigned_length(int128_t value) {
        if (absl::Int128High64(value) == 0) {
            return unsigned_length<uint64_t>(absl::Int128Low64(value));
        }
        // binary search over POWERS_OF_TEN
        // desired length is between 17 and 38
        if (value >= POWERS_OF_TEN[27]) {
            // [27..38]
            if (value >= POWERS_OF_TEN[32]) {
                if (value >= POWERS_OF_TEN[36]) {
                    int length = 37;
                    length += value >= POWERS_OF_TEN[37];
                    length += value >= POWERS_OF_TEN[38];
                    return length;
                } else {
                    int length = 33;
                    length += value >= POWERS_OF_TEN[33];
                    length += value >= POWERS_OF_TEN[34];
                    length += value >= POWERS_OF_TEN[35];
                    return length;
                }
            } else {
                if (value >= POWERS_OF_TEN[30]) {
                    int length = 31;
                    length += value >= POWERS_OF_TEN[31];
                    length += value >= POWERS_OF_TEN[32];
                    return length;
                } else {
                    int length = 28;
                    length += value >= POWERS_OF_TEN[28];
                    length += value >= POWERS_OF_TEN[29];
                    return length;
                }
            }
        } else {
            // [17..27]
            if (value >= POWERS_OF_TEN[22]) {
                // [22..27]
                if (value >= POWERS_OF_TEN[25]) {
                    int length = 26;
                    length += value >= POWERS_OF_TEN[26];
                    return length;
                } else {
                    int length = 23;
                    length += value >= POWERS_OF_TEN[23];
                    length += value >= POWERS_OF_TEN[24];
                    return length;
                }
            } else {
                // [17..22]
                if (value >= POWERS_OF_TEN[20]) {
                    int length = 21;
                    length += value >= POWERS_OF_TEN[21];
                    return length;
                } else {
                    int length = 18;
                    length += value >= POWERS_OF_TEN[18];
                    length += value >= POWERS_OF_TEN[19];
                    return length;
                }
            }
        }
    }

    int decimal_length(int128_t value, uint8_t width, uint8_t scale) {
        int negative;

        if (value < 0) {
            value = -value;
            negative = 1;
        } else {
            negative = 0;
        }
        if (scale == 0) {
            // scale is 0: regular number
            return unsigned_length(value) + negative;
        }
        auto extra_numbers = width > scale ? 2 : 1;
        return std::max(scale + extra_numbers, unsigned_length(value) + 1) + negative;
    }

    template<typename T>
    char* format_unsigned(T value, char* ptr) {
        while (value >= 10) {
            auto index = static_cast<size_t>(value % 10);
            value /= 10;
            *--ptr = static_cast<char>('0' + index);
        }
        *--ptr = static_cast<char>('0' + value);
        return ptr;
    }

    template<>
    char* format_unsigned(int128_t value, char* ptr) {
        while (absl::Int128High64(value) > 0) {
            static constexpr uint64_t divisor = 100000000000000000ULL;
            int128_t divided = value / divisor;
            uint64_t remainder = static_cast<uint64_t>(value - divided * divisor);
            value = divided;

            auto start_ptr = ptr;
            // now we format the remainder: note that we need to pad with zero's in case
            // the remainder is small (i.e. less than 10000000000000000)
            ptr = format_unsigned<uint64_t>(remainder, ptr);

            int format_length = static_cast<int>(start_ptr - ptr);
            // pad with zero
            for (int i = format_length; i < 17; i++) {
                *--ptr = '0';
            }
        }
        return format_unsigned<uint64_t>(absl::Int128Low64(value), ptr);
    }

    inline void format_decimal(char* data_ptr, char* end_ptr, int128_t value, uint8_t width, uint8_t scale) {
        if (value < 0) {
            value = -value;
            *data_ptr = '-';
            data_ptr++;
        }
        if (scale == 0) {
            // with scale=0 we format the number as a regular number
            format_unsigned(value, end_ptr);
            return;
        }

        int128_t major = value / POWERS_OF_TEN[scale];
        int128_t minor = value - major * POWERS_OF_TEN[scale];

        data_ptr = format_unsigned(minor, end_ptr);
        // pad with zeros and add the decimal point
        while (data_ptr > end_ptr - scale) {
            *--data_ptr = '0';
        }
        *--data_ptr = '.';
        if (width > scale) {
            format_unsigned(major, data_ptr);
        }
    }

    std::pmr::string format_decimal(std::pmr::memory_resource* resource, int128_t value, uint8_t width, uint8_t scale) {
        int length = decimal_length(value, width, scale);
        std::pmr::string result(resource);
        result.resize(static_cast<size_t>(length));
        format_decimal(result.data(), result.data() + length, value, width, scale);
        return result;
    }

    std::string format_decimal(int128_t value, uint8_t width, uint8_t scale) {
        int length = decimal_length(value, width, scale);
        std::string result;
        result.resize(static_cast<size_t>(length));
        format_decimal(result.data(), result.data() + length, value, width, scale);
        return result;
    }

} // namespace components::types