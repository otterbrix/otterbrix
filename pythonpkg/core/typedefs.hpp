#pragma once

#include <cstdint>
#include <cstring>

namespace otterbrix {

    typedef uint64_t idx_t;
    typedef int64_t row_t;
    typedef uint64_t hash_t;
    typedef uint8_t data_t;
    typedef data_t *data_ptr_t;
    typedef const data_t *const_data_ptr_t;
    typedef uint32_t sel_t;
    typedef idx_t transaction_t;
    typedef idx_t column_t;
    typedef idx_t storage_t;

    template<class Target>
    data_ptr_t data_ptr_cast(Target *src) { // NOLINT: naming
        return reinterpret_cast<data_ptr_t>(src);
    }

    template<class Target>
    const_data_ptr_t const_data_ptr_cast(const Target *src) { // NOLINT: naming
        return reinterpret_cast<const_data_ptr_t>(src);
    }

    template<class Target>
    char *char_ptr_cast(Target *src) { // NOLINT: naming
        return reinterpret_cast<char *>(src);
    }

    template<class Target>
    const char *const_char_ptr_cast(const Target *src) { // NOLINT: naming
        return reinterpret_cast<const char *>(src);
    }

    template<class Target>
    const unsigned char *const_uchar_ptr_cast(const Target *src) { // NOLINT: naming
        return reinterpret_cast<const unsigned char *>(src);
    }

    template<class Target>
    uintptr_t CastPointerToValue(Target *src) {
        return reinterpret_cast<uintptr_t>(src);
    }

    template<class Target>
    uint64_t cast_pointer_to_uint64(Target *src) {
        return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(src));
    }

    template<class Target = data_t>
    Target *cast_uint64_to_pointer(uint64_t value) {
        return reinterpret_cast<Target *>(static_cast<uintptr_t>(value));
    }

    template <typename T>
    const T Load(const_data_ptr_t ptr) {
        T ret;
        std::memcpy(&ret, ptr, sizeof(ret)); // NOLINT
        return ret;
    }

    template <typename T>
    void Store(const T &val, data_ptr_t ptr) {
        std::memcpy(ptr, (void *)&val, sizeof(val)); // NOLINT
    }

}
