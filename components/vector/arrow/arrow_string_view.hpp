#pragma once
#include <cassert>
#include <cstdint>
#include <cstring>

namespace components::vector::arrow {

    union arrow_string_view_t {
        static constexpr uint8_t MAX_INLINED_BYTES = 12 * sizeof(char);
        static constexpr uint8_t PREFIX_BYTES = 4 * sizeof(char);

        arrow_string_view_t() = default;

        arrow_string_view_t(int32_t length, const char* data) {
            assert(length <= MAX_INLINED_BYTES);
            inlined.length = length;
            std::memcpy(inlined.data, data, static_cast<size_t>(length));
            if (length < MAX_INLINED_BYTES) {
                uint8_t remaining_bytes = MAX_INLINED_BYTES - static_cast<uint8_t>(length);

                std::memset(&inlined.data[length], '\0', remaining_bytes);
            }
        }

        arrow_string_view_t(int32_t length, const char* data, int32_t buffer_idx, int32_t offset) {
            assert(length > MAX_INLINED_BYTES);
            ref.length = length;
            std::memcpy(ref.prefix, data, PREFIX_BYTES);
            ref.buffer_index = buffer_idx;
            ref.offset = offset;
        }

        struct {
            int32_t length;
            char data[MAX_INLINED_BYTES];
        } inlined;

        struct {
            int32_t length;
            char prefix[PREFIX_BYTES];
            int32_t buffer_index;
            int32_t offset;
        } ref;

        int32_t length() const { return inlined.length; }
        bool is_inlined() const { return length() <= MAX_INLINED_BYTES; }

        const char* inlined_data() const { return is_inlined() ? inlined.data : ref.prefix; }
        int32_t buffer_index() {
            assert(!is_inlined());
            return ref.buffer_index;
        }
        int32_t get_offset() {
            assert(!is_inlined());
            return ref.offset;
        }
    };

} // namespace components::vector::arrow