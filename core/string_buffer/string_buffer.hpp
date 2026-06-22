#pragma once

#include "object_buffer.hpp"

#include <memory_resource>
#include <string_view>

namespace core {

    // A string buffer is an object_buffer of characters: a string is just a
    // variable-length run of chars. The char-specific convenience overloads
    // (null-terminated / string_view inserts) are layered on top.
    class string_buffer_t {
    public:
        explicit string_buffer_t(std::pmr::memory_resource* resource);

        void reset();
        void* insert(char* c) { return insert(std::string_view(c)); }
        void* insert(const char* c) { return insert(std::string_view(c)); }
        void* insert(const void* data, size_t size);
        template<typename T>
        void* insert(T&& str_like);
        void* empty_string(size_t size);

    private:
        object_buffer_t<char> buffer_;
    };

    template<typename T>
    void* string_buffer_t::insert(T&& str_like) {
        return insert(str_like.data(), str_like.size());
    }

} // namespace core
