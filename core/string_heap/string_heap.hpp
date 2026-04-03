#pragma once

#include <memory_resource>
#include <string_view>

namespace core {

    class string_heap_t {
    public:
        explicit string_heap_t(std::pmr::memory_resource* resource);

        void reset();
        void* insert(char* c) { return insert(std::string_view(c)); }
        void* insert(const char* c) { return insert(std::string_view(c)); }
        void* insert(const void* data, size_t size);
        template<typename T>
        void* insert(T&& str_like);
        void* empty_string(size_t size);

    private:
        std::pmr::monotonic_buffer_resource arena_allocator_;
    };

    template<typename T>
    void* string_heap_t::insert(T&& str_like) {
        return insert(str_like.data(), str_like.size());
    }

} // namespace core