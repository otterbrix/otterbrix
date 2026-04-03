#include "string_heap.hpp"
#include <cstring>

namespace core {

    string_heap_t::string_heap_t(std::pmr::memory_resource* resource)
        : arena_allocator_(resource) {}

    void string_heap_t::reset() { arena_allocator_.release(); }

    void* string_heap_t::insert(const void* data, size_t size) {
        void* ptr = arena_allocator_.allocate(size);
        std::memcpy(ptr, data, size);
        return ptr;
    }

    void* string_heap_t::empty_string(size_t size) { return arena_allocator_.allocate(size); }
} // namespace core
