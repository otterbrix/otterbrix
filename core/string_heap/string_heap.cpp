#include "string_heap.hpp"
#include <cstring>

namespace core {

    string_heap_t::string_heap_t(std::pmr::memory_resource* resource)
        : arena_allocator_(resource) {}

    void string_heap_t::reset() {
        interned_.clear();
        arena_allocator_.release();
    }

    void* string_heap_t::insert(const void* data, size_t size) {
        std::string_view key(static_cast<const char*>(data), size);
        auto it = interned_.find(key);
        if (it != interned_.end()) {
            return it->second;
        }
        void* ptr = arena_allocator_.allocate(size);
        std::memcpy(ptr, data, size);
        interned_.emplace(std::string_view(static_cast<const char*>(ptr), size), ptr);
        return ptr;
    }

    void* string_heap_t::empty_string(size_t size) { return arena_allocator_.allocate(size); }
} // namespace core
