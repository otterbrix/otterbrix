#pragma once

#include <cstring>
#include <memory_resource>

namespace core {

    // An arena that stores contiguous, variable-length runs of `T` objects.
    //
    // A string buffer is exactly this with T = char: a string is a variable-length
    // run of characters, the same shape as a LIST column's child elements (a
    // variable-length run of elements). Both store their payload out-of-line and
    // hand back a pointer that an owning (offset/length)-style handle refers to —
    // so the same buffer backs both string_buffer_t and LIST update payloads.
    template<typename T>
    class object_buffer_t {
    public:
        explicit object_buffer_t(std::pmr::memory_resource* resource)
            : arena_allocator_(resource) {}

        void reset() { arena_allocator_.release(); }

        // Reserve room for `count` objects, left uninitialized.
        T* allocate(size_t count) { return static_cast<T*>(arena_allocator_.allocate(count * sizeof(T), alignof(T))); }

        // Copy `count` objects into the arena and return the stored run.
        T* insert(const T* data, size_t count) {
            T* ptr = allocate(count);
            std::memcpy(ptr, data, count * sizeof(T));
            return ptr;
        }

    private:
        std::pmr::monotonic_buffer_resource arena_allocator_;
    };

} // namespace core
