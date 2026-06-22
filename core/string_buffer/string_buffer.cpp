#include "string_buffer.hpp"

namespace core {

    string_buffer_t::string_buffer_t(std::pmr::memory_resource* resource)
        : buffer_(resource) {}

    void string_buffer_t::reset() { buffer_.reset(); }

    void* string_buffer_t::insert(const void* data, size_t size) {
        return buffer_.insert(static_cast<const char*>(data), size);
    }

    void* string_buffer_t::empty_string(size_t size) { return buffer_.allocate(size); }

} // namespace core
