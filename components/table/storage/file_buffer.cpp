#include "file_buffer.hpp"

#include <cassert>
#include <core/file/file_handle.hpp>
#include <cstring>

namespace components::table::storage {

    file_buffer_t::file_buffer_t(std::pmr::memory_resource* resource, file_buffer_type type, uint64_t user_size)
        : resource_(resource)
        , type_(type) {
        if (user_size) {
            resize(user_size);
        }
    }

    file_buffer_t::file_buffer_t(file_buffer_t& source, file_buffer_type type)
        : resource_(source.resource_)
        , type_(type)
        , buffer_(source.buffer_)
        , size_(source.size_)
        , internal_buffer_(source.internal_buffer_)
        , internal_size_(source.internal_size_) {
        source.buffer_ = nullptr;
        source.internal_buffer_ = nullptr;
        source.size_ = 0;
        source.internal_size_ = 0;
    }

    file_buffer_t::~file_buffer_t() {
        if (!internal_buffer_) {
            return;
        }
        resource_->deallocate(internal_buffer_, internal_size_);
    }

    void file_buffer_t::read(core::filesystem::file_handle_t& handle, uint64_t location) {
        assert(type_ != file_buffer_type::TINY_BUFFER);
        handle.read(internal_buffer_, internal_size_, location);
    }

    void file_buffer_t::write(core::filesystem::file_handle_t& handle, uint64_t location) {
        assert(type_ != file_buffer_type::TINY_BUFFER);
        handle.write(internal_buffer_, internal_size_, location);
    }

    void file_buffer_t::reallocate_buffer(size_t new_size) {
        std::byte* new_buffer = static_cast<std::byte*>(resource_->allocate(new_size));
        if (internal_buffer_) {
            std::memcpy(new_buffer, internal_buffer_, std::min(new_size, size_));
            resource_->deallocate(internal_buffer_, internal_size_);
        }
        internal_buffer_ = new_buffer;
        internal_size_ = new_size;
        buffer_ = nullptr;
        size_ = 0;
    }

    file_buffer_t::memory_requirement_t file_buffer_t::calculate_memory(uint64_t user_size) {
        memory_requirement_t result;

        if (type_ == file_buffer_type::TINY_BUFFER) {
            result.header_size = 0;
            result.alloc_size = user_size;
        } else {
            result.header_size = DEFAULT_BLOCK_HEADER_SIZE;
            result.alloc_size = ((result.header_size + user_size + (SECTOR_SIZE - 1)) / SECTOR_SIZE) * SECTOR_SIZE;
        }
        return result;
    }

    void file_buffer_t::resize(uint64_t new_size) {
        auto req = calculate_memory(new_size);
        reallocate_buffer(req.alloc_size);

        if (new_size > 0) {
            buffer_ = internal_buffer_ + req.header_size;
            size_ = internal_size_ - req.header_size;
        }
    }

    void file_buffer_t::clear() { std::memset(internal_buffer_, 0, internal_size_); }

    block_t::block_t(std::pmr::memory_resource* resource, uint64_t id, uint64_t block_size)
        : file_buffer_t(resource, file_buffer_type::BLOCK, block_size)
        , id(id) {}

    block_t::block_t(std::pmr::memory_resource* resource, uint64_t id, uint32_t internal_size)
        : file_buffer_t(resource, file_buffer_type::BLOCK, internal_size)
        , id(id) {
        assert((allocation_size() & (SECTOR_SIZE - 1)) == 0);
    }

    block_t::block_t(file_buffer_t& source, uint64_t id)
        : file_buffer_t(source, file_buffer_type::BLOCK)
        , id(id) {
        assert((allocation_size() & (SECTOR_SIZE - 1)) == 0);
    }

} // namespace components::table::storage