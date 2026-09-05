#pragma once

#include <cstdint>
#include <memory_resource>

namespace core::filesystem {
    struct file_handle_t;
}

namespace components::table::storage {

    enum class file_buffer_type : uint8_t
    {
        BLOCK = 1,
        MANAGED_BUFFER = 2,
        TINY_BUFFER = 3
    };
    static constexpr size_t FILE_BUFFER_TYPE_COUNT = 3;
    static constexpr uint64_t DEFAULT_BLOCK_HEADER_SIZE = sizeof(uint64_t);
    static constexpr uint64_t SECTOR_SIZE = 4096U;
    static constexpr uint64_t INVALID_INDEX = uint64_t(-1);
    static constexpr uint32_t INVALID_BLOCK = uint32_t(-1);
    static constexpr uint64_t MAXIMUM_BLOCK = uint64_t(1) << 62; // 4611686018427388000ULL

    class file_buffer_t {
    public:
        friend class buffer_handle_t;

        file_buffer_t(std::pmr::memory_resource* resource, file_buffer_type type, uint64_t user_size);
        file_buffer_t(file_buffer_t& source, file_buffer_type type);
        virtual ~file_buffer_t();

        // The deepest link of the durability chain. `void` here — dropping the bool the file
        // handle returns — leaves EVERY data-block write in the system unobserved: a short
        // write, an ENOSPC or an EIO reaches nobody, and the checkpoint that follows commits a
        // root over blocks that were never laid down. Both report the handle's answer, and
        // every caller up to write_header() reads it.
        [[nodiscard]] bool read(core::filesystem::file_handle_t& handle, uint64_t location);
        [[nodiscard]] bool write(core::filesystem::file_handle_t& handle, uint64_t location);

        void clear();

        file_buffer_type buffer_type() const { return type_; }

        void resize(uint64_t user_size);

        uint64_t allocation_size() const { return internal_size_; }
        std::byte* internal_buffer() { return internal_buffer_; }
        std::byte* buffer() { return buffer_; }

        struct memory_requirement_t {
            uint64_t alloc_size;
            uint64_t header_size;
        };

        memory_requirement_t calculate_memory(uint64_t user_size);

        size_t size() const noexcept { return size_; }
        size_t& size() noexcept { return size_; }
        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    protected:
        void reallocate_buffer(size_t malloc_size);

        std::pmr::memory_resource* resource_;
        file_buffer_type type_;
        std::byte* buffer_ = nullptr;
        size_t size_ = 0;
        std::byte* internal_buffer_ = nullptr;
        uint64_t internal_size_ = 0;
    };

    class block_t : public file_buffer_t {
    public:
        block_t(std::pmr::memory_resource* resource, uint64_t id, uint64_t block_size);
        block_t(std::pmr::memory_resource* resource, uint64_t id, uint32_t internal_size);
        block_t(file_buffer_t& source, uint64_t id);

        uint64_t id;
    };

    struct block_pointer_t {
        block_pointer_t(uint64_t block_id, uint32_t offset)
            : block_id(block_id)
            , offset(offset) {}
        block_pointer_t()
            : block_id(INVALID_BLOCK)
            , offset(0) {}

        uint64_t block_id;
        uint32_t offset;
        uint32_t unused_padding{0};

        bool is_valid() const { return block_id != INVALID_BLOCK; }
    };

    struct meta_block_pointer_t {
        meta_block_pointer_t(uint64_t block_pointer, uint32_t offset)
            : block_pointer(block_pointer)
            , offset(offset) {}
        meta_block_pointer_t()
            : block_pointer(INVALID_INDEX)
            , offset(0) {}

        uint64_t block_pointer;
        uint32_t offset;
        uint32_t unused_padding{0};

        bool is_valid() const { return block_pointer != INVALID_INDEX; }
        uint32_t block_id() const;
        uint32_t GetBlockIndex() const;
    };

} // namespace components::table::storage