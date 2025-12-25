#pragma once

#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "file_buffer.hpp"

namespace components::table::storage {
    class block_handle_t;
    class buffer_handle_t;
    class buffer_manager_t;

    class block_manager_t {
    public:
        block_manager_t() = delete;
        block_manager_t(buffer_manager_t& buffer_manager, uint64_t block_alloc_size);
        virtual ~block_manager_t() = default;

        buffer_manager_t& buffer_manager;

        virtual std::unique_ptr<block_t> convert_block(uint64_t block_id, file_buffer_t& source_buffer) = 0;
        virtual std::unique_ptr<block_t> create_block(uint64_t block_id, file_buffer_t* source_buffer) = 0;

        virtual uint64_t free_block_id() = 0;
        virtual uint64_t peek_free_block_id() = 0;
        virtual bool is_root_block(meta_block_pointer_t root) = 0;
        virtual void mark_as_free(uint64_t block_id) = 0;
        virtual void mark_as_used(uint64_t block_id) = 0;
        virtual void mark_as_modified(uint64_t block_id) = 0;
        virtual void increase_block_ref_count(uint64_t block_id) = 0;
        virtual uint64_t meta_block() = 0;
        virtual void read(block_t& block) = 0;
        virtual void read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t block_count) = 0;
        virtual void write(file_buffer_t& block, uint64_t block_id) = 0;
        void write(block_t& block) { write(block, block.id); }

        virtual uint64_t total_blocks() = 0;
        virtual uint64_t free_blocks() = 0;
        virtual bool is_remote() { return false; }
        virtual bool in_memory() = 0;
        virtual void file_sync() = 0;
        virtual void truncate();

        std::shared_ptr<block_handle_t> register_block(uint64_t block_id);

        void unregister_block(block_handle_t& block);
        void unregister_block(uint64_t id);

        uint64_t block_allocation_size() const { return block_alloc_size_; }
        uint64_t block_size() const { return block_alloc_size_ - DEFAULT_BLOCK_HEADER_SIZE; }
        void set_block_allocation_size(uint64_t block_alloc_size) {
            if (block_alloc_size_ == INVALID_INDEX) {
                throw std::runtime_error("the block allocation size must be set once");
            }
            block_alloc_size_ = block_alloc_size;
        }

    private:
        std::mutex blocks_lock_;
        std::unordered_map<uint64_t, std::weak_ptr<block_handle_t>> blocks_;
        uint64_t block_alloc_size_;
    };

} // namespace components::table::storage