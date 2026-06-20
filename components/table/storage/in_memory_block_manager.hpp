#pragma once

#include "block_manager.hpp"
#include "buffer_manager.hpp" // complete buffer_manager_t so read() can use its resource() (no null/default resource)

namespace components::table::storage {

    class in_memory_block_manager_t : public block_manager_t {
    public:
        using block_manager_t::block_manager_t;

        std::unique_ptr<block_t> convert_block(uint64_t, file_buffer_t&) override {
            throw std::logic_error("Cannot perform IO in in-memory database - convert_block!");
        }
        std::unique_ptr<block_t> create_block(uint64_t, file_buffer_t*) override {
            throw std::logic_error("Cannot perform IO in in-memory database - create_block!");
        }
        uint64_t free_block_id() override {
            throw std::logic_error("Cannot perform IO in in-memory database - free_block_id!");
        }
        uint64_t peek_free_block_id() override {
            throw std::logic_error("Cannot perform IO in in-memory database - peek_free_block_id!");
        }
        bool is_root_block(meta_block_pointer_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - is_root_block!");
        }
        void mark_as_free(uint64_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - mark_as_free!");
        }
        void mark_as_used(uint64_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - mark_as_used!");
        }
        void mark_as_modified(uint64_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - mark_as_modified!");
        }
        void increase_block_ref_count(uint64_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - increase_block_ref_count!");
        }
        uint64_t meta_block() override {
            throw std::logic_error("Cannot perform IO in in-memory database - meta_block!");
        }
        // Unreachable for in-memory blocks (block_id >= MAXIMUM_BLOCK are never disk-reloaded; block_handle_t::load
        // returns {} for them without calling read()). Returns io_error rather than throwing, to keep the virtual
        // signature uniform with the disk manager. The message string uses the buffer manager's own resource.
        core::result_wrapper_t<bool> read(block_t&) override {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"in-memory block manager cannot perform disk read",
                                                  buffer_manager.resource()});
        }
        void read_blocks(file_buffer_t&, uint64_t, uint64_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - read_blocks!");
        }
        void write(file_buffer_t&, uint64_t) override {
            throw std::logic_error("Cannot perform IO in in-memory database - write!");
        }
        bool in_memory() override { return true; }
        void file_sync() override { throw std::logic_error("Cannot perform IO in in-memory database - file_sync!"); }
        uint64_t total_blocks() override {
            throw std::logic_error("Cannot perform IO in in-memory database - total_blocks!");
        }
        uint64_t free_blocks() override {
            throw std::logic_error("Cannot perform IO in in-memory database - free_blocks!");
        }
    };

} // namespace components::table::storage