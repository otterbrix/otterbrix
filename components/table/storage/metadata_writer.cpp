#include "metadata_writer.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace components::table::storage {

    metadata_writer_t::metadata_writer_t(metadata_manager_t& manager)
        : manager_(manager)
        , sub_block_size_(manager.sub_block_size()) {
        start_pointer_ = manager_.allocate_handle();
        current_pointer_ = start_pointer_;
        current_data_ = manager_.pin(current_pointer_);

        // initialize sub-block header: next_ptr = INVALID, next_offset = 0
        auto* next_ptr = reinterpret_cast<uint64_t*>(current_data_);
        *next_ptr = INVALID_INDEX;
        auto* next_off = reinterpret_cast<uint32_t*>(current_data_ + sizeof(uint64_t));
        *next_off = 0;

        current_offset_ = SUB_BLOCK_HEADER_SIZE;
    }

    void metadata_writer_t::ensure_space(uint64_t needed) {
        if (current_offset_ + needed <= sub_block_size_) {
            return;
        }

        // allocate new sub-block and chain
        auto new_pointer = manager_.allocate_handle();
        auto* new_data = manager_.pin(new_pointer);

        // initialize new sub-block header
        auto* new_next_ptr = reinterpret_cast<uint64_t*>(new_data);
        *new_next_ptr = INVALID_INDEX;
        auto* new_next_off = reinterpret_cast<uint32_t*>(new_data + sizeof(uint64_t));
        *new_next_off = 0;

        // update current sub-block header to point to new one
        auto* curr_next_ptr = reinterpret_cast<uint64_t*>(current_data_);
        *curr_next_ptr = new_pointer.block_pointer;
        auto* curr_next_off = reinterpret_cast<uint32_t*>(current_data_ + sizeof(uint64_t));
        *curr_next_off = new_pointer.offset;

        current_pointer_ = new_pointer;
        current_data_ = new_data;
        current_offset_ = SUB_BLOCK_HEADER_SIZE;
    }

    void metadata_writer_t::write_data(const std::byte* data, uint64_t size) {
        uint64_t written = 0;
        while (written < size) {
            ensure_space(1);
            uint64_t available = sub_block_size_ - current_offset_;
            uint64_t to_write = std::min(available, size - written);
            std::memcpy(current_data_ + current_offset_, data + written, to_write);
            current_offset_ += to_write;
            written += to_write;
        }
    }

    void metadata_writer_t::flush() { manager_.flush(); }

} // namespace components::table::storage
