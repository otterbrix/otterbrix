#include "metadata_reader.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace components::table::storage {

    metadata_reader_t::metadata_reader_t(metadata_manager_t& manager, meta_block_pointer_t start)
        : manager_(manager)
        , current_pointer_(start)
        , sub_block_size_(manager.sub_block_size()) {
        if (!start.is_valid()) {
            finished_ = true;
            return;
        }
        current_data_ = manager_.pin(current_pointer_);
        current_offset_ = SUB_BLOCK_HEADER_SIZE;
    }

    void metadata_reader_t::follow_chain() {
        auto* next_ptr = reinterpret_cast<uint64_t*>(current_data_);
        auto* next_off = reinterpret_cast<uint32_t*>(current_data_ + sizeof(uint64_t));

        uint64_t next_bp = *next_ptr;
        uint32_t next_offset = *next_off;

        if (next_bp == INVALID_INDEX) {
            finished_ = true;
            return;
        }

        current_pointer_ = meta_block_pointer_t(next_bp, next_offset);
        current_data_ = manager_.pin(current_pointer_);
        current_offset_ = SUB_BLOCK_HEADER_SIZE;
    }

    void metadata_reader_t::read_data(std::byte* data, uint64_t size) {
        uint64_t read_bytes = 0;
        while (read_bytes < size) {
            if (finished_) {
                throw std::runtime_error("metadata_reader_t: attempted to read past end of chain");
            }

            uint64_t available = sub_block_size_ - current_offset_;
            if (available == 0) {
                follow_chain();
                continue;
            }

            uint64_t to_read = std::min(available, size - read_bytes);
            std::memcpy(data + read_bytes, current_data_ + current_offset_, to_read);
            current_offset_ += to_read;
            read_bytes += to_read;
        }
    }

} // namespace components::table::storage
