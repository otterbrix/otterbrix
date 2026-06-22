#include "metadata_reader.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

#include "buffer_manager.hpp"

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
        // pin returns nullptr + records a sticky error on a disk-read failure instead of throwing. Adopt it
        // as our own corrupt-stream error so every subsequent read is a no-op.
        if (current_data_ == nullptr && manager_.has_error()) {
            error_ = manager_.error();
        }
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
        if (current_data_ == nullptr && manager_.has_error()) {
            error_ = manager_.error();
        }
        current_offset_ = SUB_BLOCK_HEADER_SIZE;
    }

    void metadata_reader_t::read_data(std::byte* data, uint64_t size) {
        // Once the stream is known corrupt, every further read is a no-op (data was zero-initialized by the
        // caller / read<T>()), so the deserialize chain unwinds safely until the load boundary checks
        // has_error().
        if (has_error()) {
            return;
        }
        uint64_t read_bytes = 0;
        while (read_bytes < size) {
            if (finished_) {
                // Read past end of chain: corrupt metadata. Record a sticky data_corruption error and stop.
                error_ = core::error_t(core::error_code_t::data_corruption,
                                       std::pmr::string{"metadata_reader_t: attempted to read past end of chain",
                                                        manager_.block_manager().buffer_manager.resource()});
                return;
            }

            uint64_t available = sub_block_size_ - current_offset_;
            if (available == 0) {
                follow_chain();
                // follow_chain may have hit a pin/read failure (recorded a sticky error + left
                // current_data_ null) or reached the end of the chain. Stop before dereferencing null.
                if (has_error()) {
                    return;
                }
                continue;
            }

            uint64_t to_read = std::min(available, size - read_bytes);
            std::memcpy(data + read_bytes, current_data_ + current_offset_, to_read);
            current_offset_ += to_read;
            read_bytes += to_read;
        }
    }

} // namespace components::table::storage
