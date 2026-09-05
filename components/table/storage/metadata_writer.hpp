#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

#include <core/result_wrapper.hpp>

#include "metadata_manager.hpp"

namespace components::table::storage {

    class metadata_writer_t {
    public:
        explicit metadata_writer_t(metadata_manager_t& manager);

        void write_data(const std::byte* data, uint64_t size);

        template<typename T>
        requires std::is_trivially_copyable_v<T> void write(const T& value) {
            write_data(reinterpret_cast<const std::byte*>(&value), sizeof(T));
        }

        void write_string(const std::string& str) {
            write<uint32_t>(static_cast<uint32_t>(str.size()));
            if (!str.empty()) {
                write_data(reinterpret_cast<const std::byte*>(str.data()), str.size());
            }
        }

        meta_block_pointer_t get_block_pointer() const { return start_pointer_; }

        // Returns io_error when the underlying metadata block writes failed — the chain this
        // writer just built is then NOT on disk and get_block_pointer() names nothing.
        [[nodiscard]] core::result_wrapper_t<bool> flush();

        // How many sub-blocks a payload of `payload_bytes` occupies in a chain of sub-blocks of
        // `sub_block_size`, counting the 12-byte chain header each carries. Exact, not an estimate:
        // write_data fills each sub-block to the byte and lets a value straddle the boundary. Exists
        // so a caller can pre-allocate a whole chain BEFORE writing — see
        // single_file_block_manager_t::serialize_free_list, where an allocation made mid-write would
        // come out of the very free list being published. Returns 0 for a sub_block_size too small to
        // hold its own header, which no caller may read as "zero sub-blocks needed": it means the
        // block geometry is unusable.
        static constexpr uint64_t sub_blocks_for(uint64_t payload_bytes, uint64_t sub_block_size) {
            if (sub_block_size <= SUB_BLOCK_HEADER_SIZE) {
                return 0;
            }
            const uint64_t usable = sub_block_size - SUB_BLOCK_HEADER_SIZE;
            return (payload_bytes + usable - 1) / usable;
        }

    private:
        void ensure_space(uint64_t needed);

        metadata_manager_t& manager_;
        meta_block_pointer_t start_pointer_;
        meta_block_pointer_t current_pointer_;
        std::byte* current_data_{nullptr};
        uint64_t current_offset_{0};
        uint64_t sub_block_size_{0};

        // header at start of each sub-block: next_block_pointer (8 bytes) + next_offset (4 bytes)
        static constexpr uint64_t SUB_BLOCK_HEADER_SIZE = sizeof(uint64_t) + sizeof(uint32_t);
    };

} // namespace components::table::storage
