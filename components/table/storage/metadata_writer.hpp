#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

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

        void flush();

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
