#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "metadata_manager.hpp"

namespace components::table::storage {

    class metadata_reader_t {
    public:
        metadata_reader_t(metadata_manager_t& manager, meta_block_pointer_t start);

        void read_data(std::byte* data, uint64_t size);

        template<typename T>
        requires std::is_trivially_copyable_v<T> T read() {
            T value;
            read_data(reinterpret_cast<std::byte*>(&value), sizeof(T));
            return value;
        }

        std::string read_string() {
            auto len = read<uint32_t>();
            if (len == 0) {
                return {};
            }
            std::string result(len, '\0');
            read_data(reinterpret_cast<std::byte*>(result.data()), len);
            return result;
        }

        bool finished() const { return finished_; }

    private:
        void follow_chain();

        metadata_manager_t& manager_;
        meta_block_pointer_t current_pointer_;
        std::byte* current_data_{nullptr};
        uint64_t current_offset_{0};
        uint64_t sub_block_size_{0};
        bool finished_{false};

        static constexpr uint64_t SUB_BLOCK_HEADER_SIZE = sizeof(uint64_t) + sizeof(uint32_t);
    };

} // namespace components::table::storage
