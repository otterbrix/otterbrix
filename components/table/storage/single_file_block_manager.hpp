#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>

#include "block_manager.hpp"
#include "buffer_manager.hpp"

namespace core::filesystem {
    class local_file_system_t;
    struct file_handle_t;
} // namespace core::filesystem

namespace components::table::storage {

    static constexpr uint64_t BLOCK_START = 3 * SECTOR_SIZE; // 12288

    struct main_header_t {
        static constexpr uint32_t MAGIC_NUMBER = 0x5842544F; // "OTBX" little-endian
        static constexpr uint32_t CURRENT_VERSION = 1;

        uint32_t magic;
        uint32_t version;
        uint64_t flags;
        uint8_t padding[SECTOR_SIZE - sizeof(uint32_t) - sizeof(uint32_t) - sizeof(uint64_t)];

        void initialize() {
            std::memset(this, 0, sizeof(*this));
            magic = MAGIC_NUMBER;
            version = CURRENT_VERSION;
            flags = 0;
        }

        bool validate() const { return magic == MAGIC_NUMBER && version <= CURRENT_VERSION; }
    };
    static_assert(sizeof(main_header_t) == SECTOR_SIZE, "main_header_t must be SECTOR_SIZE");

    struct database_header_t {
        uint64_t iteration;
        uint64_t meta_block;
        uint64_t free_list;
        uint64_t block_count;
        uint64_t block_alloc_size;
        uint64_t checksum;
        uint8_t padding[SECTOR_SIZE - 6 * sizeof(uint64_t)];

        void initialize() {
            std::memset(this, 0, sizeof(*this));
            iteration = 0;
            meta_block = INVALID_INDEX;
            free_list = INVALID_INDEX;
            block_count = 0;
            block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE;
            checksum = 0;
        }
    };
    static_assert(sizeof(database_header_t) == SECTOR_SIZE, "database_header_t must be SECTOR_SIZE");

    class single_file_block_manager_t : public block_manager_t {
    public:
        single_file_block_manager_t(buffer_manager_t& buffer_manager,
                                    core::filesystem::local_file_system_t& fs,
                                    const std::string& path,
                                    uint64_t block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE);
        ~single_file_block_manager_t() override;

        void create_new_database();
        void load_existing_database();

        std::unique_ptr<block_t> convert_block(uint64_t block_id, file_buffer_t& source_buffer) override;
        std::unique_ptr<block_t> create_block(uint64_t block_id, file_buffer_t* source_buffer) override;

        uint64_t free_block_id() override;
        uint64_t peek_free_block_id() override;
        bool is_root_block(meta_block_pointer_t root) override;
        void mark_as_free(uint64_t block_id) override;
        void mark_as_used(uint64_t block_id) override;
        void mark_as_modified(uint64_t block_id) override;
        void increase_block_ref_count(uint64_t block_id) override;
        uint64_t meta_block() override;
        void set_meta_block(uint64_t block) { meta_block_ = block; }
        void read(block_t& block) override;
        void read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t block_count) override;
        void write(file_buffer_t& block, uint64_t block_id) override;

        uint64_t total_blocks() override;
        uint64_t free_blocks() override;
        bool in_memory() override { return false; }
        void file_sync() override;
        void truncate() override;

        void write_header(const database_header_t& header);

        meta_block_pointer_t serialize_free_list();
        void deserialize_free_list(meta_block_pointer_t pointer);

        core::filesystem::file_handle_t& handle() const { return *handle_; }

    private:
        uint64_t block_location(uint64_t block_id) const;
        void checksum_and_write(file_buffer_t& buffer, uint64_t block_id);
        bool verify_checksum(file_buffer_t& buffer);

        core::filesystem::local_file_system_t& fs_;
        std::string path_;
        std::unique_ptr<core::filesystem::file_handle_t> handle_;

        std::mutex allocation_lock_;
        std::set<uint64_t> free_list_;
        std::set<uint64_t> used_blocks_;
        std::set<uint64_t> modified_blocks_;
        uint64_t max_block_{0};
        uint64_t iteration_{0};
        uint64_t meta_block_{INVALID_INDEX};
    };

} // namespace components::table::storage
