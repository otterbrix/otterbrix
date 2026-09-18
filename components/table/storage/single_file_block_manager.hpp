#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

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
        // PRE-RELEASE: the layout changes in place and the version stays 0 — wipe stale test/dev dirs.
        static constexpr uint32_t CURRENT_VERSION = 0;

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

        // EXACT match, not <=: an older file is unreadable rather than degraded; no compatibility path.
        bool validate() const { return magic == MAGIC_NUMBER && version == CURRENT_VERSION; }
        bool magic_ok() const { return magic == MAGIC_NUMBER; }
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

        // Does NOT catch a TORN write (reassembles within one hardware sector); the two-slot layout survives that.
        [[nodiscard]] uint64_t compute_checksum() const;
        [[nodiscard]] bool checksum_ok() const { return checksum == compute_checksum(); }
    };
    static_assert(sizeof(database_header_t) == SECTOR_SIZE, "database_header_t must be SECTOR_SIZE");

    class single_file_block_manager_t : public block_manager_t {
    public:
        single_file_block_manager_t(buffer_manager_t& buffer_manager,
                                    core::filesystem::local_file_system_t& fs,
                                    const std::string& path,
                                    uint64_t block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE);
        ~single_file_block_manager_t() override;

        // A MANAGER WHOSE LOAD WAS REFUSED IS NOT FIT FOR USE — destroy it.
        [[nodiscard]] core::result_wrapper_t<bool> create_new_database();
        [[nodiscard]] core::result_wrapper_t<bool> load_existing_database();

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
        [[nodiscard]] core::result_wrapper_t<bool> read(block_t& block) override;
        [[nodiscard]] core::result_wrapper_t<bool>
        read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t block_count) override;
        [[nodiscard]] core::result_wrapper_t<bool> write(file_buffer_t& block, uint64_t block_id) override;

        void adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& block_ids) override;
        [[nodiscard]] core::result_wrapper_t<uint64_t>
        reclaim_superseded_root(const std::pmr::vector<uint64_t>& new_root_data_blocks) override;

        // PRECONDITION: the round's header must NOT have become the durable root, or giving its
        // blocks back is exactly the corruption shadow paging prevents.
        uint64_t roll_back_uncommitted_round();

        bool degraded() const override {
            return durability_error_.contains_error() || allocation_error_.contains_error();
        }

        uint64_t total_blocks() override;
        uint64_t free_blocks() override;
        [[nodiscard]] core::result_wrapper_t<bool> file_sync() override;
        [[nodiscard]] core::result_wrapper_t<bool> truncate() override;

        // The single point of checkpoint durability; a caller must not ignore a failure here.
        [[nodiscard]] core::result_wrapper_t<bool> write_header(const database_header_t& header);

        // Persists reusable_ ∪ pending_free_ as a real block write, so it can fail like any other.
        [[nodiscard]] core::result_wrapper_t<meta_block_pointer_t> serialize_free_list();
        // Installs the DURABLE root's list all-or-nothing; requires max_block_ already set.
        [[nodiscard]] core::result_wrapper_t<bool> deserialize_free_list(meta_block_pointer_t pointer);

        // free_block_id() can't return an error, so a corrupt free list latches here instead (sticky).
        [[nodiscard]] bool has_allocation_error() const { return allocation_error_.contains_error(); }
        [[nodiscard]] const core::error_t& allocation_error() const { return allocation_error_; }

        // Every block write/fsync failure latches here (sticky); write_header() then refuses to commit.
        [[nodiscard]] bool has_durability_error() const { return durability_error_.contains_error(); }
        [[nodiscard]] const core::error_t& durability_error() const { return durability_error_; }

        core::filesystem::file_handle_t& handle() const { return *handle_; }

#ifdef DEV_MODE
        // Fault-injection seam: wraps the freshly opened database file handle. Process-wide, read once per open.
        struct file_handle_interposer_t {
            virtual ~file_handle_interposer_t() = default;
            virtual std::unique_ptr<core::filesystem::file_handle_t>
            wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) = 0;
        };
        static void dev_set_file_interposer(file_handle_interposer_t* interposer); // nullptr = off

        // Block-reachability walker: an id in none of durable-root-reachable/registry-live/free-listed is a hole.
        const std::pmr::vector<uint64_t>& dev_issued_ids() const { return dev_issued_; }
        const std::pmr::vector<uint64_t>& dev_freed_ids() const { return dev_freed_; }
        std::set<uint64_t> dev_free_list_snapshot() {
            std::set<uint64_t> all = reusable_;
            all.insert(pending_free_.begin(), pending_free_.end());
            return all;
        }
        std::set<uint64_t> dev_reusable_snapshot() { return reusable_; }
        std::set<uint64_t> dev_pending_free_snapshot() { return pending_free_; }
        std::set<uint64_t> dev_durable_root_data_snapshot() { return durable_root_data_; }
        void dev_reset_tracking() {
            dev_issued_.clear();
            dev_freed_.clear();
        }
#endif

    private:
        uint64_t block_location(uint64_t block_id) const;
        [[nodiscard]] core::result_wrapper_t<bool> checksum_and_write(file_buffer_t& buffer, uint64_t block_id);
        bool verify_checksum(file_buffer_t& buffer);
        void latch_allocation_error(uint64_t block_id, const std::string& reason);
        core::error_t latch_reclaim_failure(const core::error_t& cause, const char* which_chain);
        core::error_t latch_durability_error(core::error_t error);
        // Reads the header slots back and lets the disk, not the return code, decide the durable root.
        [[nodiscard]] core::result_wrapper_t<bool>
        reconcile_failed_header_write(uint64_t next_iteration, bool write_ok, bool sync_ok);
        // pending_free_ merges into reusable_ only once the new root is proven durable.
        void promote_durable_root(uint64_t meta_block, uint64_t free_list);

        core::filesystem::local_file_system_t& fs_;
        std::string path_;
        std::unique_ptr<core::filesystem::file_handle_t> handle_;

        // NO LOCK: one table_storage_t owns one block manager on exactly one disk agent thread.
        // Shadow paging: reusable_ is free under the CURRENT durable root (the only pool
        // free_block_id draws from); pending_free_ is released by the in-flight checkpoint but
        // still named by that root, so issuing one would overwrite a block it still reads. They
        // merge in promote_durable_root(), once write_header and its fsync both succeed.
        std::set<uint64_t> reusable_;
        std::set<uint64_t> pending_free_;
        std::set<uint64_t> used_blocks_;
        std::set<uint64_t> modified_blocks_;
        uint64_t max_block_{0};
        uint64_t iteration_{0};
        uint64_t meta_block_{INVALID_INDEX};

        // Root N's own chains, remembered separately from meta_block_ (which flips to root N+1 immediately).
        uint64_t durable_meta_block_{INVALID_INDEX};
        uint64_t durable_free_list_{INVALID_INDEX};
        std::set<uint64_t> durable_root_data_;
        std::set<uint64_t> pending_root_data_;
        // Every id issued since the durable root committed; root N can never own one of these.
        std::set<uint64_t> issued_since_root_;
        // Set when reconcile_failed_header_write can't say which root landed.
        bool durable_root_indeterminate_{false};
        core::error_t allocation_error_{core::error_t::no_error()};
        core::error_t durability_error_{core::error_t::no_error()};

#ifdef DEV_MODE
        std::pmr::vector<uint64_t> dev_issued_;
        std::pmr::vector<uint64_t> dev_freed_;
#endif
    };

} // namespace components::table::storage
