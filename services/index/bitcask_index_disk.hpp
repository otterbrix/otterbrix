#pragma once

#include "bitcask_task_executor.hpp"
#include "disk_hash_table.hpp"
#include "index_disk.hpp"

#include <components/types/logical_value.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <memory_resource>
#include <shared_mutex>
#include <vector>

namespace services::index {

    class bitcask_index_disk_t final : public index_disk_t {
    public:
        static constexpr uint64_t default_flush_threshold_{1000};
        static constexpr uint64_t default_segment_record_limit_{10000};

        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold = default_flush_threshold_,
                             uint64_t segment_record_limit = default_segment_record_limit_);
        ~bitcask_index_disk_t() override;

        // Factory returning the instance, or a core::error_t when on-disk
        // recovery fails (e.g. segment CRC mismatch). Production code MUST use
        // this: the direct ctor below loads from disk and aborts on corruption.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<bitcask_index_disk_t>>
        create(const path_t& path,
               std::pmr::memory_resource* resource,
               uint64_t flush_threshold = default_flush_threshold_,
               uint64_t segment_record_limit = default_segment_record_limit_);

        using entry_t = std::pair<value_t, size_t>;
        using entries_t = std::pmr::vector<entry_t>;

        void insert(const value_t& key, size_t value) override;
        void remove(value_t key) override;
        void remove(const value_t& key, size_t row_id) override;
        void find(const value_t& value, result& res) const override;
        result find(const value_t& value) const override;
        void lower_bound(const value_t& value, result& res) const override;
        result lower_bound(const value_t& value) const override;
        void upper_bound(const value_t& value, result& res) const override;
        result upper_bound(const value_t& value) const override;

        void drop() override;
        void force_flush() override;
        void load_entries(entries_t& entries) const;
        void enqueue_task(std::function<void()> task);
        void set_bulk_mode(bool enabled);
        void apply_txn_inserts(uint64_t txn_id, const std::vector<std::pair<value_t, size_t>>& values);
        void apply_txn_deletes(uint64_t txn_id, const std::vector<std::pair<value_t, size_t>>& values);

    private:
        enum class record_kind_t : uint8_t
        {
            value = 1,
            tombstone = 2
        };

        struct skip_load_tag {};

        // Skip-load ctor used by create() — performs no disk I/O so the
        // factory can stage load_from_disk() and check crc_failure_ before
        // running the rest of the recovery pipeline.
        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             skip_load_tag);

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;

        void initialize_storage();
        void load_from_disk();
        std::vector<segment_info_t> collect_segments() const;
        void open_active_segment();
        void rotate_active_segment();
        void rotate_active_segment_if_needed();
        uint64_t allocate_next_segment_id();
        void merge_immutable_segments();
        static uint32_t segment_id_from_path(const std::filesystem::path& path);
        row_ids_t current_rows(const value_t& key) const;
        bool read_rows_at(uint32_t segment_id, uint64_t value_offset, row_ids_t& rows, value_t* out_key = nullptr) const;
        std::string key_bytes_for_hash(const value_t& key) const;
        bool load_full_key_for_hash_ref(uint32_t log_file_id, uint64_t log_offset, std::string& out_key) const;
        void erase_all_refs_for_key(std::string_view key_bytes);
        void append_snapshot(const value_t& key, const row_ids_t& rows);
        void append_tombstone(const value_t& key);
        void append_txn_record_unlocked(uint64_t txn_id,
                                        uint8_t op_kind,
                                        const std::vector<std::pair<value_t, size_t>>& values);
        void recover_txn_log_unlocked();
        std::filesystem::path txn_log_file_path() const;
        std::filesystem::path txn_applied_file_path() const;
        uint64_t read_applied_log_offset() const;
        void write_applied_log_offset(uint64_t offset) const;
        void flush_if_needed();
        void force_flush_unlocked();

        std::filesystem::path path_;
        std::filesystem::path hash_index_file_path_;
        std::filesystem::path active_data_file_path_;
        std::pmr::memory_resource* resource_;
        mutable core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        std::unique_ptr<core::filesystem::file_handle_t> txn_log_file_;
        std::unique_ptr<disk_hash_table_t> hash_index_;
        uint64_t next_timestamp_{0};
        std::atomic<uint64_t> next_segment_id_{1};
        uint64_t active_segment_id_{0};
        uint64_t active_segment_records_{0};
        uint64_t segment_record_limit_{default_segment_record_limit_};
        bool bulk_mode_{false};
        mutable std::shared_mutex mutex_;
        std::unique_ptr<bitcask_task_executor_t> task_executor_;
        // Set by load_from_disk when a segment's CRC check fails. The
        // factory checks this flag to convert the failure into a
        // core::error_t; the direct ctor asserts.
        bool crc_failure_{false};
    };

} // namespace services::index
