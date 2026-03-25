#pragma once

#include "index_disk.hpp"

#include <components/types/logical_value.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory_resource>
#include <shared_mutex>
#include <vector>

namespace services::index {

    class bitcask_index_disk_t final : public index_disk_t {
    public:
        static constexpr uint64_t default_flush_threshold_{1000};
        static constexpr uint64_t default_segment_record_limit_{100};

        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold = default_flush_threshold_,
                             uint64_t segment_record_limit = default_segment_record_limit_);
        ~bitcask_index_disk_t() override;

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

    private:
        enum class record_kind_t : uint8_t
        {
            value = 1,
            tombstone = 2
        };

        struct keydir_entry_t {
            uint64_t segment_id{0};
            uint64_t value_offset{0};
            uint64_t value_size{0};
            uint64_t timestamp{0};
        };

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;
        using ordered_index_t = std::map<value_t, row_ids_t, std::less<>>;

        void initialize_storage();
        void load_from_disk();
        std::vector<segment_info_t> collect_segments() const;
        void open_active_segment();
        void rotate_active_segment();
        void rotate_active_segment_if_needed();
        void merge_immutable_segments();
        row_ids_t clone_rows(const row_ids_t& rows) const;
        row_ids_t current_rows(const value_t& key) const;
        void append_snapshot(const value_t& key, const row_ids_t& rows);
        void append_tombstone(const value_t& key);
        void upsert_state(const value_t& key, const row_ids_t& rows, const keydir_entry_t& entry);
        void erase_state(const value_t& key);
        void flush_if_needed();
        void force_flush_unlocked();

        std::filesystem::path path_;
        std::filesystem::path active_data_file_path_;
        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        ordered_index_t index_;
        std::map<value_t, keydir_entry_t, std::less<>> keydir_;
        uint64_t next_timestamp_{0};
        uint64_t active_segment_id_{0};
        uint64_t active_segment_records_{0};
        uint64_t segment_record_limit_{default_segment_record_limit_};
        mutable std::shared_mutex mutex_;
    };

} // namespace services::index
