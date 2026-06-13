#pragma once

#include <components/index/disk_hash_storage.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace services::index {

    class disk_hash_table_t final : public components::index::disk_hash_storage_t {
    public:
        static constexpr uint32_t page_size = 4096;
        static constexpr uint32_t default_bucket_count = 1024;
        static constexpr uint16_t inline_key_limit = 64;
        static constexpr uint16_t truncated_prefix_len = 32;
        using value_ref_t = components::index::disk_hash_storage_t::value_ref_t;
        using full_key_loader_t = components::index::disk_hash_storage_t::full_key_loader_t;
        using byte_buffer_t = std::pmr::vector<uint8_t>;

        explicit disk_hash_table_t(const std::filesystem::path& file_path,
                                   uint32_t bucket_count = default_bucket_count,
                                   std::pmr::memory_resource* memory_resource = nullptr);
        ~disk_hash_table_t();

        bool put(std::string_view key,
                 int64_t value,
                 uint32_t log_file_id,
                 uint64_t log_offset) override;
        std::optional<value_ref_t> get(std::string_view key, bool lock_bitcask = true) const override;
        std::vector<value_ref_t> get_all(std::string_view key) const override;
        bool erase(std::string_view key, bool lock_bitcask = true) override;
        bool erase(std::string_view key, int64_t value, bool lock_bitcask = true) override;
        void set_full_key_loader(full_key_loader_t loader);
        void for_each(const std::function<void(const value_ref_t&)>& cb) const;
        bool rehash(uint32_t new_bucket_count);
        bool trigger_rehash_if_needed();
        bool set_auto_rehash_suppressed(bool suppressed) noexcept;
        uint32_t bucket_count() const;
        double load_factor() const;
        void sync() override;

    private:
        struct slot_t {
            uint16_t offset{0};
            uint16_t length{0};
            uint8_t flags{0};
            uint32_t key_hash{0};
        };

        struct decoded_entry_t {
            uint16_t stored_key_len{0};
            uint32_t full_key_len{0};
            uint8_t entry_flags{0};
            std::string_view stored_key;
            int64_t value{0};
            uint32_t log_file_id{0};
            uint64_t log_offset{0};
        };

        static constexpr uint8_t slot_flag_free = 0;
        static constexpr uint8_t slot_flag_used = 1;
        static constexpr uint8_t entry_flag_truncated = 1U << 0U;

        static constexpr uint16_t page_header_size = 12;
        static constexpr uint16_t slot_size = 9;

        struct header_t {
            uint32_t page_size_value{page_size};
            uint32_t bucket_count_value{default_bucket_count};
            uint64_t next_overflow_page{0};
            uint32_t level_value{0};
            uint32_t split_bucket_value{0};
        };

        void open_or_create();
        void initialize_new_file();
        void load_existing_file();
        void open_overflow_file();
        void sync_files();

        static bool is_overflow_page_id(uint64_t page_id);
        uint64_t main_page_count() const;
        uint64_t overflow_page_count() const;
        uint64_t bucket_primary_page_id(uint32_t bucket_id) const;

        static uint32_t hash_key(std::string_view key);

        void read_page(uint64_t page_id, byte_buffer_t& page) const;
        void write_page(uint64_t page_id, const byte_buffer_t& page);
        void init_empty_page(byte_buffer_t& page) const;

        uint16_t page_count(const byte_buffer_t& page) const;
        uint16_t page_free_offset(const byte_buffer_t& page) const;
        uint64_t page_overflow(const byte_buffer_t& page) const;
        void set_page_count(byte_buffer_t& page, uint16_t v) const;
        void set_page_free_offset(byte_buffer_t& page, uint16_t v) const;
        void set_page_overflow(byte_buffer_t& page, uint64_t v) const;

        slot_t read_slot(const byte_buffer_t& page, uint16_t slot_index) const;
        void write_slot(byte_buffer_t& page, uint16_t slot_index, const slot_t& slot) const;
        uint16_t slot_dir_offset(uint16_t slot_index) const;

        decoded_entry_t decode_entry(const byte_buffer_t& page, const slot_t& slot) const;
        bool keys_equal(std::string_view query_key, const decoded_entry_t& entry, bool lock_bitcask) const;
        std::vector<value_ref_t> get_all(std::string_view key, bool lock_bitcask) const;
        bool erase(std::string_view key, std::optional<int64_t> expected_value, bool lock_bitcask);

        bool try_insert_payload_in_page(byte_buffer_t& page,
                                        uint32_t key_hash,
                                        const byte_buffer_t& payload,
                                        bool& changed);
        bool try_erase_in_page(byte_buffer_t& page,
                               std::string_view key,
                               uint32_t key_hash,
                               std::optional<int64_t> expected_value,
                               bool lock_bitcask,
                               bool& erased);
        bool put_unlocked(std::string_view key,
                          int64_t value,
                          uint32_t log_file_id,
                          uint64_t log_offset);
        bool insert_payload_into_bucket_unlocked(uint32_t bucket_id, uint32_t key_hash, const byte_buffer_t& payload);
        uint64_t count_entries_unlocked() const;
        bool rehash_unlocked(uint32_t new_bucket_count);
        bool maybe_rehash_if_needed_unlocked();
        bool split_one_bucket_unlocked(bool durable_commit = true);
        bool slot_belongs_to_bucket_unlocked(uint32_t key_hash, uint32_t bucket_id) const;
        void initialize_linear_state_from_bucket_count();
        uint32_t bucket_id_for_hash(uint32_t key_hash) const;

        byte_buffer_t
        make_entry_payload(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) const;
        uint64_t allocate_overflow_page();
        void persist_header();

        std::filesystem::path file_path_;
        full_key_loader_t key_loader_;
        std::filesystem::path overflow_file_path_;
        mutable std::shared_mutex mutex_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        std::unique_ptr<core::filesystem::file_handle_t> ovf_file_;
        mutable header_t header_{};
        uint64_t entry_count_{0};
        bool rehash_in_progress_{false};
        double max_load_factor_{0.75};
        std::atomic<bool> suppress_auto_rehash_{false};
        std::pmr::memory_resource* memory_resource_{nullptr};
    };

    using disk_hash_table_ptr = boost::intrusive_ptr<disk_hash_table_t>;

} // namespace services::index
