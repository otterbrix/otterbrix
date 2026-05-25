#pragma once

#include <components/index/disk_hash_storage.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
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

        explicit disk_hash_table_t(const std::filesystem::path& file_path,
                                   uint32_t bucket_count = default_bucket_count);
        ~disk_hash_table_t();

        bool put(std::string_view key,
                 int64_t value,
                 uint32_t log_file_id,
                 uint64_t log_offset,
                 const full_key_loader_t& key_loader = {}) override;
        std::optional<value_ref_t> get(std::string_view key, const full_key_loader_t& key_loader = {}) const override;
        std::vector<value_ref_t> get_all(std::string_view key, const full_key_loader_t& key_loader = {}) const override;
        bool erase(std::string_view key, const full_key_loader_t& key_loader = {}) override;
        bool erase(std::string_view key, int64_t value, const full_key_loader_t& key_loader = {}) override;
        void for_each(const std::function<void(const value_ref_t&)>& cb) const;
        void sync() override;
        void append_pending_insert(uint64_t txn_id, std::string_view key, int64_t row_id) override;
        void append_pending_delete(uint64_t txn_id, std::string_view key, int64_t row_id) override;
        void finalize_txn(uint64_t txn_id, bool apply_inserts, bool apply_deletes) override;
        uint64_t checkpoint_txn_id() const override;

    private:
        struct pending_record_t {
            char op{'I'}; // I=insert, D=delete
            uint64_t txn_id{0};
            int64_t row_id{0};
            std::string key;
        };

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
        };

        void open_or_create();
        void initialize_new_file();
        void load_existing_file();

        uint64_t data_page_count() const;
        uint64_t page_offset(uint64_t page_id) const;
        uint64_t bucket_primary_page_id(uint32_t bucket_id) const;

        static uint32_t hash_key(std::string_view key);
        uint32_t bucket_id_for_key(std::string_view key) const;

        void read_page(uint64_t page_id, std::vector<uint8_t>& page) const;
        void write_page(uint64_t page_id, const std::vector<uint8_t>& page);
        void init_empty_page(std::vector<uint8_t>& page) const;

        uint16_t page_count(const std::vector<uint8_t>& page) const;
        uint16_t page_free_offset(const std::vector<uint8_t>& page) const;
        uint64_t page_overflow(const std::vector<uint8_t>& page) const;
        void set_page_count(std::vector<uint8_t>& page, uint16_t v) const;
        void set_page_free_offset(std::vector<uint8_t>& page, uint16_t v) const;
        void set_page_overflow(std::vector<uint8_t>& page, uint64_t v) const;

        slot_t read_slot(const std::vector<uint8_t>& page, uint16_t slot_index) const;
        void write_slot(std::vector<uint8_t>& page, uint16_t slot_index, const slot_t& slot) const;
        uint16_t slot_dir_offset(uint16_t slot_index) const;

        decoded_entry_t decode_entry(const std::vector<uint8_t>& page, const slot_t& slot) const;
        bool keys_equal(std::string_view query_key,
                        const decoded_entry_t& entry,
                        const full_key_loader_t& key_loader) const;

        bool try_update_in_page(std::vector<uint8_t>& page,
                                std::string_view key,
                                uint32_t key_hash,
                                int64_t value,
                                uint32_t log_file_id,
                                uint64_t log_offset,
                                const full_key_loader_t& key_loader,
                                bool& changed);
        bool try_insert_in_page(std::vector<uint8_t>& page,
                                std::string_view key,
                                uint32_t key_hash,
                                int64_t value,
                                uint32_t log_file_id,
                                uint64_t log_offset,
                                bool& changed);
        bool try_erase_in_page(std::vector<uint8_t>& page,
                               std::string_view key,
                               uint32_t key_hash,
                               std::optional<int64_t> expected_value,
                               const full_key_loader_t& key_loader,
                               bool& erased);

        std::vector<uint8_t> make_entry_payload(std::string_view key,
                                                int64_t value,
                                                uint32_t log_file_id,
                                                uint64_t log_offset) const;
        uint64_t allocate_overflow_page();
        void persist_header();
        std::filesystem::path pending_log_path() const;
        std::filesystem::path checkpoint_file_path() const;
        void initialize_pending_files_if_needed() const;
        void append_pending_record(char op, uint64_t txn_id, std::string_view key, int64_t row_id);
        std::vector<pending_record_t> read_pending_records() const;
        void write_pending_records(const std::vector<pending_record_t>& records);
        void write_checkpoint(uint64_t finalized_txn_id);
        bool erase(std::string_view key, std::optional<int64_t> expected_value, const full_key_loader_t& key_loader);

        std::filesystem::path file_path_;
        mutable std::shared_mutex mutex_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        header_t header_{};
    };

} // namespace services::index
