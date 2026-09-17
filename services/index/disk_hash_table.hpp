#pragma once

#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <concepts>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace services::index {

    // An entry whose key exceeds inline_key_limit stores only a truncated prefix plus
    // (log_file_id, log_offset); resolving it means reading that record back through the store
    // that wrote it. Passed as a template parameter, not a virtual hook: one
    // implementation, one caller, both known at compile time.
    template<typename loader_t>
    concept hash_key_loader = requires(const loader_t& load_full_key, uint32_t log_file_id, uint64_t log_offset) {
        // std::pmr::string, not std::string: the key comes back from a store with its own resource.
        { load_full_key(log_file_id, log_offset) }
        ->std::same_as<core::result_wrapper_t<std::pmr::string>>;
    };

    class disk_hash_table_t final {
    public:
        static constexpr uint32_t page_size = 4096;
        static constexpr uint32_t default_bucket_count = 1024;
        static constexpr uint16_t inline_key_limit = 64;
        static constexpr uint16_t truncated_prefix_len = 32;
        using byte_buffer_t = std::pmr::vector<uint8_t>;

        struct value_ref_t {
            int64_t value{0};
            uint32_t log_file_id{0};
            uint64_t log_offset{0};
            bool key_truncated{false};
        };

        // Production code must use this, not the direct ctor below, which aborts on the same
        // open failures instead of returning them.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<disk_hash_table_t>>
        create(const std::filesystem::path& file_path,
               uint32_t bucket_count,
               std::pmr::memory_resource* memory_resource);

        // No defaulted arguments: a null memory_resource would only be caught by an
        // assert, which NDEBUG compiles out.
        disk_hash_table_t(const std::filesystem::path& file_path,
                          uint32_t bucket_count,
                          std::pmr::memory_resource* memory_resource);
        ~disk_hash_table_t();

        // Failure means the entry could not be placed, or an auto-rehash it tripped could not
        // finish; the table stays consistent either way (see split_one_bucket_unlocked).
        [[nodiscard]] core::error_t put(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset);

        // A page chain walk that cannot finish refuses rather than returning a partial row set —
        // a silent `break` would make "three rows" indistinguishable from a read failure mid-count.
        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<std::vector<value_ref_t>> get_all(std::string_view key,
                                                                               const loader_t& load_full_key) const {
            const uint32_t key_hash = hash_key(key);
            uint64_t page_id = bucket_primary_page_id(bucket_id_for_hash(key_hash));
            std::pmr::vector<value_ref_t> values(memory_resource_);

            byte_buffer_t page(memory_resource_);
            page.resize(page_size);
            while (page_id != 0) {
                if (!read_page(page_id, page)) {
                    return page_read_failure(page_id);
                }
                const auto cnt = page_count(page);
                for (uint16_t i = 0; i < cnt; ++i) {
                    auto slot = read_slot(page, i);
                    if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                        continue;
                    }
                    const auto entry = decode_entry(page, slot);
                    if (!entry.valid) {
                        continue; // corrupt slot: skip it rather than read past the page
                    }
                    VALUE_OR_RETURN(const bool matched, keys_equal(key, entry, load_full_key));
                    if (!matched) {
                        continue;
                    }
                    values.push_back(value_ref_t{entry.value,
                                                 entry.log_file_id,
                                                 entry.log_offset,
                                                 (entry.entry_flags & entry_flag_truncated) != 0});
                }
                page_id = page_overflow(page);
            }
            return std::vector<value_ref_t>(values.begin(), values.end());
        }

        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<std::optional<value_ref_t>> get(std::string_view key,
                                                                             const loader_t& load_full_key) const {
            VALUE_OR_RETURN(auto all, get_all(key, load_full_key));
            if (all.empty()) {
                return std::optional<value_ref_t>{};
            }
            return std::optional<value_ref_t>{all.front()};
        }

        // error_t here is distinct from "not found" (false): folding the two would tell
        // erase_all_refs_for_key's loop it was done when the walk had actually failed.
        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool> erase(std::string_view key, const loader_t& load_full_key) {
            return erase_matching(key, std::nullopt, load_full_key);
        }

        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool>
        erase(std::string_view key, int64_t value, const loader_t& load_full_key) {
            return erase_matching(key, std::optional<int64_t>(value), load_full_key);
        }

        // Template parameter, not std::function: avoids heap allocation for the
        // capturing lambdas both callers pass. Order (buckets ascending, primary page before
        // overflow) is part of the contract -- load_entries and merge_immutable_segments rely on it.
        template<typename callback_t>
        [[nodiscard]] core::error_t for_each(callback_t&& cb) const {
            byte_buffer_t page(memory_resource_);
            page.resize(page_size);
            for (uint32_t bucket = 0; bucket < header_.bucket_count_value; ++bucket) {
                uint64_t page_id = bucket_primary_page_id(bucket);
                while (page_id != 0) {
                    if (!read_page(page_id, page)) {
                        return page_read_failure(page_id);
                    }
                    const auto cnt = page_count(page);
                    for (uint16_t i = 0; i < cnt; ++i) {
                        const auto slot = read_slot(page, i);
                        if (slot.flags != slot_flag_used || slot.length == 0) {
                            continue;
                        }
                        if (!slot_belongs_to_bucket_unlocked(slot.key_hash, bucket)) {
                            continue;
                        }
                        const auto entry = decode_entry(page, slot);
                        if (!entry.valid) {
                            continue;
                        }
                        cb(value_ref_t{entry.value,
                                       entry.log_file_id,
                                       entry.log_offset,
                                       (entry.entry_flags & entry_flag_truncated) != 0});
                    }
                    page_id = page_overflow(page);
                }
            }
            return core::error_t::no_error();
        }

        // A split that cannot copy every entry it owes the new bucket refuses instead of
        // publishing; on failure the addressing state is unchanged.
        [[nodiscard]] core::error_t rehash(uint32_t new_bucket_count);
        [[nodiscard]] core::error_t trigger_rehash_if_needed();
        bool set_auto_rehash_suppressed(bool suppressed) noexcept;
        uint32_t bucket_count() const;
        double load_factor() const;
        // A refused fsync must reach bitcask_index_disk_t::sync_if_dirty, whose force_flush
        // result gates the checkpoint that trims the WAL.
        [[nodiscard]] core::error_t sync();
        // Re-creates an empty table with the same width and hash seed (object identity is kept so
        // the owning store can reuse it instead of reopening the file). Called from the open path
        // (bitcask_index_disk_t::load_from_disk), so failure is reported rather than aborting --
        // it costs the index its registration, not the whole engine.
        [[nodiscard]] core::error_t reset_storage();
        void close_storage();

    private:
        struct slot_t {
            uint16_t offset{0};
            uint16_t length{0};
            uint8_t flags{0};
            uint32_t key_hash{0};
        };

        struct decoded_entry_t {
            // False on a corrupt page; callers skip rather than throw — throwing here would unwind
            // into an actor coroutine with an empty unhandled_exception(), hanging instead of failing.
            bool valid{false};
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
            uint32_t hash_seed_value{0};
        };

        // Sets the members up and opens nothing; callers run open_or_create() themselves.
        struct defer_open_tag {};
        disk_hash_table_t(const std::filesystem::path& file_path,
                          uint32_t bucket_count,
                          std::pmr::memory_resource* memory_resource,
                          defer_open_tag);

        // Builds an index_create_fail error_t on this table's resource, which is why it's a
        // member rather than a free function.
        [[nodiscard]] core::error_t io_failure(const std::string& message) const;
        [[nodiscard]] core::error_t page_read_failure(uint64_t page_id) const;
        [[nodiscard]] core::error_t page_write_failure(uint64_t page_id) const;
        [[nodiscard]] core::error_t open_or_create();
        // Tail of reset_storage, deliberately not open_or_create: refuses a file that outlived
        // the unlink instead of loading it as an existing table.
        [[nodiscard]] core::error_t open_after_wipe_or_refuse();
        [[nodiscard]] core::error_t initialize_new_file();
        [[nodiscard]] core::error_t load_existing_file();
        [[nodiscard]] core::error_t open_overflow_file();
        // False means at least one of the two backing files did not reach the device.
        [[nodiscard]] bool sync_files();

        static bool is_overflow_page_id(uint64_t page_id);
        uint64_t main_page_count() const;
        uint64_t overflow_page_count() const;
        uint64_t bucket_primary_page_id(uint32_t bucket_id) const;

        uint32_t hash_key(std::string_view key) const;

        [[nodiscard]] bool read_page(uint64_t page_id, byte_buffer_t& page) const;
        [[nodiscard]] bool write_page(uint64_t page_id, const byte_buffer_t& page);
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

        // No AB-BA risk: the loader is handed in by the owning store, which takes its own lock
        // first and does not take it again inside the loader.
        // error_t is distinct from "false" here too, for the reason noted on erase() above.
        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool>
        keys_equal(std::string_view query_key, const decoded_entry_t& entry, const loader_t& load_full_key) const {
            if ((entry.entry_flags & entry_flag_truncated) == 0) {
                return query_key.size() == entry.full_key_len && query_key == entry.stored_key;
            }
            if (query_key.size() < entry.stored_key.size() ||
                query_key.substr(0, entry.stored_key.size()) != entry.stored_key) {
                return false;
            }
            VALUE_OR_RETURN(const auto full, load_full_key(entry.log_file_id, entry.log_offset));
            return full == query_key;
        }

        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool>
        erase_matching(std::string_view key, std::optional<int64_t> expected_value, const loader_t& load_full_key) {
            const uint32_t key_hash = hash_key(key);
            uint64_t page_id = bucket_primary_page_id(bucket_id_for_hash(key_hash));
            byte_buffer_t page(memory_resource_);
            page.resize(page_size);
            while (page_id != 0) {
                if (!read_page(page_id, page)) {
                    return page_read_failure(page_id);
                }
                VALUE_OR_RETURN(const bool erased,
                                try_erase_in_page(page, key, key_hash, expected_value, load_full_key));
                if (erased) {
                    if (!write_page(page_id, page)) {
                        return page_write_failure(page_id);
                    }
                    if (entry_count_ > 0) {
                        --entry_count_;
                    }
                    return true;
                }
                page_id = page_overflow(page);
            }
            return false;
        }

        // Three-way result, not a `bool& erased` out-param: only two of its four combinations
        // would be reachable. A refusal only guarantees this page's buffer is untouched; a partial
        // removal already committed by erase_all_refs_for_key's earlier passes is fixed by the
        // next open's keydir rebuild, not rolled back here.
        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool> try_erase_in_page(byte_buffer_t& page,
                                                                     std::string_view key,
                                                                     uint32_t key_hash,
                                                                     std::optional<int64_t> expected_value,
                                                                     const loader_t& load_full_key) {
            const auto cnt = page_count(page);
            for (uint16_t i = 0; i < cnt; ++i) {
                auto slot = read_slot(page, i);
                if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                    continue;
                }
                const auto entry = decode_entry(page, slot);
                if (!entry.valid) {
                    continue;
                }
                VALUE_OR_RETURN(const bool matched, keys_equal(key, entry, load_full_key));
                if (!matched) {
                    continue;
                }
                if (expected_value.has_value() && entry.value != *expected_value) {
                    continue;
                }
                slot.flags = slot_flag_free;
                write_slot(page, i, slot);
                return true;
            }
            return false;
        }

        bool
        try_insert_payload_in_page(byte_buffer_t& page, uint32_t key_hash, const byte_buffer_t& payload, bool& changed);
        [[nodiscard]] core::error_t
        put_unlocked(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset);
        [[nodiscard]] core::error_t
        insert_payload_into_bucket_unlocked(uint32_t bucket_id, uint32_t key_hash, const byte_buffer_t& payload);
        // Refuses rather than returning a partial count, which open_or_create would otherwise
        // publish as entry_count_ -- a load factor quietly understating the file.
        [[nodiscard]] core::result_wrapper_t<uint64_t> count_entries_unlocked() const;
        [[nodiscard]] core::error_t rehash_unlocked(uint32_t new_bucket_count);
        [[nodiscard]] core::error_t maybe_rehash_if_needed_unlocked();
        [[nodiscard]] core::error_t split_one_bucket_unlocked(bool durable_commit = true);
        bool slot_belongs_to_bucket_unlocked(uint32_t key_hash, uint32_t bucket_id) const;
        // Refuses a zero bucket count by value, not assert: an assert compiles out under NDEBUG,
        // and the arithmetic behind it would compute split_bucket = 0 - 1 = UINT32_MAX.
        [[nodiscard]] core::error_t initialize_linear_state_from_bucket_count();
        uint32_t bucket_id_for_hash(uint32_t key_hash) const;

        byte_buffer_t
        make_entry_payload(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) const;
        uint64_t allocate_overflow_page();
        [[nodiscard]] bool persist_header();

        std::filesystem::path file_path_;
        std::filesystem::path overflow_file_path_;
        // No mutex: single owner (bitcask_index_disk_t) behind a single actor mailbox already
        // serializes every call here; a lock would be a redundant third layer.
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

} // namespace services::index
