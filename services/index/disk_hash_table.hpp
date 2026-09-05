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
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace services::index {

    // The one question a hash page cannot answer on its own. An entry whose encoded key
    // was longer than disk_hash_table_t::inline_key_limit stores only a PREFIX
    // (truncated_prefix_len bytes) plus the (log_file_id, log_offset) of the record that
    // carries the whole key, so deciding whether such an entry matches a probe means
    // reading that record back -- and only the store that WROTE the record can do that.
    //
    // Removing the hook is not an option, and the cost of removing it is silent:
    // keys_equal() would answer false for EVERY key longer than inline_key_limit, so a
    // hashed index on a long key would return zero rows and report nothing -- on the path
    // C1 made the only read path.
    //
    // A TEMPLATE parameter, not a virtual interface and not a std::function (rule 14).
    // The customization point is NOT virtual, and there is one implementation
    // (bitcask_index_disk_t) and one production caller, both known at compile time, so
    // the callable is simply deduced and the erasure goes -- the same reasoning C0b
    // recorded for for_each below, and the opposite of the case components/index/index.hpp
    // records for pending_inserts/pending_deletes, where the customization point IS
    // virtual and the callable therefore could not be a parameter.
    //
    // The loader travels WITH the call instead of being installed on the table: nothing
    // is stored, so it cannot dangle, there is no unhook to forget in a destructor, and
    // no null state for keys_equal to silently answer false from.
    template<typename loader_t>
    concept hash_key_loader =
        requires(const loader_t& load_full_key, uint32_t log_file_id, uint64_t log_offset, std::string& out_key) {
        // out_key <- the whole encoded key of the record at (log_file_id, log_offset);
        // false when it cannot be read, which makes the entry a non-match rather than a
        // guessed one.
        { load_full_key(log_file_id, log_offset, out_key) } -> std::same_as<bool>;
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

        // Factory returning the instance, or a core::error_t when the on-disk
        // storage cannot be brought up (file/overflow-file open failure, an
        // unreadable or incompatible header). Production code MUST use this: the
        // direct ctor below aborts on the same failures, mirroring
        // bitcask_index_disk_t's deferred-open ctor plus open() vs its
        // construct-and-open ctor.
        //
        // The failure is a VALUE the whole way down -- open_or_create() returns it, this
        // hands it on, and result_wrapper_t makes a caller confront it before it can
        // reach the table. Nothing is recorded in the object and nothing has to be asked
        // for afterwards.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<disk_hash_table_t>>
        create(const std::filesystem::path& file_path,
               uint32_t bucket_count,
               std::pmr::memory_resource* memory_resource);

        // No defaulted arguments, and the resource in particular is never defaulted to
        // null (rule 14): a null resource used to be caught only by an assert, which
        // NDEBUG compiles out. Both parameters are now stated at every call site.
        disk_hash_table_t(const std::filesystem::path& file_path,
                          uint32_t bucket_count,
                          std::pmr::memory_resource* memory_resource);
        ~disk_hash_table_t();

        bool put(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset);

        // THE READS, each carrying the loader that resolves a truncated entry. The
        // parameter is what makes the resolution impossible to forget: a caller that has
        // no way to read a record back cannot call these at all, instead of calling them
        // and quietly missing every long key.
        template<hash_key_loader loader_t>
        std::vector<value_ref_t> get_all(std::string_view key, const loader_t& load_full_key) const {
            std::unique_lock lock(mutex_);
            const uint32_t key_hash = hash_key(key);
            uint64_t page_id = bucket_primary_page_id(bucket_id_for_hash(key_hash));
            std::pmr::vector<value_ref_t> values(memory_resource_);

            byte_buffer_t page(memory_resource_);
            page.resize(page_size);
            while (page_id != 0) {
                if (!read_page(page_id, page)) {
                    break; // unreadable page: stop walking this chain
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
                    if (!keys_equal(key, entry, load_full_key)) {
                        continue;
                    }
                    values.push_back(value_ref_t{entry.value,
                                                 entry.log_file_id,
                                                 entry.log_offset,
                                                 (entry.entry_flags & entry_flag_truncated) != 0});
                }
                page_id = page_overflow(page);
            }
            return {values.begin(), values.end()};
        }

        template<hash_key_loader loader_t>
        std::optional<value_ref_t> get(std::string_view key, const loader_t& load_full_key) const {
            auto all = get_all(key, load_full_key);
            if (all.empty()) {
                return std::nullopt;
            }
            return all.front();
        }

        template<hash_key_loader loader_t>
        bool erase(std::string_view key, const loader_t& load_full_key) {
            return erase_matching(key, std::nullopt, load_full_key);
        }

        template<hash_key_loader loader_t>
        bool erase(std::string_view key, int64_t value, const loader_t& load_full_key) {
            return erase_matching(key, std::optional<int64_t>(value), load_full_key);
        }

        // Rule 14: the callable is a TEMPLATE parameter, not a type-erased `function` wrapper.
        // Nothing here forces erasure -- for_each is not a virtual customization point, so the
        // callable can simply be deduced, and the erased form heap-allocated for the capturing
        // lambdas both callers pass. The body lives in the header so the callable stays a
        // template parameter at every call site.
        //
        // THE ORDER IS PART OF THE CONTRACT, not an implementation detail: buckets ascending,
        // each bucket's primary page before its overflow chain, slots 0..count-1 within a page.
        // Both production callers (bitcask_index_disk_t::load_entries and
        // ::merge_immutable_segments) accumulate through a by-reference capture, so this is the
        // order they observe and hand on. `cb` is invoked synchronously, once per live entry,
        // and is never stored or deferred -- it is NOT forwarded, because it is called in a loop.
        template<typename callback_t>
        void for_each(callback_t&& cb) const {
            std::shared_lock lock(mutex_);
            byte_buffer_t page(memory_resource_);
            page.resize(page_size);
            for (uint32_t bucket = 0; bucket < header_.bucket_count_value; ++bucket) {
                uint64_t page_id = bucket_primary_page_id(bucket);
                while (page_id != 0) {
                    if (!read_page(page_id, page)) {
                        break; // unreadable page: stop walking this chain
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
                            continue; // corrupt slot: skip it rather than read past the page
                        }
                        cb(value_ref_t{entry.value,
                                       entry.log_file_id,
                                       entry.log_offset,
                                       (entry.entry_flags & entry_flag_truncated) != 0});
                    }
                    page_id = page_overflow(page);
                }
            }
        }

        bool rehash(uint32_t new_bucket_count);
        bool trigger_rehash_if_needed();
        bool set_auto_rehash_suppressed(bool suppressed) noexcept;
        uint32_t bucket_count() const;
        double load_factor() const;
        void sync();
        // Wipe all buckets in place, keeping the object identity: the store that owns
        // this table re-uses it across clear() instead of re-opening the file.
        void clear();

    private:
        struct slot_t {
            uint16_t offset{0};
            uint16_t length{0};
            uint8_t flags{0};
            uint32_t key_hash{0};
        };

        struct decoded_entry_t {
            // False when the slot could not be decoded (a corrupt page): the caller skips the
            // entry instead of receiving a throw. This class reports failure by value — its
            // public API already does (put/erase/rehash return bool, get returns optional), and
            // an exception here would unwind through an actor coroutine whose
            // unhandled_exception() is empty, turning a bad page into a hang.
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

        // Tag ctor: sets the members up and opens NOTHING. Both doors above run
        // open_or_create() themselves and act on the value it returns -- the direct ctor
        // by aborting, create() by handing it back -- so there is no failure recorded in
        // the object for one of them to remember to ask about.
        struct defer_open_tag {};
        disk_hash_table_t(const std::filesystem::path& file_path,
                          uint32_t bucket_count,
                          std::pmr::memory_resource* memory_resource,
                          defer_open_tag);

        // The open path, reporting by value the whole way: each of these returns the
        // reason it could not finish, and its caller either hands that reason up or
        // aborts on it. error_t is [[nodiscard]], so a dropped failure does not compile.
        //
        // io_failure is the one way they say why: an index_create_fail error_t carrying
        // the message, built on THIS table's resource (which is why it is a member and
        // not a free function).
        [[nodiscard]] core::error_t io_failure(const std::string& message) const;
        [[nodiscard]] core::error_t open_or_create();
        [[nodiscard]] core::error_t initialize_new_file();
        [[nodiscard]] core::error_t load_existing_file();
        [[nodiscard]] core::error_t open_overflow_file();
        void sync_files();

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

        // A TRUNCATED entry is resolved through the loader the CALLER handed in, which
        // reads the record the whole key was written with. That read wants the owning
        // store's reader lock, and every caller left is the owning store itself, which
        // takes its own lock FIRST and hands in a loader that does NOT take it again --
        // one order, no cycle.
        //
        // The AB-BA inversion this used to sit on is GONE rather than merely unlikely:
        // it needed a thread that takes THIS table's mutex first and the store's second,
        // and the only such caller was the index facade's find_impl reading the keydir
        // across the actor boundary (removed with the shared handle in C2c).
        template<hash_key_loader loader_t>
        bool keys_equal(std::string_view query_key, const decoded_entry_t& entry, const loader_t& load_full_key) const {
            if ((entry.entry_flags & entry_flag_truncated) == 0) {
                return query_key.size() == entry.full_key_len && query_key == entry.stored_key;
            }
            if (query_key.size() < entry.stored_key.size() ||
                query_key.substr(0, entry.stored_key.size()) != entry.stored_key) {
                return false;
            }
            std::string full;
            if (!load_full_key(entry.log_file_id, entry.log_offset, full)) {
                return false;
            }
            return full == query_key;
        }

        template<hash_key_loader loader_t>
        bool erase_matching(std::string_view key, std::optional<int64_t> expected_value, const loader_t& load_full_key) {
            std::unique_lock lock(mutex_);
            const uint32_t key_hash = hash_key(key);
            uint64_t page_id = bucket_primary_page_id(bucket_id_for_hash(key_hash));
            byte_buffer_t page(memory_resource_);
            page.resize(page_size);
            while (page_id != 0) {
                if (!read_page(page_id, page)) {
                    break; // unreadable page: stop walking this chain
                }
                bool erased = false;
                if (try_erase_in_page(page, key, key_hash, expected_value, load_full_key, erased)) {
                    if (erased) {
                        if (!write_page(page_id, page)) {
                            return false;
                        }
                        if (entry_count_ > 0) {
                            --entry_count_;
                        }
                    }
                    return erased;
                }
                page_id = page_overflow(page);
            }
            return false;
        }

        template<hash_key_loader loader_t>
        bool try_erase_in_page(byte_buffer_t& page,
                               std::string_view key,
                               uint32_t key_hash,
                               std::optional<int64_t> expected_value,
                               const loader_t& load_full_key,
                               bool& erased) {
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
                if (!keys_equal(key, entry, load_full_key)) {
                    continue;
                }
                if (expected_value.has_value() && entry.value != *expected_value) {
                    continue;
                }
                slot.flags = slot_flag_free;
                write_slot(page, i, slot);
                erased = true;
                return true;
            }
            return false;
        }

        bool
        try_insert_payload_in_page(byte_buffer_t& page, uint32_t key_hash, const byte_buffer_t& payload, bool& changed);
        bool put_unlocked(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset);
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
        [[nodiscard]] bool persist_header();

        std::filesystem::path file_path_;
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

} // namespace services::index
