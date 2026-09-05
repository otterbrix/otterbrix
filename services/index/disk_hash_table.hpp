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

    // The one question a hash page cannot answer on its own. An entry whose encoded key was longer
    // than disk_hash_table_t::inline_key_limit stores only a PREFIX (truncated_prefix_len bytes)
    // plus the (log_file_id, log_offset) of the record that carries the whole key, so deciding
    // whether such an entry matches a probe means reading that record back -- and only the store
    // that WROTE the record can do that. Removing the hook is not an option, and its cost is
    // silent: keys_equal() would answer false for EVERY key longer than inline_key_limit, so a
    // hashed index on a long key would return zero rows and report nothing.
    //
    // A TEMPLATE parameter, not a virtual interface and not a std::function (rule 14). The
    // customization point is NOT virtual, and there is one implementation (bitcask_index_disk_t)
    // and one production caller, both known at compile time, so the callable is simply deduced and
    // the erasure goes -- the same reasoning for_each below records. The loader travels WITH the
    // call instead of being installed on the table: nothing is stored, so it cannot dangle, there
    // is no unhook to forget in a destructor, and no null state for keys_equal to silently answer
    // false from.
    template<typename loader_t>
    concept hash_key_loader = requires(const loader_t& load_full_key, uint32_t log_file_id, uint64_t log_offset) {
        // THE ANSWER IS THE KEY ITSELF, not a flag saying a key was put somewhere. Success cannot
        // be reported without producing the key, and a read that could not happen cannot be
        // reported silently: it is a core::error_t travelling as a value. The record's KIND is not
        // asked here -- a tombstone carries the same full key a value record does, and whether the
        // key still holds rows is answered one layer up by read_rows_at's own three-way result.
        //
        // std::pmr::string, not std::string (rule 8): the key comes back from a store that has a
        // resource of its own. It is spelled in the CONCEPT because that is what makes it binding
        // -- a loader that answered with a default-allocated string would not satisfy this and
        // would not compile.
        { load_full_key(log_file_id, log_offset) } -> std::same_as<core::result_wrapper_t<std::pmr::string>>;
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

        // Factory returning the instance, or a core::error_t when the on-disk storage cannot be
        // brought up (file/overflow-file open failure, an unreadable or incompatible header).
        // Production code MUST use this: the direct ctor below aborts on the same failures,
        // mirroring bitcask_index_disk_t's deferred-open ctor plus open() vs its construct-and-open
        // ctor. The failure is a VALUE the whole way down, and result_wrapper_t makes a caller
        // confront it before it can reach the table; nothing is recorded in the object and nothing
        // has to be asked for afterwards.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<disk_hash_table_t>>
        create(const std::filesystem::path& file_path,
               uint32_t bucket_count,
               std::pmr::memory_resource* memory_resource);

        // No defaulted arguments, and the resource in particular is never defaulted to null
        // (rule 14): an assert is the only thing that would catch a null one, and NDEBUG
        // compiles it out. Both parameters are stated at every call site.
        disk_hash_table_t(const std::filesystem::path& file_path,
                          uint32_t bucket_count,
                          std::pmr::memory_resource* memory_resource);
        ~disk_hash_table_t();

        // A WRITE THAT DID NOT LAND SAYS SO, and says why: an entry that could not be
        // placed, or an auto-rehash the entry tripped that could not finish. Both are
        // environmental, both leave the table CONSISTENT (see split_one_bucket_unlocked),
        // and both mean the next write is likely to fail too -- so the caller is told
        // rather than left to discover it from a load factor that never comes down.
        [[nodiscard]] core::error_t
        put(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset);

        // THE READS, each carrying the loader that resolves a truncated entry. The parameter is
        // what makes the resolution impossible to forget: a caller that has no way to read a record
        // back cannot call these at all, instead of calling them and quietly missing every long
        // key.
        //
        // A WALK THAT COULD NOT FINISH REFUSES. Each of these follows a bucket's page chain, and a
        // `break` out of the chain when read_page says no would hand back whatever had been
        // collected so far -- making "this key has three rows" indistinguishable from "the disk
        // would not let me finish counting", a SUBSET presented as the whole answer on the only
        // read path there is. The failure is a VALUE, and result_wrapper_t is [[nodiscard]], so a
        // caller cannot go on reading the rows without meeting it first.
        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<std::vector<value_ref_t>>
        get_all(std::string_view key, const loader_t& load_full_key) const {
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
        [[nodiscard]] core::result_wrapper_t<std::optional<value_ref_t>>
        get(std::string_view key, const loader_t& load_full_key) const {
            VALUE_OR_RETURN(auto all, get_all(key, load_full_key));
            if (all.empty()) {
                return std::optional<value_ref_t>{};
            }
            return std::optional<value_ref_t>{all.front()};
        }

        // TRUE means an entry was removed, FALSE means the key (or the key/value pair) is
        // not in the table -- and the error means the walk could not reach the answer, or
        // reached it and could not persist the removal. Folding the third case into the second
        // would tell erase_all_refs_for_key's loop that it was done.
        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool> erase(std::string_view key, const loader_t& load_full_key) {
            return erase_matching(key, std::nullopt, load_full_key);
        }

        template<hash_key_loader loader_t>
        [[nodiscard]] core::result_wrapper_t<bool>
        erase(std::string_view key, int64_t value, const loader_t& load_full_key) {
            return erase_matching(key, std::optional<int64_t>(value), load_full_key);
        }

        // Rule 14: the callable is a TEMPLATE parameter, not a type-erased `function` wrapper.
        // Nothing here forces erasure -- for_each is not a virtual customization point, so the
        // callable is simply deduced, and the erased form would heap-allocate for the capturing
        // lambdas both callers pass. The body lives in the header so the callable stays a template
        // parameter at every call site.
        //
        // THE ORDER IS PART OF THE CONTRACT, not an implementation detail: buckets ascending, each
        // bucket's primary page before its overflow chain, slots 0..count-1 within a page. Both
        // production callers (bitcask_index_disk_t::load_entries and ::merge_immutable_segments)
        // accumulate through a by-reference capture, so this is the order they observe and hand on.
        // `cb` is invoked synchronously, once per live entry, never stored or deferred, and NOT
        // forwarded, because it is called in a loop.
        //
        // AND IT REFUSES rather than stopping early, for the reason get_all does: a walk that broke
        // out of a chain would hand its caller a PREFIX of the order it promises -- load_entries
        // would rebuild a table's index from part of it, and the merge would relocate part of a
        // segment and then delete the whole segment.
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
            return core::error_t::no_error();
        }

        // GROW TO new_bucket_count, one linear-hashing split at a time, and say why it
        // could not: a split that cannot copy every entry it owes the new bucket refuses
        // instead of publishing, so a failure here means the addressing state was NOT
        // advanced and the table still answers exactly as it did before the call.
        [[nodiscard]] core::error_t rehash(uint32_t new_bucket_count);
        [[nodiscard]] core::error_t trigger_rehash_if_needed();
        bool set_auto_rehash_suppressed(bool suppressed) noexcept;
        uint32_t bucket_count() const;
        double load_factor() const;
        // A REFUSED fsync IS AN ANSWER, not two dropped bools: the one caller that has to know
        // -- bitcask_index_disk_t::sync_if_dirty, whose value force_flush hands to the
        // checkpoint before it trims the WAL -- would otherwise be told the keydir was durable
        // whatever the device said.
        [[nodiscard]] core::error_t sync();
        // WIPE AND RE-CREATE AN EMPTY TABLE OF THE SAME WIDTH, reporting the reason it could not by
        // value. The object identity survives: the store that owns this table re-uses it instead of
        // re-opening the file.
        //
        // It stands on the OPEN path (bitcask_index_disk_t::load_from_disk), which is why it cannot
        // be a void wipe: with no channel it would have to end in std::abort() one call away from
        // every start of the engine, and an environmental refusal has to cost the INDEX its
        // registration, never the ENGINE its process. There is deliberately no void door beside
        // this one, so that abort is structurally unreachable rather than guarded by a test.
        //
        // The width (bucket_count) and the hash seed OUTLIVE the wipe: the replay that follows
        // refills a table of the size it just had, rather than 1024 buckets with auto-rehash
        // suppressed for the whole replay, and a fixed seed keeps the layout reproducible across
        // runs. The suppression flag is NOT touched -- the caller set it for the length of its
        // replay, and clearing it here would let a rehash run in the middle of one.
        //
        // WHAT IT NEEDS FROM THE FILESYSTEM: `w` on the DIRECTORY holding these two files, for the
        // unlinks. Not a requirement this adds -- the owning index publishes its CURRENT pointer
        // through a temp file and a rename on every open, so `w` on that directory is already the
        // price of opening the index at all (see the contract above bitcask_index_disk_t::open).
        [[nodiscard]] core::error_t reset_storage();
        // RELEASE BOTH BACKING FILES, leaving the table addressable and loud. For a caller
        // whose wipe could not finish -- see the definition for why this needs no flag.
        void close_storage();

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
        // The one reason every chain walk in this class can stop: a page the chain points
        // at could not be read (a short/rotten file, a truncated overflow file). io_error
        // rather than index_create_fail -- nothing is being created, an existing structure
        // could not be read.
        [[nodiscard]] core::error_t page_read_failure(uint64_t page_id) const;
        [[nodiscard]] core::error_t page_write_failure(uint64_t page_id) const;
        [[nodiscard]] core::error_t open_or_create();
        // The tail of reset_storage, and deliberately NOT open_or_create: it refuses a file
        // that outlived the unlink instead of loading it as an existing table. See the body.
        [[nodiscard]] core::error_t open_after_wipe_or_refuse();
        [[nodiscard]] core::error_t initialize_new_file();
        [[nodiscard]] core::error_t load_existing_file();
        [[nodiscard]] core::error_t open_overflow_file();
        // FALSE means at least one of the two backing files did not reach the device.
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

        // A TRUNCATED entry is resolved through the loader the CALLER handed in, which reads the
        // record the whole key was written with. That read wants the owning store's reader lock,
        // and every caller left is the owning store itself, which takes its own lock FIRST and
        // hands in a loader that does NOT take it again -- one order, no cycle. An AB-BA inversion
        // would need a thread that takes THIS table's structures first and the store's second;
        // nothing outside the store reaches the keydir.
        //
        // TRUE = this entry's key IS the probe. FALSE = it is a different key. AN ERROR = the
        // question could not be decided, because the record carrying the whole key could not be
        // read. Folding the third case into the second would tell get_all "no such row" and
        // try_erase_in_page "no such key" -- a SUBSET presented as the whole answer. Same three-way
        // shape, and the same reason, as erase() above and bitcask_index_disk_t::read_rows_at.
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

        // TRUE = removed here, FALSE = not in this page, walk the overflow chain, ERROR = could not
        // decide. No `bool& erased` out-parameter beside the return: only two of its four
        // combinations would be reachable, so it would duplicate the return value and still have no
        // room for the third answer.
        //
        // THE ONE THING AN ERROR HERE GUARANTEES IS ABOUT THIS BUFFER, AND NOTHING WIDER. The page
        // is mutated on the two lines before `return true` and nowhere else, so a refusal hands
        // erase_matching back the bytes it was given and no write_page follows. That is NOT the
        // same as "an erase that refused changed nothing": erase_all_refs_for_key calls erase() in
        // a LOOP, and every pass that answered true already wrote its page and dropped
        // entry_count_, so a refusal on the third pass leaves the first two removals in the file.
        // That PARTIAL removal is reported rather than rolled back -- which is why the callers
        // return the refusal instead of carrying on (append_snapshot hands it up BEFORE the put
        // that would re-point the key). What repairs it is not an undo but the next open, whose
        // rebuild is the keydir's only author and derives every entry from the segments.
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
                    continue; // corrupt slot: skip it rather than read past the page
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
        // A COUNT THAT COULD NOT FINISH REFUSES. Breaking out of a chain whose page will not
        // read and answering with the count of the readable part would have open_or_create
        // publish it as entry_count_ -- a load factor quietly understating the file. Same rule
        // as every other walk in this class.
        [[nodiscard]] core::result_wrapper_t<uint64_t> count_entries_unlocked() const;
        [[nodiscard]] core::error_t rehash_unlocked(uint32_t new_bucket_count);
        [[nodiscard]] core::error_t maybe_rehash_if_needed_unlocked();
        [[nodiscard]] core::error_t split_one_bucket_unlocked(bool durable_commit = true);
        bool slot_belongs_to_bucket_unlocked(uint32_t key_hash, uint32_t bucket_id) const;
        // Refuses a zero bucket count as a VALUE, not by assert: an assert is compiled out
        // under NDEBUG and the arithmetic behind it computes split_bucket = 0 - 1 = UINT32_MAX
        // and keeps running.
        [[nodiscard]] core::error_t initialize_linear_state_from_bucket_count();
        uint32_t bucket_id_for_hash(uint32_t key_hash) const;

        byte_buffer_t
        make_entry_payload(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) const;
        uint64_t allocate_overflow_page();
        [[nodiscard]] bool persist_header();

        std::filesystem::path file_path_;
        std::filesystem::path overflow_file_path_;
        // NO MUTEX, deliberately. This table has exactly one owner -- bitcask_index_disk_t --
        // and that store has exactly one owner, its agent, whose mailbox is the serialization
        // domain for every call that reaches here (rule 10). A shared_mutex here would be a
        // THIRD serialization domain under two that already guarantee single-threaded access,
        // and a lock that only ever runs uncontended still taxes every read and hides the
        // ownership story.
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
