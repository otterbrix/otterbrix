#include "disk_hash_table.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <stdexcept>

namespace services::index {
    namespace codec = components::index::codec;

    namespace {
        uint32_t fnv1a_32(std::string_view s) {
            constexpr uint32_t offset = 2166136261u;
            constexpr uint32_t prime = 16777619u;
            uint32_t h = offset;
            for (char ch : s) {
                const auto c = static_cast<uint8_t>(ch);
                h ^= c;
                h *= prime;
            }
            return h;
        }

        constexpr uint64_t overflow_page_id_base = 1ULL << 40;

#ifdef DEV_MODE
        bool split_crash_failpoint(const char* stage) {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT");
            return v != nullptr && std::strcmp(v, stage) == 0;
        }
#else
        bool split_crash_failpoint(const char*) {
            return false;
        }
#endif
    } // namespace

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;
    using core::filesystem::open_file;

    disk_hash_table_t::disk_hash_table_t(const std::filesystem::path& file_path,
                                         uint32_t bucket_count,
                                         std::pmr::memory_resource* memory_resource)
        : file_path_(file_path)
        , overflow_file_path_(std::filesystem::path(file_path).concat(".ovf"))
        , memory_resource_(memory_resource) {
        if (!memory_resource) {
            throw std::runtime_error("disk_hash_table: resource required");
        }
        if (bucket_count == 0) {
            throw std::runtime_error("disk_hash_table: bucket_count must be > 0");
        }
        header_.bucket_count_value = bucket_count;
        open_or_create();
    }

    disk_hash_table_t::~disk_hash_table_t() {
        std::unique_lock lock(mutex_);
        if (file_) {
            persist_header();
            sync_files();
        }
    }

    bool disk_hash_table_t::put(std::string_view key,
                                int64_t value,
                                uint32_t log_file_id,
                                uint64_t log_offset) {
        std::unique_lock lock(mutex_);
        return put_unlocked(key, value, log_file_id, log_offset);
    }

    bool disk_hash_table_t::put_unlocked(std::string_view key,
                                         int64_t value,
                                         uint32_t log_file_id,
                                         uint64_t log_offset) {
        const uint32_t key_hash = hash_key(key);
        const uint32_t bucket_id = bucket_id_for_hash(key_hash);
        auto payload = make_entry_payload(key, value, log_file_id, log_offset);
        if (!insert_payload_into_bucket_unlocked(bucket_id, key_hash, payload)) {
            return false;
        }
        ++entry_count_;
        maybe_rehash_if_needed_unlocked();
        return true;
    }

    bool disk_hash_table_t::insert_payload_into_bucket_unlocked(uint32_t bucket_id,
                                                                uint32_t key_hash,
                                                                const byte_buffer_t& payload) {
        uint64_t page_id = bucket_primary_page_id(bucket_id);
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        while (true) {
            read_page(page_id, page);
            bool changed = false;
            if (try_insert_payload_in_page(page, key_hash, payload, changed)) {
                if (changed) {
                    write_page(page_id, page);
                }
                return true;
            }
            auto overflow = page_overflow(page);
            if (overflow == 0) {
                const auto new_page = allocate_overflow_page();
                set_page_overflow(page, new_page);
                write_page(page_id, page);
                page_id = new_page;
                continue;
            }
            page_id = overflow;
        }
    }

    void disk_hash_table_t::set_full_key_loader(full_key_loader_t loader) {
        std::unique_lock lock(mutex_);
        key_loader_ = std::move(loader);
    }

    std::vector<disk_hash_table_t::value_ref_t> disk_hash_table_t::get_all(std::string_view key,
                                                                           bool lock_bitcask) const {
        std::unique_lock lock(mutex_);
        const uint32_t key_hash = hash_key(key);
        uint64_t page_id = bucket_primary_page_id(bucket_id_for_hash(key_hash));
        std::pmr::vector<value_ref_t> values(memory_resource_);

        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        while (page_id != 0) {
            read_page(page_id, page);
            const auto cnt = page_count(page);
            for (uint16_t i = 0; i < cnt; ++i) {
                auto slot = read_slot(page, i);
                if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                    continue;
                }
                const auto entry = decode_entry(page, slot);
                if (!keys_equal(key, entry, lock_bitcask)) {
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

    std::optional<disk_hash_table_t::value_ref_t> disk_hash_table_t::get(std::string_view key,
                                                                         bool lock_bitcask) const {
        auto all = get_all(key, lock_bitcask);
        if (all.empty()) {
            return std::nullopt;
        }
        return all.front();
    }

    std::vector<disk_hash_table_t::value_ref_t> disk_hash_table_t::get_all(std::string_view key) const {
        return get_all(key, true);
    }

    bool disk_hash_table_t::erase(std::string_view key,
                                  std::optional<int64_t> expected_value,
                                  bool lock_bitcask) {
        std::unique_lock lock(mutex_);
        const uint32_t key_hash = hash_key(key);
        uint64_t page_id = bucket_primary_page_id(bucket_id_for_hash(key_hash));
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        while (page_id != 0) {
            read_page(page_id, page);
            bool erased = false;
            if (try_erase_in_page(page, key, key_hash, expected_value, lock_bitcask, erased)) {
                if (erased) {
                    write_page(page_id, page);
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

    bool disk_hash_table_t::erase(std::string_view key, bool lock_bitcask) {
        return erase(key, std::nullopt, lock_bitcask);
    }

    bool disk_hash_table_t::erase(std::string_view key, int64_t value, bool lock_bitcask) {
        return erase(key, std::optional<int64_t>(value), lock_bitcask);
    }

    void disk_hash_table_t::for_each(const std::function<void(const value_ref_t&)>& cb) const {
        std::shared_lock lock(mutex_);
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        for (uint32_t bucket = 0; bucket < header_.bucket_count_value; ++bucket) {
            uint64_t page_id = bucket_primary_page_id(bucket);
            while (page_id != 0) {
                read_page(page_id, page);
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
                    cb(value_ref_t{entry.value,
                                   entry.log_file_id,
                                   entry.log_offset,
                                   (entry.entry_flags & entry_flag_truncated) != 0});
                }
                page_id = page_overflow(page);
            }
        }
    }

    bool disk_hash_table_t::rehash(uint32_t new_bucket_count) {
        std::unique_lock lock(mutex_);
        return rehash_unlocked(new_bucket_count);
    }

    bool disk_hash_table_t::trigger_rehash_if_needed() {
        std::unique_lock lock(mutex_);
        return maybe_rehash_if_needed_unlocked();
    }

    bool disk_hash_table_t::set_auto_rehash_suppressed(bool suppressed) noexcept {
        return suppress_auto_rehash_.exchange(suppressed, std::memory_order_acq_rel);
    }

    double disk_hash_table_t::load_factor() const {
        std::shared_lock lock(mutex_);
        if (header_.bucket_count_value == 0) {
            return 0.0;
        }
        return static_cast<double>(entry_count_) / static_cast<double>(header_.bucket_count_value);
    }

    bool disk_hash_table_t::rehash_unlocked(uint32_t new_bucket_count) {
        if (new_bucket_count == 0) {
            throw std::runtime_error("disk_hash_table: rehash bucket_count must be > 0");
        }
        if (new_bucket_count <= header_.bucket_count_value) {
            return false;
        }
        rehash_in_progress_ = true;
        struct reset_flag_t {
            bool& flag;
            ~reset_flag_t() { flag = false; }
        } reset{rehash_in_progress_};
        bool changed = false;
        while (header_.bucket_count_value < new_bucket_count) {
            changed = split_one_bucket_unlocked() || changed;
        }
        sync_files();
        return changed;
    }

    bool disk_hash_table_t::split_one_bucket_unlocked(bool durable_commit) {
        if (header_.bucket_count_value == UINT32_MAX) {
            return false;
        }
        const uint32_t base = 1U << header_.level_value;
        if (base == 0 || header_.split_bucket_value >= base) {
            throw std::runtime_error("disk_hash_table: invalid linear hash state");
        }
        const uint32_t split_bucket = header_.split_bucket_value;
        const uint32_t new_bucket = base + split_bucket;
        if (new_bucket != header_.bucket_count_value) {
            throw std::runtime_error("disk_hash_table: inconsistent bucket progression");
        }
        const uint64_t mod = static_cast<uint64_t>(base) << 1U;

        byte_buffer_t empty(memory_resource_);
        empty.resize(page_size);
        init_empty_page(empty);
        write_page(bucket_primary_page_id(new_bucket), empty);

        uint64_t page_id = bucket_primary_page_id(split_bucket);
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        byte_buffer_t payload(memory_resource_);

        // Phase 1 (copy): move-candidates are appended to the new bucket, source remains intact.
        // A crash here is safe because lookups still use the old addressing state.
        while (page_id != 0) {
            read_page(page_id, page);
            const auto cnt = page_count(page);
            for (uint16_t i = 0; i < cnt; ++i) {
                const auto slot = read_slot(page, i);
                if (slot.flags != slot_flag_used || slot.length == 0) {
                    continue;
                }
                if (!slot_belongs_to_bucket_unlocked(slot.key_hash, split_bucket)) {
                    continue;
                }
                if ((static_cast<uint64_t>(slot.key_hash) % mod) == split_bucket) {
                    continue;
                }
                payload.resize(slot.length);
                std::memcpy(payload.data(), page.data() + slot.offset, slot.length);
                insert_payload_into_bucket_unlocked(new_bucket, slot.key_hash, payload);
            }
            page_id = page_overflow(page);
        }

        if (durable_commit) {
            // Ensure copied entries are durable before publishing metadata.
            // Until the header is advanced, a crash must reopen with the old
            // addressing state; the copied new-bucket entries are merely
            // unreachable duplicates.
            sync_files();
            if (split_crash_failpoint("after_copy_sync")) {
                throw std::runtime_error("disk_hash_table: simulated crash after copy sync");
            }
        }

        // Phase 2 (commit): publish new addressing state in-memory.
        // For durable_commit=false (auto-rehash batch), on-disk header update is deferred to caller.
        ++header_.bucket_count_value;
        ++header_.split_bucket_value;
        if (header_.split_bucket_value == base) {
            header_.split_bucket_value = 0;
            ++header_.level_value;
        }

        if (durable_commit) {
            persist_header();
            sync_files();
            if (split_crash_failpoint("after_header_sync")) {
                throw std::runtime_error("disk_hash_table: simulated crash after header sync");
            }
        }

        // Phase 3 (lazy cleanup): intentionally skipped in split hot path.
        // Stale source copies remain physically present, but are ignored by ownership
        // checks in iteration/recount paths and by future split scans.
        return true;
    }

    bool disk_hash_table_t::maybe_rehash_if_needed_unlocked() {
        if (rehash_in_progress_ || header_.bucket_count_value == 0) {
            return false;
        }
        if (suppress_auto_rehash_.load(std::memory_order_acquire)) {
            return false;
        }
        if (header_.bucket_count_value == UINT32_MAX) {
            return false;
        }
        // Only trigger rehash when load factor significantly exceeds threshold
        // to reduce frequency of rehash operations during bulk inserts.
        const auto curr_lf = static_cast<double>(entry_count_) / static_cast<double>(header_.bucket_count_value);
        if (curr_lf <= max_load_factor_) {
            return false;
        }
        bool changed = false;
        // Batch multiple splits together before syncing to reduce fsync overhead.
        // Target load factor slightly below threshold to avoid immediate re-trigger.
        const double target_lf = max_load_factor_ * 0.6;
        const uint32_t target_buckets = static_cast<uint32_t>(
            std::min(static_cast<double>(UINT32_MAX),
                     static_cast<double>(entry_count_) / target_lf));
        while (header_.bucket_count_value < target_buckets && header_.bucket_count_value < UINT32_MAX) {
            // Auto-rehash path batches split durability barriers to avoid one fsync pair
            // per split. Crash safety is preserved because source buckets are never
            // destructively cleaned before header publication.
            changed = split_one_bucket_unlocked(false) || changed;
        }
        if (changed) {
            // Publish all split data first, then atomically advance addressing state.
            // Single sync barrier at the end of batch.
            persist_header();
            sync_files();
        }
        return changed;
    }

    uint32_t disk_hash_table_t::bucket_count() const {
        std::shared_lock lock(mutex_);
        return header_.bucket_count_value;
    }

    void disk_hash_table_t::sync() {
        std::shared_lock lock(mutex_);
        sync_files();
    }

    void disk_hash_table_t::sync_files() {
        if (file_) {
            file_->sync();
        }
        if (ovf_file_) {
            ovf_file_->sync();
        }
    }

    void disk_hash_table_t::open_or_create() {
        file_ = open_file(fs_,
                          file_path_,
                          file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK);
        if (!file_) {
            throw std::runtime_error("disk_hash_table: failed to open file " + file_path_.string());
        }
        if (file_->file_size() == 0) {
            initialize_new_file();
            return;
        }
        load_existing_file();
        entry_count_ = count_entries_unlocked();
    }

    void disk_hash_table_t::open_overflow_file() {
        ovf_file_ = open_file(fs_,
                              overflow_file_path_,
                              file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                              file_lock_type::NO_LOCK);
        if (!ovf_file_) {
            throw std::runtime_error("disk_hash_table: failed to open overflow file " +
                                     overflow_file_path_.string());
        }
    }

    void disk_hash_table_t::initialize_new_file() {
        header_.page_size_value = page_size;
        header_.next_overflow_page = overflow_page_id_base;
        initialize_linear_state_from_bucket_count();

        open_overflow_file();
        persist_header();
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        for (uint32_t i = 0; i < header_.bucket_count_value; ++i) {
            init_empty_page(page);
            write_page(bucket_primary_page_id(i), page);
        }
        entry_count_ = 0;
        sync_files();
    }

    void disk_hash_table_t::load_existing_file() {
        byte_buffer_t hdr(memory_resource_);
        hdr.resize(page_size, 0);
        if (!file_->read(hdr.data(), page_size, 0)) {
            throw std::runtime_error("disk_hash_table: failed to read header page");
        }
        header_.page_size_value = codec::read_le_ptr<uint32_t>(hdr.data() + 12);
        header_.bucket_count_value = codec::read_le_ptr<uint32_t>(hdr.data() + 16);
        header_.next_overflow_page = codec::read_le_ptr<uint64_t>(hdr.data() + 20);
        header_.level_value = codec::read_le_ptr<uint32_t>(hdr.data() + 28);
        header_.split_bucket_value = codec::read_le_ptr<uint32_t>(hdr.data() + 32);
        if (header_.page_size_value != page_size ||
            header_.bucket_count_value == 0) {
            throw std::runtime_error("disk_hash_table: incompatible header");
        }
        const uint32_t base = header_.level_value > 31 ? 0 : (1U << header_.level_value);
        if (base == 0 ||
            base > header_.bucket_count_value ||
            header_.split_bucket_value > base ||
            (base + header_.split_bucket_value) != header_.bucket_count_value) {
            initialize_linear_state_from_bucket_count();
        }

        open_overflow_file();
        if (header_.next_overflow_page < overflow_page_id_base) {
            header_.next_overflow_page = overflow_page_id_base;
        }
    }

    bool disk_hash_table_t::is_overflow_page_id(uint64_t page_id) {
        return page_id >= overflow_page_id_base;
    }

    uint64_t disk_hash_table_t::main_page_count() const {
        return file_ ? (file_->file_size() / page_size) : 0;
    }

    uint64_t disk_hash_table_t::overflow_page_count() const {
        return ovf_file_ ? (ovf_file_->file_size() / page_size) : 0;
    }

    uint64_t disk_hash_table_t::bucket_primary_page_id(uint32_t bucket_id) const { return 1 + bucket_id; }

    uint32_t disk_hash_table_t::hash_key(std::string_view key) { return fnv1a_32(key); }

    uint32_t disk_hash_table_t::bucket_id_for_hash(uint32_t key_hash) const {
        if (header_.bucket_count_value == 0) {
            return 0;
        }
        if (header_.level_value > 31) {
            throw std::runtime_error("disk_hash_table: invalid linear hash level");
        }
        const uint32_t base = 1U << header_.level_value;
        uint32_t bucket = key_hash % base;
        if (bucket < header_.split_bucket_value) {
            const uint64_t doubled = static_cast<uint64_t>(base) << 1U;
            bucket = static_cast<uint32_t>(static_cast<uint64_t>(key_hash) % doubled);
        }
        return bucket;
    }

    void disk_hash_table_t::initialize_linear_state_from_bucket_count() {
        if (header_.bucket_count_value == 0) {
            throw std::runtime_error("disk_hash_table: bucket_count must be > 0");
        }
        uint32_t base = 1;
        uint32_t level = 0;
        while ((base << 1U) != 0 && (base << 1U) <= header_.bucket_count_value) {
            base <<= 1U;
            ++level;
        }
        header_.level_value = level;
        header_.split_bucket_value = header_.bucket_count_value - base;
    }

    uint64_t disk_hash_table_t::count_entries_unlocked() const {
        uint64_t count = 0;
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        for (uint32_t bucket = 0; bucket < header_.bucket_count_value; ++bucket) {
            uint64_t page_id = bucket_primary_page_id(bucket);
            while (page_id != 0) {
                read_page(page_id, page);
                const auto cnt = page_count(page);
                for (uint16_t i = 0; i < cnt; ++i) {
                    const auto slot = read_slot(page, i);
                    if (slot.flags == slot_flag_used && slot.length != 0 &&
                        slot_belongs_to_bucket_unlocked(slot.key_hash, bucket)) {
                        ++count;
                    }
                }
                page_id = page_overflow(page);
            }
        }
        return count;
    }

    bool disk_hash_table_t::slot_belongs_to_bucket_unlocked(uint32_t key_hash, uint32_t bucket_id) const {
        return bucket_id_for_hash(key_hash) == bucket_id;
    }

    void disk_hash_table_t::read_page(uint64_t page_id, byte_buffer_t& page) const {
        if (page.size() != page_size) {
            page.resize(page_size);
        }
        if (is_overflow_page_id(page_id)) {
            const uint64_t physical = page_id - overflow_page_id_base;
            if (physical >= overflow_page_count()) {
                throw std::runtime_error("disk_hash_table: overflow page read out of range");
            }
            if (!ovf_file_->read(page.data(), page_size, physical * page_size)) {
                throw std::runtime_error("disk_hash_table: failed to read overflow page");
            }
            return;
        }
        if (page_id >= main_page_count()) {
            throw std::runtime_error("disk_hash_table: page read out of range");
        }
        if (!file_->read(page.data(), page_size, page_id * page_size)) {
            throw std::runtime_error("disk_hash_table: failed to read page");
        }
    }

    void disk_hash_table_t::write_page(uint64_t page_id, const byte_buffer_t& page) {
        if (page.size() != page_size) {
            throw std::runtime_error("disk_hash_table: invalid page size");
        }
        if (is_overflow_page_id(page_id)) {
            const uint64_t physical = page_id - overflow_page_id_base;
            if (!ovf_file_->write(const_cast<uint8_t*>(page.data()), page_size, physical * page_size)) {
                throw std::runtime_error("disk_hash_table: failed to write overflow page");
            }
            return;
        }
        if (!file_->write(const_cast<uint8_t*>(page.data()), page_size, page_id * page_size)) {
            throw std::runtime_error("disk_hash_table: failed to write page");
        }
    }

    void disk_hash_table_t::init_empty_page(byte_buffer_t& page) const {
        page.assign(page_size, 0);
        set_page_count(page, 0);
        set_page_free_offset(page, page_header_size);
        set_page_overflow(page, 0);
    }

    uint16_t disk_hash_table_t::page_count(const byte_buffer_t& page) const {
        return codec::read_le_ptr<uint16_t>(page.data());
    }

    uint16_t disk_hash_table_t::page_free_offset(const byte_buffer_t& page) const {
        return codec::read_le_ptr<uint16_t>(page.data() + 2);
    }

    uint64_t disk_hash_table_t::page_overflow(const byte_buffer_t& page) const {
        return codec::read_le_ptr<uint64_t>(page.data() + 4);
    }

    void disk_hash_table_t::set_page_count(byte_buffer_t& page, uint16_t v) const {
        codec::write_le_ptr<uint16_t>(page.data(), v);
    }

    void disk_hash_table_t::set_page_free_offset(byte_buffer_t& page, uint16_t v) const {
        codec::write_le_ptr<uint16_t>(page.data() + 2, v);
    }

    void disk_hash_table_t::set_page_overflow(byte_buffer_t& page, uint64_t v) const {
        codec::write_le_ptr<uint64_t>(page.data() + 4, v);
    }

    disk_hash_table_t::slot_t disk_hash_table_t::read_slot(const byte_buffer_t& page, uint16_t slot_index) const {
        const auto off = slot_dir_offset(slot_index);
        slot_t s{};
        s.offset = codec::read_le_ptr<uint16_t>(page.data() + off);
        s.length = codec::read_le_ptr<uint16_t>(page.data() + off + 2);
        s.flags = page[off + 4];
        s.key_hash = codec::read_le_ptr<uint32_t>(page.data() + off + 5);
        return s;
    }

    void disk_hash_table_t::write_slot(byte_buffer_t& page, uint16_t slot_index, const slot_t& slot) const {
        const auto off = slot_dir_offset(slot_index);
        codec::write_le_ptr<uint16_t>(page.data() + off, slot.offset);
        codec::write_le_ptr<uint16_t>(page.data() + off + 2, slot.length);
        page[off + 4] = slot.flags;
        codec::write_le_ptr<uint32_t>(page.data() + off + 5, slot.key_hash);
    }

    uint16_t disk_hash_table_t::slot_dir_offset(uint16_t slot_index) const {
        const auto idx = static_cast<uint32_t>(slot_index) + 1U;
        return static_cast<uint16_t>(static_cast<uint32_t>(page_size) - static_cast<uint32_t>(slot_size) * idx);
    }

    disk_hash_table_t::decoded_entry_t disk_hash_table_t::decode_entry(const byte_buffer_t& page,
                                                                       const slot_t& slot) const {
        if (slot.offset + slot.length > page_size || slot.length < (2 + 4 + 1 + 8 + 4 + 8)) {
            throw std::runtime_error("disk_hash_table: invalid entry slot");
        }
        const auto* p = page.data() + slot.offset;
        decoded_entry_t e{};
        e.stored_key_len = codec::read_le_ptr<uint16_t>(p);
        e.full_key_len = codec::read_le_ptr<uint32_t>(p + 2);
        e.entry_flags = *(p + 6);
        const uint16_t header_len = 7;
        const uint16_t min_tail = 8 + 4 + 8;
        if (header_len + e.stored_key_len + min_tail > slot.length) {
            throw std::runtime_error("disk_hash_table: invalid entry length");
        }
        e.stored_key = std::string_view(reinterpret_cast<const char*>(p + header_len), e.stored_key_len);
        const auto* vptr = p + header_len + e.stored_key_len;
        e.value = codec::read_le_ptr<int64_t>(vptr);
        e.log_file_id = codec::read_le_ptr<uint32_t>(vptr + 8);
        e.log_offset = codec::read_le_ptr<uint64_t>(vptr + 12);
        return e;
    }

    bool disk_hash_table_t::keys_equal(std::string_view query_key,
                                       const decoded_entry_t& entry,
                                       bool lock_bitcask) const {
        if ((entry.entry_flags & entry_flag_truncated) == 0) {
            return query_key.size() == entry.full_key_len && query_key == entry.stored_key;
        }
        if (query_key.size() < entry.stored_key.size() ||
            query_key.substr(0, entry.stored_key.size()) != entry.stored_key) {
            return false;
        }
        if (!key_loader_) {
            return false;
        }
        std::string full;
        if (!key_loader_(entry.log_file_id, entry.log_offset, full, lock_bitcask)) {
            return false;
        }
        return full == query_key;
    }

    bool disk_hash_table_t::try_insert_payload_in_page(byte_buffer_t& page,
                                                       uint32_t key_hash,
                                                       const byte_buffer_t& payload,
                                                       bool& changed) {
        const uint16_t free_off = page_free_offset(page);
        const uint16_t cnt = page_count(page);
        const uint16_t dir_start = slot_dir_offset(cnt);
        const auto required = static_cast<size_t>(free_off) + payload.size() + static_cast<size_t>(slot_size);
        const auto available_limit = static_cast<size_t>(dir_start) + static_cast<size_t>(slot_size);
        if (required > available_limit) {
            return false;
        }

        const uint16_t new_off = free_off;
        std::memcpy(page.data() + new_off, payload.data(), payload.size());
        slot_t slot{};
        slot.offset = new_off;
        slot.length = static_cast<uint16_t>(payload.size());
        slot.flags = slot_flag_used;
        slot.key_hash = key_hash;
        write_slot(page, cnt, slot);
        set_page_count(page, cnt + 1);
        set_page_free_offset(page, static_cast<uint16_t>(free_off + payload.size()));
        changed = true;
        return true;
    }

    bool disk_hash_table_t::try_erase_in_page(byte_buffer_t& page,
                                              std::string_view key,
                                              uint32_t key_hash,
                                              std::optional<int64_t> expected_value,
                                              bool lock_bitcask,
                                              bool& erased) {
        const auto cnt = page_count(page);
        for (uint16_t i = 0; i < cnt; ++i) {
            auto slot = read_slot(page, i);
            if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                continue;
            }
            const auto entry = decode_entry(page, slot);
            if (!keys_equal(key, entry, lock_bitcask)) {
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

    disk_hash_table_t::byte_buffer_t disk_hash_table_t::make_entry_payload(std::string_view key,
                                                                           int64_t value,
                                                                           uint32_t log_file_id,
                                                                           uint64_t log_offset) const {
        const bool truncated = key.size() > inline_key_limit;
        const uint16_t stored_len =
            static_cast<uint16_t>(truncated ? std::min<size_t>(truncated_prefix_len, key.size()) : key.size());
        const uint32_t full_len = static_cast<uint32_t>(std::min<size_t>(key.size(), UINT32_MAX));
        const size_t total = 2 + 4 + 1 + stored_len + 8 + 4 + 8;
        byte_buffer_t payload(memory_resource_);
        payload.resize(total);
        codec::write_le_ptr<uint16_t>(payload.data(), stored_len);
        codec::write_le_ptr<uint32_t>(payload.data() + 2, full_len);
        payload[6] = truncated ? entry_flag_truncated : 0;
        if (stored_len > 0) {
            std::memcpy(payload.data() + 7, key.data(), stored_len);
        }
        auto* tail = payload.data() + 7 + stored_len;
        codec::write_le_ptr<int64_t>(tail, value);
        codec::write_le_ptr<uint32_t>(tail + 8, log_file_id);
        codec::write_le_ptr<uint64_t>(tail + 12, log_offset);
        return payload;
    }

    uint64_t disk_hash_table_t::allocate_overflow_page() {
        if (header_.next_overflow_page < overflow_page_id_base) {
            header_.next_overflow_page = overflow_page_id_base;
        }
        const uint64_t page_id = header_.next_overflow_page++;
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        init_empty_page(page);
        write_page(page_id, page);
        return page_id;
    }

    void disk_hash_table_t::persist_header() {
        byte_buffer_t hdr(memory_resource_);
        hdr.resize(page_size, 0);
        codec::write_le_ptr<uint32_t>(hdr.data() + 12, header_.page_size_value);
        codec::write_le_ptr<uint32_t>(hdr.data() + 16, header_.bucket_count_value);
        codec::write_le_ptr<uint64_t>(hdr.data() + 20, header_.next_overflow_page);
        codec::write_le_ptr<uint32_t>(hdr.data() + 28, header_.level_value);
        codec::write_le_ptr<uint32_t>(hdr.data() + 32, header_.split_bucket_value);
        if (!file_->write(hdr.data(), page_size, 0)) {
            throw std::runtime_error("disk_hash_table: failed to write header page");
        }
    }

} // namespace services::index
