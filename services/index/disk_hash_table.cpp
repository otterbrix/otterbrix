#include "disk_hash_table.hpp"

#include <cstring>
#include <mutex>
#include <stdexcept>

namespace services::index {

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
    } // namespace

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;
    using core::filesystem::open_file;

    disk_hash_table_t::disk_hash_table_t(const std::filesystem::path& file_path, uint32_t bucket_count)
        : file_path_(file_path) {
        if (bucket_count == 0) {
            throw std::runtime_error("disk_hash_table: bucket_count must be > 0");
        }
        header_.bucket_count_value = bucket_count;
        open_or_create();
    }

    disk_hash_table_t::~disk_hash_table_t() {
        sync();
    }

    bool disk_hash_table_t::put(std::string_view key,
                                int64_t value,
                                uint32_t log_file_id,
                                uint64_t log_offset,
                                const full_key_loader_t& key_loader) {
        std::unique_lock lock(mutex_);
        const uint32_t key_hash = hash_key(key);
        const uint32_t bucket_id = bucket_id_for_key(key);
        uint64_t page_id = bucket_primary_page_id(bucket_id);

        std::vector<uint8_t> page(page_size);
        while (true) {
            read_page(page_id, page);
            bool changed = false;
            if (try_update_in_page(page, key, key_hash, value, log_file_id, log_offset, key_loader, changed)) {
                if (changed) {
                    write_page(page_id, page);
                }
                return true;
            }
            if (try_insert_in_page(page, key, key_hash, value, log_file_id, log_offset, changed)) {
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

    std::optional<disk_hash_table_t::value_ref_t> disk_hash_table_t::get(std::string_view key,
                                                                          const full_key_loader_t& key_loader) const {
        std::shared_lock lock(mutex_);
        const uint32_t key_hash = hash_key(key);
        uint64_t page_id = bucket_primary_page_id(bucket_id_for_key(key));

        std::vector<uint8_t> page(page_size);
        while (page_id != 0) {
            read_page(page_id, page);
            const auto cnt = page_count(page);
            for (uint16_t i = 0; i < cnt; ++i) {
                auto slot = read_slot(page, i);
                if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                    continue;
                }
                const auto entry = decode_entry(page, slot);
                if (!keys_equal(key, entry, key_loader)) {
                    continue;
                }
                return value_ref_t{entry.value, entry.log_file_id, entry.log_offset, (entry.entry_flags & entry_flag_truncated) != 0};
            }
            page_id = page_overflow(page);
        }
        return std::nullopt;
    }

    bool disk_hash_table_t::erase(std::string_view key, const full_key_loader_t& key_loader) {
        std::unique_lock lock(mutex_);
        const uint32_t key_hash = hash_key(key);
        uint64_t page_id = bucket_primary_page_id(bucket_id_for_key(key));
        std::vector<uint8_t> page(page_size);
        while (page_id != 0) {
            read_page(page_id, page);
            bool erased = false;
            if (try_erase_in_page(page, key, key_hash, key_loader, erased)) {
                if (erased) {
                    write_page(page_id, page);
                }
                return erased;
            }
            page_id = page_overflow(page);
        }
        return false;
    }

    void disk_hash_table_t::sync() {
        std::shared_lock lock(mutex_);
        if (file_) {
            file_->sync();
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
    }

    void disk_hash_table_t::initialize_new_file() {
        header_.magic_value = magic;
        header_.version_value = version;
        header_.page_size_value = page_size;
        header_.next_overflow_page = 1 + header_.bucket_count_value;

        persist_header();
        std::vector<uint8_t> page(page_size);
        for (uint32_t i = 0; i < header_.bucket_count_value; ++i) {
            init_empty_page(page);
            write_page(bucket_primary_page_id(i), page);
        }
        file_->sync();
    }

    void disk_hash_table_t::load_existing_file() {
        std::vector<uint8_t> hdr(page_size, 0);
        if (!file_->read(hdr.data(), page_size, 0)) {
            throw std::runtime_error("disk_hash_table: failed to read header page");
        }
        header_.magic_value = read_u64(hdr.data());
        header_.version_value = read_u32(hdr.data() + 8);
        header_.page_size_value = read_u32(hdr.data() + 12);
        header_.bucket_count_value = read_u32(hdr.data() + 16);
        header_.next_overflow_page = read_u64(hdr.data() + 20);
        header_.checkpoint_log_file_id = read_u32(hdr.data() + 28);
        header_.checkpoint_log_offset = read_u64(hdr.data() + 32);
        if (header_.magic_value != magic || header_.version_value != version || header_.page_size_value != page_size ||
            header_.bucket_count_value == 0) {
            throw std::runtime_error("disk_hash_table: incompatible header");
        }
        if (header_.next_overflow_page < 1 + header_.bucket_count_value) {
            header_.next_overflow_page = 1 + header_.bucket_count_value;
        }
    }

    uint64_t disk_hash_table_t::data_page_count() const {
        return file_ ? (file_->file_size() / page_size) : 0;
    }

    uint64_t disk_hash_table_t::page_offset(uint64_t page_id) const {
        return page_id * page_size;
    }

    uint64_t disk_hash_table_t::bucket_primary_page_id(uint32_t bucket_id) const {
        return 1 + bucket_id;
    }

    uint16_t disk_hash_table_t::read_u16(const uint8_t* p) {
        uint16_t v;
        std::memcpy(&v, p, sizeof(v));
        return v;
    }
    uint32_t disk_hash_table_t::read_u32(const uint8_t* p) {
        uint32_t v;
        std::memcpy(&v, p, sizeof(v));
        return v;
    }
    uint64_t disk_hash_table_t::read_u64(const uint8_t* p) {
        uint64_t v;
        std::memcpy(&v, p, sizeof(v));
        return v;
    }
    int64_t disk_hash_table_t::read_i64(const uint8_t* p) {
        int64_t v;
        std::memcpy(&v, p, sizeof(v));
        return v;
    }
    void disk_hash_table_t::write_u16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, sizeof(v)); }
    void disk_hash_table_t::write_u32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, sizeof(v)); }
    void disk_hash_table_t::write_u64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, sizeof(v)); }
    void disk_hash_table_t::write_i64(uint8_t* p, int64_t v) { std::memcpy(p, &v, sizeof(v)); }

    uint32_t disk_hash_table_t::hash_key(std::string_view key) {
        return fnv1a_32(key);
    }

    uint32_t disk_hash_table_t::bucket_id_for_key(std::string_view key) const {
        return hash_key(key) % header_.bucket_count_value;
    }

    void disk_hash_table_t::read_page(uint64_t page_id, std::vector<uint8_t>& page) const {
        if (page.size() != page_size) {
            page.resize(page_size);
        }
        const auto pages = data_page_count();
        if (page_id >= pages) {
            throw std::runtime_error("disk_hash_table: page read out of range");
        }
        if (!file_->read(page.data(), page_size, page_offset(page_id))) {
            throw std::runtime_error("disk_hash_table: failed to read page");
        }
    }

    void disk_hash_table_t::write_page(uint64_t page_id, const std::vector<uint8_t>& page) {
        if (page.size() != page_size) {
            throw std::runtime_error("disk_hash_table: invalid page size");
        }
        if (!file_->write(const_cast<uint8_t*>(page.data()), page_size, page_offset(page_id))) {
            throw std::runtime_error("disk_hash_table: failed to write page");
        }
    }

    void disk_hash_table_t::init_empty_page(std::vector<uint8_t>& page) const {
        page.assign(page_size, 0);
        set_page_count(page, 0);
        set_page_free_offset(page, page_header_size);
        set_page_overflow(page, 0);
    }

    uint16_t disk_hash_table_t::page_count(const std::vector<uint8_t>& page) const {
        return read_u16(page.data());
    }

    uint16_t disk_hash_table_t::page_free_offset(const std::vector<uint8_t>& page) const {
        return read_u16(page.data() + 2);
    }

    uint64_t disk_hash_table_t::page_overflow(const std::vector<uint8_t>& page) const {
        return read_u64(page.data() + 4);
    }

    void disk_hash_table_t::set_page_count(std::vector<uint8_t>& page, uint16_t v) const {
        write_u16(page.data(), v);
    }

    void disk_hash_table_t::set_page_free_offset(std::vector<uint8_t>& page, uint16_t v) const {
        write_u16(page.data() + 2, v);
    }

    void disk_hash_table_t::set_page_overflow(std::vector<uint8_t>& page, uint64_t v) const {
        write_u64(page.data() + 4, v);
    }

    disk_hash_table_t::slot_t disk_hash_table_t::read_slot(const std::vector<uint8_t>& page, uint16_t slot_index) const {
        const auto off = slot_dir_offset(slot_index);
        slot_t s{};
        s.offset = read_u16(page.data() + off);
        s.length = read_u16(page.data() + off + 2);
        s.flags = page[off + 4];
        s.key_hash = read_u32(page.data() + off + 5);
        return s;
    }

    void disk_hash_table_t::write_slot(std::vector<uint8_t>& page, uint16_t slot_index, const slot_t& slot) const {
        const auto off = slot_dir_offset(slot_index);
        write_u16(page.data() + off, slot.offset);
        write_u16(page.data() + off + 2, slot.length);
        page[off + 4] = slot.flags;
        write_u32(page.data() + off + 5, slot.key_hash);
    }

    uint16_t disk_hash_table_t::slot_dir_offset(uint16_t slot_index) const {
        const auto idx = static_cast<uint32_t>(slot_index) + 1U;
        return static_cast<uint16_t>(static_cast<uint32_t>(page_size) - static_cast<uint32_t>(slot_size) * idx);
    }

    uint16_t disk_hash_table_t::slot_count_capacity(const std::vector<uint8_t>& page) const {
        const auto free_off = page_free_offset(page);
        uint16_t n = 0;
        while (n < page_count(page)) {
            ++n;
        }
        while (slot_dir_offset(n) >= free_off + slot_size) {
            ++n;
            if (n > (page_size / slot_size)) {
                break;
            }
        }
        return n;
    }

    disk_hash_table_t::decoded_entry_t disk_hash_table_t::decode_entry(const std::vector<uint8_t>& page, const slot_t& slot) const {
        if (slot.offset + slot.length > page_size || slot.length < (2 + 2 + 1 + 8 + 4 + 8)) {
            throw std::runtime_error("disk_hash_table: invalid entry slot");
        }
        const auto* p = page.data() + slot.offset;
        decoded_entry_t e{};
        e.stored_key_len = read_u16(p);
        e.full_key_len = read_u16(p + 2);
        e.entry_flags = *(p + 4);
        const uint16_t header_len = 5;
        const uint16_t min_tail = 8 + 4 + 8;
        if (header_len + e.stored_key_len + min_tail > slot.length) {
            throw std::runtime_error("disk_hash_table: invalid entry length");
        }
        e.stored_key = std::string_view(reinterpret_cast<const char*>(p + header_len), e.stored_key_len);
        const auto* vptr = p + header_len + e.stored_key_len;
        e.value = read_i64(vptr);
        e.log_file_id = read_u32(vptr + 8);
        e.log_offset = read_u64(vptr + 12);
        return e;
    }

    bool disk_hash_table_t::keys_equal(std::string_view query_key,
                                       const decoded_entry_t& entry,
                                       const full_key_loader_t& key_loader) const {
        if ((entry.entry_flags & entry_flag_truncated) == 0) {
            return query_key.size() == entry.full_key_len && query_key == entry.stored_key;
        }
        if (query_key.size() < entry.stored_key.size() || query_key.substr(0, entry.stored_key.size()) != entry.stored_key) {
            return false;
        }
        if (!key_loader) {
            return query_key.size() == entry.full_key_len;
        }
        std::string full;
        if (!key_loader(entry.log_file_id, entry.log_offset, full)) {
            return false;
        }
        return full == query_key;
    }

    bool disk_hash_table_t::try_update_in_page(std::vector<uint8_t>& page,
                                               std::string_view key,
                                               uint32_t key_hash,
                                               int64_t value,
                                               uint32_t log_file_id,
                                               uint64_t log_offset,
                                               const full_key_loader_t& key_loader,
                                               bool& changed) {
        const auto cnt = page_count(page);
        for (uint16_t i = 0; i < cnt; ++i) {
            auto slot = read_slot(page, i);
            if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                continue;
            }
            const auto entry = decode_entry(page, slot);
            if (!keys_equal(key, entry, key_loader)) {
                continue;
            }
            auto payload = make_entry_payload(key, value, log_file_id, log_offset);
            if (payload.size() <= slot.length) {
                std::memcpy(page.data() + slot.offset, payload.data(), payload.size());
                if (payload.size() < slot.length) {
                    std::memset(page.data() + slot.offset + payload.size(), 0, slot.length - payload.size());
                }
                changed = true;
                return true;
            }
            slot.flags = slot_flag_free;
            write_slot(page, i, slot);
            break;
        }
        return false;
    }

    bool disk_hash_table_t::try_insert_in_page(std::vector<uint8_t>& page,
                                               std::string_view key,
                                               uint32_t key_hash,
                                               int64_t value,
                                               uint32_t log_file_id,
                                               uint64_t log_offset,
                                               bool& changed) {
        auto payload = make_entry_payload(key, value, log_file_id, log_offset);
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

    bool disk_hash_table_t::try_erase_in_page(std::vector<uint8_t>& page,
                                              std::string_view key,
                                              uint32_t key_hash,
                                              const full_key_loader_t& key_loader,
                                              bool& erased) {
        const auto cnt = page_count(page);
        for (uint16_t i = 0; i < cnt; ++i) {
            auto slot = read_slot(page, i);
            if (slot.flags != slot_flag_used || slot.key_hash != key_hash || slot.length == 0) {
                continue;
            }
            const auto entry = decode_entry(page, slot);
            if (!keys_equal(key, entry, key_loader)) {
                continue;
            }
            slot.flags = slot_flag_free;
            write_slot(page, i, slot);
            erased = true;
            return true;
        }
        return false;
    }

    std::vector<uint8_t> disk_hash_table_t::make_entry_payload(std::string_view key,
                                                                int64_t value,
                                                                uint32_t log_file_id,
                                                                uint64_t log_offset) const {
        const bool truncated = key.size() > inline_key_limit;
        const uint16_t stored_len =
            static_cast<uint16_t>(truncated ? std::min<size_t>(truncated_prefix_len, key.size()) : key.size());
        const uint16_t full_len = static_cast<uint16_t>(std::min<size_t>(key.size(), UINT16_MAX));
        const size_t total = 2 + 2 + 1 + stored_len + 8 + 4 + 8;
        std::vector<uint8_t> payload(total);
        write_u16(payload.data(), stored_len);
        write_u16(payload.data() + 2, full_len);
        payload[4] = truncated ? entry_flag_truncated : 0;
        if (stored_len > 0) {
            std::memcpy(payload.data() + 5, key.data(), stored_len);
        }
        auto* tail = payload.data() + 5 + stored_len;
        write_i64(tail, value);
        write_u32(tail + 8, log_file_id);
        write_u64(tail + 12, log_offset);
        return payload;
    }

    uint64_t disk_hash_table_t::allocate_overflow_page() {
        const uint64_t page_id = header_.next_overflow_page++;
        std::vector<uint8_t> page(page_size);
        init_empty_page(page);
        write_page(page_id, page);
        persist_header();
        return page_id;
    }

    void disk_hash_table_t::persist_header() {
        std::vector<uint8_t> hdr(page_size, 0);
        write_u64(hdr.data(), header_.magic_value);
        write_u32(hdr.data() + 8, header_.version_value);
        write_u32(hdr.data() + 12, header_.page_size_value);
        write_u32(hdr.data() + 16, header_.bucket_count_value);
        write_u64(hdr.data() + 20, header_.next_overflow_page);
        write_u32(hdr.data() + 28, header_.checkpoint_log_file_id);
        write_u64(hdr.data() + 32, header_.checkpoint_log_offset);
        if (!file_->write(hdr.data(), page_size, 0)) {
            throw std::runtime_error("disk_hash_table: failed to write header page");
        }
    }

} // namespace services::index
