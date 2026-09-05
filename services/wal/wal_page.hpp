#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#ifdef DEV_MODE
#include <core/file/file_handle.hpp>
#include <filesystem>
#include <memory>
#endif

namespace services::wal {

#ifdef DEV_MODE
    // Fault-injection seam for WAL SEGMENT FILES.
    //
    // The .otbx seam (single_file_block_manager_t::dev_set_file_interposer) wraps the handle
    // of a database file and covers nothing else; the WAL opens its segments itself through
    // core::filesystem::open_file, so without this no test can tell the journal "this page
    // write fails" or "this segment will not open". Plain virtual interface, NOT
    // std::function (rule 14); process-wide, DEV_MODE-only, read once per open by
    // wal_page_writer_t and wal_page_reader_t.
    //
    // Returning nullptr from wrap() MODELS AN UNOPENABLE SEGMENT, and it is faithful rather
    // than a shortcut: open_file's own failure answer IS nullptr (local_file_system.cpp
    // returns it whenever open(2) reports -1), so the interposed path and the real one hand
    // the caller the identical value.
    struct wal_file_interposer_t {
        virtual ~wal_file_interposer_t() = default;
        virtual std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) = 0;
    };

    void dev_set_wal_file_interposer(wal_file_interposer_t* interposer); // nullptr = off
    wal_file_interposer_t* dev_wal_file_interposer();
#endif

    static constexpr uint32_t PAGE_SIZE = 4096;
    static constexpr uint32_t PAGE_HEADER_SIZE = 32;
    static constexpr uint32_t PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE; // 4064
    static constexpr char WAL_MAGIC[4] = {'O', 'W', 'A', 'L'};
    static constexpr uint16_t WAL_VERSION = 1;

    enum page_flags : uint16_t
    {
        PAGE_NORMAL = 0,
        PAGE_PARTIAL_CONT = 1, // page contains partial/continuation data (record spans pages)
        PAGE_PARTIAL_END = 2,  // page ends the spanning sequence
    };

    /// File header occupies page 0 (4096 bytes).
    struct wal_file_header_t {
        char magic[4];    // "OWAL"
        uint16_t version; // 1
        uint16_t reserved0;
        uint32_t page_size;     // 4096
        uint32_t segment_index; // segment number
        uint16_t database_name_len;
        char database_name[256];
        uint64_t created_timestamp;
        // rest is padding to PAGE_SIZE

        void init(uint32_t seg_idx, const std::string& db_name) {
            std::memset(this, 0, sizeof(*this));
            std::memcpy(magic, WAL_MAGIC, 4);
            version = WAL_VERSION;
            reserved0 = 0;
            page_size = PAGE_SIZE;
            segment_index = seg_idx;
            database_name_len = static_cast<uint16_t>(
                db_name.size() < sizeof(database_name) ? db_name.size() : sizeof(database_name) - 1);
            std::memcpy(database_name, db_name.data(), database_name_len);
            created_timestamp = 0; // caller may set
        }

        bool validate() const {
            return std::memcmp(magic, WAL_MAGIC, 4) == 0 && version == WAL_VERSION && page_size == PAGE_SIZE;
        }
    };

    /// Page header (32 bytes) at start of each data page.
    /// Fields are ordered to avoid padding on all platforms.
    struct wal_page_header_t {
        uint64_t page_lsn;     // 0: wal_id of first record starting in this page
        uint64_t page_end_lsn; // 8: wal_id of last record referenced by this page
        uint32_t num_records;  // 16: complete records that START in this page
        uint32_t data_size;    // 20: bytes used in data area (0..PAGE_DATA_SIZE)
        uint32_t checksum;     // 24: CRC32 of entire page (with checksum field zeroed)
        uint16_t flags;        // 28: page_flags
        uint16_t reserved;     // 30: padding to 32 bytes

        /// Compute CRC32 over the full page (PAGE_SIZE bytes starting at page_data).
        /// The checksum field within the header is zeroed during computation.
        void compute_checksum(char* page_data) {
            checksum = 0;
            std::memcpy(page_data, this, PAGE_HEADER_SIZE);
            checksum = compute_page_crc(page_data);
            std::memcpy(page_data + offsetof(wal_page_header_t, checksum), &checksum, sizeof(checksum));
        }

        /// Verify the checksum of the full page.
        bool verify_checksum(const char* page_data) const {
            // Make a mutable copy of the page for verification
            alignas(16) char temp[PAGE_SIZE];
            std::memcpy(temp, page_data, PAGE_SIZE);
            // Zero the checksum field in the copy
            uint32_t zero = 0;
            std::memcpy(temp + offsetof(wal_page_header_t, checksum), &zero, sizeof(zero));
            uint32_t computed = compute_page_crc(temp);
            return computed == checksum;
        }

    private:
        static uint32_t compute_page_crc(const char* page_data);
    };

    static_assert(sizeof(wal_page_header_t) == 32, "page header must be 32 bytes");

} // namespace services::wal
