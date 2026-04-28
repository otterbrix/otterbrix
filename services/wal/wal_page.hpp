#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace services::wal {

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t PAGE_HEADER_SIZE = 32;
static constexpr uint32_t PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE; // 4064
static constexpr char WAL_MAGIC[4] = {'O', 'W', 'A', 'L'};
static constexpr uint16_t WAL_VERSION = 1;

enum page_flags : uint16_t {
    PAGE_NORMAL = 0,
    PAGE_PARTIAL_CONT = 1, // page contains partial/continuation data (record spans pages)
    PAGE_PARTIAL_END = 2,  // page ends the spanning sequence
};

/// File header occupies page 0 (4096 bytes).
struct wal_file_header_t {
    char magic[4];             // "OWAL"
    uint16_t version;          // 1
    uint16_t reserved0;
    uint32_t page_size;        // 4096
    uint32_t segment_index;    // segment number
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
        return std::memcmp(magic, WAL_MAGIC, 4) == 0 && version == WAL_VERSION &&
               page_size == PAGE_SIZE;
    }
};

/// Page header (32 bytes) at start of each data page.
/// Fields are ordered to avoid padding on all platforms.
struct wal_page_header_t {
    uint64_t page_lsn;     // 0: wal_id of first record starting in this page
    uint64_t page_end_lsn; // 8: wal_id of last record referenced by this page
    uint32_t num_records;   // 16: complete records that START in this page
    uint32_t data_size;     // 20: bytes used in data area (0..PAGE_DATA_SIZE)
    uint32_t checksum;      // 24: CRC32 of entire page (with checksum field zeroed)
    uint16_t flags;         // 28: page_flags
    uint16_t reserved;      // 30: padding to 32 bytes

    /// Compute CRC32 over the full page (PAGE_SIZE bytes starting at page_data).
    /// The checksum field within the header is zeroed during computation.
    void compute_checksum(char* page_data) {
        // Zero the checksum field before computing.
        // checksum lives at offset 24 within the header (8+8+4+4+2 = 26... let's compute).
        // Actually we store checksum in this struct, which is at the start of the page.
        // Offset of checksum within wal_page_header_t:
        //   page_lsn(8) + page_end_lsn(8) + num_records(4) + data_size(4) + flags(2) = 26
        uint32_t saved = checksum;
        checksum = 0;
        // Write header to page buffer so CRC covers the zeroed checksum
        std::memcpy(page_data, this, PAGE_HEADER_SIZE);
        checksum = compute_page_crc(page_data);
        // Write final checksum back to page buffer
        std::memcpy(page_data + offsetof(wal_page_header_t, checksum), &checksum, sizeof(checksum));
        (void)saved;
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

/// Result of seek_to_lsn.
struct wal_page_position_t {
    size_t page_index{0};
};

} // namespace services::wal
