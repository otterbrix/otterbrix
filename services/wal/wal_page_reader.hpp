#pragma once

#include <services/wal/wal_page.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <filesystem>
#include <vector>
#include <memory>

namespace services::wal {

class wal_page_reader_t {
public:
    explicit wal_page_reader_t(const std::filesystem::path& segment_path);

    /// Read all records with wal_id > after_id.
    /// Stops at the first corrupted page (STOP-A behavior).
    std::vector<record_t> read_all_records(id_t after_id);

    /// Read the page header at a given page index.
    /// Page 0 is the file header; data pages start at index 1.
    wal_page_header_t read_page_header(size_t page_index);

    /// Binary search: find the page containing target_lsn.
    /// Returns a position whose page_index satisfies:
    ///   header.page_lsn <= target_lsn <= header.page_end_lsn
    wal_page_position_t seek_to_lsn(id_t target_lsn);

    /// Verify CRC of every data page. Returns true if all are valid.
    bool verify_chain();

    /// Verify the checksum of a specific page.
    bool verify_page_checksum(size_t page_index);

    /// Number of data pages (excluding the file header page).
    size_t page_count() const;

private:
    /// Read a full page into buf. Returns false on read error.
    bool read_page(size_t page_index, char* buf);

    std::filesystem::path path_;
    core::filesystem::local_file_system_t fs_;
    std::unique_ptr<core::filesystem::file_handle_t> file_;
    size_t file_size_{0};
};

} // namespace services::wal
