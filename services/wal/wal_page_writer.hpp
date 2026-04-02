#pragma once

#include <services/wal/wal_page.hpp>
#include <services/wal/base.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <filesystem>
#include <string>
#include <memory>

namespace services::wal {

class wal_page_writer_t {
public:
    /// Construct a page writer.
    /// @param path        Path to the segment file (created if it does not exist).
    /// @param db_name     Database name stored in the file header.
    /// @param seg_index   Segment index stored in the file header.
    /// @param max_seg_sz  Maximum segment file size before rotation (default 4 MiB).
    wal_page_writer_t(const std::filesystem::path& path,
                      const std::string& db_name,
                      uint32_t seg_index,
                      size_t max_seg_sz = 4 * 1024 * 1024);

    ~wal_page_writer_t();

    wal_page_writer_t(const wal_page_writer_t&) = delete;
    wal_page_writer_t& operator=(const wal_page_writer_t&) = delete;

    /// Append an encoded record. May span multiple pages.
    /// @return false on write error (e.g. disk full).
    bool append(const char* data, size_t size, id_t wal_id);

    /// Flush current page to disk (even if not full).
    bool flush();

    /// Flush + fsync.
    bool flush_and_sync();

    /// Path to the current segment file.
    std::filesystem::path current_segment_path() const;

    /// Last WAL id written.
    id_t last_wal_id() const { return page_end_lsn_; }

private:
    bool write_file_header();
    bool flush_page();
    void start_new_page();

    std::filesystem::path path_;
    std::string database_name_;
    uint32_t segment_index_;
    size_t max_segment_size_;

    core::filesystem::local_file_system_t fs_;
    std::unique_ptr<core::filesystem::file_handle_t> file_;

    alignas(4096) char current_page_[PAGE_SIZE];
    size_t current_offset_{PAGE_HEADER_SIZE}; // write position within current_page_
    uint32_t num_records_{0};
    id_t page_lsn_{0};
    id_t page_end_lsn_{0};
    uint16_t page_flags_{PAGE_NORMAL};
    size_t file_size_{0}; // total bytes written to file so far
    bool has_data_{false}; // whether current page has any data
};

} // namespace services::wal
