#pragma once

#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <services/wal/base.hpp>
#include <services/wal/wal_page.hpp>
#include <string>

namespace services::wal {

    class wal_page_writer_t {
    public:
        /// Construct a page writer.
        /// @param resource    Backs the diagnostics carried by every error_t below.
        /// @param path        Path to the segment file (created if it does not exist).
        /// @param db_name     Database name stored in the file header.
        /// @param seg_index   Segment index stored in the file header.
        /// @param max_seg_sz  Maximum segment file size before rotation (default 4 MiB).
        wal_page_writer_t(std::pmr::memory_resource* resource,
                          const std::filesystem::path& path,
                          const std::string& db_name,
                          uint32_t seg_index,
                          size_t max_seg_sz = 4 * 1024 * 1024);

        ~wal_page_writer_t();

        wal_page_writer_t(const wal_page_writer_t&) = delete;
        wal_page_writer_t& operator=(const wal_page_writer_t&) = delete;

        /// The reason the segment could not be opened / initialised; no_error() when usable.
        /// A failed open is reported here rather than left as a null file_ that
        /// write_file_header() would dereference; every entry point below refuses while this
        /// is set.
        [[nodiscard]] const core::error_t& open_error() const noexcept { return open_error_; }
        [[nodiscard]] bool is_open() const noexcept { return file_ != nullptr; }

        /// Append an encoded record. May span multiple pages.
        /// Refuses with io_error on a write error (e.g. disk full). THE RETURN IS THE ONLY
        /// EVIDENCE the record reached the segment: dropping it — which a bare bool invites —
        /// lets wal.cpp return the wal_id of a record that was never written.
        [[nodiscard]] core::error_t append(const char* data, size_t size, id_t wal_id);

        /// Flush current page to disk (even if not full).
        [[nodiscard]] core::error_t flush();

        /// Flush + fsync. Refuses when EITHER half fails — dropping the fsync result here lets
        /// a FULL-sync commit report durability over a page that never reached the device.
        [[nodiscard]] core::error_t flush_and_sync();

        /// Path to the current segment file.
        std::filesystem::path current_segment_path() const;

        /// Last WAL id written.
        id_t last_wal_id() const { return page_end_lsn_; }

        /// The failure of the destructor's last-resort flush, if any. A destructor has no
        /// caller to answer, so it latches here; the ENGINE path never depends on it,
        /// because wal_worker_t flushes explicitly (and reads the answer) before every
        /// teardown and rotation, which leaves this flush with nothing to write.
        [[nodiscard]] const core::error_t& last_error() const noexcept { return last_error_; }

        /// True when a refused mid-record flush left an ORPHAN SPAN on the disk: at least one
        /// page of a record that was then refused is in the file, flagged PARTIAL_CONT, and
        /// nothing will ever complete it. The next page appended to THIS segment would be read
        /// as that span's continuation bytes, so the owner must rotate to a fresh segment
        /// before writing anything else (wal_worker_t::ensure_writer does). The buffered page
        /// itself is already discarded — only the on-disk tail is unusable.
        [[nodiscard]] bool torn_tail() const noexcept { return torn_tail_; }

    private:
        [[nodiscard]] core::error_t write_file_header();
        [[nodiscard]] core::error_t flush_page();
        void start_new_page();

        [[nodiscard]] core::error_t io_failure(const char* what) const;

        std::pmr::memory_resource* resource_;
        std::filesystem::path path_;
        std::string database_name_;
        uint32_t segment_index_;

        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;

        alignas(4096) char current_page_[PAGE_SIZE];
        size_t current_offset_{PAGE_HEADER_SIZE}; // write position within current_page_
        uint32_t num_records_{0};
        id_t page_lsn_{0};
        id_t page_end_lsn_{0};
        uint16_t page_flags_{PAGE_NORMAL};
        size_t file_size_{0};  // total bytes written to file so far
        bool has_data_{false};   // whether current page has any data
        bool torn_tail_{false};  // an orphan PARTIAL_CONT span sits at the end of the file
        core::error_t open_error_;
        core::error_t last_error_;
    };

} // namespace services::wal
