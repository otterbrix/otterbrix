#pragma once

#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_page.hpp>
#include <vector>

namespace services::wal {

    class wal_page_reader_t {
    public:
        wal_page_reader_t(std::pmr::memory_resource* resource, const std::filesystem::path& segment_path);

        [[nodiscard]] bool is_open() const noexcept { return file_ != nullptr; }

        /// The reason the segment could not be opened; no_error() when it is open.
        [[nodiscard]] const core::error_t& open_error() const noexcept { return open_error_; }

        /// Stops at the first corrupted page (STOP-A). Refuses with io_error if unopened, so
        /// empty means "no records here," never "unreadable."
        core::result_wrapper_t<std::vector<record_t>> read_all_records(id_t after_id);

        /// Page 0 is the file header, data pages start at index 1. TEST-ONLY OBSERVER: answers
        /// zeros for an unreadable page; production code must use read_verified_page_header.
        wal_page_header_t read_page_header(size_t page_index);

        /// The header comes from the same read that verified the checksum, so a failed re-read
        /// cannot substitute zeros; false covers "unreadable" and "does not verify" alike.
        [[nodiscard]] bool read_verified_page_header(size_t page_index, wal_page_header_t& out);

        /// Distinct from read_all_records: conflating the two once let a CRC break move the id
        /// allocator backwards; this is a high-water mark, not gated by the first broken page.
        struct segment_scan_t {
            /// Only among pages whose checksum verifies, even past a break; an unverified header is never trusted.
            id_t highest_page_end_lsn{0};
            /// False when a page failed its checksum OR could not be read -- separate paths in the .cpp.
            bool chain_intact{true};
            /// 1-based index of the first data page that failed; 0 when none did.
            size_t first_broken_page{0};
            /// Non-zero means committed transactions sit past the break that replay will not reach.
            size_t verified_pages_after_break{0};

            // Only wal_worker_t::load consults the two fields below.

            /// Non-zero means read_all_records stopped short of ids this segment still holds.
            id_t first_verified_lsn_after_break{0};
            /// A break on the previous segment's last page hides nothing there; only this field reveals the jump.
            id_t first_verified_page_lsn{0};
        };

        segment_scan_t scan_pages();

        bool verify_chain();

        bool verify_page_checksum(size_t page_index);

        /// Excludes the file header page; zero on an unopened segment too, so callers must check is_open() first.
        size_t page_count() const;

    private:
        bool read_page(size_t page_index, char* buf);

        std::pmr::memory_resource* resource_;
        std::filesystem::path path_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        size_t file_size_{0};
        core::error_t open_error_;
    };

} // namespace services::wal
