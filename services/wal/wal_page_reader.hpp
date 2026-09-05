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
        /// Open a segment for reading.
        ///
        /// THE OPEN CAN FAIL AND THE FAILURE IS KEPT. The constructor used to leave file_
        /// null and file_size_ zero on a failed open, which made an UNREADABLE segment
        /// indistinguishable from an EMPTY one at every accessor: page_count() answered 0
        /// and read_all_records() answered {}. truncate_before read the first as "safe to
        /// delete" and unlinked the file; startup replay read the second as "this segment
        /// held nothing" and came up without every committed transaction that lived in it.
        /// is_open()/open_error() and the wrapper on read_all_records are what separate the
        /// two cases now.
        ///
        /// resource backs the decoded records AND the diagnostic in open_error(); it
        /// replaces the std::pmr::get_default_resource() this class used to reach for
        /// (rule 14).
        wal_page_reader_t(std::pmr::memory_resource* resource, const std::filesystem::path& segment_path);

        /// True when the segment file is open and readable.
        [[nodiscard]] bool is_open() const noexcept { return file_ != nullptr; }

        /// The reason the segment could not be opened; no_error() when it is open.
        [[nodiscard]] const core::error_t& open_error() const noexcept { return open_error_; }

        /// Read all records with wal_id > after_id.
        /// Stops at the first corrupted page (STOP-A behavior).
        /// Refuses with io_error when the segment could not be opened at all — an empty
        /// vector means "this segment holds no record past after_id", never "unreadable".
        core::result_wrapper_t<std::vector<record_t>> read_all_records(id_t after_id);

        /// Read the page header at a given page index.
        /// Page 0 is the file header; data pages start at index 1.
        /// TEST-ONLY OBSERVER: answers a ZEROED header for an unreadable page. Production
        /// decisions go through read_verified_page_header, which cannot answer zeros.
        wal_page_header_t read_page_header(size_t page_index);

        /// Read ONE page and answer its header only when the page's checksum vouches for it —
        /// the header comes from the same bytes the checksum verified, so there is no window
        /// between "the page verifies" and "here is its header" in which a failed re-read can
        /// substitute zeros. False covers both "unreadable" and "does not verify"; either way
        /// the page's fields are unknown and the caller must not act on a guess.
        [[nodiscard]] bool read_verified_page_header(size_t page_index, wal_page_header_t& out);

        /// What one physical pass over every data page says about this segment.
        ///
        /// THIS ANSWERS A DIFFERENT QUESTION FROM read_all_records, and conflating the two is
        /// what let a CRC break move the id allocator BACKWARDS. read_all_records stops at the
        /// first broken page (STOP-A) because replay must not apply a range with a hole in it;
        /// but "where may the allocator resume" is a high-water mark over what the FILE
        /// physically holds, and the pages past the break hold ids just as firmly as the ones
        /// before it.
        struct segment_scan_t {
            /// Highest page_end_lsn over the data pages whose checksum VERIFIES — including
            /// pages after a corruption point. A page whose checksum fails is not trusted
            /// here either: its header is precisely what the checksum failed to vouch for, so
            /// a flipped byte in page_end_lsn would otherwise drag the allocator to garbage.
            id_t highest_page_end_lsn{0};
            /// False when at least one data page failed its checksum or could not be read.
            bool chain_intact{true};
            /// 1-based index of the first data page that failed; 0 when none did.
            size_t first_broken_page{0};
            /// Data pages that still verify BEYOND first_broken_page. Zero is the ordinary
            /// crash-torn tail — replay loses nothing by stopping. Non-zero means committed
            /// transactions sit past the break that replay will not reach.
            size_t verified_pages_after_break{0};

            // THE TWO FIELDS BELOW ANSWER A THIRD QUESTION: NOT "how far did replay get" and
            // not "where may the allocator resume", but "WHAT DID THE BREAK HIDE" — where
            // does the next id the segment can still vouch for sit, so a caller can tell an
            // interval it merely stopped short of from an interval it SKIPPED OVER. Nothing
            // above reads them; the three existing consumers (recover_from_disk,
            // wal_reader_t, manager_wal_replicate_t's ctor) keep the exact meanings they had.

            /// page_lsn of the first data page that verifies AFTER first_broken_page; 0 when
            /// nothing verifies past the break (the ordinary torn tail) or when there is no
            /// break at all. Non-zero means read_all_records stopped short of ids this same
            /// segment still holds.
            id_t first_verified_lsn_after_break{0};
            /// page_lsn of the FIRST data page of this segment that verifies at all; 0 for an
            /// unopened, empty, or wholly unverifiable segment. This is what CLOSES a hole
            /// that opened in an EARLIER segment — a break on a segment's last page hides
            /// nothing that segment can show, and only the next segment's first id reveals
            /// the jump.
            id_t first_verified_page_lsn{0};
        };

        /// One pass over every data page. verify_chain() is this scan's chain_intact.
        segment_scan_t scan_pages();

        /// Verify CRC of every data page. Returns true if all are valid.
        bool verify_chain();

        /// Verify the checksum of a specific page.
        bool verify_page_checksum(size_t page_index);

        /// Number of data pages (excluding the file header page).
        /// Zero on an unopened segment too — callers that act on emptiness MUST consult
        /// is_open() first.
        size_t page_count() const;

    private:
        /// Read a full page into buf. Returns false on read error.
        bool read_page(size_t page_index, char* buf);

        std::pmr::memory_resource* resource_;
        std::filesystem::path path_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        size_t file_size_{0};
        core::error_t open_error_;
    };

} // namespace services::wal
