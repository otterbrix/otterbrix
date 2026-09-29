#include "wal_page_reader.hpp"
#include "wal_binary.hpp"

#include <algorithm>
#include <cstring>

namespace services::wal {

    wal_page_reader_t::wal_page_reader_t(std::pmr::memory_resource* resource, const std::filesystem::path& segment_path)
        : resource_(resource)
        , path_(segment_path)
        , open_error_(core::error_t::no_error()) {
        auto flags = core::filesystem::file_flags::READ;
        file_ = core::filesystem::open_file(fs_, path_, flags, core::filesystem::file_lock_type::NO_LOCK);
#ifdef DEV_MODE
        if (auto* interposer = dev_wal_file_interposer(); interposer != nullptr) {
            file_ = interposer->wrap(path_, std::move(file_));
        }
#endif
        if (file_) {
            file_size_ = file_->file_size();
            return;
        }
        // Keep the reason. Everything downstream reads zero pages from an unopened
        // segment, and without this the zero is read as "the segment is empty".
        open_error_ = core::error_t(
            core::error_code_t::io_error,
            std::pmr::string{"wal segment could not be opened for reading: " + path_.string(), resource_});
    }

    bool wal_page_reader_t::read_page(size_t page_index, char* buf) {
        uint64_t offset = static_cast<uint64_t>(page_index) * PAGE_SIZE;
        if (offset + PAGE_SIZE > file_size_) {
            return false;
        }
        return file_->read(buf, static_cast<uint64_t>(PAGE_SIZE), offset);
    }

    // TEST-ONLY OBSERVER: answers a zeroed header for a page it cannot read, deliberately —
    // no production decision hangs off it any more. truncate_before, the one caller that
    // unlinked files from this answer, reads through read_verified_page_header below, which
    // CANNOT answer zeros. The read goes through read_page so a failed read yields the whole
    // zeroed header rather than a partially filled one.
    wal_page_header_t wal_page_reader_t::read_page_header(size_t page_index) {
        wal_page_header_t hdr;
        std::memset(&hdr, 0, sizeof(hdr));

        alignas(4096) char page_buf[PAGE_SIZE];
        if (!read_page(page_index, page_buf)) {
            return hdr;
        }
        std::memcpy(&hdr, page_buf, PAGE_HEADER_SIZE);
        return hdr;
    }

    // ONE PAGE, ONE READ. The header this answers comes from the SAME bytes the checksum
    // verified. Verifying the page with one read and re-reading the header with a second — the
    // shape its caller truncate_before would otherwise take — answers a ZEROED header when
    // that second read fails, and page_end_lsn == 0 is <= every checkpoint id: the segment
    // gets unlinked for a read failure. Returning false covers both "unreadable" and "does not
    // verify"; the caller cannot tell them apart and must not: both mean "this file's bound is
    // unknown, keep it".
    bool wal_page_reader_t::read_verified_page_header(size_t page_index, wal_page_header_t& out) {
        alignas(4096) char page_buf[PAGE_SIZE];
        if (!read_page(page_index, page_buf)) {
            return false;
        }
        wal_page_header_t hdr;
        std::memcpy(&hdr, page_buf, PAGE_HEADER_SIZE);
        if (!hdr.verify_checksum(page_buf)) {
            return false;
        }
        out = hdr;
        return true;
    }

    size_t wal_page_reader_t::page_count() const {
        if (file_size_ <= PAGE_SIZE) {
            return 0; // only file header or empty
        }
        return (file_size_ / PAGE_SIZE) - 1; // subtract file header page
    }

    bool wal_page_reader_t::verify_page_checksum(size_t page_index) {
        wal_page_header_t ignored;
        return read_verified_page_header(page_index, ignored);
    }

    wal_page_reader_t::segment_scan_t wal_page_reader_t::scan_pages() {
        segment_scan_t scan;
        if (!is_open()) {
            return scan;
        }

        alignas(4096) char page_buf[PAGE_SIZE];
        const size_t count = page_count();
        for (size_t pi = 1; pi <= count; ++pi) { // data pages start at index 1
            wal_page_header_t hdr{};
            const bool readable = read_page(pi, page_buf);
            if (readable) {
                std::memcpy(&hdr, page_buf, PAGE_HEADER_SIZE);
            }
            if (!readable || !hdr.verify_checksum(page_buf)) {
                scan.chain_intact = false;
                if (scan.first_broken_page == 0) {
                    scan.first_broken_page = pi;
                }
                continue;
            }

            // The page vouches for its own header, so page_end_lsn is usable — and it is
            // usable whether or not an earlier page failed.
            if (scan.first_broken_page != 0) {
                ++scan.verified_pages_after_break;
                if (scan.first_verified_lsn_after_break == 0) {
                    scan.first_verified_lsn_after_break = hdr.page_lsn;
                }
            }
            if (scan.first_verified_page_lsn == 0) {
                scan.first_verified_page_lsn = hdr.page_lsn;
            }
            if (hdr.page_end_lsn > scan.highest_page_end_lsn) {
                scan.highest_page_end_lsn = hdr.page_end_lsn;
            }
        }
        return scan;
    }

    // The chain answer is one field of the scan above; keeping a second loop here would be a
    // second place for the two to disagree.
    bool wal_page_reader_t::verify_chain() { return scan_pages().chain_intact; }

    core::error_t wal_page_reader_t::malformed(size_t page_index, const char* what) const {
        std::pmr::string message{resource_};
        message.append("wal segment ");
        message.append(path_.string());
        message.append(", data page ");
        message.append(std::to_string(page_index));
        message.append(" verifies its checksum but ");
        message.append(what);
        return core::error_t(core::error_code_t::data_corruption, std::move(message));
    }

    // The data areas of consecutive pages form one stream of [size:4][body][crc:4] records; a record that does not
    // fit spills into the next page. PARTIAL_CONT alone cannot say whether a page starts with a spill or ends with
    // one, so a page continues the previous one exactly when that page spills and this page's page_lsn names the
    // spilled record (ids are unique, and a page appended after a restart starts past every id already written).
    core::result_wrapper_t<std::vector<record_t>> wal_page_reader_t::read_all_records(id_t after_id) {
        // AN UNREADABLE SEGMENT IS NOT AN EMPTY ONE. Returning {} here is what made every
        // committed transaction living in this segment disappear from startup replay in
        // silence; the caller now has to look at the refusal before it looks at the rows.
        if (!is_open()) {
            return open_error_;
        }

        std::vector<record_t> records;
        const size_t count = page_count();

        std::pmr::vector<char> span(resource_);
        bool prev_spills = false;
        id_t prev_end_lsn = 0;

        alignas(4096) char page_buf[PAGE_SIZE];

        for (size_t pi = 1; pi <= count; ++pi) {
            if (!read_page(pi, page_buf)) {
                break; // read error -- stop
            }
            wal_page_header_t hdr;
            std::memcpy(&hdr, page_buf, PAGE_HEADER_SIZE);
            if (!hdr.verify_checksum(page_buf)) {
                break; // STOP-A
            }
            if (hdr.data_size > PAGE_DATA_SIZE || (hdr.flags & ~(PAGE_PARTIAL_CONT | PAGE_PARTIAL_END)) != 0) {
                return malformed(pi, "its header is out of range");
            }

            const char* data = page_buf + PAGE_HEADER_SIZE;
            const size_t data_size = hdr.data_size;
            const bool spills = (hdr.flags & PAGE_PARTIAL_CONT) != 0;
            const bool ends_span = (hdr.flags & PAGE_PARTIAL_END) != 0;
            const bool continues = prev_spills && hdr.page_lsn == prev_end_lsn;

            auto take = [&](const char* rec, size_t size) -> core::error_t {
                auto r = decode_record(rec, size, resource_);
                if (!r.is_valid()) {
                    return malformed(pi, "holds a record that does not decode");
                }
                if (r.id > after_id) {
                    records.push_back(std::move(r));
                }
                return core::error_t::no_error();
            };

            size_t offset = 0;
            if (!continues) {
                if (ends_span) {
                    return malformed(pi, "ends a record no earlier page started");
                }
                // A crash cut the spilled record short and this page was appended after the restart.
                span.clear();
            } else {
                if (span.size() < sizeof(uint32_t)) {
                    offset = std::min(sizeof(uint32_t) - span.size(), data_size);
                    span.insert(span.end(), data, data + offset);
                }
                if (span.size() >= sizeof(uint32_t)) {
                    uint32_t body = 0;
                    std::memcpy(&body, span.data(), sizeof(uint32_t));
                    const size_t total = size_t{body} + 8;
                    const size_t n = std::min(total - span.size(), data_size - offset);
                    span.insert(span.end(), data + offset, data + offset + n);
                    offset += n;
                    if (span.size() == total) {
                        if (!ends_span) {
                            return malformed(pi, "completes a spilled record without PARTIAL_END");
                        }
                        if (auto err = take(span.data(), total); err.contains_error()) {
                            return err;
                        }
                        span.clear();
                    }
                }
                if (!span.empty() || offset == 0) {
                    if (offset != PAGE_DATA_SIZE || data_size != PAGE_DATA_SIZE || ends_span || !spills ||
                        hdr.num_records != 0) {
                        return malformed(pi, "neither ends the spilled record nor is wholly its middle");
                    }
                    prev_spills = true;
                    prev_end_lsn = hdr.page_end_lsn;
                    continue;
                }
            }

            uint32_t whole = 0;
            while (data_size - offset >= sizeof(uint32_t)) {
                uint32_t body = 0;
                std::memcpy(&body, data + offset, sizeof(uint32_t));
                if (body == 0) {
                    return malformed(pi, "holds a zero-length record");
                }
                const size_t total = size_t{body} + 8;
                if (total > data_size - offset) {
                    break;
                }
                if (auto err = take(data + offset, total); err.contains_error()) {
                    return err;
                }
                ++whole;
                offset += total;
            }
            if (whole != hdr.num_records) {
                return malformed(pi, "holds a different number of whole records than its header counts");
            }
            if (offset < data_size) {
                if (!spills) {
                    return malformed(pi, "has bytes after its last whole record and does not spill");
                }
                span.assign(data + offset, data + data_size);
            }
            prev_spills = spills;
            prev_end_lsn = hdr.page_end_lsn;
        }

        return records;
    }

} // namespace services::wal
