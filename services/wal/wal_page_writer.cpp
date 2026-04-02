#include "wal_page_writer.hpp"

#include <cstring>
#include <filesystem>

namespace services::wal {

wal_page_writer_t::wal_page_writer_t(const std::filesystem::path& path,
                                     const std::string& db_name,
                                     uint32_t seg_index,
                                     size_t max_seg_sz)
    : path_(path)
    , database_name_(db_name)
    , segment_index_(seg_index)
    , max_segment_size_(max_seg_sz) {
    // Ensure parent directory exists.
    auto parent = path_.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }

    // Open segment file.
    auto flags = core::filesystem::file_flags::WRITE | core::filesystem::file_flags::READ |
                 core::filesystem::file_flags::FILE_CREATE;
    file_ = core::filesystem::open_file(fs_, path_, flags, core::filesystem::file_lock_type::NO_LOCK);

    // Check if the file already has content (reopening an existing segment).
    std::error_code ec;
    auto existing_size = std::filesystem::file_size(path_, ec);
    if (!ec && existing_size > PAGE_SIZE) {
        // Existing segment file -- resume appending after the last written page.
        // Round down to a page boundary to handle any partial trailing page.
        file_size_ = (existing_size / PAGE_SIZE) * PAGE_SIZE;
    } else {
        // New segment file -- write the file header (page 0).
        write_file_header();
    }

    // Initialize the first data page.
    start_new_page();
}

wal_page_writer_t::~wal_page_writer_t() {
    // Flush any pending data on destruction.
    if (has_data_) {
        flush_page();
    }
}

bool wal_page_writer_t::append(const char* data, size_t size, id_t wal_id) {
    if (size == 0) {
        return true;
    }

    // Track the first LSN in the current page.
    if (page_lsn_ == 0) {
        page_lsn_ = wal_id;
    }
    page_end_lsn_ = wal_id;

    size_t remaining = size;
    const char* src = data;
    bool is_first_chunk = true;

    while (remaining > 0) {
        size_t space = PAGE_DATA_SIZE - (current_offset_ - PAGE_HEADER_SIZE);

        if (remaining <= space) {
            // Record (or remainder) fits in the current page.
            std::memcpy(current_page_ + current_offset_, src, remaining);
            current_offset_ += remaining;
            has_data_ = true;

            if (!is_first_chunk) {
                // This is the last page of a spanning record.
                page_flags_ |= PAGE_PARTIAL_END;
            }

            if (is_first_chunk) {
                // Entire record fits in one page -- counted as a complete record.
                num_records_++;
            }
            remaining = 0;
        } else {
            // Fill the rest of the current page.
            std::memcpy(current_page_ + current_offset_, src, space);
            current_offset_ += space;
            src += space;
            remaining -= space;
            has_data_ = true;

            // Mark current page: record data continues past this page.
            page_flags_ |= PAGE_PARTIAL_CONT;

            // Record starts here but does not finish — it's partial.
            // Do NOT count it in num_records (only complete records counted).

            // Flush the full page.
            if (!flush_page()) {
                return false;
            }

            // Start a new page for the continuation.
            start_new_page();

            // The new page continues a spanning record.
            page_flags_ |= PAGE_PARTIAL_CONT;
            page_lsn_ = wal_id;
            page_end_lsn_ = wal_id;

            is_first_chunk = false;
        }
    }

    return true;
}

bool wal_page_writer_t::flush() {
    if (has_data_) {
        return flush_page();
    }
    return true;
}

bool wal_page_writer_t::flush_and_sync() {
    if (!flush()) {
        return false;
    }
    if (file_) {
        file_->sync();
    }
    return true;
}

std::filesystem::path wal_page_writer_t::current_segment_path() const {
    return path_;
}

bool wal_page_writer_t::write_file_header() {
    // Prepare a full page-sized buffer for the file header.
    alignas(4096) char header_page[PAGE_SIZE];
    std::memset(header_page, 0, PAGE_SIZE);

    wal_file_header_t hdr;
    hdr.init(segment_index_, database_name_);

    // Copy the header struct into the page buffer.
    std::memcpy(header_page, &hdr, sizeof(hdr));

    // Write at position 0.
    auto written = file_->write(static_cast<void*>(header_page),
                                static_cast<uint64_t>(PAGE_SIZE),
                                static_cast<uint64_t>(0));
    if (!written) {
        return false;
    }

    file_size_ = PAGE_SIZE;
    return true;
}

bool wal_page_writer_t::flush_page() {
    if (!has_data_ && num_records_ == 0 && (page_flags_ & PAGE_PARTIAL_CONT) == 0) {
        return true; // nothing to flush
    }

    // Zero out unused portion of the page.
    if (current_offset_ < PAGE_SIZE) {
        std::memset(current_page_ + current_offset_, 0, PAGE_SIZE - current_offset_);
    }

    // Fill in the page header.
    wal_page_header_t hdr;
    std::memset(&hdr, 0, sizeof(hdr));
    hdr.page_lsn = page_lsn_;
    hdr.page_end_lsn = page_end_lsn_;
    hdr.num_records = num_records_;
    hdr.data_size = static_cast<uint32_t>(current_offset_ - PAGE_HEADER_SIZE);
    hdr.flags = page_flags_;
    hdr.checksum = 0;
    hdr.reserved = 0;

    // Write header into the page buffer, then compute checksum over the whole page.
    std::memcpy(current_page_, &hdr, PAGE_HEADER_SIZE);
    hdr.compute_checksum(current_page_);
    // compute_checksum writes the final header (with checksum) back into current_page_

    // Write the full page to file.
    auto ok = file_->write(static_cast<void*>(current_page_),
                           static_cast<uint64_t>(PAGE_SIZE),
                           static_cast<uint64_t>(file_size_));
    if (!ok) {
        return false;
    }

    file_size_ += PAGE_SIZE;

    // Prepare for the next page.
    start_new_page();
    return true;
}

void wal_page_writer_t::start_new_page() {
    std::memset(current_page_, 0, PAGE_SIZE);
    current_offset_ = PAGE_HEADER_SIZE;
    num_records_ = 0;
    page_lsn_ = 0;
    page_end_lsn_ = 0;
    page_flags_ = PAGE_NORMAL;
    has_data_ = false;
}

} // namespace services::wal
