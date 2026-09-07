#include "wal_page_writer.hpp"

#include <cstring>
#include <filesystem>

namespace services::wal {

    wal_page_writer_t::wal_page_writer_t(std::pmr::memory_resource* resource,
                                         const std::filesystem::path& path,
                                         const std::string& db_name,
                                         uint32_t seg_index,
                                         size_t /*max_seg_sz*/)
        : resource_(resource)
        , path_(path)
        , database_name_(db_name)
        , segment_index_(seg_index)
        , open_error_(core::error_t::no_error())
        , last_error_(core::error_t::no_error()) {
        auto parent = path_.parent_path();
        if (!parent.empty()) {
            std::filesystem::create_directories(parent);
        }

        auto flags = core::filesystem::file_flags::WRITE | core::filesystem::file_flags::READ |
                     core::filesystem::file_flags::FILE_CREATE;
        file_ = core::filesystem::open_file(fs_, path_, flags, core::filesystem::file_lock_type::NO_LOCK);
#ifdef DEV_MODE
        if (auto* interposer = dev_wal_file_interposer(); interposer != nullptr) {
            file_ = interposer->wrap(path_, std::move(file_));
        }
#endif
        if (!file_) {
            // Refuse now, before write_file_header() below dereferences this null handle.
            open_error_ = io_failure("wal segment could not be opened for writing");
            return;
        }

        std::error_code ec;
        auto existing_size = std::filesystem::file_size(path_, ec);
        if (!ec && existing_size > PAGE_SIZE) {
            // Existing segment: round down to a page boundary in case the trailing page is partial.
            file_size_ = (existing_size / PAGE_SIZE) * PAGE_SIZE;
        } else {
            if (auto header_error = write_file_header(); header_error.contains_error()) {
                open_error_ = header_error;
                return;
            }
        }

        start_new_page();
    }

    wal_page_writer_t::~wal_page_writer_t() {
        // A destructor can't report failure, so the refusal is latched into last_error_; the
        // engine never leans on this, since wal_worker_t always flushes explicitly first, so
        // has_data_ is false here on every path that carries durability.
        if (has_data_) {
            last_error_ = flush_page();
        }
    }

    core::error_t wal_page_writer_t::io_failure(const char* what) const {
        std::pmr::string message{resource_};
        message.append(what);
        message.append(": ");
        message.append(path_.string());
        return core::error_t(core::error_code_t::io_error, std::move(message));
    }

    core::error_t wal_page_writer_t::append(const char* data, size_t size, id_t wal_id) {
        if (open_error_.contains_error()) {
            return open_error_;
        }
        if (size == 0) {
            return core::error_t::no_error();
        }

        // Snapshot for rollback: a refused flush must not leave a CONT-flagged prefix behind, or
        // the next appended record gets swallowed into a span no reader completes.
        const auto saved_offset = current_offset_;
        const auto saved_flags = page_flags_;
        const auto saved_page_lsn = page_lsn_;
        const auto saved_page_end_lsn = page_end_lsn_;
        const auto saved_num_records = num_records_;
        const auto saved_has_data = has_data_;

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
                std::memcpy(current_page_ + current_offset_, src, remaining);
                current_offset_ += remaining;
                has_data_ = true;

                if (!is_first_chunk) {
                    page_flags_ |= PAGE_PARTIAL_END;
                }

                if (is_first_chunk) {
                    num_records_++;
                }
                remaining = 0;
            } else {
                std::memcpy(current_page_ + current_offset_, src, space);
                current_offset_ += space;
                src += space;
                remaining -= space;
                has_data_ = true;

                page_flags_ |= PAGE_PARTIAL_CONT;

                // Not counted in num_records_ — the record is only partial here.

                if (auto page_error = flush_page(); page_error.contains_error()) {
                    // First chunk restores the snapshot; a later chunk instead discards the page
                    // and sets torn_tail_, so the owner rotates rather than resume onto a
                    // PARTIAL_CONT page.
                    if (is_first_chunk) {
                        current_offset_ = saved_offset;
                        page_flags_ = saved_flags;
                        page_lsn_ = saved_page_lsn;
                        page_end_lsn_ = saved_page_end_lsn;
                        num_records_ = saved_num_records;
                        has_data_ = saved_has_data;
                    } else {
                        start_new_page();
                        torn_tail_ = true;
                    }
                    return page_error;
                }

                start_new_page();

                page_flags_ |= PAGE_PARTIAL_CONT;
                page_lsn_ = wal_id;
                page_end_lsn_ = wal_id;

                is_first_chunk = false;
            }
        }

        return core::error_t::no_error();
    }

    core::error_t wal_page_writer_t::flush() {
        if (open_error_.contains_error()) {
            return open_error_;
        }
        if (has_data_) {
            return flush_page();
        }
        return core::error_t::no_error();
    }

    core::error_t wal_page_writer_t::flush_and_sync() {
        if (auto flush_error = flush(); flush_error.contains_error()) {
            return flush_error;
        }
        if (file_ && !file_->sync()) {
            // Dropping this answer let commit_txn under wal_sync_mode::FULL report a durable
            // commit over a page that never reached the device.
            return io_failure("fsync of the wal segment failed");
        }
        return core::error_t::no_error();
    }

    std::filesystem::path wal_page_writer_t::current_segment_path() const { return path_; }

    core::error_t wal_page_writer_t::write_file_header() {
        alignas(4096) char header_page[PAGE_SIZE];
        std::memset(header_page, 0, PAGE_SIZE);

        wal_file_header_t hdr;
        hdr.init(segment_index_, database_name_);

        std::memcpy(header_page, &hdr, sizeof(hdr));

        auto written =
            file_->write(static_cast<void*>(header_page), static_cast<uint64_t>(PAGE_SIZE), static_cast<uint64_t>(0));
        if (!written) {
            return io_failure("wal segment file header could not be written");
        }

        file_size_ = PAGE_SIZE;
        return core::error_t::no_error();
    }

    core::error_t wal_page_writer_t::flush_page() {
        if (!has_data_ && num_records_ == 0 && (page_flags_ & PAGE_PARTIAL_CONT) == 0) {
            return core::error_t::no_error(); // nothing to flush
        }
        if (!file_) {
            return open_error_.contains_error() ? open_error_ : io_failure("wal segment is not open for writing");
        }

        if (current_offset_ < PAGE_SIZE) {
            std::memset(current_page_ + current_offset_, 0, PAGE_SIZE - current_offset_);
        }

        wal_page_header_t hdr;
        std::memset(&hdr, 0, sizeof(hdr));
        hdr.page_lsn = page_lsn_;
        hdr.page_end_lsn = page_end_lsn_;
        hdr.num_records = num_records_;
        hdr.data_size = static_cast<uint32_t>(current_offset_ - PAGE_HEADER_SIZE);
        hdr.flags = page_flags_;
        hdr.checksum = 0;
        hdr.reserved = 0;

        std::memcpy(current_page_, &hdr, PAGE_HEADER_SIZE);
        hdr.compute_checksum(current_page_);
        // compute_checksum writes the final header (with checksum) back into current_page_

        auto ok = file_->write(static_cast<void*>(current_page_),
                               static_cast<uint64_t>(PAGE_SIZE),
                               static_cast<uint64_t>(file_size_));
        if (!ok) {
            return io_failure("wal page could not be written");
        }

        file_size_ += PAGE_SIZE;

        start_new_page();
        return core::error_t::no_error();
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
