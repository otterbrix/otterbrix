#include "wal_page_reader.hpp"
#include "wal_binary.hpp"

#include <algorithm>
#include <cstring>

namespace services::wal {

wal_page_reader_t::wal_page_reader_t(const std::filesystem::path& segment_path)
    : path_(segment_path) {
    auto flags = core::filesystem::file_flags::READ;
    file_ = core::filesystem::open_file(fs_, path_, flags, core::filesystem::file_lock_type::NO_LOCK);
    if (file_) {
        file_size_ = file_->file_size();
    }
}

bool wal_page_reader_t::read_page(size_t page_index, char* buf) {
    uint64_t offset = static_cast<uint64_t>(page_index) * PAGE_SIZE;
    if (offset + PAGE_SIZE > file_size_) {
        return false;
    }
    return file_->read(buf, static_cast<uint64_t>(PAGE_SIZE), offset);
}

wal_page_header_t wal_page_reader_t::read_page_header(size_t page_index) {
    wal_page_header_t hdr;
    std::memset(&hdr, 0, sizeof(hdr));

    uint64_t offset = static_cast<uint64_t>(page_index) * PAGE_SIZE;
    if (offset + PAGE_HEADER_SIZE > file_size_) {
        return hdr;
    }
    file_->read(&hdr, static_cast<uint64_t>(PAGE_HEADER_SIZE), offset);
    return hdr;
}

size_t wal_page_reader_t::page_count() const {
    if (file_size_ <= PAGE_SIZE) {
        return 0; // only file header or empty
    }
    return (file_size_ / PAGE_SIZE) - 1; // subtract file header page
}

bool wal_page_reader_t::verify_page_checksum(size_t page_index) {
    alignas(4096) char page_buf[PAGE_SIZE];
    if (!read_page(page_index, page_buf)) {
        return false;
    }
    wal_page_header_t hdr;
    std::memcpy(&hdr, page_buf, PAGE_HEADER_SIZE);
    return hdr.verify_checksum(page_buf);
}

bool wal_page_reader_t::verify_chain() {
    size_t count = page_count();
    for (size_t i = 0; i < count; ++i) {
        if (!verify_page_checksum(i + 1)) { // data pages start at index 1
            return false;
        }
    }
    return true;
}

wal_page_position_t wal_page_reader_t::seek_to_lsn(id_t target_lsn) {
    size_t count = page_count();
    if (count == 0) {
        return {0};
    }

    // Binary search over data pages (indices 1..count).
    // Each page has page_lsn (first LSN) and page_end_lsn (last LSN).
    // We want the page where page_lsn <= target_lsn <= page_end_lsn.
    size_t lo = 1;
    size_t hi = count; // inclusive page index = count (last data page)
    size_t result = 1;

    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        auto hdr = read_page_header(mid);

        if (hdr.page_lsn <= target_lsn) {
            result = mid;
            if (hdr.page_end_lsn >= target_lsn) {
                // Found exact page.
                break;
            }
            lo = mid + 1;
        } else {
            if (mid == 0) {
                break;
            }
            hi = mid - 1;
        }
    }

    return {result};
}

std::vector<record_t> wal_page_reader_t::read_all_records(id_t after_id) {
    std::vector<record_t> records;
    size_t count = page_count();
    if (count == 0) {
        return records;
    }

    auto* resource = std::pmr::get_default_resource();

    // Buffer for accumulating spanning records.
    std::vector<char> span_buffer;
    bool in_span = false;

    alignas(4096) char page_buf[PAGE_SIZE];

    for (size_t pi = 1; pi <= count; ++pi) {
        if (!read_page(pi, page_buf)) {
            break; // read error -- stop
        }

        // Verify checksum. Stop at corruption (STOP-A).
        wal_page_header_t hdr;
        std::memcpy(&hdr, page_buf, PAGE_HEADER_SIZE);
        if (!hdr.verify_checksum(page_buf)) {
            break;
        }

        const char* data = page_buf + PAGE_HEADER_SIZE;
        uint32_t data_size = hdr.data_size;

        bool is_cont = (hdr.flags & PAGE_PARTIAL_CONT) != 0;
        bool is_end = (hdr.flags & PAGE_PARTIAL_END) != 0;

        // ---- Spanning record: middle page (continuation, not the end) ----
        if (in_span && is_cont && !is_end) {
            span_buffer.insert(span_buffer.end(), data, data + data_size);
            continue;
        }

        // ---- Spanning record: continuation/last page ----
        if (in_span && (is_cont || is_end)) {
            // Determine how many bytes we still need.
            uint32_t needed = data_size;
            if (span_buffer.size() >= 4) {
                uint32_t body_size = 0;
                std::memcpy(&body_size, span_buffer.data(), sizeof(uint32_t));
                uint32_t total_record = body_size + 8; // size(4) + body + crc(4)
                if (total_record > span_buffer.size()) {
                    needed = static_cast<uint32_t>(total_record - span_buffer.size());
                } else {
                    needed = 0;
                }
            }

            uint32_t to_take = std::min(needed, data_size);
            span_buffer.insert(span_buffer.end(), data, data + to_take);

            // Check if we have the complete record now.
            if (span_buffer.size() >= 4) {
                uint32_t body_size = 0;
                std::memcpy(&body_size, span_buffer.data(), sizeof(uint32_t));
                uint32_t total_record = body_size + 8;
                if (span_buffer.size() >= total_record) {
                    auto rec = decode_record(span_buffer.data(), total_record, resource);
                    if (rec.is_valid() && rec.id > after_id) {
                        records.push_back(std::move(rec));
                    }
                    span_buffer.clear();
                    in_span = false;

                    // Parse remaining complete records after the spanning data.
                    size_t offset = to_take;
                    while (offset < data_size) {
                        if (offset + sizeof(uint32_t) > data_size) {
                            break;
                        }
                        uint32_t rec_size = 0;
                        std::memcpy(&rec_size, data + offset, sizeof(uint32_t));
                        if (rec_size == 0) {
                            break;
                        }
                        uint32_t total_rec_bytes = rec_size + 8;
                        if (offset + total_rec_bytes > data_size) {
                            break;
                        }
                        auto r = decode_record(data + offset, total_rec_bytes, resource);
                        if (r.is_valid() && r.id > after_id) {
                            records.push_back(std::move(r));
                        }
                        offset += total_rec_bytes;
                    }
                }
            }
            continue;
        }

        // ---- Not currently in a span ----
        if (in_span) {
            // Unexpected: we were accumulating but this page doesn't continue.
            span_buffer.clear();
            in_span = false;
        }

        // Parse individual complete records from this page.
        // Each record is: [size:4][payload:size][crc:4], total = size + 8 bytes.
        size_t offset = 0;
        while (offset < data_size) {
            if (offset + sizeof(uint32_t) > data_size) {
                break;
            }

            uint32_t rec_size = 0;
            std::memcpy(&rec_size, data + offset, sizeof(uint32_t));

            if (rec_size == 0) {
                break; // zero-length: end of valid data in this page
            }

            uint32_t total_record_bytes = rec_size + 8;

            if (offset + total_record_bytes > data_size) {
                // This record doesn't fit -- it's the start of a spanning record.
                break;
            }

            auto rec = decode_record(data + offset, total_record_bytes, resource);
            if (rec.is_valid() && rec.id > after_id) {
                records.push_back(std::move(rec));
            }
            // Even if invalid/filtered, advance past this record.
            offset += total_record_bytes;
        }

        // If this page has PAGE_PARTIAL_CONT, the data from offset onward
        // is the start of a spanning record that continues to the next page(s).
        if (is_cont) {
            if (offset < data_size) {
                span_buffer.assign(data + offset, data + data_size);
            }
            in_span = true;
        }
    }

    return records;
}

} // namespace services::wal
