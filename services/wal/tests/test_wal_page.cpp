#include <catch2/catch.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <fstream>
#include <services/wal/wal_page.hpp>
#include <services/wal/wal_page_writer.hpp>
#include <services/wal/wal_page_reader.hpp>
#include <services/wal/wal_binary.hpp>
#include <components/tests/generaty.hpp>

namespace {

    // RAII temp directory helper
    struct tmp_dir_t {
        std::filesystem::path path;

        explicit tmp_dir_t(const std::string& name)
            : path(std::filesystem::temp_directory_path() / name) {
            std::filesystem::create_directories(path);
        }

        ~tmp_dir_t() {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }

        tmp_dir_t(const tmp_dir_t&) = delete;
        tmp_dir_t& operator=(const tmp_dir_t&) = delete;

        std::filesystem::path file(const std::string& filename) const {
            return path / filename;
        }
    };

    using namespace services::wal;

    // Helper to encode a COMMIT record into a binary buffer.
    // Returns the encoded buffer and the wal_id assigned.
    struct encoded_record_info {
        std::vector<char> data;
        uint64_t wal_id;
        uint64_t txn_id;
        wal_record_type type;
    };

    // Helper: convert a buffer_t (std::pmr::string) to std::vector<char>.
    std::vector<char> buffer_to_vec(const buffer_t& buf) {
        return std::vector<char>(buf.begin(), buf.end());
    }

    encoded_record_info encode_commit_rec(uint64_t wal_id, uint64_t txn_id, crc32_t last_crc) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::COMMIT;
        buffer_t buf;
        services::wal::encode_commit(buf, last_crc, wal_id, txn_id);
        info.data = buffer_to_vec(buf);
        return info;
    }

    encoded_record_info encode_insert_rec(uint64_t wal_id,
                                      uint64_t txn_id,
                                      crc32_t last_crc,
                                      const std::string& database,
                                      const std::string& collection,
                                      const components::vector::data_chunk_t& chunk,
                                      uint64_t row_start,
                                      uint64_t row_count) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::PHYSICAL_INSERT;
        buffer_t buf;
        services::wal::encode_insert(buf, std::pmr::get_default_resource(), last_crc, wal_id, txn_id, database, collection, chunk, row_start, row_count);
        info.data = buffer_to_vec(buf);
        return info;
    }

    encoded_record_info encode_delete_rec(uint64_t wal_id,
                                      uint64_t txn_id,
                                      crc32_t last_crc,
                                      const std::string& database,
                                      const std::string& collection,
                                      const std::pmr::vector<int64_t>& row_ids,
                                      uint64_t count) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::PHYSICAL_DELETE;
        buffer_t buf;
        services::wal::encode_delete(buf, last_crc, wal_id, txn_id, database, collection, row_ids.data(), count);
        info.data = buffer_to_vec(buf);
        return info;
    }

    encoded_record_info encode_update_rec(uint64_t wal_id,
                                      uint64_t txn_id,
                                      crc32_t last_crc,
                                      const std::string& database,
                                      const std::string& collection,
                                      const std::pmr::vector<int64_t>& row_ids,
                                      const components::vector::data_chunk_t& chunk,
                                      uint64_t count) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::PHYSICAL_UPDATE;
        buffer_t buf;
        services::wal::encode_update(buf, std::pmr::get_default_resource(), last_crc, wal_id, txn_id, database, collection, row_ids.data(), chunk, count);
        info.data = buffer_to_vec(buf);
        return info;
    }

} // anonymous namespace

TEST_CASE("small_records_fill_page") {
    tmp_dir_t dir("test_wal_page_small_records");
    auto filepath = dir.file("wal_segment_0");
    auto* resource = std::pmr::get_default_resource();

    // Create writer and write 5 small COMMIT records (29 bytes each = 145 bytes total).
    {
        wal_page_writer_t writer(filepath, "testdb", 0);

        crc32_t last_crc = 0;
        uint64_t first_wal_id = 1;
        uint64_t last_wal_id = 5;

        for (uint64_t i = first_wal_id; i <= last_wal_id; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/100 + i, last_crc);
            writer.append(rec.data.data(), rec.data.size(), i);
            // Extract the trailing CRC from the encoded data for chaining
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        writer.flush();
    }

    // Read back with page reader.
    {
        wal_page_reader_t reader(filepath);

        // Page 0 is the file header. Page 1 is the first data page.
        auto header = reader.read_page_header(1);

        REQUIRE(header.num_records == 5);
        // 5 COMMIT records at 29 bytes each = 145 bytes.
        // data_size should be close to 145 (exact value depends on encoding).
        REQUIRE(header.data_size >= 140);
        REQUIRE(header.data_size <= 160);
        REQUIRE(header.page_lsn == 1);
        REQUIRE(header.page_end_lsn == 5);
    }
}

TEST_CASE("large_record_spanning") {
    tmp_dir_t dir("test_wal_page_large_record");
    auto filepath = dir.file("wal_segment_0");
    auto* resource = std::pmr::get_default_resource();

    // Generate a large data_chunk with 500 rows -- should exceed PAGE_DATA_SIZE (4064 bytes).
    auto chunk = gen_data_chunk(500, resource);

    {
        wal_page_writer_t writer(filepath, "testdb", 0);

        auto rec = encode_insert_rec(/*wal_id=*/1, /*txn_id=*/42, /*last_crc=*/0,
                                 "testdb", "large_collection", chunk, 0, 500);

        // Confirm the encoded record is larger than one page's data area.
        REQUIRE(rec.data.size() > PAGE_DATA_SIZE);

        writer.append(rec.data.data(), rec.data.size(), 1);
        writer.flush();
    }

    // Read back and verify spanning.
    {
        wal_page_reader_t reader(filepath);

        // The file should have: file_header (page 0) + at least 2 data pages.
        auto file_size = std::filesystem::file_size(filepath);
        auto total_pages = file_size / PAGE_SIZE;
        REQUIRE(total_pages >= 3); // file header + at least 2 data pages

        // First data page should have PARTIAL flag set.
        auto header1 = reader.read_page_header(1);
        REQUIRE((header1.flags & PAGE_PARTIAL_CONT) != 0);

        // Last data page of this record should have PARTIAL_END flag.
        // Walk pages to find the one with PARTIAL_END.
        bool found_partial_end = false;
        for (uint64_t p = 2; p < total_pages; ++p) {
            auto h = reader.read_page_header(p);
            if ((h.flags & PAGE_PARTIAL_END) != 0) {
                found_partial_end = true;
                break;
            }
        }
        REQUIRE(found_partial_end);
    }
}

TEST_CASE("read_back_all_records") {
    tmp_dir_t dir("test_wal_page_read_back");
    auto filepath = dir.file("wal_segment_0");
    auto* resource = std::pmr::get_default_resource();

    auto small_chunk = gen_data_chunk(5, resource);
    std::pmr::vector<int64_t> row_ids(resource);
    for (int64_t i = 0; i < 5; ++i) {
        row_ids.push_back(i);
    }

    // Write 20 mixed records.
    struct written_record {
        uint64_t wal_id;
        uint64_t txn_id;
        wal_record_type type;
    };
    std::vector<written_record> written;

    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= 20; ++i) {
            encoded_record_info rec;
            uint64_t txn_id = 200 + i;
            wal_record_type type;

            switch (i % 4) {
                case 0:
                    type = wal_record_type::COMMIT;
                    rec = encode_commit_rec(i, txn_id, last_crc);
                    break;
                case 1:
                    type = wal_record_type::PHYSICAL_INSERT;
                    rec = encode_insert_rec(i, txn_id, last_crc, "testdb", "coll", small_chunk, 0, 5);
                    break;
                case 2:
                    type = wal_record_type::PHYSICAL_DELETE;
                    rec = encode_delete_rec(i, txn_id, last_crc, "testdb", "coll", row_ids, 5);
                    break;
                case 3:
                    type = wal_record_type::PHYSICAL_UPDATE;
                    rec = encode_update_rec(i, txn_id, last_crc, "testdb", "coll", row_ids, small_chunk, 5);
                    break;
            }

            writer.append(rec.data.data(), rec.data.size(), i);
            last_crc = extract_crc(rec.data.data(), rec.data.size());
            written.push_back({i, txn_id, type});
        }
        writer.flush();
    }

    // Read all records back.
    {
        wal_page_reader_t reader(filepath);
        auto records = reader.read_all_records(0);

        REQUIRE(records.size() == 20);

        for (size_t i = 0; i < records.size(); ++i) {
            REQUIRE(records[i].id == written[i].wal_id);
            REQUIRE(records[i].transaction_id == written[i].txn_id);
            REQUIRE(records[i].record_type == written[i].type);
        }
    }
}

TEST_CASE("binary_search_by_lsn") {
    tmp_dir_t dir("test_wal_page_binary_search");
    auto filepath = dir.file("wal_segment_0");

    // Write 100 COMMIT records with wal_ids 1..100.
    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= 100; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/1000 + i, last_crc);
            writer.append(rec.data.data(), rec.data.size(), i);
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        writer.flush();
    }

    // Seek to LSN 50.
    {
        wal_page_reader_t reader(filepath);
        auto position = reader.seek_to_lsn(50);

        // The position should point to a page whose page_lsn <= 50 and page_end_lsn >= 50.
        auto header = reader.read_page_header(position.page_index);
        REQUIRE(header.page_lsn <= 50);
        REQUIRE(header.page_end_lsn >= 50);

        // Read records from that position -- the first record with wal_id >= 50 should be found.
        auto records = reader.read_all_records(49); // read records after wal_id 49
        REQUIRE(!records.empty());
        REQUIRE(records.front().id == 50);
    }
}

TEST_CASE("page_checksum_corruption") {
    tmp_dir_t dir("test_wal_page_checksum_corrupt");
    auto filepath = dir.file("wal_segment_0");

    // Write some records.
    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= 10; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/300 + i, last_crc);
            writer.append(rec.data.data(), rec.data.size(), i);
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        writer.flush();
    }

    // Corrupt page 1 (first data page): flip a byte in the data area.
    {
        std::fstream file(filepath, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());

        // Page 1 starts at offset PAGE_SIZE (4096). Data area starts after page header (32 bytes).
        uint64_t corrupt_offset = PAGE_SIZE + PAGE_HEADER_SIZE + 10;
        file.seekp(static_cast<std::streamoff>(corrupt_offset));

        char byte = 0;
        file.read(&byte, 1);
        byte ^= 0xFF; // flip all bits
        file.seekp(static_cast<std::streamoff>(corrupt_offset));
        file.write(&byte, 1);
        file.flush();
    }

    // Open with reader and verify corruption is detected.
    {
        wal_page_reader_t reader(filepath);
        auto header = reader.read_page_header(1);

        // The page should be detected as corrupt (checksum mismatch).
        REQUIRE(reader.verify_page_checksum(1) == false);
    }
}

TEST_CASE("crc_chain_across_pages") {
    tmp_dir_t dir("test_wal_page_crc_chain");
    auto filepath = dir.file("wal_segment_0");
    auto* resource = std::pmr::get_default_resource();

    // Write enough records to fill 3+ pages.
    // Each COMMIT is 29 bytes, PAGE_DATA_SIZE is 4064.
    // ~140 COMMITs per page, so 500 records should give us 3-4 pages.
    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= 500; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/i * 10, last_crc);
            writer.append(rec.data.data(), rec.data.size(), i);
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        writer.flush();
    }

    // Verify we have at least 3 data pages.
    auto file_size = std::filesystem::file_size(filepath);
    auto total_pages = file_size / PAGE_SIZE;
    REQUIRE(total_pages >= 4); // file header + 3+ data pages

    // Verify the chain is valid.
    {
        wal_page_reader_t reader(filepath);
        REQUIRE(reader.verify_chain() == true);
    }
}

TEST_CASE("stop_at_corruption") {
    tmp_dir_t dir("test_wal_page_stop_corruption");
    auto filepath = dir.file("wal_segment_0");

    // Write 50 records spanning multiple pages.
    // Use slightly larger records to ensure multi-page usage.
    // 50 COMMITs at 29 bytes = 1450 bytes -- that fits in one page.
    // Use more records or larger records to span multiple pages.
    // 500 COMMITs should span several pages.
    uint64_t total_records = 500;

    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= total_records; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/i * 10, last_crc);
            writer.append(rec.data.data(), rec.data.size(), i);
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        writer.flush();
    }

    // Determine how many pages we have.
    auto file_size = std::filesystem::file_size(filepath);
    auto total_pages = file_size / PAGE_SIZE;
    REQUIRE(total_pages >= 4); // file header + at least 3 data pages

    // Read page 1 header to find how many records are in the first data page.
    uint32_t records_in_first_page = 0;
    {
        wal_page_reader_t reader(filepath);
        auto header = reader.read_page_header(1);
        records_in_first_page = header.num_records;
    }
    REQUIRE(records_in_first_page > 0);

    // Corrupt a page in the middle (page 2, the second data page).
    {
        std::fstream file(filepath, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());

        // Page 2 starts at offset 2 * PAGE_SIZE.
        uint64_t corrupt_offset = 2 * PAGE_SIZE + PAGE_HEADER_SIZE + 20;
        file.seekp(static_cast<std::streamoff>(corrupt_offset));

        char byte = 0;
        file.read(&byte, 1);
        byte ^= 0xFF;
        file.seekp(static_cast<std::streamoff>(corrupt_offset));
        file.write(&byte, 1);
        file.flush();
    }

    // Read all records -- should stop at corruption (STOP-A behavior).
    {
        wal_page_reader_t reader(filepath);
        auto records = reader.read_all_records(0);

        // Only records from page 1 (before the corrupted page 2) should be returned.
        REQUIRE(records.size() == records_in_first_page);

        // Verify the returned records are the first N records.
        for (size_t i = 0; i < records.size(); ++i) {
            REQUIRE(records[i].id == static_cast<uint64_t>(i + 1));
        }
    }
}

TEST_CASE("edge_empty_file") {
    tmp_dir_t dir("test_wal_page_empty_file");
    auto filepath = dir.file("wal_segment_0");

    // Create writer, immediately flush with no records.
    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        writer.flush();
    }

    // Reader should return 0 records.
    {
        wal_page_reader_t reader(filepath);
        auto records = reader.read_all_records(0);
        REQUIRE(records.empty());
    }
}

TEST_CASE("edge_single_record") {
    tmp_dir_t dir("test_wal_page_single_record");
    auto filepath = dir.file("wal_segment_0");

    // Write exactly one COMMIT record.
    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        auto rec = encode_commit_rec(/*wal_id=*/1, /*txn_id=*/999, /*last_crc=*/0);
        writer.append(rec.data.data(), rec.data.size(), 1);
        writer.flush();
    }

    // Read back, verify exactly 1 record.
    {
        wal_page_reader_t reader(filepath);
        auto records = reader.read_all_records(0);
        REQUIRE(records.size() == 1);
        REQUIRE(records[0].id == 1);
        REQUIRE(records[0].transaction_id == 999);
        REQUIRE(records[0].record_type == wal_record_type::COMMIT);
    }
}

TEST_CASE("edge_exact_fit") {
    tmp_dir_t dir("test_wal_page_exact_fit");
    auto filepath = dir.file("wal_segment_0");

    // Construct a record that is exactly PAGE_DATA_SIZE (4064) bytes.
    // A COMMIT record is 29 bytes:
    //   size:4 + last_crc:4 + wal_id:8 + txn_id:8 + record_type:1 + crc:4 = 29
    // An INSERT record has a fixed header of:
    //   size:4 + last_crc:4 + wal_id:8 + txn_id:8 + record_type:1 +
    //   database_len:2 + collection_len:2 + row_start:8 + row_count:8 +
    //   payload_size:4 + crc:4 = 53 bytes (without db/coll/payload strings)
    // We need: db_name + coll_name + payload + 53 = PAGE_DATA_SIZE
    // Use short db/coll names (e.g., 2 + 2 = 4 bytes for names).
    // payload = PAGE_DATA_SIZE - 53 - 4 = 4007 bytes

    // Build a synthetic binary record of exactly PAGE_DATA_SIZE bytes.
    // We rely on wal_binary::encode_insert with a carefully sized data_chunk
    // OR we construct the buffer manually.
    //
    // Strategy: create a data_chunk whose serialized payload results in
    // an encoded record of exactly PAGE_DATA_SIZE bytes.
    // Since exact sizing depends on column types and counts, we use
    // wal_binary::encode_insert and iterate to find the right chunk size.
    // For simplicity, encode with known-size data and pad accordingly.

    auto* resource = std::pmr::get_default_resource();

    // First, figure out the overhead: encode a minimal INSERT record.
    auto minimal_chunk = gen_data_chunk(1, resource);
    auto minimal_rec = encode_insert_rec(1, 1, 0, "d", "c", minimal_chunk, 0, 1);
    size_t minimal_size = minimal_rec.data.size();

    // We need a record of exactly PAGE_DATA_SIZE bytes.
    // The difference is the payload size: we need to grow the data_chunk payload.
    // Encode progressively larger chunks until we bracket the target, then pad.
    // Since this is fragile, we test with a buffer approach instead:
    // create a raw buffer of exactly PAGE_DATA_SIZE and write it through the page writer.

    // Use a synthetic approach: create a buffer of exactly PAGE_DATA_SIZE bytes
    // that looks like a valid encoded record. The key test is that the page writer
    // places it in exactly one data page with no spanning.
    std::vector<char> exact_record(PAGE_DATA_SIZE, 0);

    // Write a minimal valid header into the buffer so it can be decoded.
    // For this edge-case test, we just need to verify the page writer behavior.
    // Write the size field (first 4 bytes LE) = PAGE_DATA_SIZE - 8 (exclude size + trailing crc).
    uint32_t payload_size = PAGE_DATA_SIZE - 8;
    std::memcpy(exact_record.data(), &payload_size, sizeof(payload_size));

    // Fill in wal_id at offset 8 (after size:4 + last_crc:4).
    uint64_t wal_id = 1;
    std::memcpy(exact_record.data() + 8, &wal_id, sizeof(wal_id));

    // Write through page writer.
    {
        wal_page_writer_t writer(filepath, "testdb", 0);
        writer.append(exact_record.data(), exact_record.size(), 1);
        writer.flush();
    }

    // Verify it fits in exactly one data page (file = file_header + 1 data page).
    {
        auto file_size = std::filesystem::file_size(filepath);
        auto total_pages = file_size / PAGE_SIZE;
        // Should be exactly 2 pages: file header (page 0) + one data page (page 1).
        REQUIRE(total_pages == 2);

        wal_page_reader_t reader(filepath);
        auto header = reader.read_page_header(1);
        REQUIRE(header.data_size == PAGE_DATA_SIZE);
        REQUIRE((header.flags & PAGE_PARTIAL_CONT) == 0);
        REQUIRE((header.flags & PAGE_PARTIAL_END) == 0);
    }
}
