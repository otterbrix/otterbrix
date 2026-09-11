#include <catch2/catch_test_macros.hpp>
#include <components/tests/generaty.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <fstream>
#include <services/wal/wal_binary.hpp>
#include <services/wal/wal_page.hpp>
#include <services/wal/wal_page_reader.hpp>
#include <services/wal/wal_page_writer.hpp>

namespace {

    struct tmp_dir_t {
        std::filesystem::path path;

        explicit tmp_dir_t(const std::string& name)
            : path(std::filesystem::temp_directory_path() / name) {
            // Clears on construction too: a crash or aborted REQUIRE can leave the directory from a dead run.
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
            std::filesystem::create_directories(path);
        }

        ~tmp_dir_t() {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }

        tmp_dir_t(const tmp_dir_t&) = delete;
        tmp_dir_t& operator=(const tmp_dir_t&) = delete;

        std::filesystem::path file(const std::string& filename) const { return path / filename; }
    };

    using namespace services::wal;

    // wal_page_reader_t/writer_t require an explicit resource; std::pmr::get_default_resource() is forbidden here.
    core::pmr::otterbrix_resource test_resource;

    struct encoded_record_info {
        std::vector<char> data;
        uint64_t wal_id;
        uint64_t txn_id;
        wal_record_type type;
    };

    std::vector<char> buffer_to_vec(const buffer_t& buf) { return std::vector<char>(buf.begin(), buf.end()); }

    encoded_record_info encode_commit_rec(uint64_t wal_id, uint64_t txn_id, crc32_t last_crc) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::COMMIT;
        buffer_t buf;
        services::wal::encode_commit(buf, last_crc, wal_id, txn_id, /*commit_id=*/0);
        info.data = buffer_to_vec(buf);
        return info;
    }

    // table_oid is 4 bytes on the wire; this placeholder stands in for pg_class.oid in production.
    constexpr components::catalog::oid_t kTestTableOid = 16500;

    std::pmr::vector<components::vector::data_chunk_t> to_chunk_batch(const components::vector::data_chunk_t& chunk) {
        std::pmr::vector<components::vector::data_chunk_t> batch(chunk.resource());
        components::vector::data_chunk_t copy(chunk.resource(), chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
        chunk.copy(copy, 0);
        batch.emplace_back(std::move(copy));
        return batch;
    }

    encoded_record_info encode_insert_rec(uint64_t wal_id,
                                          uint64_t txn_id,
                                          crc32_t last_crc,
                                          components::catalog::oid_t table_oid,
                                          const components::vector::data_chunk_t& chunk,
                                          uint64_t row_start,
                                          uint64_t row_count) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::PHYSICAL_INSERT;
        buffer_t buf;
        services::wal::encode_insert(buf,
                                     std::pmr::get_default_resource(),
                                     last_crc,
                                     wal_id,
                                     txn_id,
                                     table_oid,
                                     to_chunk_batch(chunk),
                                     row_start,
                                     row_count);
        info.data = buffer_to_vec(buf);
        return info;
    }

    encoded_record_info encode_delete_rec(uint64_t wal_id,
                                          uint64_t txn_id,
                                          crc32_t last_crc,
                                          components::catalog::oid_t table_oid,
                                          const std::pmr::vector<int64_t>& row_ids,
                                          uint64_t count) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::PHYSICAL_DELETE;
        buffer_t buf;
        services::wal::encode_delete(buf, last_crc, wal_id, txn_id, table_oid, row_ids.data(), count);
        info.data = buffer_to_vec(buf);
        return info;
    }

    encoded_record_info encode_update_rec(uint64_t wal_id,
                                          uint64_t txn_id,
                                          crc32_t last_crc,
                                          components::catalog::oid_t table_oid,
                                          const std::pmr::vector<int64_t>& row_ids,
                                          const components::vector::data_chunk_t& chunk,
                                          uint64_t count) {
        encoded_record_info info;
        info.wal_id = wal_id;
        info.txn_id = txn_id;
        info.type = wal_record_type::PHYSICAL_UPDATE;
        buffer_t buf;
        services::wal::encode_update(buf,
                                     std::pmr::get_default_resource(),
                                     last_crc,
                                     wal_id,
                                     txn_id,
                                     table_oid,
                                     row_ids.data(),
                                     to_chunk_batch(chunk),
                                     count);
        info.data = buffer_to_vec(buf);
        return info;
    }

} // anonymous namespace

TEST_CASE("small_records_fill_page") {
    tmp_dir_t dir("test_wal_page_small_records");
    auto filepath = dir.file("wal_segment_0");

    // A COMMIT record is 37 bytes on disk: size 4 + last_crc 4 + wal_id 8 + txn_id 8 + type 1 + commit_id 8 + crc 4.
    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);

        crc32_t last_crc = 0;
        uint64_t first_wal_id = 1;
        uint64_t last_wal_id = 5;

        for (uint64_t i = first_wal_id; i <= last_wal_id; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/100 + i, last_crc);
            REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), i).contains_error());
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    // Read back with page reader.
    {
        wal_page_reader_t reader(&test_resource, filepath);

        // Page 0 is the file header; page 1 is the first data page.
        auto header = reader.read_page_header(1);

        REQUIRE(header.num_records == 5);
        // A range, not an exact byte count: the exact size depends on encoding.
        REQUIRE(header.data_size >= 180);
        REQUIRE(header.data_size <= 200);
        REQUIRE(header.page_lsn == 1);
        REQUIRE(header.page_end_lsn == 5);
    }
}

TEST_CASE("large_record_spanning") {
    tmp_dir_t dir("test_wal_page_large_record");
    auto filepath = dir.file("wal_segment_0");
    auto* resource = std::pmr::get_default_resource();

    // 500 rows is chosen to exceed PAGE_DATA_SIZE and force spanning across pages.
    auto chunk = gen_data_chunk(500, resource);

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);

        auto rec = encode_insert_rec(/*wal_id=*/1, /*txn_id=*/42, /*last_crc=*/0, kTestTableOid, chunk, 0, 500);

        REQUIRE(rec.data.size() > PAGE_DATA_SIZE);

        REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), 1).contains_error());
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    {
        wal_page_reader_t reader(&test_resource, filepath);

        auto file_size = std::filesystem::file_size(filepath);
        auto total_pages = file_size / PAGE_SIZE;
        REQUIRE(total_pages >= 3);

        auto header1 = reader.read_page_header(1);
        REQUIRE((header1.flags & PAGE_PARTIAL_CONT) != 0);

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

    struct written_record {
        uint64_t wal_id;
        uint64_t txn_id;
        wal_record_type type;
    };
    std::vector<written_record> written;

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
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
                    rec = encode_insert_rec(i, txn_id, last_crc, kTestTableOid, small_chunk, 0, 5);
                    break;
                case 2:
                    type = wal_record_type::PHYSICAL_DELETE;
                    rec = encode_delete_rec(i, txn_id, last_crc, kTestTableOid, row_ids, 5);
                    break;
                case 3:
                    type = wal_record_type::PHYSICAL_UPDATE;
                    rec = encode_update_rec(i, txn_id, last_crc, kTestTableOid, row_ids, small_chunk, 5);
                    break;
            }

            REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), i).contains_error());
            last_crc = extract_crc(rec.data.data(), rec.data.size());
            written.push_back({i, txn_id, type});
        }
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    {
        wal_page_reader_t reader(&test_resource, filepath);
        auto records_result = reader.read_all_records(0);
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();

        REQUIRE(records.size() == 20);

        for (size_t i = 0; i < records.size(); ++i) {
            REQUIRE(records[i].id == written[i].wal_id);
            REQUIRE(records[i].transaction_id == written[i].txn_id);
            REQUIRE(records[i].record_type == written[i].type);
        }
    }
}

// seek_to_lsn is gone: its binary search read headers via the unverified observer, so a failed read's zeroed
// header could silently skew the answer -- same defect family read_verified_page_header exists to fix.

TEST_CASE("page_checksum_corruption") {
    tmp_dir_t dir("test_wal_page_checksum_corrupt");
    auto filepath = dir.file("wal_segment_0");

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= 10; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/300 + i, last_crc);
            REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), i).contains_error());
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    {
        std::fstream file(filepath, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());

        // Page 1 starts at offset PAGE_SIZE; the data area begins right after the page header.
        uint64_t corrupt_offset = PAGE_SIZE + PAGE_HEADER_SIZE + 10;
        file.seekp(static_cast<std::streamoff>(corrupt_offset));

        char byte = 0;
        file.read(&byte, 1);
        byte ^= static_cast<char>(0xFF);
        file.seekp(static_cast<std::streamoff>(corrupt_offset));
        file.write(&byte, 1);
        file.flush();
    }

    {
        wal_page_reader_t reader(&test_resource, filepath);
        [[maybe_unused]] auto header = reader.read_page_header(1);

        REQUIRE(reader.verify_page_checksum(1) == false);
    }
}

TEST_CASE("crc_chain_across_pages") {
    tmp_dir_t dir("test_wal_page_crc_chain");
    auto filepath = dir.file("wal_segment_0");

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= 500; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/i * 10, last_crc);
            REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), i).contains_error());
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    auto file_size = std::filesystem::file_size(filepath);
    auto total_pages = file_size / PAGE_SIZE;
    REQUIRE(total_pages >= 4);

    {
        wal_page_reader_t reader(&test_resource, filepath);
        REQUIRE(reader.verify_chain() == true);
    }
}

TEST_CASE("stop_at_corruption") {
    tmp_dir_t dir("test_wal_page_stop_corruption");
    auto filepath = dir.file("wal_segment_0");

    uint64_t total_records = 500;

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
        crc32_t last_crc = 0;

        for (uint64_t i = 1; i <= total_records; ++i) {
            auto rec = encode_commit_rec(i, /*txn_id=*/i * 10, last_crc);
            REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), i).contains_error());
            last_crc = extract_crc(rec.data.data(), rec.data.size());
        }
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    auto file_size = std::filesystem::file_size(filepath);
    auto total_pages = file_size / PAGE_SIZE;
    REQUIRE(total_pages >= 4);

    uint32_t records_in_first_page = 0;
    {
        wal_page_reader_t reader(&test_resource, filepath);
        auto header = reader.read_page_header(1);
        records_in_first_page = header.num_records;
    }
    REQUIRE(records_in_first_page > 0);

    {
        std::fstream file(filepath, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());

        uint64_t corrupt_offset = 2 * PAGE_SIZE + PAGE_HEADER_SIZE + 20;
        file.seekp(static_cast<std::streamoff>(corrupt_offset));

        char byte = 0;
        file.read(&byte, 1);
        byte ^= static_cast<char>(0xFF);
        file.seekp(static_cast<std::streamoff>(corrupt_offset));
        file.write(&byte, 1);
        file.flush();
    }

    {
        wal_page_reader_t reader(&test_resource, filepath);
        auto records_result = reader.read_all_records(0);
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();

        REQUIRE(records.size() == records_in_first_page);

        for (size_t i = 0; i < records.size(); ++i) {
            REQUIRE(records[i].id == static_cast<uint64_t>(i + 1));
        }
    }
}

TEST_CASE("edge_empty_file") {
    tmp_dir_t dir("test_wal_page_empty_file");
    auto filepath = dir.file("wal_segment_0");

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    {
        wal_page_reader_t reader(&test_resource, filepath);
        auto records_result = reader.read_all_records(0);
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();
        REQUIRE(records.empty());
    }
}

TEST_CASE("edge_single_record") {
    tmp_dir_t dir("test_wal_page_single_record");
    auto filepath = dir.file("wal_segment_0");

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
        auto rec = encode_commit_rec(/*wal_id=*/1, /*txn_id=*/999, /*last_crc=*/0);
        REQUIRE_FALSE(writer.append(rec.data.data(), rec.data.size(), 1).contains_error());
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    {
        wal_page_reader_t reader(&test_resource, filepath);
        auto records_result = reader.read_all_records(0);
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();
        REQUIRE(records.size() == 1);
        REQUIRE(records[0].id == 1);
        REQUIRE(records[0].transaction_id == 999);
        REQUIRE(records[0].record_type == wal_record_type::COMMIT);
    }
}

TEST_CASE("edge_exact_fit") {
    tmp_dir_t dir("test_wal_page_exact_fit");
    auto filepath = dir.file("wal_segment_0");

    auto* resource = std::pmr::get_default_resource();

    auto minimal_chunk = gen_data_chunk(1, resource);
    auto minimal_rec = encode_insert_rec(1, 1, 0, kTestTableOid, minimal_chunk, 0, 1);
    [[maybe_unused]] size_t minimal_size = minimal_rec.data.size();

    // A synthetic PAGE_DATA_SIZE buffer stands in for a real encoded record; the point is that the
    // writer places it in one page with no spanning.
    std::vector<char> exact_record(PAGE_DATA_SIZE, 0);

    // Only size (offset 0, = PAGE_DATA_SIZE - 8) and wal_id (offset 8) are filled in -- enough to test
    // placement, not full decoding.
    uint32_t payload_size = PAGE_DATA_SIZE - 8;
    std::memcpy(exact_record.data(), &payload_size, sizeof(payload_size));

    uint64_t wal_id = 1;
    std::memcpy(exact_record.data() + 8, &wal_id, sizeof(wal_id));

    {
        wal_page_writer_t writer(&test_resource, filepath, "testdb", 0);
        REQUIRE_FALSE(writer.append(exact_record.data(), exact_record.size(), 1).contains_error());
        REQUIRE_FALSE(writer.flush().contains_error());
    }

    {
        auto file_size = std::filesystem::file_size(filepath);
        auto total_pages = file_size / PAGE_SIZE;
        REQUIRE(total_pages == 2);

        wal_page_reader_t reader(&test_resource, filepath);
        auto header = reader.read_page_header(1);
        REQUIRE(header.data_size == PAGE_DATA_SIZE);
        REQUIRE((header.flags & PAGE_PARTIAL_CONT) == 0);
        REQUIRE((header.flags & PAGE_PARTIAL_END) == 0);
    }
}
