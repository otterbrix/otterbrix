#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <services/wal/wal_binary.hpp>
#include <services/wal/wal_page.hpp>
#include <services/wal/wal_page_reader.hpp>
#include <services/wal/wal_page_writer.hpp>
#include <string>
#include <vector>

// Every page layout the writer can produce, built with the real writer and read back record by record.

namespace {

    using namespace services::wal;

    core::pmr::otterbrix_resource resource;

    constexpr size_t D = PAGE_DATA_SIZE;
    constexpr size_t kCommitSize = 37;
    constexpr size_t kDeleteBase = 53;
    constexpr uint16_t kContEnd = PAGE_PARTIAL_CONT | PAGE_PARTIAL_END;

    struct expected_t {
        uint64_t id;
        std::vector<char> bytes;
    };

    // The encoders hit exactly two size families: a COMMIT (37 bytes) and a DELETE (53 + 8n bytes).
    std::vector<char> encode(uint64_t id, size_t size) {
        buffer_t buf{&resource};
        if (size == kCommitSize) {
            encode_commit(buf, 0, id, id + 1000, id);
        } else {
            REQUIRE(size >= kDeleteBase);
            REQUIRE((size - kDeleteBase) % 8 == 0);
            std::vector<int64_t> row_ids((size - kDeleteBase) / 8);
            for (size_t i = 0; i < row_ids.size(); ++i) {
                row_ids[i] = static_cast<int64_t>(id * 100000 + i);
            }
            encode_delete(buf, 0, id, id + 1000, 16500, row_ids.data(), row_ids.size());
        }
        REQUIRE(buf.size() == size);
        return std::vector<char>(buf.begin(), buf.end());
    }

    size_t span_size(size_t at_least) {
        if (at_least <= kDeleteBase) {
            return kDeleteBase;
        }
        return kDeleteBase + (at_least - kDeleteBase + 7) / 8 * 8;
    }

    struct segment_t {
        explicit segment_t(const std::string& name)
            : dir(std::filesystem::temp_directory_path() / name)
            , path(dir / "wal_segment_0") {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
            std::filesystem::create_directories(dir);
            open();
        }

        ~segment_t() {
            writer.reset();
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }

        void open() {
            writer = std::make_unique<wal_page_writer_t>(&resource, path, "testdb", 0);
            REQUIRE_FALSE(writer->open_error().contains_error());
            pos = 0;
        }

        void put(size_t size) {
            auto bytes = encode(next_id, size);
            REQUIRE_FALSE(writer->append(bytes.data(), bytes.size(), next_id).contains_error());
            expected.push_back({next_id, std::move(bytes)});
            ++next_id;
            if (size <= D - pos) {
                pos += size;
            } else {
                size_t rest = size - (D - pos);
                while (rest > D) {
                    rest -= D;
                }
                pos = rest;
            }
        }

        // Exactly `bytes` bytes of small records.
        void fill(size_t bytes) {
            if (bytes == 0) {
                return;
            }
            for (size_t k = 1; k <= 24; ++k) {
                if (kCommitSize * k > bytes) {
                    break;
                }
                const size_t rest = bytes - kCommitSize * k;
                if (rest % 8 != 0 || (rest != 0 && rest < 16)) {
                    continue;
                }
                for (size_t i = 1; i < k; ++i) {
                    put(kCommitSize);
                }
                put(kCommitSize + rest);
                return;
            }
            FAIL("no record mix covers exactly " << bytes << " bytes");
        }

        void some(size_t bytes) {
            for (size_t done = 0; done < bytes; done += kCommitSize) {
                put(kCommitSize);
            }
        }

        void fill_to(size_t target) {
            REQUIRE(target >= pos);
            fill(target - pos);
        }

        void flush() {
            REQUIRE_FALSE(writer->flush().contains_error());
            pos = 0;
        }

        void close() {
            flush();
            writer.reset();
        }

        wal_page_header_t header(size_t page) {
            wal_page_reader_t reader(&resource, path);
            return reader.read_page_header(page);
        }

        std::filesystem::path dir;
        std::filesystem::path path;
        std::unique_ptr<wal_page_writer_t> writer;
        std::vector<expected_t> expected;
        uint64_t next_id{1};
        size_t pos{0}; // bytes used in the writer's current page
    };

    void verify(const std::filesystem::path& path, const std::vector<expected_t>& expected) {
        wal_page_reader_t reader(&resource, path);
        auto got = reader.read_all_records(0);
        if (got.has_error()) {
            FAIL("read_all_records refused: " << got.error().what);
        }
        const auto& records = got.value();
        REQUIRE(records.size() == expected.size());
        for (size_t i = 0; i < records.size(); ++i) {
            const auto& e = expected[i];
            INFO("record " << i << " id " << e.id << " size " << e.bytes.size());
            REQUIRE(records[i].is_valid());
            REQUIRE(records[i].id == e.id);
            REQUIRE(static_cast<size_t>(records[i].size) == e.bytes.size());
            REQUIRE(records[i].crc32 == extract_crc(e.bytes.data(), e.bytes.size()));
            if (e.bytes.size() != kCommitSize) {
                REQUIRE(records[i].physical_row_ids.size() == (e.bytes.size() - kDeleteBase) / 8);
                for (size_t r = 0; r < records[i].physical_row_ids.size(); ++r) {
                    REQUIRE(records[i].physical_row_ids[r] == static_cast<int64_t>(e.id * 100000 + r));
                }
            }
        }
    }

    template<typename Edit>
    void rewrite_page(const std::filesystem::path& path, size_t page, Edit edit) {
        alignas(16) char buf[PAGE_SIZE];
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekg(static_cast<std::streamoff>(page * PAGE_SIZE));
        f.read(buf, PAGE_SIZE);
        REQUIRE(f.good());
        wal_page_header_t hdr;
        std::memcpy(&hdr, buf, PAGE_HEADER_SIZE);
        edit(hdr, buf + PAGE_HEADER_SIZE);
        hdr.compute_checksum(buf);
        f.seekp(static_cast<std::streamoff>(page * PAGE_SIZE));
        f.write(buf, PAGE_SIZE);
        REQUIRE(f.good());
    }

    bool refuses(const std::filesystem::path& path) {
        wal_page_reader_t reader(&resource, path);
        REQUIRE(reader.verify_chain());
        auto got = reader.read_all_records(0);
        return got.has_error() && got.error().type == core::error_code_t::data_corruption;
    }

} // namespace

TEST_CASE("wal::page_spans::cont_end_page_ending_with_the_start_of_a_new_span") {
    segment_t s("wal_spans_cont_end_new_span");
    s.fill_to(1000);
    s.put(span_size(5000)); // A: page 1 -> page 2
    s.fill_to(3000);
    s.put(span_size(3000)); // B starts on the CONT|END page that ends A
    s.some(400);
    s.close();
    REQUIRE(s.header(1).flags == PAGE_PARTIAL_CONT);
    REQUIRE(s.header(2).flags == kContEnd);
    REQUIRE(s.header(3).flags == kContEnd);
    verify(s.path, s.expected);
}

TEST_CASE("wal::page_spans::a_span_ends_and_the_next_span_starts_right_after_it") {
    segment_t s("wal_spans_back_to_back");
    s.fill_to(1000);
    s.put(span_size(5000));
    s.put(span_size(6000));
    s.put(span_size(7000));
    s.some(300);
    s.close();
    verify(s.path, s.expected);
}

TEST_CASE("wal::page_spans::spans_across_three_and_more_pages") {
    segment_t s("wal_spans_many_pages");
    s.fill_to(500);
    s.put(span_size(3 * D + 777));
    s.some(300);
    s.put(span_size(20 * D + 5));
    s.some(200);
    s.put(span_size(2 * D + 1));
    s.close();
    REQUIRE(s.header(2).flags == PAGE_PARTIAL_CONT);
    REQUIRE(s.header(3).flags == PAGE_PARTIAL_CONT);
    verify(s.path, s.expected);
}

TEST_CASE("wal::page_spans::a_span_that_ends_exactly_on_a_page_boundary") {
    const size_t a = span_size(5000);
    SECTION("then an explicit flush and a fresh page") {
        segment_t s("wal_spans_exact_end_flush");
        s.fill(2 * D - a);
        s.put(a);
        REQUIRE(s.pos == D);
        s.flush();
        s.some(500);
        s.put(span_size(3000));
        s.some(100);
        s.close();
        verify(s.path, s.expected);
    }
    SECTION("then a small record") {
        segment_t s("wal_spans_exact_end_small");
        s.fill(2 * D - a);
        s.put(a);
        REQUIRE(s.pos == D);
        s.put(kCommitSize);
        s.some(500);
        s.close();
        verify(s.path, s.expected);
    }
    SECTION("then a spanning record") {
        segment_t s("wal_spans_exact_end_span");
        s.fill(2 * D - a);
        s.put(a);
        REQUIRE(s.pos == D);
        s.put(span_size(D + 900));
        s.some(500);
        s.close();
        verify(s.path, s.expected);
    }
}

TEST_CASE("wal::page_spans::records_exactly_fill_a_page_then_the_next_record_arrives") {
    SECTION("a small record") {
        segment_t s("wal_spans_full_page_small");
        s.fill(D);
        s.put(kCommitSize);
        s.some(200);
        s.close();
        REQUIRE(s.header(1).flags == PAGE_PARTIAL_CONT);
        REQUIRE(s.header(1).data_size == D);
        verify(s.path, s.expected);
    }
    SECTION("a spanning record") {
        segment_t s("wal_spans_full_page_span");
        s.fill(D);
        s.put(span_size(2 * D + 300));
        s.some(200);
        s.close();
        verify(s.path, s.expected);
    }
    SECTION("a full CONT|END page, then a small record") {
        segment_t s("wal_spans_full_cont_end_small");
        const size_t a = span_size(5000);
        s.fill(1000);
        s.put(a);
        s.fill_to(D);
        s.put(kCommitSize);
        s.some(200);
        s.close();
        REQUIRE(s.header(2).flags == kContEnd);
        REQUIRE(s.header(3).flags == kContEnd);
        verify(s.path, s.expected);
    }
}

TEST_CASE("wal::page_spans::the_size_header_of_a_span_is_split_across_pages") {
    for (size_t k = 1; k <= 12; ++k) {
        INFO("bytes of the new record on the first page: " << k);
        {
            segment_t s("wal_spans_split_header_normal");
            s.fill(D - k);
            s.put(span_size(D + 100));
            s.some(200);
            s.close();
            verify(s.path, s.expected);
        }
        {
            segment_t s("wal_spans_split_header_cont_end");
            s.fill(1000);
            s.put(span_size(5000));
            s.fill_to(D - k);
            s.put(span_size(D + 100));
            s.some(200);
            s.close();
            REQUIRE(s.header(2).flags == kContEnd);
            verify(s.path, s.expected);
        }
    }
}

TEST_CASE("wal::page_spans::a_span_starting_at_offset_zero") {
    segment_t s("wal_spans_offset_zero");
    s.put(span_size(2 * D + 10));
    s.some(100);
    s.flush();
    s.put(span_size(D + 50));
    s.some(100);
    s.close();
    verify(s.path, s.expected);
}

TEST_CASE("wal::page_spans::a_cont_end_page_with_nothing_after_the_span") {
    segment_t s("wal_spans_empty_remainder");
    s.fill(1000);
    s.put(span_size(5000));
    s.flush();
    s.some(500);
    s.put(span_size(3000));
    s.close();
    verify(s.path, s.expected);
}

TEST_CASE("wal::page_spans::every_span_boundary_offset_on_a_cont_end_page") {
    for (size_t k = 0; k <= 96; ++k) {
        INFO("the new span starts " << k << " bytes before the end of the CONT|END page");
        segment_t s("wal_spans_sweep");
        s.fill(1000);
        s.put(span_size(5000));
        s.fill_to(D - k);
        s.put(span_size(D + 300));
        s.put(span_size(700));
        s.some(100);
        s.close();
        verify(s.path, s.expected);
    }
}

TEST_CASE("wal::page_spans::a_random_stream_reads_back_whole") {
    std::mt19937_64 rng(20260929);
    segment_t s("wal_spans_random");
    for (int i = 0; i < 3000; ++i) {
        const auto pick = rng() % 100;
        if (pick < 45) {
            s.put(kCommitSize);
        } else if (pick < 80) {
            s.put(span_size(kDeleteBase + rng() % 600));
        } else if (pick < 95) {
            s.put(span_size(D / 2 + rng() % (2 * D)));
        } else {
            s.put(span_size(3 * D + rng() % (6 * D)));
        }
        if (rng() % 40 == 0) {
            s.flush();
        }
    }
    s.close();
    verify(s.path, s.expected);
}

// A crash between two page writes of one spanning record leaves its first page with nothing after it; the next
// run reopens the segment and appends fresh pages there. The unfinished record was never acknowledged.
TEST_CASE("wal::page_spans::an_orphan_span_left_by_a_crash") {
    SECTION("at the end of the segment") {
        segment_t s("wal_spans_orphan_eof");
        s.fill(1000);
        auto before = s.expected;
        s.put(span_size(2 * D));
        s.close();
        std::filesystem::resize_file(s.path, 2 * PAGE_SIZE);
        REQUIRE(s.header(1).flags == PAGE_PARTIAL_CONT);
        verify(s.path, before);
    }
    SECTION("followed by pages appended after the restart") {
        segment_t s("wal_spans_orphan_reopen");
        s.fill(1000);
        auto kept = s.expected;
        s.put(span_size(2 * D));
        s.close();
        std::filesystem::resize_file(s.path, 2 * PAGE_SIZE);
        s.expected = kept;
        s.open();
        s.some(500);
        s.put(span_size(D + 300));
        s.some(200);
        s.close();
        REQUIRE(s.header(2).flags == PAGE_PARTIAL_CONT);
        verify(s.path, s.expected);
    }
    SECTION("followed by a span at offset zero after the restart") {
        segment_t s("wal_spans_orphan_reopen_span");
        s.fill(1000);
        auto kept = s.expected;
        s.put(span_size(2 * D));
        s.close();
        std::filesystem::resize_file(s.path, 2 * PAGE_SIZE);
        s.expected = kept;
        s.open();
        s.put(span_size(D + 300));
        s.some(200);
        s.close();
        verify(s.path, s.expected);
    }
}

// Bytes inside an intact checksum chain that do not parse into the records the headers promise are a refusal,
// never a silent skip.
TEST_CASE("wal::page_spans::an_inconsistent_page_is_refused_not_skipped") {
    auto build = [](segment_t& s, size_t& a_end) {
        s.fill(1000);
        s.put(span_size(5000));
        a_end = s.pos;
        s.fill_to(3000);
        s.put(span_size(3000));
        s.some(400);
        s.close();
        verify(s.path, s.expected);
    };
    size_t a_end = 0;
    SECTION("the page completing a span is not flagged END") {
        segment_t s("wal_spans_refuse_no_end");
        build(s, a_end);
        rewrite_page(s.path, 2, [](wal_page_header_t& h, char*) { h.flags = PAGE_PARTIAL_CONT; });
        REQUIRE(refuses(s.path));
    }
    SECTION("a page counts more records than it holds") {
        segment_t s("wal_spans_refuse_count");
        build(s, a_end);
        rewrite_page(s.path, 2, [](wal_page_header_t& h, char*) { ++h.num_records; });
        REQUIRE(refuses(s.path));
    }
    SECTION("a whole record fails its own checksum") {
        segment_t s("wal_spans_refuse_record_crc");
        build(s, a_end);
        rewrite_page(s.path, 2, [a_end](wal_page_header_t&, char* data) { data[a_end + 10] ^= 0x5a; });
        REQUIRE(refuses(s.path));
    }
    SECTION("bytes left over on a page that does not spill") {
        segment_t s("wal_spans_refuse_leftover");
        build(s, a_end);
        rewrite_page(s.path, 2, [](wal_page_header_t& h, char*) { h.flags = PAGE_PARTIAL_END; });
        REQUIRE(refuses(s.path));
    }
    SECTION("a page ends a span no earlier page started") {
        segment_t s("wal_spans_refuse_stray_end");
        build(s, a_end);
        rewrite_page(s.path, 3, [](wal_page_header_t& h, char*) { h.page_lsn += 1000; });
        REQUIRE(refuses(s.path));
    }
    SECTION("a record size of zero inside the used area") {
        segment_t s("wal_spans_refuse_zero_size");
        build(s, a_end);
        rewrite_page(s.path, 2, [a_end](wal_page_header_t&, char* data) { std::memset(data + a_end, 0, 4); });
        REQUIRE(refuses(s.path));
    }
}
