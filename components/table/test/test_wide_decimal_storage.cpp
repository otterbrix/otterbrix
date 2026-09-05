// A DECIMAL wider than 18 digits is stored as a 128-bit scaled integer
// (types::decimal_storage_for_width: width <= 18 -> INT64, width <= 38 -> INT128).
// Every OTHER layer already carries that storage class -- the append switch, the
// point fetch, update_segment_t's six dispatchers, vector_t::set_value /
// value_internal / flatten -- but column_segment_t::scan and
// column_segment_t::scan_partial had their INT128/UINT128 arms COMMENTED OUT since
// the file was written, so a 128-bit column fell through to the `default:` leg and
// threw std::logic_error out of the read path.
//
// That made a third of the legal DECIMAL window (NUMERIC(19..38, s)) declarable,
// insertable and then UNREADABLE:
//   * a plain scan of the column threw;
//   * data_table_t::compact -- which the disk agent runs before EVERY checkpoint,
//     over EVERY column -- scans the whole table, so the throw crossed an
//     actor-zeta coroutine whose unhandled_exception() aborts the process. One
//     NUMERIC(38,4) column killed the database on its first checkpoint.
//
// These cases pin the storage class, not the SQL surface: the payloads are exact
// int128 boundary values (+-(10^width - 1)) that no double literal can spell, and
// they are checked ELEMENT BY ELEMENT through three readers -- the in-memory scan,
// the compact rebuild, and a checkpoint + reopen.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <limits>
#include <string>
#include <unistd.h>

#include "table_segment_scan.hpp"

namespace {

    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_wide_decimal_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_test_file() { std::remove(test_db_path().c_str()); }

    struct test_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;

        test_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    components::types::int128_t pow10_128(uint8_t exponent) {
        components::types::int128_t result = 1;
        for (uint8_t i = 0; i < exponent; ++i) {
            result *= 10;
        }
        return result;
    }

    // The scaled-integer payload written into row `row` of a DECIMAL(width, s) column.
    //
    // Rows 0..6 are the boundary set: zero, the smallest non-zero magnitude either way,
    // and +-(10^width - 1) -- the largest magnitude the window admits, which for width 38
    // needs all 128 bits and is therefore the value a 64-bit read path truncates. The tail
    // is deliberately HIGH-CARDINALITY so the checkpoint's analysis cannot fold the segment
    // into CONSTANT / RLE / DICTIONARY: those three scan legs are size-generic memcpy and
    // work today, so a low-cardinality pattern would checkpoint straight past the gap under
    // test and only the UNCOMPRESSED leg proves it.
    components::types::int128_t decimal_payload(uint64_t row, uint8_t width) {
        using components::types::int128_t;
        const int128_t largest = pow10_128(width) - 1;
        switch (row) {
            case 0:
                return int128_t{0};
            case 1:
                return int128_t{1};
            case 2:
                return int128_t{-1};
            case 3:
                return largest;
            case 4:
                return -largest;
            case 5:
                return largest / 2;
            case 6:
                return -(largest / 2);
            default: {
                const int128_t stepped = largest - static_cast<int128_t>(row) * 1000003;
                return (row % 2 == 0) ? stepped : -stepped;
            }
        }
    }

    // Every 13th row is NULL: the validity bitmap rides a separate child column, so a
    // 128-bit value column must keep its NULLs aligned through the same three readers.
    bool is_null_row(uint64_t row) { return row % 13 == 12; }

    constexpr uint64_t ROW_COUNT = 3000;
    constexpr uint64_t BATCH = 250;
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    void append_decimal_rows(components::table::data_table_t& table,
                             std::pmr::memory_resource* resource,
                             const components::types::complex_logical_type& decimal_type,
                             uint8_t width) {
        using namespace components::types;
        using namespace components::vector;
        using namespace components::table;

        auto types = table.copy_types();
        for (uint64_t offset = 0; offset < ROW_COUNT; offset += BATCH) {
            const uint64_t batch = std::min(BATCH, ROW_COUNT - offset);
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = offset + i;
                if (is_null_row(row)) {
                    chunk.set_value(0, i, logical_value_t{resource, nullptr});
                } else {
                    chunk.set_value(0,
                                    i,
                                    logical_value_t::create_decimal(resource,
                                                                    decimal_type,
                                                                    decimal_payload(row, width)));
                }
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
        }
    }

    void verify_decimal_rows(components::table::data_table_t& table, uint8_t width, const char* stage) {
        using namespace components::vector;
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(table, 0, ROW_COUNT, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                INFO(stage << ": row " << row);
                const auto value = chunk.data[0].value(i);
                REQUIRE(value.is_null() == is_null_row(row));
                if (!is_null_row(row)) {
                    REQUIRE(value.type().type() == components::types::logical_type::DECIMAL);
                    // The comparison is on the raw scaled integer, so a truncated high word
                    // or a 64-bit read shows up as a wrong value rather than as a wrong scale.
                    REQUIRE(value.value<components::types::int128_t>() == decimal_payload(row, width));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == ROW_COUNT);
    }

    // One width, all three readers: the live in-memory scan, the compact rebuild that runs
    // before every checkpoint, and a checkpoint + reopen from disk.
    void run_wide_decimal_round_trip(uint8_t width, uint8_t scale) {
        using namespace components::table;
        using namespace components::table::storage;
        using namespace components::types;

        cleanup_test_file();
        test_env_t env;
        auto decimal_type_result = complex_logical_type::create_decimal(width, scale);
        REQUIRE_FALSE(decimal_type_result.has_error());
        const auto decimal_type = decimal_type_result.value();
        REQUIRE(decimal_type.to_physical_type() == physical_type::INT128);

        meta_block_pointer_t table_pointer;
        {
            single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
            REQUIRE_FALSE(bm.create_new_database().has_error());

            std::vector<column_definition_t> columns;
            columns.emplace_back("v", decimal_type);
            auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "wide_decimal");

            append_decimal_rows(*table, &env.resource, decimal_type, width);
            REQUIRE(table->calculate_size() == ROW_COUNT);

            // Reader 1 -- a plain scan of the live table.
            verify_decimal_rows(*table, width, "after append");

            // Reader 2 -- the rebuild the disk agent runs before every checkpoint. It scans
            // every column of every row group into one growing chunk, so it goes through
            // scan_partial with a non-zero result offset.
            REQUIRE(table->compact(WATERMARK));
            REQUIRE(table->calculate_size() == ROW_COUNT);
            verify_decimal_rows(*table, width, "after compact");

            metadata_manager_t meta_mgr(bm);
            metadata_writer_t writer(meta_mgr);
            REQUIRE_FALSE(table->checkpoint(writer).has_error());
            table_pointer = writer.get_block_pointer();
            database_header_t header;
            header.initialize();
            REQUIRE_FALSE(bm.write_header(header).has_error());
        }

        // Reader 3 -- reopened from the .otbx file.
        {
            single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
            REQUIRE_FALSE(bm.load_existing_database().has_error());
            metadata_manager_t meta_mgr(bm);
            metadata_reader_t reader(meta_mgr, table_pointer);
            auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
            REQUIRE_FALSE(loaded.has_error());
            REQUIRE(loaded.value()->calculate_size() == ROW_COUNT);
            verify_decimal_rows(*loaded.value(), width, "after reopen");
        }

        cleanup_test_file();
    }

} // namespace

// The width the bug report names: NUMERIC(38,4) is the widest practical money/measure type
// and it killed the process on the first checkpoint.
TEST_CASE("wide_decimal: NUMERIC(38,4) round-trips scan, compact and reopen element by element") {
    run_wide_decimal_round_trip(38, 4);
}

// The FIRST width past int64 storage -- one digit narrower and the whole column takes the
// INT64 arm that always existed. This is the exact seam.
TEST_CASE("wide_decimal: NUMERIC(19,0) is the first width past int64 storage") {
    run_wide_decimal_round_trip(19, 0);
}

// The maximum scale the window admits: every digit is a fraction digit, so the payload is
// still a full-width 128-bit integer while the value is < 1.
TEST_CASE("wide_decimal: NUMERIC(38,38) carries the maximum scale") { run_wide_decimal_round_trip(38, 38); }
