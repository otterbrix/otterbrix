// The compression byte of a data_pointer_t is DISK-FED and was accepted unvalidated: any
// uint8_t static_cast into compression_type, and every read dispatch treated "not CONSTANT,
// not RLE, not DICTIONARY" as raw fixed-size bytes. The trap that arms itself: the first
// writer to implement BITPACKING gets a green build, green tests, a green reopen — and a
// scan that silently reads the bitpacked stream as raw values. The block CRC cannot catch
// it: the byte is exactly what the new writer wrote.
//
// Two pins:
//   * data_pointer_t::deserialize refuses a compression byte no reader understands —
//     data_corruption on the metadata reader's sticky channel, the same channel the load
//     boundary (data_table_t::load_from_disk) already surfaces;
//   * a segment stamped with an unreadable compression refuses scan and fetch on the
//     state's error channel instead of falling through to the raw fixed-size leg.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/compression/compression_type.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/data_pointer.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/vector.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;
namespace tcompress = components::table::compression;

namespace {

    std::string cbv_db_path() {
        static std::string path = "/tmp/test_otterbrix_compression_byte_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    struct cbv_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;
        tstorage::single_file_block_manager_t block_manager;

        cbv_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, (std::remove(cbv_db_path().c_str()), cbv_db_path())) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~cbv_env_t() { std::remove(cbv_db_path().c_str()); }
    };

    // Serialize a pointer whose compression byte is `raw` and deserialize it back,
    // returning the reader's verdict. Writes the field layout of
    // data_pointer_t::serialize by hand so an OUT-OF-ENUM byte can be injected too.
    core::error_t roundtrip_compression_byte(cbv_env_t& env, uint8_t raw) {
        tstorage::metadata_manager_t manager(env.block_manager);
        tstorage::meta_block_pointer_t pointer;
        {
            tstorage::metadata_writer_t writer(manager);
            writer.write<uint64_t>(11);        // row_start
            writer.write<uint64_t>(3);         // tuple_count
            writer.write<uint64_t>(42);        // block_pointer.block_id
            writer.write<uint32_t>(0);         // block_pointer.offset
            writer.write<uint8_t>(raw);        // compression
            writer.write<uint64_t>(128);       // segment_size
            writer.write<uint32_t>(0);         // overflow count
            pointer = writer.get_block_pointer();
            REQUIRE_FALSE(writer.flush().has_error());
        }
        tstorage::metadata_reader_t reader(manager, pointer);
        auto dp = tstorage::data_pointer_t::deserialize(reader);
        if (!reader.has_error()) {
            // Round-tripped clean: the byte was readable and must have landed unchanged.
            REQUIRE(static_cast<uint8_t>(dp.compression) == raw);
            REQUIRE(dp.row_start == 11);
            REQUIRE(dp.segment_size == 128);
        }
        return reader.has_error() ? core::error_t(reader.error()) : core::error_t::no_error();
    }

} // namespace

TEST_CASE("compression_byte: deserialize refuses a byte no reader understands", "[compression]") {
    cbv_env_t env;

    // The four forms the reader implements round-trip clean.
    CHECK_FALSE(roundtrip_compression_byte(env, static_cast<uint8_t>(tcompress::compression_type::UNCOMPRESSED))
                    .contains_error());
    CHECK_FALSE(roundtrip_compression_byte(env, static_cast<uint8_t>(tcompress::compression_type::CONSTANT))
                    .contains_error());
    CHECK_FALSE(
        roundtrip_compression_byte(env, static_cast<uint8_t>(tcompress::compression_type::RLE)).contains_error());
    CHECK_FALSE(roundtrip_compression_byte(env, static_cast<uint8_t>(tcompress::compression_type::DICTIONARY))
                    .contains_error());

    // Enum members with no read implementation: accepting them silently is the armed trap.
    for (auto unreadable : {tcompress::compression_type::INVALID,
                            tcompress::compression_type::BITPACKING,
                            tcompress::compression_type::VALIDITY_UNCOMPRESSED}) {
        auto verdict = roundtrip_compression_byte(env, static_cast<uint8_t>(unreadable));
        INFO("compression byte " << static_cast<int>(unreadable));
        CHECK(verdict.contains_error());
        CHECK(verdict.type == core::error_code_t::data_corruption);
    }

    // A byte outside the enum entirely (a future format, or plain garbage under a valid CRC).
    auto verdict = roundtrip_compression_byte(env, 0xB7);
    CHECK(verdict.contains_error());
    CHECK(verdict.type == core::error_code_t::data_corruption);
}

namespace {

    // A standalone BIGINT segment with 100 appended rows, for stamping arbitrary
    // compression bytes onto.
    struct stamped_segment_t {
        std::unique_ptr<column_segment_t> segment;

        stamped_segment_t(cbv_env_t& env, uint64_t rows) {
            auto created = column_segment_t::create_segment(env.buffer_manager,
                                                            complex_logical_type(logical_type::BIGINT),
                                                            0,
                                                            DEFAULT_VECTOR_CAPACITY * sizeof(int64_t),
                                                            env.block_manager.block_size());
            REQUIRE_FALSE(created.has_error());
            segment = std::move(created.value());

            vector_t v(&env.resource, complex_logical_type(logical_type::BIGINT), rows);
            for (uint64_t i = 0; i < rows; i++) {
                v.set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
            }
            unified_vector_format uvf(&env.resource, rows);
            v.to_unified_format(rows, uvf);

            column_append_state state;
            REQUIRE_FALSE(segment->initialize_append(state).has_error());
            auto appended = segment->append(state, uvf, 0, rows);
            REQUIRE_FALSE(appended.has_error());
            REQUIRE(appended.value() == rows);
        }
    };

} // namespace

TEST_CASE("compression_byte: a segment stamped with an unreadable compression refuses to scan", "[compression]") {
    cbv_env_t env;
    constexpr uint64_t ROWS = 100;
    stamped_segment_t stamped(env, ROWS);
    auto& segment = *stamped.segment;

    // Sanity: UNCOMPRESSED scans the appended values back.
    {
        vector_t result(&env.resource, complex_logical_type(logical_type::BIGINT), ROWS);
        column_scan_state state;
        segment.initialize_scan(state);
        REQUIRE_FALSE(state.has_error());
        state.row_index = 0;
        segment.scan(state, ROWS, result, 0, scan_vector_type::SCAN_ENTIRE_VECTOR);
        REQUIRE_FALSE(state.has_error());
        REQUIRE(result.value(7).value<int64_t>() == 7);
    }

    // BITPACKING is the byte the first new writer will stamp. Today the dispatch falls
    // through to the raw fixed-size leg and "successfully" scans the stream as values.
    segment.set_compression(tcompress::compression_type::BITPACKING);

    {
        vector_t result(&env.resource, complex_logical_type(logical_type::BIGINT), ROWS);
        column_scan_state state;
        segment.initialize_scan(state);
        REQUIRE_FALSE(state.has_error());
        state.row_index = 0;
        segment.scan(state, ROWS, result, 0, scan_vector_type::SCAN_ENTIRE_VECTOR);
        INFO("an entire-vector scan of a BITPACKING-stamped segment must refuse, not read raw");
        CHECK(state.has_error());
    }
    {
        vector_t result(&env.resource, complex_logical_type(logical_type::BIGINT), ROWS);
        column_scan_state state;
        segment.initialize_scan(state);
        REQUIRE_FALSE(state.has_error());
        state.row_index = 0;
        segment.scan(state, ROWS, result, 0, scan_vector_type::SCAN_FLAT_VECTOR);
        INFO("a partial scan of a BITPACKING-stamped segment must refuse, not read raw");
        CHECK(state.has_error());
    }
    {
        vector_t result(&env.resource, complex_logical_type(logical_type::BIGINT), ROWS);
        column_fetch_state state;
        segment.fetch_row(state, 5, result, 0);
        INFO("a row fetch from a BITPACKING-stamped segment must refuse, not read raw");
        CHECK(state.fetch_error.contains_error());
    }

    // The other two unreadable enum members take the same refusal.
    for (auto unreadable : {tcompress::compression_type::INVALID,
                            tcompress::compression_type::VALIDITY_UNCOMPRESSED}) {
        segment.set_compression(unreadable);
        vector_t result(&env.resource, complex_logical_type(logical_type::BIGINT), ROWS);
        column_scan_state state;
        segment.initialize_scan(state);
        REQUIRE_FALSE(state.has_error());
        state.row_index = 0;
        segment.scan(state, ROWS, result, 0, scan_vector_type::SCAN_ENTIRE_VECTOR);
        INFO("compression byte " << static_cast<int>(unreadable));
        CHECK(state.has_error());
    }
}
