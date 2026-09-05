// A0b (plan): RED tests for the big-string (>= DEFAULT_STRING_BLOCK_LIMIT = 4096 bytes)
// overflow path. Known defects this pins:
//   D2 — write_string_marker writes 16 bytes (uint64 + int64) into a dictionary slot that
//        reserved BIG_STRING_MARKER_BASE_SIZE = 8 (column_segment.cpp:19, :414-418, :475):
//        the first big string writes past the reservation, every next one overwrites the
//        previous dictionary entry; read_string_marker memcpys 8 bytes into a 4-byte
//        uint32_t.
//   D1 — the overflow block id comes from the buffer manager's TRANSIENT range
//        (temp_id_ seeded with MAXIMUM_BLOCK = 2^62); the D2 truncation turns 2^62+k into k,
//        fetch_string_owned then resolves block k on the wrong manager and the load path
//        throws std::logic_error — reachable in DISK mode too, because
//        transition_segment_to_disk explicitly skips STRING segments.
//
// There is NO test coverage of this zone on HEAD (the longest string in the repo is 199
// chars). These cases are expected RED: a throw, a corrupted value, or an ASAN report.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstring>

#include <algorithm>
#include <optional>
#include <limits>
#include <string>
#include <unistd.h>

#include "table_segment_scan.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string bigstr_db_path() {
        static std::string path =
            "/tmp/test_otterbrix_big_strings_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_bigstr_file() { std::remove(bigstr_db_path().c_str()); }

    struct bigstr_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        bigstr_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    void append_string_rows(data_table_t& table,
                            bigstr_env_t& env,
                            const std::vector<std::string>& values) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, values.size());
        chunk.set_cardinality(values.size());
        for (uint64_t i = 0; i < values.size(); i++) {
            chunk.set_value(0, i, static_cast<int64_t>(i));
            chunk.set_value(1, i, std::string_view{values[i]});
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    std::vector<std::string> scan_strings(data_table_t& table, uint64_t upper_bound) {
        std::vector<std::string> out;
        otterbrix_test::scan_table_segment(table, 0, upper_bound, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto cell = chunk.value(1, i);
                out.emplace_back(cell.value<std::string_view>());
            }
        });
        return out;
    }

} // namespace

TEST_CASE("big_strings: a single >=4096-byte string survives append and read-back") {
    cleanup_bigstr_file();
    bigstr_env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "big_strings");

        const std::string big(5000, 'x');
        append_string_rows(*table, env, {big});

        auto values = scan_strings(*table, 10);
        REQUIRE(values.size() == 1);
        CHECK(values[0].size() == big.size());
        CHECK(values[0] == big);
    }
    cleanup_bigstr_file();
}

TEST_CASE("big_strings: two big strings in one segment do not overwrite each other") {
    cleanup_bigstr_file();
    bigstr_env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "big_strings");

        const std::string first(4500, 'a');
        const std::string second(4700, 'b');
        append_string_rows(*table, env, {first, second});

        auto values = scan_strings(*table, 10);
        REQUIRE(values.size() == 2);
        CHECK(values[0] == first);  // D2: the second marker overwrites the first entry
        CHECK(values[1] == second);
    }
    cleanup_bigstr_file();
}

// F1 (adversarial review): the two cases above read the table while it is STILL IN MEMORY,
// so they pin only half the path. This one takes a big string through a real checkpoint and
// a real reload — the half nothing covers.
//
// The claim under test: the overflow block's PAYLOAD is never written to the file.
// write_string_memory puts the bytes in a TRANSIENT block and records it only in
// state.overflow_blocks (column_segment.cpp) — the sole writer of that map. The checkpoint
// copies the segment block verbatim, marker included, but nothing serializes the block the
// marker points at; uncompressed_string_segment_state::register_block / on_disk_blocks have
// zero callers. On reload initialize_column builds a FRESH state with an empty
// overflow_blocks, so the first read misses and resolve_overflow_block aborts.
//
// WARNING: if the claim holds, this case does not fail — it takes the whole test_table
// binary down with it (std::abort is unconditional, NDEBUG included).
TEST_CASE("big_strings: a >=4096-byte string survives checkpoint and reload") {
    using namespace components::table::storage;
    cleanup_bigstr_file();
    bigstr_env_t env;

    const std::string big(5000, 'x');
    const std::string small("small");
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "big_strings");

        append_string_rows(*table, env, {big, small});

        // Sanity before the checkpoint: in memory it is already correct today.
        auto pre = scan_strings(*table, 10);
        REQUIRE(pre.size() == 2);
        REQUIRE(pre[0] == big);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        auto values = scan_strings(*loaded, 10);
        REQUIRE(values.size() == 2);
        CHECK(values[0].size() == big.size());
        CHECK(values[0] == big);
        CHECK(values[1] == small);
    }

    cleanup_bigstr_file();
}

// ---------------------------------------------------------------------------------------
// F1 (fix): the checkpoint now moves the overflow PAYLOAD into real file blocks, records
// their ids in data_pointer_t::overflow_blocks, and rewrites every dictionary marker out of
// the transient id domain (>= storage::MAXIMUM_BLOCK) into the on-disk one. The reload path
// registers those blocks on the segment, so a marker resolves on the first read. The cases
// below pin that round trip, the reclaim path, and the error channel that replaced the abort.
// ---------------------------------------------------------------------------------------

namespace {

    using maybe_string = std::optional<std::string>;

    // Appends in DEFAULT_VECTOR_CAPACITY-sized chunks so the rows spill past the first vector
    // AND past the first row group (row group size == DEFAULT_VECTOR_CAPACITY == 1024).
    // std::nullopt means NULL.
    void append_string_rows_batched(data_table_t& table,
                                    bigstr_env_t& env,
                                    const std::vector<maybe_string>& values) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < values.size()) {
            uint64_t batch = std::min<uint64_t>(values.size() - offset, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                if (values[row].has_value()) {
                    chunk.set_value(1, i, std::string_view{*values[row]});
                } else {
                    chunk.set_value(1, i, logical_value_t{&env.resource, nullptr});
                }
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    std::vector<maybe_string> scan_maybe_strings(data_table_t& table, uint64_t upper_bound) {
        std::vector<maybe_string> out;
        otterbrix_test::scan_table_segment(table, 0, upper_bound, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto cell = chunk.value(1, i); // named local: chunk.value() is a temporary
                if (cell.is_null()) {
                    out.emplace_back(std::nullopt);
                } else {
                    out.emplace_back(std::string(cell.value<std::string_view>()));
                }
            }
        });
        return out;
    }

    struct bigstr_round_trip_t {
        tstorage::meta_block_pointer_t table_pointer;
        uint64_t blocks_after_checkpoint{0};
    };

    // Writes `values` into a fresh database and checkpoints it; the caller reopens the file
    // through the returned pointer and verifies what came back.
    bigstr_round_trip_t write_and_checkpoint(bigstr_env_t& env, const std::vector<maybe_string>& values) {
        bigstr_round_trip_t result;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "big_strings");

        append_string_rows_batched(*table, env, values);
        REQUIRE(table->calculate_size() == values.size());

        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        result.table_pointer = writer.get_block_pointer();

        tstorage::database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
        result.blocks_after_checkpoint = bm.total_blocks();
        REQUIRE(result.blocks_after_checkpoint > 0);
        return result;
    }

} // namespace

TEST_CASE("big_strings: several big strings in one segment survive checkpoint and reload") {
    cleanup_bigstr_file();
    bigstr_env_t env;

    // Four distinct big strings, each >= DEFAULT_STRING_BLOCK_LIMIT, all in ONE segment.
    // Distinct fill characters AND distinct lengths: a marker resolved to the wrong block, or
    // a payload offset off by one record, would read a neighbour's bytes and be caught here.
    std::vector<maybe_string> values;
    values.emplace_back(std::string(4096, 'a'));
    values.emplace_back(std::string(5000, 'b'));
    values.emplace_back(std::string(9000, 'c'));
    values.emplace_back(std::string(4097, 'd'));

    auto written = write_and_checkpoint(env, values);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_reader_t reader(meta_mgr, written.table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        auto read_back = scan_maybe_strings(*loaded, values.size());
        REQUIRE(read_back.size() == values.size());
        for (uint64_t i = 0; i < values.size(); i++) {
            INFO("row " << i);
            REQUIRE(read_back[i].has_value());
            CHECK(read_back[i]->size() == values[i]->size());
            CHECK(*read_back[i] == *values[i]);
        }
    }

    cleanup_bigstr_file();
}

TEST_CASE("big_strings: a big string past the first row group survives checkpoint and reload") {
    cleanup_bigstr_file();
    bigstr_env_t env;

    // Row group size == DEFAULT_VECTOR_CAPACITY == 1024, so rows 1024+ live in the SECOND row
    // group and rows 2048+ in the third. This branch has a documented history of bugs that
    // only appear past the first vector, so the big strings sit there on purpose: the first
    // row group holds none at all.
    constexpr uint64_t NUM_ROWS = 2600;
    constexpr uint64_t ROW_SECOND = 1500;
    constexpr uint64_t ROW_THIRD = 2500;
    const std::string big_second_group(6000, 'p');
    const std::string big_third_group(7000, 'q');

    std::vector<maybe_string> values;
    values.reserve(NUM_ROWS);
    for (uint64_t row = 0; row < NUM_ROWS; row++) {
        if (row == ROW_SECOND) {
            values.emplace_back(big_second_group);
        } else if (row == ROW_THIRD) {
            values.emplace_back(big_third_group);
        } else {
            values.emplace_back(std::string("r") + std::to_string(row));
        }
    }

    auto written = write_and_checkpoint(env, values);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_reader_t reader(meta_mgr, written.table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        auto read_back = scan_maybe_strings(*loaded, NUM_ROWS);
        REQUIRE(read_back.size() == NUM_ROWS);
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            INFO("row " << row);
            REQUIRE(read_back[row].has_value());
            REQUIRE(*read_back[row] == *values[row]);
        }
        // Spell the two interesting rows out separately so a failure names them.
        CHECK(read_back[ROW_SECOND]->size() == big_second_group.size());
        CHECK(*read_back[ROW_SECOND] == big_second_group);
        CHECK(read_back[ROW_THIRD]->size() == big_third_group.size());
        CHECK(*read_back[ROW_THIRD] == big_third_group);
    }

    cleanup_bigstr_file();
}

TEST_CASE("big_strings: big and small strings interleaved with NULLs survive checkpoint and reload") {
    cleanup_bigstr_file();
    bigstr_env_t env;

    // The interleaving matters: a NULL row copies the PREVIOUS row's dictionary offset
    // verbatim, so a NULL that follows a big string names the very same overflow marker. The
    // checkpoint must not rewrite that marker twice (the second pass would re-resolve an
    // already-rewritten id), and the reload must still report the row as NULL.
    std::vector<maybe_string> values;
    values.emplace_back(std::string("small-0"));
    values.emplace_back(std::string(4500, 'A'));
    values.emplace_back(std::nullopt); // NULL directly after a big string
    values.emplace_back(std::string("small-1"));
    values.emplace_back(std::string(8000, 'B'));
    values.emplace_back(std::string("small-2"));
    values.emplace_back(std::nullopt);
    values.emplace_back(std::string(4200, 'C'));
    values.emplace_back(std::string("")); // empty string, NOT null
    values.emplace_back(std::string(6100, 'D'));

    auto written = write_and_checkpoint(env, values);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_reader_t reader(meta_mgr, written.table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        auto read_back = scan_maybe_strings(*loaded, values.size());
        REQUIRE(read_back.size() == values.size());
        for (uint64_t i = 0; i < values.size(); i++) {
            INFO("row " << i);
            CHECK(read_back[i].has_value() == values[i].has_value());
            if (values[i].has_value() && read_back[i].has_value()) {
                CHECK(read_back[i]->size() == values[i]->size());
                CHECK(*read_back[i] == *values[i]);
            }
        }
    }

    cleanup_bigstr_file();
}

TEST_CASE("big_strings: reopening a table with big strings allocates no new blocks") {
    cleanup_bigstr_file();
    bigstr_env_t env;

    std::vector<maybe_string> values;
    values.emplace_back(std::string(5000, 'x'));
    values.emplace_back(std::string("small"));
    values.emplace_back(std::string(12000, 'y'));

    auto written = write_and_checkpoint(env, values);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        REQUIRE(bm.total_blocks() == written.blocks_after_checkpoint);

        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_reader_t reader(meta_mgr, written.table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        // Registering the persisted overflow blocks must REVIVE existing file blocks, never
        // take fresh ones off the free list: the count is exactly what the checkpoint left.
        CHECK(bm.total_blocks() == written.blocks_after_checkpoint);

        auto read_back = scan_maybe_strings(*loaded, values.size());
        REQUIRE(read_back.size() == values.size());
        REQUIRE(read_back[0].has_value());
        REQUIRE(read_back[2].has_value());
        CHECK(*read_back[0] == *values[0]);
        CHECK(*read_back[2] == *values[2]);

        // ...and reading them does not allocate either.
        CHECK(bm.total_blocks() == written.blocks_after_checkpoint);
    }

    cleanup_bigstr_file();
}

namespace {

    // Builds a one-column STRING column_data_t and appends `values` to it in a single chunk.
    // Working one level below data_table_t is what makes the two cases below possible at all:
    // they need the column_segment_t itself (to corrupt a marker) and the persistent record
    // (to check what the checkpoint wrote down).
    std::pmr::vector<complex_logical_type> string_column_types(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(logical_type::STRING_LITERAL);
        return types;
    }

    // Overwrites the block id named by the single big-string marker of `segment`. The layout
    // asserted here is exactly what string_append writes: [uint32 dict_size][uint32 dict_end]
    // at the segment start, and the 16-byte (uint64 block id, int64 offset) marker packed at
    // dict_end - dict_size. The REQUIREs make a layout change fail loudly instead of letting
    // the test silently patch unrelated bytes.
    void overwrite_only_overflow_marker(bigstr_env_t& env,
                                        column_segment_t& segment,
                                        uint64_t new_block_id,
                                        bool expect_transient = true) {
        auto pinned = env.buffer_manager.pin(segment.block);
        REQUIRE_FALSE(pinned.has_error());
        auto* base = pinned.value().ptr() + segment.block_offset();
        uint32_t dict_size = 0;
        uint32_t dict_end = 0;
        std::memcpy(&dict_size, base, sizeof(uint32_t));
        std::memcpy(&dict_end, base + sizeof(uint32_t), sizeof(uint32_t));
        REQUIRE(dict_size == 16); // exactly one big string == exactly one 16-byte marker
        auto* marker = base + dict_end - dict_size;
        uint64_t named_block = 0;
        std::memcpy(&named_block, marker, sizeof(uint64_t));
        if (expect_transient) {
            REQUIRE(named_block >= tstorage::MAXIMUM_BLOCK); // it really is a transient overflow id
        } else {
            REQUIRE(named_block < tstorage::MAXIMUM_BLOCK); // rewritten into the on-disk domain
        }
        std::memcpy(marker, &new_block_id, sizeof(uint64_t));
    }

} // namespace

// The failure path, proved rather than asserted in a comment. resolve_overflow_block used to
// fprintf + assert(false) + std::abort() on a marker it could not resolve -- unconditional,
// NDEBUG included, and reachable from a plain SELECT, so a single bad byte in one dictionary
// made the whole database impossible to open. It must report through the fetch/scan error
// channel and leave the process standing.
TEST_CASE("big_strings: an unresolvable overflow block reports an error and does not abort") {
    bigstr_env_t env;
    tstorage::in_memory_block_manager_t block_manager(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

    const std::string big(5000, 'z');

    auto column = column_data_t::create_column(&env.resource,
                                               block_manager,
                                               0,
                                               0,
                                               complex_logical_type{logical_type::STRING_LITERAL});
    column_append_state append_state;
    REQUIRE_FALSE(column->initialize_append(append_state).has_error());

    auto types = string_column_types(&env.resource);
    data_chunk_t input(&env.resource, types, 1);
    input.set_cardinality(1);
    input.set_value(0, 0, std::string_view{big});
    REQUIRE_FALSE(column->append(append_state, input.data[0], 1).has_error());
    REQUIRE(append_state.current != nullptr);

    // Sanity: intact, the very same fetch reads the string back.
    {
        vector_t result(&env.resource, logical_type::STRING_LITERAL, DEFAULT_VECTOR_CAPACITY);
        column_fetch_state fetch_state;
        column->fetch_row(fetch_state, 0, result, 0);
        REQUIRE_FALSE(fetch_state.fetch_error.contains_error());
        const auto cell = result.value(0); // named local: value() returns a temporary
        REQUIRE(cell.value<std::string_view>() == big);
    }

    // 2^62 + k is the TRANSIENT domain -- the exact shape of the id in the original crash
    // report (4611686018427387909 == 2^62 + 5). 4242 is the on-disk domain. Both must be
    // reported, neither may abort.
    const uint64_t bogus_transient = tstorage::MAXIMUM_BLOCK + 424242;
    const uint64_t bogus_on_disk = 4242;

    SECTION("fetch_row: unregistered TRANSIENT overflow block") {
        overwrite_only_overflow_marker(env, *append_state.current, bogus_transient);
        vector_t result(&env.resource, logical_type::STRING_LITERAL, DEFAULT_VECTOR_CAPACITY);
        column_fetch_state fetch_state;
        column->fetch_row(fetch_state, 0, result, 0); // used to std::abort() here
        CHECK(fetch_state.fetch_error.contains_error());
        CHECK(fetch_state.fetch_error.type == core::error_code_t::data_corruption);
    }

    SECTION("fetch_row: unregistered ON-DISK overflow block") {
        overwrite_only_overflow_marker(env, *append_state.current, bogus_on_disk);
        vector_t result(&env.resource, logical_type::STRING_LITERAL, DEFAULT_VECTOR_CAPACITY);
        column_fetch_state fetch_state;
        column->fetch_row(fetch_state, 0, result, 0);
        CHECK(fetch_state.fetch_error.contains_error());
        CHECK(fetch_state.fetch_error.type == core::error_code_t::data_corruption);
    }

    SECTION("fetch_row that OWNS its result: unregistered overflow block") {
        // The other leg of the fetch: result_outlives_pins routes through fetch_string_owned
        // (the late-materialisation gather), which resolved the very same way.
        overwrite_only_overflow_marker(env, *append_state.current, bogus_transient);
        vector_t result(&env.resource, logical_type::STRING_LITERAL, DEFAULT_VECTOR_CAPACITY);
        column_fetch_state fetch_state;
        fetch_state.result_outlives_pins = true;
        column->fetch_row(fetch_state, 0, result, 0);
        CHECK(fetch_state.fetch_error.contains_error());
        CHECK(fetch_state.fetch_error.type == core::error_code_t::data_corruption);
    }

    SECTION("scan: unregistered overflow block") {
        overwrite_only_overflow_marker(env, *append_state.current, bogus_transient);
        column_scan_state scan_state;
        scan_state.initialize(complex_logical_type{logical_type::STRING_LITERAL});
        column->initialize_scan(scan_state);
        REQUIRE_FALSE(scan_state.has_error());
        vector_t result(&env.resource, logical_type::STRING_LITERAL, DEFAULT_VECTOR_CAPACITY);
        column->scan(0, scan_state, result); // used to std::abort() here
        CHECK(scan_state.has_error());
        CHECK(scan_state.scan_error.type == core::error_code_t::data_corruption);
    }
}

// Blocks must be RECLAIMED. data_table_t::compact frees the old collection's disk blocks via
// collect_disk_block_ids; if the big-string overflow blocks are not reported there, the file
// grows by the whole payload on every compact round, forever.
TEST_CASE("big_strings: a reloaded segment reports its overflow blocks for compact reclaim") {
    cleanup_bigstr_file();
    bigstr_env_t env;
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    // The 220 000-byte value matters: its [uint32 length][bytes] record is above the
    // partial_block_manager's 0.8-of-a-block threshold, so it gets a DEDICATED overflow block
    // that no segment lives in. Without that, a small overflow record can land in the same
    // packed block as the validity segment, and collect_disk_block_ids would report the id
    // through the segment leg -- the test would pass while the overflow leg did nothing.
    std::vector<std::string> values{std::string(220000, 'm'), std::string("small"), std::string(7000, 'n')};

    auto column =
        column_data_t::create_column(&env.resource, bm, 0, 0, complex_logical_type{logical_type::STRING_LITERAL});
    {
        // The append state must be gone before the checkpoint: checkpointing re-points the
        // still-managed validity segment to disk, which drops the block_handle the append
        // state's pin refers to (documented in column_data_t::transition_segment_to_disk).
        column_append_state append_state;
        REQUIRE_FALSE(column->initialize_append(append_state).has_error());

        auto types = string_column_types(&env.resource);
        data_chunk_t input(&env.resource, types, values.size());
        input.set_cardinality(values.size());
        for (uint64_t i = 0; i < values.size(); i++) {
            input.set_value(0, i, std::string_view{values[i]});
        }
        REQUIRE_FALSE(column->append(append_state, input.data[0], values.size()).has_error());
    }

    tstorage::partial_block_manager_t pbm(bm);
    auto persistent = column->checkpoint(pbm);
    REQUIRE_FALSE(persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());

    // The checkpoint must have written the payload somewhere and said where.
    std::vector<uint64_t> recorded;
    for (const auto& dp : persistent.value().data_pointers) {
        for (uint64_t id : dp.overflow_blocks) {
            recorded.push_back(id);
        }
    }
    REQUIRE_FALSE(recorded.empty());
    for (uint64_t id : recorded) {
        INFO("overflow block " << id);
        // The marker's id domain after the fix is unambiguous: a persisted overflow id is a
        // REAL file block, never a transient one.
        CHECK(id < tstorage::MAXIMUM_BLOCK);
    }

    auto reloaded =
        column_data_t::create_column(&env.resource, bm, 0, 0, complex_logical_type{logical_type::STRING_LITERAL});
    REQUIRE_FALSE(reloaded->initialize_column(persistent.value()).has_error());
    REQUIRE(reloaded->count() == values.size());

    {
        column_scan_state scan_state;
        scan_state.initialize(complex_logical_type{logical_type::STRING_LITERAL});
        reloaded->initialize_scan(scan_state);
        vector_t result(&env.resource, logical_type::STRING_LITERAL, DEFAULT_VECTOR_CAPACITY);
        auto scanned = reloaded->scan(0, scan_state, result);
        REQUIRE_FALSE(scan_state.has_error());
        REQUIRE(scanned == values.size());
        for (uint64_t i = 0; i < values.size(); i++) {
            INFO("row " << i);
            const auto cell = result.value(i); // named local: value() returns a temporary
            REQUIRE_FALSE(cell.is_null());
            CHECK(cell.value<std::string_view>() == values[i]);
        }
    }

    // The reloaded overflow blocks must behave like every other packed block: DISK-BACKED
    // (reloadable) and unpinned once the read is done, so the pool can evict and re-read
    // them. A managed or permanently pinned overflow block would trade the correctness bug
    // for pool exhaustion. register_block dedupes through the weak registry, so this hands
    // back the very handle the segment state holds.
    for (uint64_t id : recorded) {
        INFO("overflow block " << id);
        auto handle = bm.register_block(id);
        REQUIRE(handle);
        CHECK(handle->is_reloadable());
        CHECK(handle->readers() == 0);
    }

    std::pmr::vector<uint64_t> reclaimable(&env.resource);
    reloaded->collect_disk_block_ids(reclaimable);
    for (uint64_t id : recorded) {
        INFO("overflow block " << id);
        CHECK(std::find(reclaimable.begin(), reclaimable.end(), id) != reclaimable.end());
    }

    cleanup_bigstr_file();
}

// T3: compact() must fail LOUDLY on a scan error — never truncate.
//
// The rebuild loop tested only chunk.size() == 0, never
// state.table_state.has_error(). collection_scan_state::scan sets
// row_group = nullptr and returns false on error, so the loop saw an "empty"
// chunk, broke, swapped the TRUNCATED collection in, and then mark_as_free +
// unregister_block recycled every disk block of the old collection. The rows
// were gone and their blocks handed out again — reachable on an UNCORRUPTED
// database too, because initialize_scan's buffer-pool OOM lands in the same
// unchecked channel. This case injects the error via a corrupted overflow
// marker (post-checkpoint, so the table is DISK-backed and the reclaim leg is
// live) and pins: compact reports failure, no row is lost, no block is freed.
// ---------------------------------------------------------------------------------------

TEST_CASE("big_strings: a scan failure mid-compact loses no rows and frees no blocks") {
    cleanup_bigstr_file();
    bigstr_env_t env;

    const std::string big(5000, 'k');

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
    REQUIRE(!bm.create_new_database().has_error());
    std::vector<column_definition_t> columns;
    columns.emplace_back("id", logical_type::BIGINT);
    columns.emplace_back("payload", logical_type::STRING_LITERAL);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "compact_loud");

    // Append ONE big-string row, keeping the payload segment pointer for the
    // marker surgery below (the segment object lives in the column's tree; the
    // append state itself is scoped out before the checkpoint re-points blocks).
    column_segment_t* payload_segment = nullptr;
    {
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, static_cast<int64_t>(0));
        chunk.set_value(1, 0, std::string_view{big});
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        REQUIRE(state.append_state.states != nullptr);
        payload_segment = state.append_state.states[1].current;
        REQUIRE(payload_segment != nullptr);
        table->finalize_append(state, transaction_data{0, 0});
    }

    // Checkpoint: the payload moves into real file blocks and the marker is
    // rewritten into the on-disk domain — compact's reclaim leg is now live.
    {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        tstorage::database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // Sanity: the intact table still reads back.
    {
        auto values = scan_strings(*table, 10);
        REQUIRE(values.size() == 1);
        REQUIRE(values[0] == big);
    }

    // Corrupt the marker so the compact rebuild scan fails. The LIVE segment is
    // still managed after the checkpoint (write-through re-points only COMPLETE
    // segments; this one holds a single row), so the live marker is still in
    // the transient domain — name an unregistered transient block.
    overwrite_only_overflow_marker(env,
                                   *payload_segment,
                                   /*new_block_id=*/tstorage::MAXIMUM_BLOCK + 424242,
                                   /*expect_transient=*/true);

    const uint64_t rows_before = table->calculate_size();
    const uint64_t total_before = bm.total_blocks();
    const uint64_t free_before = bm.free_blocks();
    REQUIRE(rows_before == 1);

    // Every version stamp is 0 (transaction_data{0,0}), so any watermark passes
    // the MVCC gate; the scan error is the only thing standing in compact's way.
    const bool compacted = table->compact(/*compact_watermark=*/std::numeric_limits<uint64_t>::max());

    CHECK_FALSE(compacted);                            // fails loudly, no silent success
    REQUIRE(table->calculate_size() == rows_before);   // no row lost
    REQUIRE(bm.total_blocks() == total_before);        // nothing rebuilt/replaced
    REQUIRE(bm.free_blocks() == free_before);          // no old block recycled

    cleanup_bigstr_file();
}

// T4: column_data_t::update must not discard the pre-image read error.
//
// update() fetches the row's PRIOR version into base_vector and hands it to
// update_internal as the version-chain pre-image. state.scan_error was never
// checked, so a failed big-string read (corrupt overflow marker, buffer-pool
// OOM) recorded an EMPTY string as the row's prior version: a rollback or an
// older snapshot then materialises "" where the big string was. The update
// must surface the fetch error instead of committing a poisoned pre-image.
// ---------------------------------------------------------------------------------------

TEST_CASE("big_strings: update surfaces a failed pre-image read instead of recording ''") {
    bigstr_env_t env;
    tstorage::in_memory_block_manager_t block_manager(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

    const std::string big(5000, 'u');

    auto column = column_data_t::create_column(&env.resource,
                                               block_manager,
                                               0,
                                               0,
                                               complex_logical_type{logical_type::STRING_LITERAL});
    column_append_state append_state;
    REQUIRE_FALSE(column->initialize_append(append_state).has_error());

    auto types = string_column_types(&env.resource);
    data_chunk_t input(&env.resource, types, 1);
    input.set_cardinality(1);
    input.set_value(0, 0, std::string_view{big});
    REQUIRE_FALSE(column->append(append_state, input.data[0], 1).has_error());
    REQUIRE(append_state.current != nullptr);

    // Break the pre-image read: the marker now names an unregistered transient block.
    overwrite_only_overflow_marker(env, *append_state.current, tstorage::MAXIMUM_BLOCK + 424242);

    vector_t update_vector(&env.resource, logical_type::STRING_LITERAL, 1);
    update_vector.set_value(0, logical_value_t(&env.resource, std::string("replacement")));
    int64_t row_ids[1] = {0};

    auto update_r = column->update(/*column_index=*/0, update_vector, row_ids, /*update_count=*/1);
    REQUIRE(update_r.has_error());
    REQUIRE(update_r.error().type == core::error_code_t::data_corruption);
}

// A duplicated overflow id in the PERSISTED list is corruption, and it must reach the load's
// error channel.
//
// uncompressed_string_segment_state::register_block answers false when the same block id is
// offered twice, and its only caller -- the column_segment_t reload constructor -- dropped the
// answer on the floor. The writer never emits a duplicate (persist_string_overflow dedupes
// out_blocks), so seeing one means the metadata stream is corrupt; silently accepting it means
// on_disk_blocks disagrees with what the file says the segment owns, and collect_disk_block_ids
// -- which drives compact's reclaim -- reports the wrong set.
//
// This branch became REACHABLE with A7.2/A7.3: block ids are now genuinely reused, so a stale
// or crossed pointer stream can name one twice. It runs on the agent thread inside the
// checkpoint/open coroutine, where a throw is fatal (rules 2/9), so the answer is latched on
// the segment and reported by column_data_t::initialize_column, which already returns a
// result_wrapper_t.
TEST_CASE("big_strings: a duplicated persisted overflow block is data_corruption, not silence") {
    cleanup_bigstr_file();
    bigstr_env_t env;
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, bigstr_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<std::string> values{std::string(220000, 'q'), std::string("small")};

    auto column =
        column_data_t::create_column(&env.resource, bm, 0, 0, complex_logical_type{logical_type::STRING_LITERAL});
    {
        column_append_state append_state;
        REQUIRE_FALSE(column->initialize_append(append_state).has_error());
        auto types = string_column_types(&env.resource);
        data_chunk_t input(&env.resource, types, values.size());
        input.set_cardinality(values.size());
        for (uint64_t i = 0; i < values.size(); i++) {
            input.set_value(0, i, std::string_view{values[i]});
        }
        REQUIRE_FALSE(column->append(append_state, input.data[0], values.size()).has_error());
    }

    tstorage::partial_block_manager_t pbm(bm);
    auto persistent = column->checkpoint(pbm);
    REQUIRE_FALSE(persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());

    // A clean reload of the untouched pointer set must still work -- the guard is about
    // DUPLICATES, not about overflow lists in general.
    {
        auto clean =
            column_data_t::create_column(&env.resource, bm, 0, 0, complex_logical_type{logical_type::STRING_LITERAL});
        REQUIRE_FALSE(clean->initialize_column(persistent.value()).has_error());
    }

    // Now corrupt the stream the way a crossed/stale pointer would: the same overflow block
    // named twice for one segment. Mutated in place -- persistent_column_data_t owns its child
    // nodes through unique_ptr and is deliberately move-only. No file bytes are laid by hand:
    // this is the production load path fed a corrupt pointer set.
    auto& corrupted = persistent.value();
    bool duplicated = false;
    for (auto& dp : corrupted.data_pointers) {
        if (!dp.overflow_blocks.empty()) {
            dp.overflow_blocks.push_back(dp.overflow_blocks.front());
            duplicated = true;
            break;
        }
    }
    REQUIRE(duplicated);

    auto reloaded =
        column_data_t::create_column(&env.resource, bm, 0, 0, complex_logical_type{logical_type::STRING_LITERAL});
    auto loaded = reloaded->initialize_column(corrupted);
    REQUIRE(loaded.has_error());
    CHECK(loaded.error().type == core::error_code_t::data_corruption);

    cleanup_bigstr_file();
}
