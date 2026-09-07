// Write-through gives a filled segment a real block_id (< MAXIMUM_BLOCK) before any checkpoint, so it's
// reloadable and a scan under a tiny pool completes bounded; without it the segment stays transient and
// pins the whole working set resident. Cases assert OBSERVABLE properties only. Harness modelled on
// test_checkpoint_load.cpp.

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
#include <unistd.h>

#include "table_segment_scan.hpp"

namespace {
    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_disk_backed_scan_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_test_file() { std::remove(test_db_path().c_str()); }

    // A SMALL buffer-pool limit (a few MiB, vs 4 GiB in test_checkpoint_load) so the working set can't fit resident.
    constexpr uint64_t SMALL_POOL_LIMIT = uint64_t(4) << 20; // 4 MiB

    struct test_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;

        test_env_t()
            : buffer_pool(&resource, SMALL_POOL_LIMIT, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    void
    append_int64_data(components::table::data_table_t& table, std::pmr::memory_resource* resource, uint64_t count) {
        using namespace components::types;
        using namespace components::vector;
        using namespace components::table;

        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, static_cast<int64_t>(offset + i));
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // Long, unique per-row content so each segment's dictionary carries real bytes to materialise, not a placeholder.
    std::string expected_string(uint64_t row) {
        std::string s = "row_string_value_";
        std::string n = std::to_string(row);
        if (n.size() < 8) {
            s.append(8 - n.size(), '0');
        }
        s += n;
        return s; // e.g. "row_string_value_00000042"
    }

    void
    append_string_data(components::table::data_table_t& table, std::pmr::memory_resource* resource, uint64_t count) {
        using namespace components::types;
        using namespace components::vector;
        using namespace components::table;

        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, logical_value_t{resource, expected_string(offset + i)});
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    uint64_t scan_and_verify_sequential(components::table::data_table_t& table, uint64_t expected_rows) {
        using namespace components::vector;
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(table, 0, expected_rows, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto val = chunk.data[0].value(i);
                REQUIRE(val.value<int64_t>() == static_cast<int64_t>(scanned + i));
            }
            scanned += chunk.size();
        });
        return scanned;
    }

    // Enough rows to dwarf SMALL_POOL_LIMIT and the per-block 256 KiB, so it can never stay resident as one chunk.
    constexpr uint64_t LARGE_ROW_COUNT = components::vector::DEFAULT_VECTOR_CAPACITY * 256; // 262144 rows
} // namespace

TEST_CASE("disk_backed_scan: large table full scan completes bounded with correct values", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    // Table lives in the outer scope so we can probe disk state mid-life and re-scan the same live object.
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    uint64_t scanned = scan_and_verify_sequential(*table, LARGE_ROW_COUNT);
    REQUIRE(scanned == LARGE_ROW_COUNT);

    cleanup_test_file();
}

TEST_CASE("disk_backed_scan: filled segments are written through to disk before checkpoint", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    REQUIRE(bm.total_blocks() == 0);

    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    // column_segment_info doesn't expose a per-segment block_id, so total_blocks() > 0 is the proxy used here.
    REQUIRE(bm.total_blocks() > 0);

    cleanup_test_file();
}

// Exercises fetch_next_batch (per-batch re-seek/re-pin), unlike the whole-scan helper used elsewhere.
TEST_CASE("disk_backed_scan: streaming fetch_next_batch reloads correctly under eviction", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(static_cast<int64_t>(0));
    auto types = table->copy_types();

    int64_t next_row = 0;
    const int64_t max_row = static_cast<int64_t>(LARGE_ROW_COUNT);
    bool drained = false;
    uint64_t scanned = 0;
    while (!drained) {
        data_chunk_t batch(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        auto r =
            table->fetch_next_batch(batch, column_ids, nullptr, transaction_data{0, 0}, next_row, max_row, drained);
        REQUIRE_FALSE(r.has_error());
        for (uint64_t i = 0; i < batch.size(); i++) {
            auto val = batch.data[0].value(i);
            REQUIRE(val.value<int64_t>() == static_cast<int64_t>(scanned + i));
        }
        scanned += batch.size();
    }
    REQUIRE(scanned == LARGE_ROW_COUNT);

    cleanup_test_file();
}

TEST_CASE("disk_backed_scan: streaming fetch_next_batch over reopened checkpointed table", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");
        append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();
        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // force eviction between fetches

        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(static_cast<int64_t>(0));
        auto types = loaded->copy_types();

        int64_t next_row = 0;
        const int64_t max_row = static_cast<int64_t>(LARGE_ROW_COUNT);
        bool drained = false;
        uint64_t scanned = 0;
        while (!drained) {
            data_chunk_t batch(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
            auto r =
                loaded
                    ->fetch_next_batch(batch, column_ids, nullptr, transaction_data{0, 0}, next_row, max_row, drained);
            REQUIRE_FALSE(r.has_error());
            for (uint64_t i = 0; i < batch.size(); i++) {
                auto val = batch.data[0].value(i);
                REQUIRE(val.value<int64_t>() == static_cast<int64_t>(scanned + i));
            }
            scanned += batch.size();
        }
        REQUIRE(scanned == LARGE_ROW_COUNT);
    }

    cleanup_test_file();
}

TEST_CASE("disk_backed_scan: forced eviction then re-scan reloads correctly", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    REQUIRE(scan_and_verify_sequential(*table, LARGE_ROW_COUNT) == LARGE_ROW_COUNT);

    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB -- forces eviction
    REQUIRE(scan_and_verify_sequential(*table, LARGE_ROW_COUNT) == LARGE_ROW_COUNT);

    cleanup_test_file();
}

TEST_CASE("disk_backed_scan: checkpoint of large table, reopen yields identical data", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

        append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
        REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        REQUIRE(loaded->table_name() == "disk_backed");
        REQUIRE(loaded->column_count() == 1);
        REQUIRE(scan_and_verify_sequential(*loaded, LARGE_ROW_COUNT) == LARGE_ROW_COUNT);
    }

    cleanup_test_file();
}

TEST_CASE("disk_backed_scan: repeated compaction does not bloat the file", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    constexpr uint64_t COMPACT_ROWS = DEFAULT_VECTOR_CAPACITY * 16; // 16384 rows
    append_int64_data(*table, &env.resource, COMPACT_ROWS);
    REQUIRE(table->calculate_size() == COMPACT_ROWS);

    // compact() alone doesn't reclaim: it only quarantines blocks until a checkpoint stops naming them.
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();
    auto compact_and_checkpoint = [&]() {
        REQUIRE(table->compact(WATERMARK));
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
    };

    compact_and_checkpoint();
    const uint64_t blocks_after_first = bm.total_blocks();

    // Round 1 has no prior root to reclaim from, so round 2 is the first steady-state baseline.
    compact_and_checkpoint();
    const uint64_t blocks_at_steady_state = bm.total_blocks();

    constexpr int EXTRA_ROUNDS = 5;
    for (int i = 0; i < EXTRA_ROUNDS; i++) {
        compact_and_checkpoint();
    }
    const uint64_t blocks_after_repeated = bm.total_blocks();

    REQUIRE(table->calculate_size() == COMPACT_ROWS);
    REQUIRE(scan_and_verify_sequential(*table, COMPACT_ROWS) == COMPACT_ROWS);

    // No per-round slack: at steady state the block count must not move at all.
    WARN("[A7.3] blocks after 1 compact+checkpoint round: "
         << blocks_after_first << ", at steady state (round 2): " << blocks_at_steady_state << ", after "
         << (EXTRA_ROUNDS + 2) << " rounds: " << blocks_after_repeated);
    REQUIRE(blocks_after_repeated == blocks_at_steady_state);
    REQUIRE(blocks_after_repeated < 2 * blocks_after_first);

    cleanup_test_file();
}

// Compact block allocation (segment packing) for the write-through path.

// Flush-before-evict guard: a LIVE segment re-pointed at a packed block that's NOT yet flushed would fail
// load() with a checksum mismatch/data_corruption if a concurrent scan/eviction hit it first. GREEN only
// when every write-through caller flushes before any eviction of a re-pointed segment can occur.
TEST_CASE("disk_backed_scan: B2 packed segments reload exactly under forced eviction", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    // Each closed row group's column segment is 8 KiB, far below 0.8*256 KiB, so it PACKS into a shared block.
    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB

    REQUIRE(scan_and_verify_sequential(*table, LARGE_ROW_COUNT) == LARGE_ROW_COUNT);

    cleanup_test_file();
}

// A dedicated 256 KiB block per column segment on a WIDE table (SSB lineorder, ~17 columns) measured
// ~127x over-allocation: 14016 blocks / 3504 MB for 27.5 MB of data. Packing tracks data size instead.
TEST_CASE("disk_backed_scan: B2 write-through packs segments, no per-segment over-allocation", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    // Measures ON-DISK block count, not eviction: a generous pool holds one row group's transient segments.
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    buffer_pool_t buffer_pool(&resource, uint64_t(256) << 20, false, uint64_t(1) << 24); // 256 MiB
    standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);

    single_file_block_manager_t bm(buffer_manager, fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    constexpr uint64_t NCOLS = 16;
    std::vector<column_definition_t> columns;
    for (uint64_t c = 0; c < NCOLS; c++) {
        columns.emplace_back("c" + std::to_string(c), logical_type::INTEGER);
    }
    auto table = std::make_unique<data_table_t>(&resource, bm, std::move(columns), "disk_backed");

    constexpr uint64_t ROW_GROUPS = 16;
    constexpr uint64_t ROWS = DEFAULT_VECTOR_CAPACITY * ROW_GROUPS; // 16384 rows

    auto types = table->copy_types();
    uint64_t offset = 0;
    while (offset < ROWS) {
        uint64_t batch = std::min(ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
        data_chunk_t chunk(&resource, types, batch);
        chunk.set_cardinality(batch);
        for (uint64_t col = 0; col < NCOLS; col++) {
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(col, i, logical_value_t{&resource, static_cast<int32_t>(offset + i)});
            }
        }
        table_append_state state(&resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
        offset += batch;
    }
    REQUIRE(table->calculate_size() == ROWS);

    const uint64_t blocks = bm.total_blocks();
    const uint64_t block_size = bm.block_size();

    // pre_b2_floor: what a per-segment allocator would consume, at least one dedicated block per CLOSED segment.
    const uint64_t pre_b2_floor = (ROW_GROUPS - 1) * (NCOLS * 2);

    // Bound by data volume with generous slack, NOT by segment count.
    const uint64_t value_bytes = ROWS * NCOLS * sizeof(int32_t);
    const uint64_t packed_blocks = (value_bytes + block_size - 1) / block_size;
    const uint64_t bound = (ROW_GROUPS * 2) + packed_blocks + 8; // per-row-group block + data + slack

    INFO("total_blocks=" << blocks << " pre_b2_floor=" << pre_b2_floor << " packed_blocks=" << packed_blocks
                         << " bound=" << bound);
    // The bound must actually separate the two regimes (sanity on the test itself).
    REQUIRE(bound < pre_b2_floor);
    REQUIRE(blocks <= bound);

    cleanup_test_file();
}

// The FLAT-fast-path string scan writes each cell as a std::string_view that BORROWS directly into the
// buffer-pool-pinned block; fetch_next_batch releases that pin per batch, so a held chunk can outlive it.
// Under eviction the block reloads at a NEW address and a borrowed view dangles -- safe here only because
// the payload is interned into the result vector's string_vector_buffer_t (INT64/gap/DICTIONARY copy bytes instead).
TEST_CASE("disk_backed_scan: streaming STRING batch survives block eviction (no UAF)", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t table_pointer;

    // Built under a generous pool (string append pins per-segment dictionary blocks, OOMing SMALL_POOL_LIMIT).
    {
        core::pmr::otterbrix_resource big_resource;
        core::filesystem::local_file_system_t big_fs;
        buffer_pool_t big_pool(&big_resource, uint64_t(256) << 20, false, uint64_t(1) << 24); // 256 MiB
        standard_buffer_manager_t big_bm_mgr(&big_resource, big_fs, big_pool);

        single_file_block_manager_t bm(big_bm_mgr, big_fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&big_resource, bm, std::move(columns), "disk_backed");
        append_string_data(*table, &big_resource, LARGE_ROW_COUNT);
        REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();
        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.load_existing_database().has_error());
    metadata_manager_t meta_mgr(bm);
    metadata_reader_t reader(meta_mgr, table_pointer);
    auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
    REQUIRE(!loaded_result.has_error());
    auto& loaded = loaded_result.value();

    // Reload at a FRESH address is the trigger that makes a borrowed view dangle.
    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(static_cast<int64_t>(0));
    auto types = loaded->copy_types();

    int64_t next_row = 0;
    const int64_t max_row = static_cast<int64_t>(LARGE_ROW_COUNT);
    bool drained = false;

    // Fetch the FIRST batch and HOLD it -- the chunk outlives its source pin.
    data_chunk_t held(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
    {
        auto r =
            loaded->fetch_next_batch(held, column_ids, nullptr, transaction_data{0, 0}, next_row, max_row, drained);
        REQUIRE_FALSE(r.has_error());
    }
    REQUIRE(held.size() > 0);
    const uint64_t held_count = held.size();

    // Drain the rest through throwaway batches; the 1 MiB pool forces the held batch's block out.
    while (!drained) {
        data_chunk_t scratch(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        auto r =
            loaded->fetch_next_batch(scratch, column_ids, nullptr, transaction_data{0, 0}, next_row, max_row, drained);
        REQUIRE_FALSE(r.has_error());
    }

    // value() materialises a std::string FROM the stored string_view: a dangling view reads garbage here.
    for (uint64_t i = 0; i < held_count; i++) {
        auto val = held.data[0].value(i);
        REQUIRE(val.type().type() == logical_type::STRING_LITERAL);
        std::string got = *val.value<std::string*>();
        REQUIRE(got == expected_string(i));
    }

    cleanup_test_file();
}

// revert_append on a validity segment PACKED into a shared block must address its bitmap at
// handle.ptr()+block_offset(), not the block BASE, or it smashes 0xFF over the packed data segment
// (regression: bounded_dml_flush::error_after_mid_flush_reverts_all).
TEST_CASE("disk_backed_scan: multi-row-group revert leaves surviving row intact", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    {
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, int64_t{42});
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }

    // Spans >= 4 row groups so several close and get packed to disk mid-append.
    REQUIRE(table->row_group()->row_group_size() == DEFAULT_VECTOR_CAPACITY);
    constexpr uint64_t kBigAppend = DEFAULT_VECTOR_CAPACITY * 4;
    {
        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < kBigAppend) {
            uint64_t batch = std::min(kBigAppend - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, static_cast<int64_t>(100000 + offset + i));
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }
    REQUIRE(table->row_group()->total_rows() == 1 + kBigAppend);

    REQUIRE_FALSE(table->revert_append(1, kBigAppend).has_error());
    REQUIRE(table->row_group()->total_rows() == 1);

    int64_t got = 0;
    uint64_t rows = 0;
    otterbrix_test::scan_table_segment(*table, 0, 1, [&](data_chunk_t& chunk) {
        for (uint64_t i = 0; i < chunk.size(); i++) {
            got = chunk.data[0].value(i).value<int64_t>();
            rows++;
        }
    });
    REQUIRE(rows == 1);
    REQUIRE(got == 42);

    cleanup_test_file();
}
