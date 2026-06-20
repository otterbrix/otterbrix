// Disk-backed write-through for a DISK-backed table whose data far exceeds its
// buffer-pool limit. These cases assert OBSERVABLE properties only (row counts,
// values, on-disk block counts) so they remain valid regardless of the internal
// mechanism. The invariant under test:
//   * Without write-through a FILLED segment is a transient in-memory block
//     (block_id >= MAXIMUM_BLOCK, not reloadable). Nothing is written to the
//     .otbx file until checkpoint, so total_blocks() stays 0 mid-append and the
//     working set is pinned fully resident -- a scan with a tiny pool cannot run
//     bounded.
//   * With write-through a filled segment is written through to disk (gets a real
//     block_id < MAXIMUM_BLOCK), so it appears in total_blocks() BEFORE any
//     checkpoint, becomes reloadable, and the pool can evict + reload it; the
//     scan then completes bounded with the correct result.
//
// Harness modelled on test_checkpoint_load.cpp (DISK-backed data_table_t over a
// single_file_block_manager_t on a temp .otbx, append / checkpoint / reopen),
// with TWO deliberate differences, documented inline below:
//   (1) a SMALL buffer-pool limit so the table's working set exceeds it;
//   (2) the table object is created in the OUTER scope (not a nested write
//       block) so we can observe total_blocks() mid-append, force eviction, and
//       re-scan the SAME live table before checkpoint.

#include <catch2/catch.hpp>
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

namespace {
    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_disk_backed_scan_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_test_file() { std::remove(test_db_path().c_str()); }

    // DIFFERENCE (1) vs test_checkpoint_load::test_env_t -- a SMALL buffer-pool
    // limit. test_checkpoint_load uses a 4 GiB limit, which holds the whole table
    // resident. Here the limit is a few MiB so a large table's working set cannot
    // fit and disk-backed segments must be evicted + reloaded to scan it. Block
    // alloc size is 256 KiB (DEFAULT_BLOCK_ALLOC_SIZE), so ~4 MiB is ~16 blocks of
    // headroom -- far below the table built below.
    constexpr uint64_t SMALL_POOL_LIMIT = uint64_t(4) << 20; // 4 MiB

    struct test_env_t {
        std::pmr::synchronized_pool_resource resource;
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;

        test_env_t()
            : buffer_pool(&resource, SMALL_POOL_LIMIT, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    void append_int64_data(components::table::data_table_t& table,
                           std::pmr::memory_resource* resource,
                           uint64_t count) {
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
                chunk.set_value(0, i, logical_value_t{resource, static_cast<int64_t>(offset + i)});
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // Full scan that verifies value[i] == i for every row and returns the count.
    uint64_t scan_and_verify_sequential(components::table::data_table_t& table, uint64_t expected_rows) {
        using namespace components::vector;
        uint64_t scanned = 0;
        table.scan_table_segment(0, expected_rows, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto val = chunk.data[0].value(i);
                REQUIRE(val.value<int64_t>() == static_cast<int64_t>(scanned + i));
            }
            scanned += chunk.size();
        });
        return scanned;
    }

    // Enough rows to dwarf SMALL_POOL_LIMIT: 256 row groups * 1024 rows = 262144
    // INT64 values = 2 MiB of raw column data spread across many segments, well
    // above the 4 MiB pool once buffers / overhead are counted, and far above the
    // per-block 256 KiB so it can never stay resident as one chunk.
    constexpr uint64_t LARGE_ROW_COUNT =
        components::vector::DEFAULT_VECTOR_CAPACITY * 256; // 262144 rows
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
    // DIFFERENCE (2) vs test_checkpoint_load -- table lives in the outer scope so
    // we can probe disk state mid-life and re-scan the same live object.
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "disk_backed");

    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    // A full scan returns ALL rows with correct values. With a tiny pool and a
    // working set far larger than it, the scan can only run bounded if segments
    // are disk-backed and so can be evicted + reloaded; transient
    // (non-reloadable) segments would stay fully resident and OOM.
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

    // Immediately after create_new_database, no user-data blocks exist on disk.
    REQUIRE(bm.total_blocks() == 0);

    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    // Write-through happened: at least one FILLED segment is disk-backed,
    // observable as on-disk blocks allocated BY THE APPEND PATH, before any
    // checkpoint. Without write-through nothing is flushed until checkpoint and
    // this would be 0; with it, filled segments get real block_ids
    // (< MAXIMUM_BLOCK) and are written through, so total_blocks() > 0 here.
    //
    // NOTE on the chosen observable: column_segment_info from
    // get_column_segment_info() does NOT carry a per-segment block_id (the field
    // is left default-initialized), so block_id < MAXIMUM_BLOCK is not directly
    // observable through that public API. total_blocks() on the on-disk block
    // manager is the equivalent observable: a non-zero pre-checkpoint count means
    // a filled segment was given a real (disk-backed) block and written through.
    REQUIRE(bm.total_blocks() > 0);

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

    // First scan populates / exercises the pool.
    REQUIRE(scan_and_verify_sequential(*table, LARGE_ROW_COUNT) == LARGE_ROW_COUNT);

    // Force eviction by shrinking the pool to the floor, then re-scan. Only
    // reloadable (disk-backed) segments can survive eviction + reload; transient
    // segments could not reproduce the data bounded. The re-scan must still be
    // fully correct.
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

    // write + checkpoint phase, under the SMALL pool.
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
        // Checkpoint of a large table completes with a pool too small to hold it
        // resident -- the column flush must stream disk-backed segments and stay
        // bounded rather than pinning the whole working set.
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    // reopen phase — identical row count + values read back from the file.
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

    // A modest table is enough to exercise the free-list reclaim observable.
    constexpr uint64_t COMPACT_ROWS = DEFAULT_VECTOR_CAPACITY * 16; // 16384 rows
    append_int64_data(*table, &env.resource, COMPACT_ROWS);
    REQUIRE(table->calculate_size() == COMPACT_ROWS);

    // Compact once (watermark above all stamps -> compaction is allowed), record
    // the on-disk block footprint, then compact several more times.
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();
    table->compact(WATERMARK);
    uint64_t blocks_after_first = bm.total_blocks();

    for (int i = 0; i < 5; i++) {
        table->compact(WATERMARK);
    }
    uint64_t blocks_after_repeated = bm.total_blocks();

    // Data must still be intact after repeated compaction.
    REQUIRE(table->calculate_size() == COMPACT_ROWS);
    REQUIRE(scan_and_verify_sequential(*table, COMPACT_ROWS) == COMPACT_ROWS);

    // Free-list reclaim: repeated compaction of unchanged data must not grow the
    // file unbounded. Each compaction should reuse reclaimed blocks rather than
    // always appending new ones, so there is no net growth beyond the first
    // compaction's footprint.
    REQUIRE(blocks_after_repeated <= blocks_after_first);

    cleanup_test_file();
}
