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

    // DIFFERENCE (1) vs test_checkpoint_load::test_env_t -- a SMALL buffer-pool
    // limit. test_checkpoint_load uses a 4 GiB limit, which holds the whole table
    // resident. Here the limit is a few MiB so a large table's working set cannot
    // fit and disk-backed segments must be evicted + reloaded to scan it. Block
    // alloc size is 256 KiB (DEFAULT_BLOCK_ALLOC_SIZE), so ~4 MiB is ~16 blocks of
    // headroom -- far below the table built below.
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

    // Deterministic per-row string content. Long enough (and unique enough) that
    // the dictionary of each segment carries real bytes -- so the FLAT string
    // scan must materialise the payload, not a sequence-style placeholder.
    std::string expected_string(uint64_t row) {
        std::string s = "row_string_value_";
        std::string n = std::to_string(row);
        // Zero-pad to a fixed width so lengths are uniform and easy to reason about.
        if (n.size() < 8) {
            s.append(8 - n.size(), '0');
        }
        s += n;
        return s; // e.g. "row_string_value_00000042"
    }

    // STRING-column analogue of append_int64_data. Appends `count` rows whose
    // single STRING column holds expected_string(row) for the absolute row index.
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

    // Full scan that verifies value[i] == i for every row and returns the count.
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

    // Enough rows to dwarf SMALL_POOL_LIMIT: 256 row groups * 1024 rows = 262144
    // INT64 values = 2 MiB of raw column data spread across many segments, well
    // above the 4 MiB pool once buffers / overhead are counted, and far above the
    // per-block 256 KiB so it can never stay resident as one chunk.
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

// Streaming fetch-next (STEP 3) over a disk-backed table under forced eviction.
// The position-only cursor re-seeks a TRANSIENT scan state on every batch
// (data_table_t::fetch_next_batch), so each fetch independently re-pins the
// segment(s) for one vector via initialize_scan_with_offset -> get_segment. This
// is the path the agent_disk streaming source drives per batch; the whole-scan
// test helper (otterbrix_test::scan_table_segment) never exercises it. Asserts: the per-batch fetch
// reads EVERY row in order, correct values, and stays bounded under a pool small
// enough to evict between fetches (reloadable disk-backed segments must reload).
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

    // Force eviction so a re-seek to an evicted, reloadable segment must reload it.
    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(static_cast<int64_t>(0));
    auto types = table->copy_types();

    int64_t next_row = 0;
    const int64_t max_row = static_cast<int64_t>(LARGE_ROW_COUNT);
    bool drained = false;
    uint64_t scanned = 0;
    // Drive the streaming source loop exactly as the executor does: fetch one
    // batch, advance, repeat until drained.
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

// Streaming fetch-next over a table CHECKPOINTED to disk and REOPENED (load_from_disk),
// then scanned per-batch under a small pool. Mirrors the e2e disk-backed path where the
// table's segments are persisted (reloadable block_ids) and the streaming source re-seeks
// each batch. Reproduces the agent_disk streaming scan over a freshly loaded disk table.
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
        REQUIRE_FALSE(bm.write_header(header).has_error());
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

    // The unit of reclaim is compact + checkpoint, not compact alone.
    // compact() releases the blocks of the collection it replaces -- which is exactly the
    // collection the DURABLE root still points at -- so those ids are quarantined until a root
    // that does NOT name them is on disk. That is also the only sequence production ever runs
    // (agent_disk_t::checkpoint_inner compacts and then immediately checkpoints the same
    // entry). Repeated compaction WITHOUT a checkpoint grows the file by one collection per
    // round, deliberately: until a header commits, every one of those blocks is still part of
    // the state a crash recovers.
    //
    // The property under test is unchanged: reclaim must actually happen, so N rounds must not
    // cost N copies of the table.
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

    // The first round is not steady state: it is the round that pays for the second copy of
    // the table (the
    // superseded root's blocks only become reusable once the new header is durable, so both
    // copies live in the file at once), and it has no previous root's chains to reclaim yet.
    // The SECOND round is the first that can spend what the first one released, so that is
    // where the closed cycle starts.
    compact_and_checkpoint();
    const uint64_t blocks_at_steady_state = bm.total_blocks();

    constexpr int EXTRA_ROUNDS = 5;
    for (int i = 0; i < EXTRA_ROUNDS; i++) {
        compact_and_checkpoint();
    }
    const uint64_t blocks_after_repeated = bm.total_blocks();

    // Data must still be intact after repeated compaction.
    REQUIRE(table->calculate_size() == COMPACT_ROWS);
    REQUIRE(scan_and_verify_sequential(*table, COMPACT_ROWS) == COMPACT_ROWS);

    // Free-list reclaim: repeated compaction of unchanged data must not grow the file.
    //
    // NO per-round slack is allowed here. data_table_t::checkpoint hands the block manager the
    // data blocks of the root it is building, and the manager walks the DURABLE root's metadata
    // and free-list chains and files everything root N owns and root N+1 does not into
    // pending_free_. From the steady state on, an unchanged table is a closed cycle and the
    // block count must not move AT ALL.
    //
    // The one thing the root reclaim does not cover -- the previous LIVE tree, which no root
    // ever named -- is released by compact() itself in the very same round, which is why this
    // loop closes exactly.
    WARN("[A7.3] blocks after 1 compact+checkpoint round: "
         << blocks_after_first << ", at steady state (round 2): " << blocks_at_steady_state << ", after "
         << (EXTRA_ROUNDS + 2) << " rounds: " << blocks_after_repeated);
    REQUIRE(blocks_after_repeated == blocks_at_steady_state);
    REQUIRE(blocks_after_repeated < 2 * blocks_after_first);

    cleanup_test_file();
}

// ----------------------------------------------------------------------------
// Compact block allocation (segment packing) for the write-through path.
// ----------------------------------------------------------------------------

// Flush-before-evict correctness guard.
//
// The write-through path packs many small column segments into shared 256 KiB
// blocks via partial_block_manager_t::write_to_block, which only fills an
// IN-MEMORY block buffer; the block reaches the data file at
// flush_partial_blocks(). If a LIVE segment is re-pointed at such a packed
// block but that block is NOT yet flushed, a concurrent scan/eviction would
// load() an unwritten on-disk block -> checksum mismatch / data_corruption.
//
// This appends MORE THAN ONE row group (so the re-pointed packed segments span
// several blocks) under a TINY buffer pool that forces eviction of the
// re-pointed segments, then full-scans and verifies EXACT data. It is GREEN
// only when every write-through caller flushes its partial blocks BEFORE any
// eviction/reload of a re-pointed segment can occur. A wrong flush ordering
// makes it fail with data_corruption on reload.
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

    // > 1 row group of small (1024-row int64) packed segments. Each row group's
    // closed column segment is 8 KiB -- far below 0.8*256 KiB, so it is PACKED
    // into a shared partial block at a non-zero offset.
    append_int64_data(*table, &env.resource, LARGE_ROW_COUNT);
    REQUIRE(table->calculate_size() == LARGE_ROW_COUNT);

    // Force eviction of the re-pointed packed segments: only correctly flushed
    // shared blocks can be reloaded; an unflushed packed block would fail the
    // checksum on reload.
    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB

    // Full scan must read back EVERY row with the correct value (no corruption).
    REQUIRE(scan_and_verify_sequential(*table, LARGE_ROW_COUNT) == LARGE_ROW_COUNT);

    cleanup_test_file();
}

// Over-allocation bound.
//
// A dedicated 256 KiB block per small column segment burns one block PER COLUMN
// PER ROW GROUP plus one per validity child on a WIDE table (the real shape --
// e.g. SSB lineorder's ~17 columns), even though all those narrow segments
// together fit in a fraction of a block: measured at ~127x over-allocation,
// 14016 blocks / 3504 MB for 27.5 MB of data. Packing every column's (and
// validity's) segment into shared blocks via partial_block_manager_t makes the
// block count track the packed DATA size, not the segment COUNT.
//
// This builds a wide INT32 table so the per-row-group cross-column packing is
// exercised, and asserts total_blocks() is within a generous packing bound far
// below the one-block-per-segment count.
TEST_CASE("disk_backed_scan: B2 write-through packs segments, no per-segment over-allocation", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    // This case measures the ON-DISK block COUNT, not eviction behaviour. A WIDE
    // table appends NCOLS full-block transient segments per row group before they
    // are written-through, so it needs a pool large enough to hold one row group's
    // worth of transient segments resident during the append (NCOLS * 256 KiB).
    // Use a generous pool rather than the tiny SMALL_POOL_LIMIT used by the
    // eviction cases.
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    buffer_pool_t buffer_pool(&resource, uint64_t(256) << 20, false, uint64_t(1) << 24); // 256 MiB
    standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);

    single_file_block_manager_t bm(buffer_manager, fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    // A WIDE table: NCOLS narrow INT32 columns -- the over-allocation-prone shape.
    constexpr uint64_t NCOLS = 16;
    std::vector<column_definition_t> columns;
    for (uint64_t c = 0; c < NCOLS; c++) {
        columns.emplace_back("c" + std::to_string(c), logical_type::INTEGER);
    }
    auto table = std::make_unique<data_table_t>(&resource, bm, std::move(columns), "disk_backed");

    // ROW_GROUPS row groups of 1024 rows each. Each row-group close finalizes one
    // segment per column (1024*4 = 4 KiB) plus a validity child segment. Unpacked
    // that is ~(NCOLS value + NCOLS validity) dedicated blocks PER ROW GROUP; packed,
    // the ~NCOLS*4 KiB = 64 KiB of value data + tiny validity per row group fits in
    // ONE shared block.
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

    // Unpacked lower bound (`pre_b2_floor`, what a per-segment allocator consumes):
    // at least one dedicated block per CLOSED segment. Closed row groups = ROW_GROUPS-1 (the
    // last stays open), each closing NCOLS value + NCOLS validity segments.
    const uint64_t pre_b2_floor = (ROW_GROUPS - 1) * (NCOLS * 2);

    // Packing bound: per closed row group the packed value data is
    // ~NCOLS*1024*4 bytes; well-packed that is a handful of blocks total. Bound it
    // by the data volume with generous slack, NOT by the segment count.
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

// ----------------------------------------------------------------------------
// STRING-column streaming use-after-free guard.
// ----------------------------------------------------------------------------
//
// The FLAT-fast-path string scan (impl::string_scan_partial) writes each cell as
// a std::string_view that BORROWS directly into the buffer-pool-pinned block
// (fetch_string -> base_ptr + dict.end - offset). The streaming source
// (data_table_t::fetch_next_batch) scopes its table_scan_state -- hence its
// buffer_handle PIN -- per batch and releases it the moment the call returns.
// The produced chunk, carrying those borrowed views, OUTLIVES the pin and is
// handed back to the caller.
//
// On a disk-backed table under memory pressure the borrowed block is then
// evicted and RELOADED at a NEW address, so every held string_view dangles:
// reading the held chunk's STRING cell yields garbage / a wrong string / a
// crash. (The INT64 / gap / DICTIONARY paths are safe because they copy bytes;
// only this FLAT string fast path borrows.)
//
// Reproducer (deterministic without ASAN -- the reload-at-new-address makes the
// dangle observable as a value mismatch):
//   1. Build a disk-backed STRING table whose working set far exceeds the pool.
//   2. fetch ONE batch and HOLD the chunk (do NOT copy its strings out).
//   3. Drain the REST of the table through throwaway fetches under a 1 MiB pool
//      so the held batch's block is evicted, then reloaded at a new address.
//   4. Read the held chunk's STRING cells -- they must still equal the original
//      values, which holds only because the payload is interned into the result
//      vector's string_vector_buffer_t.
TEST_CASE("disk_backed_scan: streaming STRING batch survives block eviction (no UAF)", "[step2]") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t table_pointer;

    // Build + checkpoint a disk-backed STRING table under a generous pool. The
    // write-through string append transiently pins per-segment dictionary blocks,
    // so the tiny SMALL_POOL_LIMIT used by the int64 eviction cases OOMs it; build
    // under a roomy pool, persist to disk, then REOPEN under SMALL_POOL_LIMIT so
    // the segments are clean reloadable disk blocks (no pinned append-time state),
    // exactly the e2e disk-backed streaming-scan shape. Mirrors the existing
    // "streaming fetch_next_batch over reopened checkpointed table" case.
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

    // Reopen under the SMALL pool and stream a single held batch under eviction.
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.load_existing_database().has_error());
    metadata_manager_t meta_mgr(bm);
    metadata_reader_t reader(meta_mgr, table_pointer);
    auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
    REQUIRE(!loaded_result.has_error());
    auto& loaded = loaded_result.value();

    // Force eviction so a re-seek to an evicted, reloadable segment must reload it
    // at a FRESH address -- the trigger that makes a borrowed view dangle.
    REQUIRE(!env.buffer_pool.set_limit(uint64_t(1) << 20).has_error()); // 1 MiB

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(static_cast<int64_t>(0));
    auto types = loaded->copy_types();

    int64_t next_row = 0;
    const int64_t max_row = static_cast<int64_t>(LARGE_ROW_COUNT);
    bool drained = false;

    // (2) Fetch the FIRST batch and HOLD it -- the chunk outlives its source pin.
    data_chunk_t held(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
    {
        auto r =
            loaded->fetch_next_batch(held, column_ids, nullptr, transaction_data{0, 0}, next_row, max_row, drained);
        REQUIRE_FALSE(r.has_error());
    }
    REQUIRE(held.size() > 0);
    const uint64_t held_count = held.size();

    // (3) Drain the rest of the table through throwaway batches. With a 1 MiB pool
    // the held batch's block is forced out and later reloaded at a new address.
    while (!drained) {
        data_chunk_t scratch(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        auto r =
            loaded->fetch_next_batch(scratch, column_ids, nullptr, transaction_data{0, 0}, next_row, max_row, drained);
        REQUIRE_FALSE(r.has_error());
    }

    // (4) The held chunk's STRING cells must still read back the ORIGINAL values.
    // value() materialises a std::string FROM the stored string_view: if that
    // view dangles into an evicted/reloaded block, this reads garbage.
    for (uint64_t i = 0; i < held_count; i++) {
        auto val = held.data[0].value(i);
        REQUIRE(val.type().type() == logical_type::STRING_LITERAL);
        std::string got = *val.value<std::string*>();
        REQUIRE(got == expected_string(i));
    }

    cleanup_test_file();
}

// ---------------------------------------------------------------------------
// Multi-row-group revert on a DISK table must leave surviving rows byte-intact.
//
// The bounded-DML abort shape: a committed seed row, then a large append that
// spans several row groups (each FILLED row group is closed mid-append and its
// segments are write-through PACKED into shared partial blocks: the data
// segment at offset 0, its validity child BEHIND it in the SAME block), then
// the whole append is reverted (constraint rejected at finalize).
//
// revert_append on the PACKED validity segment must address its bitmap at
// handle.ptr() + block_offset(); addressing the block BASE instead smashes
// 0xFF over the data segment packed at offset 0 -- the surviving row 0 of the
// FIRST column then reads a mask-shaped value (-1/-2), which is exactly the
// integration failure bounded_dml_flush::error_after_mid_flush_reverts_all.
// ---------------------------------------------------------------------------
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

    // Seed ONE committed row (value 42) in its own finalized append -- the survivor.
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

    // Large append: rows 1..kBigAppend with values DISTINCT from the seed, crossing
    // >= 4 row-group boundaries (row_group_size == DEFAULT_VECTOR_CAPACITY) so several
    // row groups fill, close, and get their segments packed to disk mid-append.
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

    // Revert the WHOLE large append (row 1 onward), exactly like the DML abort does.
    REQUIRE_FALSE(table->revert_append(1, kBigAppend).has_error());
    REQUIRE(table->row_group()->total_rows() == 1);

    // The surviving row 0 must still hold its original value, byte-for-byte.
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
