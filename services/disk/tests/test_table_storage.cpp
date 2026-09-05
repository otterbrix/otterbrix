#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/table/column_definition.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/table/test/block_reachability_walker.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <filesystem>
#include <set>
#include <string>
#include <unistd.h>

using namespace services::disk;
using namespace components::table;
using namespace components::types;
using namespace components::vector;

namespace {
    std::string test_dir() {
        static std::string path = "/tmp/test_otterbrix_table_storage_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_test_dir() { std::filesystem::remove_all(test_dir()); }

    std::vector<storage_index_t> make_column_indices(uint64_t count) {
        std::vector<storage_index_t> indices;
        indices.reserve(count);
        for (uint64_t i = 0; i < count; i++) {
            indices.emplace_back(static_cast<int64_t>(i));
        }
        return indices;
    }

    void append_int64_data(data_table_t& table, std::pmr::memory_resource* resource, uint64_t count) {
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
            auto append_lock_result = table.append_lock(state);
            REQUIRE_FALSE(append_lock_result.has_error());
            auto initialize_append_result = table.initialize_append(state);
            REQUIRE_FALSE(initialize_append_result.has_error());
            auto append_result = table.append(chunk, state);
            REQUIRE_FALSE(append_result.has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // ---- B3c drop_column-on-DISK helpers (see the test at the bottom of this file) ----

    // 40 UBIGINTs per row: 40 * 8 B * 2048 rows = 640 KiB of child payload per row group,
    // past partial_block_manager_t's FULL_THRESHOLD, so the child segments take DEDICATED
    // blocks. Without that, B2 packs every child segment alongside the flat column's and the
    // drop can provably free nothing.
    constexpr uint64_t DROP_LIST_LENGTH = 40;

    complex_logical_type drop_list_type() { return complex_logical_type::create_list(logical_type::UBIGINT); }

    void append_drop_rows(data_table_t& table, std::pmr::memory_resource* resource, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                std::vector<uint64_t> list;
                list.reserve(DROP_LIST_LENGTH);
                for (uint64_t j = 0; j < DROP_LIST_LENGTH; j++) {
                    list.emplace_back(row * 100 + j);
                }
                chunk.set_value(1, i, list);
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // The walker judges the DURABLE file, so it needs the storage's own block manager. The
    // counted collection copy row_group() hands back is scoped to reading the reference out of
    // it (ITEM C): a holder kept across a later compact would keep a replaced collection's
    // block handles alive past their reclaim.
    otterbrix_test::walk_report_t
    walk_storage(table_storage_t& ts, const std::string& path, std::pmr::memory_resource* resource) {
        components::table::storage::single_file_block_manager_t* bm = nullptr;
        {
            auto collection = ts.table().row_group();
            bm = static_cast<components::table::storage::single_file_block_manager_t*>(&collection->block_manager());
        }
        return otterbrix_test::walk_blocks(*bm, path, resource);
    }

    // Content-addressed: every surviving row must still carry its own row number in "a", so a
    // block that was freed while something still read it shows up as wrong data, not as a
    // count that happens to match.
    uint64_t scan_a_column(data_table_t& table, std::pmr::memory_resource* resource) {
        std::vector<storage_index_t> column_ids{storage_index_t(0)};
        table_scan_state state(resource);
        table.initialize_scan(state, column_ids, nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(seen));
                seen++;
            }
        }
        return seen;
    }

    std::string dump_ids(const std::set<uint64_t>& ids) {
        std::string out = "{";
        for (auto id : ids) {
            if (out.size() > 1) {
                out += ", ";
            }
            out += std::to_string(id);
        }
        out += "}";
        return out;
    }

    uint64_t file_size_or_zero(const std::string& path) {
        std::error_code ec;
        auto size = std::filesystem::file_size(path, ec);
        return ec ? uint64_t{0} : static_cast<uint64_t>(size);
    }
} // namespace

TEST_CASE("services::disk::table_storage::in_memory") {
    core::pmr::otterbrix_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);

    // Insert data
    append_int64_data(ts.table(), &resource, 100);
    REQUIRE(ts.table().calculate_size() == 100);

    // Scan and verify
    auto types = ts.table().copy_types();
    data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
    table_scan_state scan_state(&resource);
    auto column_indices = make_column_indices(ts.table().column_count());
    ts.table().initialize_scan(scan_state, column_indices);
    ts.table().scan(result, scan_state);
    REQUIRE(result.size() == 100);

    for (uint64_t i = 0; i < result.size(); i++) {
        auto val = result.data[0].value(i);
        REQUIRE(val.value<int64_t>() == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::disk::table_storage::disk_checkpoint_and_load") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "test_table.otbx";
    constexpr uint64_t NUM_ROWS = 500;

    // Create, insert, checkpoint
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);

        append_int64_data(ts.table(), &resource, NUM_ROWS);
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);

        auto checkpoint_result = ts.checkpoint();
        REQUIRE_FALSE(checkpoint_result.has_error());
    }

    // Load and verify
    {
        table_storage_t ts(&resource, otbx_path, {});
        REQUIRE(ts.mode() == storage_mode_t::DISK);

        auto& table = ts.table();
        REQUIRE(table.calculate_size() == NUM_ROWS);

        auto types = table.copy_types();
        data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
        table_scan_state scan_state(&resource);
        auto column_indices = make_column_indices(table.column_count());
        table.initialize_scan(scan_state, column_indices);
        table.scan(result, scan_state);
        REQUIRE(result.size() == static_cast<uint64_t>(std::min(NUM_ROWS, uint64_t(DEFAULT_VECTOR_CAPACITY))));

        for (uint64_t i = 0; i < result.size(); i++) {
            auto val = result.data[0].value(i);
            REQUIRE(val.value<int64_t>() == static_cast<int64_t>(i));
        }
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::mode_query") {
    core::pmr::otterbrix_resource resource;

    // In-memory (schema-less)
    {
        table_storage_t ts(&resource);
        REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);
    }

    // In-memory (with columns)
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("x", logical_type::DOUBLE);
        table_storage_t ts(&resource, std::move(columns));
        REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);
    }

    // Disk (new)
    {
        cleanup_test_dir();
        std::filesystem::create_directories(test_dir());
        auto otbx_path = std::filesystem::path(test_dir()) / "mode_test.otbx";
        std::vector<column_definition_t> columns;
        columns.emplace_back("x", logical_type::DOUBLE);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);
        cleanup_test_dir();
    }
}

TEST_CASE("services::disk::table_storage::checkpoint_preserves_multi_column") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "multi_col.otbx";
    constexpr uint64_t NUM_ROWS = 200;

    // Create multi-column disk table, insert, checkpoint
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("score", logical_type::DOUBLE);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);

        auto types = ts.table().copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            uint64_t batch = std::min(NUM_ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, static_cast<int64_t>(offset + i));
                chunk.set_value(1, i, static_cast<double>(offset + i) * 1.5);
            }
            table_append_state state(&resource);
            auto append_lock_result = ts.table().append_lock(state);
            REQUIRE_FALSE(append_lock_result.has_error());
            auto initialize_append_result = ts.table().initialize_append(state);
            REQUIRE_FALSE(initialize_append_result.has_error());
            auto append_result = ts.table().append(chunk, state);
            REQUIRE_FALSE(append_result.has_error());
            ts.table().finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);
        auto checkpoint_result = ts.checkpoint();
        REQUIRE_FALSE(checkpoint_result.has_error());
    }

    // Load and verify both columns
    {
        table_storage_t ts(&resource, otbx_path, {});
        REQUIRE(ts.mode() == storage_mode_t::DISK);
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);
        REQUIRE(ts.table().column_count() == 2);

        auto types = ts.table().copy_types();
        data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
        table_scan_state scan_state(&resource);
        auto column_indices = make_column_indices(ts.table().column_count());
        ts.table().initialize_scan(scan_state, column_indices);
        ts.table().scan(result, scan_state);
        REQUIRE(result.size() == NUM_ROWS);

        for (uint64_t i = 0; i < result.size(); i++) {
            auto id_val = result.data[0].value(i);
            auto score_val = result.data[1].value(i);
            REQUIRE(id_val.value<int64_t>() == static_cast<int64_t>(i));
            REQUIRE(score_val.value<double>() == Catch::Approx(static_cast<double>(i) * 1.5));
        }
    }

    cleanup_test_dir();
}

// Physical column compaction primitive, IN_MEMORY half. table_storage_t::drop_column removes
// the named column from the data_table_t via the rebuild constructor
// (data_table_t(parent, removed_column) backed by collection_t::remove_column per row_group
// segment). An in-memory table has no block manager to charge, so the rebuild IS the whole
// release; the DISK half — where the blocks have to come back through a committed header — is
// gated by drop_column_disk_frees_blocks at the bottom of this file.
TEST_CASE("services::disk::table_storage::drop_column_in_memory") {
    core::pmr::otterbrix_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("a", logical_type::BIGINT);
    columns.emplace_back("b", logical_type::BIGINT);
    columns.emplace_back("c", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));
    REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);
    REQUIRE(ts.table().column_count() == 3);

    // Append 32 rows: a=i, b=i*10, c=i*100.
    constexpr uint64_t NUM_ROWS = 32;
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; ++i) {
            chunk.set_value(0, i, static_cast<int64_t>(i));
            chunk.set_value(1, i, static_cast<int64_t>(i * 10));
            chunk.set_value(2, i, static_cast<int64_t>(i * 100));
        }
        table_append_state state(&resource);
        auto append_lock_result = ts.table().append_lock(state);
        REQUIRE_FALSE(append_lock_result.has_error());
        auto initialize_append_result = ts.table().initialize_append(state);
        REQUIRE_FALSE(initialize_append_result.has_error());
        auto append_result = ts.table().append(chunk, state);
        REQUIRE_FALSE(append_result.has_error());
        ts.table().finalize_append(state, transaction_data{0, 0});
    }
    REQUIRE(ts.table().calculate_size() == NUM_ROWS);

    // Drop the middle column "b". Rebuild constructor must produce {a, c} with
    // physical data preserved for the remaining columns.
    REQUIRE(ts.drop_column("b"));
    REQUIRE(ts.table().column_count() == 2);
    REQUIRE(ts.table().columns()[0].name() == "a");
    REQUIRE(ts.table().columns()[1].name() == "c");
    REQUIRE(ts.table().calculate_size() == NUM_ROWS);

    // Scan and verify that a/c data is intact.
    {
        auto types = ts.table().copy_types();
        data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
        table_scan_state scan_state(&resource);
        auto column_indices = make_column_indices(ts.table().column_count());
        ts.table().initialize_scan(scan_state, column_indices);
        ts.table().scan(result, scan_state);
        REQUIRE(result.size() == NUM_ROWS);
        for (uint64_t i = 0; i < result.size(); ++i) {
            REQUIRE(result.data[0].get_value<int64_t>(i) == static_cast<int64_t>(i));
            REQUIRE(result.data[1].get_value<int64_t>(i) == static_cast<int64_t>(i * 100));
        }
    }

    // Dropping a non-existent column is a no-op (false).
    REQUIRE(!ts.drop_column("missing"));
    REQUIRE(ts.table().column_count() == 2);
}

// B3c — dropping a column from a DISK-backed table must give its physical blocks back.
//
// The inverse of the test this replaces (`drop_column_disk_is_noop`, which pinned the DISK
// branch as an unconditional `false`). Both of its assertions invert; the owner consented to
// this ONE rewrite.
//
// The gate is deliberately NOT "the column count dropped". A rebuild that merely forgets the
// column passes that and is the WORSE outcome: the dropped column's `column_data_t` is
// destroyed with the superseded collection, so its block_handle_t's die and its registry
// entries go with them — and a block that no root names, no registry holds and no free list
// publishes is durably orphaned. A7.3's reclaim_superseded_root cannot find those: it walks
// the DURABLE ROOT's own data blocks, and every block the column acquired SINCE that root
// (the write-through at row-group close, the re-pointed tail segments) is invisible to it.
// So the walker judges the file instead, and the shape below is built to produce exactly
// those invisible blocks:
//   * column "b" is a LIST of 40 UBIGINTs — 40 * 8 B * 2048 rows = 640 KiB of child payload
//     per row group, past partial_block_manager_t's FULL_THRESHOLD, so its child segments
//     take DEDICATED blocks that column "a" cannot share (B2 packing would otherwise hand
//     every one of b's ids to a's walk by accident and hide the whole question);
//   * the table is REOPENED before the drop, so the loader — not the appender — is what owns
//     b's blocks (F6's finding: nested children own disk blocks only after a load);
//   * more rows are appended AFTER that reopen's root, so some of b's blocks are named by no
//     durable root at all.
// A7.4's lesson is the last step: a leak or a bad free often only shows on a REOPENED file,
// so the walk is repeated after closing and reopening the .otbx.
TEST_CASE("services::disk::table_storage::drop_column_disk_frees_blocks") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::pmr::otterbrix_resource resource;

    const auto otbx_path = std::filesystem::path(test_dir()) / "test_drop_disk.otbx";
    const std::string path = otbx_path.string();

    constexpr uint64_t FIRST_ROWS = 6000;
    constexpr uint64_t SECOND_ROWS = 4000;
    constexpr uint64_t TOTAL_ROWS = FIRST_ROWS + SECOND_ROWS;

    // Round 1: create, fill, checkpoint. Root R1 names both columns' blocks.
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type{logical_type::BIGINT});
        columns.emplace_back("b", drop_list_type());
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE_FALSE(ts.construction_failed());
        REQUIRE(ts.mode() == storage_mode_t::DISK);
        REQUIRE(ts.table().column_count() == 2);

        append_drop_rows(ts.table(), &resource, 0, FIRST_ROWS);
        REQUIRE_FALSE(ts.checkpoint().has_error());
    }

    uint64_t blocks_after_drop = 0;
    uint64_t size_after_drop = 0;
    {
        // Reopen: the LOADER now owns b's blocks (F6). catalog_columns is ignored for a
        // checkpointed file — the file's own schema is authoritative.
        table_storage_t ts(&resource, otbx_path, std::vector<column_definition_t>{});
        REQUIRE_FALSE(ts.construction_failed());
        REQUIRE(ts.table().column_count() == 2);

        // Blocks acquired AFTER the durable root: reclaim_superseded_root will never see these.
        append_drop_rows(ts.table(), &resource, FIRST_ROWS, SECOND_ROWS);

        auto before = walk_storage(ts, path, &resource);
        REQUIRE(before.ok);
        CHECK(before.reachable_free_overlap.empty());

        REQUIRE(ts.drop_column("b"));
        REQUIRE(ts.table().column_count() == 1);
        REQUIRE(ts.table().columns()[0].name() == "a");

        REQUIRE_FALSE(ts.checkpoint().has_error());

        auto after = walk_storage(ts, path, &resource);
        REQUIRE(after.ok);

        // The blocks the durable root named before the drop and does not name after it. The
        // premise is asserted, not assumed: a vacuous set would let every claim below pass.
        std::set<uint64_t> gone;
        for (auto id : before.root_data) {
            if (after.root_data.count(id) == 0) {
                gone.insert(id);
            }
        }
        INFO("root before=" << before.root_data.size() << " after=" << after.root_data.size()
                            << " left the root=" << dump_ids(gone) << " unexplained="
                            << dump_ids(after.unexplained));
        REQUIRE_FALSE(gone.empty());
        for (auto id : gone) {
            INFO("block " << id << " left the durable root when column b was dropped");
            CHECK((after.free_list_content.count(id) != 0 || after.registry_live.count(id) != 0));
        }
        CHECK(after.reachable_free_overlap.empty());
        CHECK(after.unexplained.empty());

        blocks_after_drop = after.block_count;
        REQUIRE(scan_a_column(ts.table(), &resource) == TOTAL_ROWS);

        // Round over round the file must not grow: nothing left un-released keeps costing.
        REQUIRE_FALSE(ts.checkpoint().has_error());
        REQUIRE_FALSE(ts.checkpoint().has_error());
        auto settled = walk_storage(ts, path, &resource);
        REQUIRE(settled.ok);
        INFO("settled unexplained=" << dump_ids(settled.unexplained));
        CHECK(settled.reachable_free_overlap.empty());
        CHECK(settled.unexplained.empty());
        CHECK(settled.block_count <= blocks_after_drop);
        size_after_drop = file_size_or_zero(path);
    }

    // A7.4: only a judged REOPENED file tells the truth about a leak.
    {
        table_storage_t ts(&resource, otbx_path, std::vector<column_definition_t>{});
        REQUIRE_FALSE(ts.construction_failed());
        REQUIRE(ts.table().column_count() == 1);
        REQUIRE(ts.table().columns()[0].name() == "a");
        REQUIRE(scan_a_column(ts.table(), &resource) == TOTAL_ROWS);

        auto reopened = walk_storage(ts, path, &resource);
        REQUIRE(reopened.ok);
        INFO("reopened unexplained=" << dump_ids(reopened.unexplained));
        CHECK(reopened.reachable_free_overlap.empty());
        CHECK(reopened.unexplained.empty());

        REQUIRE_FALSE(ts.checkpoint().has_error());
        auto reopened_round = walk_storage(ts, path, &resource);
        REQUIRE(reopened_round.ok);
        CHECK(reopened_round.unexplained.empty());
        CHECK(file_size_or_zero(path) <= size_after_drop);
    }

    cleanup_test_dir();
}
