#include <catch2/catch.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/table/column_definition.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <filesystem>
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
                chunk.set_value(0, i, logical_value_t{resource, static_cast<int64_t>(offset + i)});
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }
} // namespace

TEST_CASE("services::disk::table_storage::in_memory") {
    std::pmr::synchronized_pool_resource resource;

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
    std::pmr::synchronized_pool_resource resource;

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

        ts.checkpoint();
    }

    // Load and verify
    {
        table_storage_t ts(&resource, otbx_path);
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
    std::pmr::synchronized_pool_resource resource;

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
    std::pmr::synchronized_pool_resource resource;

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
                chunk.set_value(0, i, logical_value_t{&resource, static_cast<int64_t>(offset + i)});
                chunk.set_value(1, i, logical_value_t{&resource, static_cast<double>(offset + i) * 1.5});
            }
            table_append_state state(&resource);
            ts.table().append_lock(state);
            ts.table().initialize_append(state);
            ts.table().append(chunk, state);
            ts.table().finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);
        ts.checkpoint();
    }

    // Load and verify both columns
    {
        table_storage_t ts(&resource, otbx_path);
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
            REQUIRE(score_val.value<double>() == Approx(static_cast<double>(i) * 1.5));
        }
    }

    cleanup_test_dir();
}

// Physical column compaction primitive. table_storage_t::drop_column removes
// the named column from the IN_MEMORY data_table_t via the rebuild
// constructor (data_table_t(parent, removed_column) backed by
// collection_t::remove_column per row_group segment). DISK-mode is out of scope.
TEST_CASE("services::disk::table_storage::drop_column_in_memory") {
    std::pmr::synchronized_pool_resource resource;

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
            chunk.set_value(0, i, logical_value_t{&resource, static_cast<int64_t>(i)});
            chunk.set_value(1, i, logical_value_t{&resource, static_cast<int64_t>(i * 10)});
            chunk.set_value(2, i, logical_value_t{&resource, static_cast<int64_t>(i * 100)});
        }
        table_append_state state(&resource);
        ts.table().append_lock(state);
        ts.table().initialize_append(state);
        ts.table().append(chunk, state);
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
            REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(i));
            REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(i * 100));
        }
    }

    // Dropping a non-existent column is a no-op (false).
    REQUIRE(!ts.drop_column("missing"));
    REQUIRE(ts.table().column_count() == 2);
}

TEST_CASE("services::disk::table_storage::drop_column_disk_is_noop") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "test_drop_disk.otbx";
    std::vector<column_definition_t> columns;
    columns.emplace_back("a", logical_type::BIGINT);
    columns.emplace_back("b", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns), otbx_path);
    REQUIRE(ts.mode() == storage_mode_t::DISK);
    REQUIRE(ts.table().column_count() == 2);

    // DISK-mode: drop_column returns false (out of scope).
    REQUIRE(!ts.drop_column("b"));
    REQUIRE(ts.table().column_count() == 2);

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_via_storage_adapter") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    // Insert 4 row groups worth of data (4 * DEFAULT_VECTOR_CAPACITY)
    append_int64_data(ts.table(), &resource, 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(ts.table().calculate_size() == 4 * DEFAULT_VECTOR_CAPACITY);

    // Use storage adapter's parallel_scan
    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);

    uint64_t chunks_seen = 0;
    uint64_t total = adapter.parallel_scan([&](data_chunk_t& /*chunk*/) { chunks_seen++; });

    REQUIRE(total == 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(chunks_seen == 4);
}
