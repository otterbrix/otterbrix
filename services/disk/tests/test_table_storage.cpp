#include <catch2/catch.hpp>
#include <disk/manager_disk.hpp>
#include <disk/disk.hpp>

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

TEST_CASE("services::disk::wal_id_round_trip") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    // Write WAL ID = 42 and verify persistence
    {
        disk_t disk(test_dir(), &resource);
        disk.fix_wal_id(42);
        REQUIRE(disk.wal_id() == 42);
    }

    // Reopen and verify persisted value
    {
        disk_t disk(test_dir(), &resource);
        REQUIRE(disk.wal_id() == 42);
    }

    // Overwrite with 999999 and verify persistence
    {
        disk_t disk(test_dir(), &resource);
        disk.fix_wal_id(999999);
    }
    {
        disk_t disk(test_dir(), &resource);
        REQUIRE(disk.wal_id() == 999999);
    }

    cleanup_test_dir();
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

TEST_CASE("services::disk::catalog_schema_update_via_disk") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    {
        disk_t disk(test_dir(), &resource);

        // Create database and disk table with columns
        disk.append_database("test_db");
        std::vector<catalog_column_entry_t> columns;
        columns.push_back({"id", logical_type::BIGINT});
        columns.push_back({"name", logical_type::STRING_LITERAL});
        disk.append_collection("test_db", "test_table",
                               table_storage_mode_t::DISK, columns);

        // Verify table entry
        auto entries = disk.table_entries("test_db");
        REQUIRE(entries.size() == 1);
        REQUIRE(entries[0].name == "test_table");
        REQUIRE(entries[0].storage_mode == table_storage_mode_t::DISK);
        REQUIRE(entries[0].columns.size() == 2);
        REQUIRE(entries[0].columns[0].name == "id");
        REQUIRE(entries[0].columns[1].name == "name");

        // Update schema via catalog
        std::vector<catalog_column_entry_t> new_columns;
        new_columns.push_back({"id", logical_type::BIGINT});
        new_columns.push_back({"name", logical_type::STRING_LITERAL});
        new_columns.push_back({"score", logical_type::DOUBLE});
        disk.catalog().update_table_columns("test_db", "test_table", new_columns);

        // Verify updated schema
        auto updated_entries = disk.table_entries("test_db");
        REQUIRE(updated_entries.size() == 1);
        REQUIRE(updated_entries[0].columns.size() == 3);
        REQUIRE(updated_entries[0].columns[2].name == "score");
        REQUIRE(updated_entries[0].columns[2].type == logical_type::DOUBLE);
    }

    // Verify persistence
    {
        disk_t disk(test_dir(), &resource);
        auto entries = disk.table_entries("test_db");
        REQUIRE(entries.size() == 1);
        REQUIRE(entries[0].columns.size() == 3);
        REQUIRE(entries[0].columns[2].name == "score");
    }

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
    uint64_t total = adapter.parallel_scan([&](data_chunk_t& /*chunk*/) {
        chunks_seen++;
    });

    REQUIRE(total == 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(chunks_seen == 4);
}
