#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <components/vector/cell_equal.hpp>
#include <cstring>
#include <fstream>
#include <functional>
#include <iterator>
#include <unistd.h>

#include "table_segment_scan.hpp"

namespace {
    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_checkpoint_load_" + std::to_string(::getpid()) + ".otbx";
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
            table.finalize_append(state, transaction_data::committed());
            offset += batch;
        }
    }
    void append_int64_data_with_fn(components::table::data_table_t& table,
                                   std::pmr::memory_resource* resource,
                                   uint64_t count,
                                   std::function<int64_t(uint64_t)> value_fn) {
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
                chunk.set_value(0, i, value_fn(offset + i));
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data::committed());
            offset += batch;
        }
    }

    void append_double_data_with_fn(components::table::data_table_t& table,
                                    std::pmr::memory_resource* resource,
                                    uint64_t count,
                                    std::function<double(uint64_t)> value_fn) {
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
                chunk.set_value(0, i, value_fn(offset + i));
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data::committed());
            offset += batch;
        }
    }
} // namespace

TEST_CASE("checkpoint_load: single INT64 column, 1000 rows") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1000;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "test_table");

        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        REQUIRE(loaded->table_name() == "test_table");
        REQUIRE(loaded->column_count() == 1);

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(scanned + i));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: three columns INT64 + STRING + DOUBLE") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 500;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        columns.emplace_back("score", logical_type::DOUBLE);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "multi_col");

        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            uint64_t batch = std::min(NUM_ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                chunk.set_value(1, i, std::string_view{std::string("name_") + std::to_string(row)});
                chunk.set_value(2, i, static_cast<double>(row) * 1.5);
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
            offset += batch;
        }

        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        REQUIRE(loaded->table_name() == "multi_col");
        REQUIRE(loaded->column_count() == 3);

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t row = scanned + i;
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(row));
                std::string expected_name = std::string("name_") + std::to_string(row);
                REQUIRE(chunk.data[1].get_value<std::string_view>(i) == expected_name);
                REQUIRE(chunk.data[2].get_value<double>(i) == Catch::Approx(static_cast<double>(row) * 1.5));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: empty table") {
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
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "empty_table");

        REQUIRE(table->calculate_size() == 0);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        REQUIRE(loaded->table_name() == "empty_table");
        REQUIRE(loaded->column_count() == 1);
        REQUIRE(loaded->calculate_size() == 0);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: multiple row groups") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = DEFAULT_VECTOR_CAPACITY * 3 + 100;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "big_table");

        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        REQUIRE(loaded->table_name() == "big_table");
        REQUIRE(loaded->column_count() == 1);

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(scanned + i));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: CONSTANT compression — all identical values") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 500;
    constexpr int64_t CONSTANT_VALUE = 42;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "const_table");

        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t) { return CONSTANT_VALUE; });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        REQUIRE(loaded->table_name() == "const_table");
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == CONSTANT_VALUE);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: RLE compression — sorted runs") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 500;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "rle_table");

        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t idx) {
            return static_cast<int64_t>(idx / 100 + 1);
        });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t global_idx = scanned + i;
                int64_t expected = static_cast<int64_t>(global_idx / 100 + 1);
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == expected);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: DICTIONARY compression — low cardinality cycling") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 500;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "dict_table");

        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t idx) {
            return static_cast<int64_t>(idx % 5 + 1);
        });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t global_idx = scanned + i;
                int64_t expected = static_cast<int64_t>(global_idx % 5 + 1);
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == expected);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: UNCOMPRESSED fallback — high cardinality") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 500;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "unique_table");

        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(scanned + i));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: mixed row groups — constant + varied") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t CONST_ROWS = DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t UNIQUE_ROWS = 500;
    constexpr uint64_t TOTAL_ROWS = CONST_ROWS + UNIQUE_ROWS;
    constexpr int64_t CONSTANT_VALUE = 99;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "mixed_table");

        append_int64_data_with_fn(*table, &env.resource, TOTAL_ROWS, [](uint64_t idx) -> int64_t {
            if (idx < CONST_ROWS)
                return CONSTANT_VALUE;
            return static_cast<int64_t>(idx - CONST_ROWS);
        });
        REQUIRE(table->calculate_size() == TOTAL_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, TOTAL_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t global_idx = scanned + i;
                int64_t expected;
                if (global_idx < CONST_ROWS) {
                    expected = CONSTANT_VALUE;
                } else {
                    expected = static_cast<int64_t>(global_idx - CONST_ROWS);
                }
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == expected);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == TOTAL_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: DOUBLE column — constant compression") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 500;
    constexpr double CONSTANT_DOUBLE = 3.14;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("score", logical_type::DOUBLE);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "double_table");

        append_double_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t) { return CONSTANT_DOUBLE; });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<double>(i) == Catch::Approx(CONSTANT_DOUBLE));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: small segment — 2 rows edge case") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 2;
    constexpr int64_t VALUE = 7;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "tiny_table");

        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t) { return VALUE; });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        REQUIRE(loaded->table_name() == "tiny_table");
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == VALUE);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

// REGRESSION: pg_attribute-like packing (8 narrow low-cardinality BIGINT columns sharing ONE
// partial block at non-zero offsets) clobbered the shared block's in-memory buffer on REOPEN
// after the table GROWS and re-checkpoints, returning garbage from the packed columns.
namespace {
    components::table::storage::meta_block_pointer_t
    full_checkpoint(components::table::data_table_t& table,
                    components::table::storage::single_file_block_manager_t& bm) {
        using namespace components::table::storage;
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table.checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        auto table_pointer = writer.get_block_pointer();

        bm.set_meta_block(table_pointer.block_pointer);
        auto free_list_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_list_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        database_header_t header{};
        header.initialize();
        header.free_list = free_list_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        return table_pointer;
    }

    int64_t pg_attr_value(uint64_t col, uint64_t row) {
        switch (col) {
            case 0:
                return static_cast<int64_t>(row);
            case 1:
                return static_cast<int64_t>(20 + row % 3);
            case 2:
                return static_cast<int64_t>(row % 8 + 1);
            case 3:
                return 1;
            case 4:
                return 0;
            case 5:
                return 0; // attisdropped: CONSTANT 0 (the SSB symptom)
            case 6:
                return 1000;
            case 7:
                return 0;
            default:
                return static_cast<int64_t>(col * 100 + row % 4);
        }
    }

    void append_pg_attr_rows(components::table::data_table_t& table,
                             std::pmr::memory_resource* resource,
                             uint64_t num_cols,
                             uint64_t row_start,
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
                uint64_t r = row_start + offset + i;
                for (uint64_t c = 0; c < num_cols; c++) {
                    chunk.set_value(c, i, pg_attr_value(c, r));
                }
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data::committed());
            offset += batch;
        }
    }

    void scan_and_verify_pg_attr(components::table::data_table_t& table, uint64_t num_cols, uint64_t total_rows) {
        using namespace components::vector;
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(table, 0, total_rows, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t r = scanned + i;
                for (uint64_t c = 0; c < num_cols; c++) {
                    INFO("col=" << c << " row=" << r);
                    REQUIRE(chunk.data[c].get_value<int64_t>(i) == pg_attr_value(c, r));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == total_rows);
    }

    struct small_pool_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;
        small_pool_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };
} // namespace

TEST_CASE("checkpoint_load: shared partial block survives reopen after table grows") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    small_pool_env_t env;
    constexpr uint64_t NUM_COLS = 8;
    constexpr uint64_t INITIAL_ROWS = 200;
    constexpr uint64_t GROW_ROWS = 200;
    constexpr uint64_t TOTAL_ROWS = INITIAL_ROWS + GROW_ROWS;

    auto make_columns = []() {
        std::vector<column_definition_t> cols;
        const char* names[NUM_COLS] = {"attrelid",
                                       "atttypid",
                                       "attnum",
                                       "attnotnull",
                                       "atthasdefault",
                                       "attisdropped",
                                       "added_at_commit_id",
                                       "dropped_at_commit_id"};
        for (uint64_t c = 0; c < NUM_COLS; c++) {
            cols.emplace_back(names[c], logical_type::BIGINT);
        }
        return cols;
    };

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = std::make_unique<data_table_t>(&env.resource, bm, make_columns(), "pg_attribute");
        append_pg_attr_rows(*table, &env.resource, NUM_COLS, 0, INITIAL_ROWS);
        REQUIRE(table->calculate_size() == INITIAL_ROWS);
        table_pointer = full_checkpoint(*table, bm);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();
        REQUIRE(loaded->column_count() == NUM_COLS);

        scan_and_verify_pg_attr(*loaded, NUM_COLS, INITIAL_ROWS);

        append_pg_attr_rows(*loaded, &env.resource, NUM_COLS, INITIAL_ROWS, GROW_ROWS);
        REQUIRE(loaded->calculate_size() == TOTAL_ROWS);
        table_pointer = full_checkpoint(*loaded, bm);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();
        REQUIRE(loaded->column_count() == NUM_COLS);

        scan_and_verify_pg_attr(*loaded, NUM_COLS, TOTAL_ROWS);
    }

    cleanup_test_file();
}

// LIST/ARRAY/STRUCT payload lives in CHILD column_data_t nodes; persisting only the parent
// leaves the reload with typed columns but EMPTY children.
TEST_CASE("checkpoint_load: LIST column round-trips its child data") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 100;
    auto list_length = [](uint64_t row) { return row % 5; };

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("l", complex_logical_type::create_list(logical_type::UBIGINT));
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "list_table");

        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            chunk.set_value(0, i, static_cast<int64_t>(i));
            std::vector<uint64_t> list;
            list.reserve(list_length(i));
            for (uint64_t j = 0; j < list_length(i); j++) {
                list.emplace_back(i * 100 + j);
            }
            chunk.set_value(1, i, list);
        }
        {
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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
        REQUIRE(loaded->column_count() == 2);

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(row));
                REQUIRE(chunk.data[1].type().type() == logical_type::LIST);
                const auto list = chunk.data[1].get_value<std::vector<uint64_t>>(i);
                REQUIRE(list.size() == list_length(row));
                for (uint64_t j = 0; j < list_length(row); j++) {
                    REQUIRE(list[j] == row * 100 + j);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: ARRAY column round-trips its child data") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 64;
    constexpr uint64_t ARRAY_SIZE = 4;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type::create_array(logical_type::UBIGINT, ARRAY_SIZE));
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "array_table");

        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            std::vector<uint64_t> arr;
            arr.reserve(ARRAY_SIZE);
            for (uint64_t j = 0; j < ARRAY_SIZE; j++) {
                arr.emplace_back(i * ARRAY_SIZE + j);
            }
            chunk.set_value(0, i, arr);
        }
        {
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                REQUIRE(chunk.data[0].type().type() == logical_type::ARRAY);
                const auto elements = chunk.data[0].get_value<std::vector<uint64_t>>(i);
                REQUIRE(elements.size() == ARRAY_SIZE);
                for (uint64_t j = 0; j < ARRAY_SIZE; j++) {
                    REQUIRE(elements[j] == row * ARRAY_SIZE + j);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: STRUCT column round-trips its fields") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 50;

    std::pmr::vector<complex_logical_type> fields(&env.resource);
    fields.emplace_back(logical_type::BIGINT, "num");
    fields.emplace_back(logical_type::STRING_LITERAL, "name");
    auto struct_type = complex_logical_type::create_struct("pair", fields);

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("s", struct_type);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "struct_table");

        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            const std::string name = std::string("row_") + std::to_string(i);
            chunk.data[0].entries()[0]->set_value(i, static_cast<int64_t>(i * 7));
            chunk.data[0].entries()[1]->set_value(i, std::string_view{name});
        }
        {
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                REQUIRE(chunk.data[0].type().type() == logical_type::STRUCT);
                REQUIRE(chunk.data[0].entries().size() == 2);
                REQUIRE(chunk.data[0].entries()[0]->get_value<int64_t>(i) == static_cast<int64_t>(row * 7));
                REQUIRE(chunk.data[0].entries()[1]->get_value<std::string_view>(i) ==
                        std::string("row_") + std::to_string(row));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

// Regression: checkpoint never wrote the validity bitmap (every NULL silently became present on
// reload), and the manufactured bitmap was then written THROUGH on every load, growing the file.
TEST_CASE("checkpoint_load: NULL validity round-trips, reopen allocates no new blocks") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 3000;
    constexpr uint64_t NULL_STEP = 7;
    auto is_null_row = [](uint64_t row) { return row % NULL_STEP == 0; };

    meta_block_pointer_t table_pointer;
    uint64_t blocks_after_checkpoint = 0;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("v", logical_type::BIGINT);
        columns.emplace_back("s", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "null_table");

        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            uint64_t batch = std::min(NUM_ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = offset + i;
                if (is_null_row(row)) {
                    chunk.data[0].set_null(i, true);
                    chunk.data[1].set_null(i, true);
                } else {
                    chunk.set_value(0, i, static_cast<int64_t>(row * 3));
                    chunk.set_value(1, i, std::string_view{std::string("r") + std::to_string(row)});
                }
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
            offset += batch;
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
        blocks_after_checkpoint = bm.total_blocks();
        REQUIRE(blocks_after_checkpoint > 0);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        REQUIRE(bm.total_blocks() == blocks_after_checkpoint);

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        // Must REGISTER persisted validity blocks, not manufacture + write through fresh ones.
        CHECK(bm.total_blocks() == blocks_after_checkpoint);

        uint64_t scanned = 0;
        uint64_t nulls_seen = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                INFO("row " << row);
                if (is_null_row(row)) {
                    CHECK(chunk.data[0].is_null(i));
                    CHECK(chunk.data[1].is_null(i));
                    if (chunk.data[0].is_null(i)) {
                        ++nulls_seen;
                    }
                } else {
                    REQUIRE_FALSE(chunk.data[0].is_null(i));
                    REQUIRE_FALSE(chunk.data[1].is_null(i));
                    REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(row * 3));
                    REQUIRE(chunk.data[1].get_value<std::string_view>(i) == std::string("r") + std::to_string(row));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
        CHECK(nulls_seen == (NUM_ROWS + NULL_STEP - 1) / NULL_STEP);
    }

    cleanup_test_file();
}

// Regression: the validity child's scan state never tracked the parent's result_offset during
// compact's rebuild, folding every row group's NULL bits into the first 1024 rows (bit = row mod
// 1024); total NULL count was preserved by the fold, which is what kept it silent.
TEST_CASE("checkpoint_load: compact preserves NULL validity across row groups") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 3000;
    constexpr uint64_t BATCH = 100;
    constexpr uint64_t NULL_STEP = 100;
    auto is_null_row = [](uint64_t row) { return (row + 1) % NULL_STEP == 0; };

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("v", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "compact_table");

    auto types = table->copy_types();
    for (uint64_t offset = 0; offset < NUM_ROWS; offset += BATCH) {
        data_chunk_t chunk(&env.resource, types, BATCH);
        chunk.set_cardinality(BATCH);
        for (uint64_t i = 0; i < BATCH; i++) {
            const uint64_t row = offset + i;
            if (is_null_row(row)) {
                chunk.data[0].set_null(i, true);
            } else {
                chunk.set_value(0, i, static_cast<int64_t>(row * 3));
            }
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data::committed());
    }
    REQUIRE(table->calculate_size() == NUM_ROWS);

    REQUIRE(table->compact(0));
    REQUIRE(table->calculate_size() == NUM_ROWS);

    uint64_t scanned = 0;
    otterbrix_test::scan_table_segment(*table, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
        for (uint64_t i = 0; i < chunk.size(); i++) {
            const uint64_t row = scanned + i;
            INFO("row " << row);
            CHECK(chunk.data[0].is_null(i) == is_null_row(row));
            if (!is_null_row(row)) {
                REQUIRE_FALSE(chunk.data[0].is_null(i));
                REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(row * 3));
            }
        }
        scanned += chunk.size();
    });
    REQUIRE(scanned == NUM_ROWS);

    cleanup_test_file();
}

// Regression: an append rolling into a fresh row group kept writing NULL bits through the
// PREVIOUS group's validity buffer, leaving later groups all-valid.
TEST_CASE("checkpoint_load: NULL validity survives boundary-crossing appends") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 3000;
    constexpr uint64_t BATCH = 100;
    constexpr uint64_t NULL_STEP = 100;
    auto is_null_row = [](uint64_t row) { return (row + 1) % NULL_STEP == 0; };

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("v", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "cross_table");

        auto types = table->copy_types();
        for (uint64_t offset = 0; offset < NUM_ROWS; offset += BATCH) {
            data_chunk_t chunk(&env.resource, types, BATCH);
            chunk.set_cardinality(BATCH);
            for (uint64_t i = 0; i < BATCH; i++) {
                const uint64_t row = offset + i;
                if (is_null_row(row)) {
                    chunk.data[0].set_null(i, true);
                } else {
                    chunk.set_value(0, i, static_cast<int64_t>(row * 3));
                }
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        uint64_t live_scanned = 0;
        otterbrix_test::scan_table_segment(*table, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = live_scanned + i;
                INFO("live row " << row);
                REQUIRE(chunk.data[0].is_null(i) == is_null_row(row));
            }
            live_scanned += chunk.size();
        });
        REQUIRE(live_scanned == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                INFO("row " << row);
                CHECK(chunk.data[0].is_null(i) == is_null_row(row));
                if (!is_null_row(row)) {
                    REQUIRE_FALSE(chunk.data[0].is_null(i));
                    REQUIRE(chunk.data[0].get_value<int64_t>(i) == static_cast<int64_t>(row * 3));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

// A LIST segment's RAW payload is one uint64/row, but complex_logical_type::size() for LIST is
// sizeof(list_entry_t) == 16 (2x); taking type_size from it made checkpoint's compression walk
// and the compressed scan write 16 bytes/row into the 8-byte-sized offset vector — an 8 KiB heap
// overrun per 1024-row vector on a plain SELECT, intermittent because it depends on pool layout.
TEST_CASE("checkpoint_load: a LIST segment is compressed at its PHYSICAL element width") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 100;

    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    auto list_type = complex_logical_type::create_list(logical_type::UBIGINT);
    auto column = column_data_t::create_column(&env.resource, bm, 0, 0, list_type);
    {
        // Must not outlive the checkpoint: checkpointing drops the block_handle its pin refers to.
        auto append_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(bm);
        column_append_state append_state{&append_pbm};
        REQUIRE_FALSE(column->initialize_append(append_state).has_error());

        std::pmr::vector<complex_logical_type> types(&env.resource);
        types.emplace_back(list_type);
        data_chunk_t input(&env.resource, types, NUM_ROWS);
        input.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            input.set_value(0, i, std::vector<uint64_t>{});
        }
        REQUIRE_FALSE(column->append(append_state, input.data[0], NUM_ROWS).has_error());
        REQUIRE_FALSE(append_pbm.flush_partial_blocks().has_error());
    }

    auto pbm = partial_block_manager_t::for_checkpoint(bm);
    auto persistent = column->checkpoint(pbm);
    REQUIRE_FALSE(persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());

    REQUIRE(persistent.value().data_pointers.size() == 1);
    const auto& dp = persistent.value().data_pointers[0];
    REQUIRE(dp.tuple_count == NUM_ROWS);
    REQUIRE(dp.compression == compression::compression_type::CONSTANT);
    CHECK(dp.segment_size == sizeof(uint64_t));

    auto reloaded = column_data_t::create_column(&env.resource, bm, 0, 0, list_type);
    REQUIRE_FALSE(reloaded->initialize_column(persistent.value()).has_error());
    REQUIRE(reloaded->count() == NUM_ROWS);

    column_scan_state scan_state;
    scan_state.initialize(list_type);
    reloaded->initialize_scan(scan_state);
    vector_t result(&env.resource, list_type, DEFAULT_VECTOR_CAPACITY);
    auto scanned = reloaded->scan_count(scan_state, result, NUM_ROWS);
    REQUIRE_FALSE(scan_state.has_error());
    REQUIRE(scanned == NUM_ROWS);
    for (uint64_t i = 0; i < NUM_ROWS; i++) {
        INFO("row " << i);
        REQUIRE(result.type().type() == logical_type::LIST);
        REQUIRE_FALSE(result.is_null(i));
        CHECK(sequence_of(result, i).length == 0);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: 4-byte CONSTANT segment must not misalign the segments packed after it") {
    // Regression: before the packer aligned placements, a 4-byte CONSTANT segment pushed everything
    // after it to offset 4 mod 8, producing undefined uint64/int64 loads on scan (-fsanitize=alignment).
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1000;
    constexpr int32_t CONSTANT_TAG = 7;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("tag", logical_type::INTEGER);
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "packed_misalign");

        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            uint64_t batch = std::min(NUM_ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = offset + i;
                chunk.set_value(0, i, CONSTANT_TAG);
                chunk.set_value(1, i, static_cast<int64_t>(row) * 1000003 + 17);
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
            offset += batch;
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
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

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t row = scanned + i;
                REQUIRE(chunk.data[0].get_value<int32_t>(i) == CONSTANT_TAG);
                REQUIRE(chunk.data[1].get_value<int64_t>(i) == static_cast<int64_t>(row) * 1000003 + 17);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

namespace {
    int64_t spread_value(uint64_t row) { return static_cast<int64_t>(row) * 1000003 + 17; }
    bool null_row(uint64_t row) { return row % 7 == 3; }

    void append_spread_bigints(test_env_t& env,
                               components::table::storage::block_manager_t& bm,
                               components::table::column_data_t& column,
                               uint64_t first_row,
                               uint64_t rows) {
        using namespace components::table;
        using namespace components::types;
        using namespace components::vector;
        auto pbm = storage::partial_block_manager_t::for_checkpoint(bm);
        column_append_state state{&pbm};
        REQUIRE_FALSE(column.initialize_append(state).has_error());
        for (uint64_t offset = 0; offset < rows; offset += DEFAULT_VECTOR_CAPACITY) {
            const uint64_t batch = std::min(rows - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            vector_t v(&env.resource, logical_type::BIGINT, batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = first_row + offset + i;
                if (null_row(row)) {
                    v.set_null(i, true);
                } else {
                    v.set_value(i, spread_value(row));
                }
            }
            REQUIRE_FALSE(column.append(state, v, batch).has_error());
        }
        REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());
    }

    void require_spread_bigints(test_env_t& env, components::table::column_data_t& column, uint64_t rows) {
        using namespace components::table;
        using namespace components::types;
        using namespace components::vector;
        REQUIRE(column.count() == rows);
        column_scan_state state;
        state.initialize(complex_logical_type{logical_type::BIGINT});
        column.initialize_scan(state);
        for (uint64_t offset = 0; offset < rows; offset += DEFAULT_VECTOR_CAPACITY) {
            const uint64_t batch = std::min(rows - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            vector_t result(&env.resource, logical_type::BIGINT, DEFAULT_VECTOR_CAPACITY);
            const auto scanned = column.scan(offset / DEFAULT_VECTOR_CAPACITY, state, result);
            REQUIRE_FALSE(state.has_error());
            REQUIRE(scanned == batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = offset + i;
                INFO("row " << row);
                REQUIRE(result.is_null(i) == null_row(row));
                if (!null_row(row)) {
                    REQUIRE(result.get_value<int64_t>(i) == spread_value(row));
                }
            }
        }
    }

    void require_no_empty_segment(const components::table::persistent_column_data_t& persistent) {
        CHECK(persistent.segment_statistics.size() == persistent.data_pointers.size());
        for (const auto& dp : persistent.data_pointers) {
            CHECK(dp.tuple_count > 0);
        }
        for (const auto& child : persistent.child_columns) {
            require_no_empty_segment(*child);
        }
    }
} // namespace

TEST_CASE("checkpoint_load: a partly filled fixed segment persists only its rows") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;

    for (const uint64_t rows : {uint64_t{1}, uint64_t{500}, uint64_t{1500}}) {
        INFO("rows " << rows);
        cleanup_test_file();
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        const complex_logical_type type{logical_type::BIGINT};
        auto column = column_data_t::create_column(&env.resource, bm, 0, 0, type);
        append_spread_bigints(env, bm, *column, 0, rows);

        auto pbm = partial_block_manager_t::for_checkpoint(bm);
        auto persistent = column->checkpoint(pbm);
        REQUIRE_FALSE(persistent.has_error());
        REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());
        const auto& p = persistent.value();

        uint64_t persisted_rows = 0;
        for (const auto& dp : p.data_pointers) {
            REQUIRE(dp.compression == compression::compression_type::UNCOMPRESSED);
            CHECK(dp.segment_size == dp.tuple_count * sizeof(int64_t));
            persisted_rows += dp.tuple_count;
        }
        REQUIRE(persisted_rows == rows);
        if (rows == 1) {
            CHECK(p.data_pointers[0].segment_size == 8);
        }
        if (rows == 500) {
            CHECK(p.data_pointers[0].segment_size == 4000);
        }

        REQUIRE(p.child_columns.size() == 1);
        uint64_t validity_rows = 0;
        for (const auto& dp : p.child_columns[0]->data_pointers) {
            CHECK(dp.segment_size == (dp.tuple_count + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY *
                                         validity_mask_t::STANDARD_MASK_SIZE);
            validity_rows += dp.tuple_count;
        }
        REQUIRE(validity_rows == rows);

        auto reloaded = column_data_t::create_column(&env.resource, bm, 0, 0, type);
        REQUIRE_FALSE(reloaded->initialize_column(p).has_error());
        require_spread_bigints(env, *reloaded, rows);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: a LIST of NULLs persists no empty child segment and reloads") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    constexpr uint64_t NULL_ROWS = 3;

    for (const auto element : {logical_type::BIGINT, logical_type::STRING_LITERAL}) {
        INFO("element type " << static_cast<int>(element));
        cleanup_test_file();
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        const auto list_type = complex_logical_type::create_list(element);
        auto column = column_data_t::create_column(&env.resource, bm, 0, 0, list_type);
        {
            auto append_pbm = partial_block_manager_t::for_checkpoint(bm);
            column_append_state append_state{&append_pbm};
            REQUIRE_FALSE(column->initialize_append(append_state).has_error());
            vector_t input(&env.resource, list_type, NULL_ROWS);
            for (uint64_t i = 0; i < NULL_ROWS; i++) {
                input.set_null(i, true);
            }
            REQUIRE_FALSE(column->append(append_state, input, NULL_ROWS).has_error());
            REQUIRE_FALSE(append_pbm.flush_partial_blocks().has_error());
        }

        auto pbm = partial_block_manager_t::for_checkpoint(bm);
        auto persistent = column->checkpoint(pbm);
        REQUIRE_FALSE(persistent.has_error());
        REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());
        require_no_empty_segment(persistent.value());

        auto reloaded = column_data_t::create_column(&env.resource, bm, 0, 0, list_type);
        REQUIRE_FALSE(reloaded->initialize_column(persistent.value()).has_error());
        REQUIRE(reloaded->count() == NULL_ROWS);
        column_scan_state scan_state;
        scan_state.initialize(list_type);
        reloaded->initialize_scan(scan_state);
        vector_t result(&env.resource, list_type, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(reloaded->scan_count(scan_state, result, NULL_ROWS) == NULL_ROWS);
        REQUIRE_FALSE(scan_state.has_error());
        for (uint64_t i = 0; i < NULL_ROWS; i++) {
            CHECK(result.is_null(i));
        }
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: dropping an empty segment keeps every segment's statistics on its own segment") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    constexpr uint64_t ROWS = 100;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    const complex_logical_type type{logical_type::BIGINT};
    auto column = column_data_t::create_column(&env.resource, bm, 0, 0, type);
    append_spread_bigints(env, bm, *column, 0, ROWS);
    auto pbm = partial_block_manager_t::for_checkpoint(bm);
    auto persistent = column->checkpoint(pbm);
    REQUIRE_FALSE(persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());
    auto& p = persistent.value();
    REQUIRE(p.data_pointers.size() == 1);
    REQUIRE(p.segment_statistics.size() == 1);
    REQUIRE(p.segment_statistics[0].has_bounds());
    const auto real_min = p.segment_statistics[0].min_as<int64_t>();
    const auto real_max = p.segment_statistics[0].max_as<int64_t>();

    // A file written before empty segments were dropped: an empty segment ahead of the real one,
    // carrying statistics of its own that must not land on the real segment.
    auto empty = p.data_pointers[0];
    empty.tuple_count = 0;
    vector_t far(&env.resource, logical_type::BIGINT, 2);
    far.set_value(0, int64_t{5'000'000'000'000});
    far.set_value(1, int64_t{6'000'000'000'000});
    base_statistics_t far_stats(&env.resource, logical_type::BIGINT);
    far_stats.update(far, 2);
    p.data_pointers.insert(p.data_pointers.begin(), empty);
    p.segment_statistics.insert(p.segment_statistics.begin(), std::move(far_stats));

    auto reloaded = column_data_t::create_column(&env.resource, bm, 0, 0, type);
    REQUIRE_FALSE(reloaded->initialize_column(p).has_error());

    auto pbm2 = partial_block_manager_t::for_checkpoint(bm);
    auto again = reloaded->checkpoint(pbm2);
    REQUIRE_FALSE(again.has_error());
    REQUIRE_FALSE(pbm2.flush_partial_blocks().has_error());
    const auto& p2 = again.value();
    REQUIRE(p2.data_pointers.size() == 1);
    REQUIRE(p2.segment_statistics.size() == 1);
    CHECK(p2.data_pointers[0].tuple_count == ROWS);
    REQUIRE(p2.segment_statistics[0].has_bounds());
    CHECK(p2.segment_statistics[0].min_as<int64_t>() == real_min);
    CHECK(p2.segment_statistics[0].max_as<int64_t>() == real_max);

    auto final_column = column_data_t::create_column(&env.resource, bm, 0, 0, type);
    REQUIRE_FALSE(final_column->initialize_column(p2).has_error());
    require_spread_bigints(env, *final_column, ROWS);

    cleanup_test_file();
}

namespace {
    constexpr unsigned char POISON = 0xA5;
    constexpr std::size_t POISON_RUN = 8;

    // Every buffer the buffer manager hands out starts as POISON, so a byte the engine never wrote
    // is recognisable in the file.
    class poison_resource_t final : public std::pmr::memory_resource {
    public:
        explicit poison_resource_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            void* p = upstream_->allocate(bytes, alignment);
            std::memset(p, POISON, bytes);
            return p;
        }
        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            upstream_->deallocate(p, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
    };

    struct poisoned_env_t {
        core::pmr::otterbrix_resource resource;
        poison_resource_t poison{&resource};
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;

        poisoned_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&poison, fs, buffer_pool) {}
    };

    // Offset of the first run of POISON_RUN poison bytes, bytes.size() when there is none.
    std::size_t first_poison_run(const std::vector<unsigned char>& bytes) {
        std::size_t run = 0;
        for (std::size_t i = 0; i < bytes.size(); i++) {
            run = bytes[i] == POISON ? run + 1 : 0;
            if (run == POISON_RUN) {
                return i + 1 - POISON_RUN;
            }
        }
        return bytes.size();
    }

    std::vector<unsigned char> file_bytes(const std::string& path) {
        std::ifstream in(path, std::ios::binary);
        return std::vector<unsigned char>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
} // namespace

TEST_CASE("checkpoint_load: the file holds no byte the engine never wrote") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    constexpr uint64_t NUM_ROWS = 300;
    cleanup_test_file();

    poisoned_env_t env;
    meta_block_pointer_t table_pointer;
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("l", complex_logical_type::create_list(logical_type::BIGINT));
        columns.emplace_back("s", logical_type::STRING_LITERAL);
        columns.emplace_back("n", complex_logical_type::create_list(logical_type::BIGINT));
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "poisoned");
        {
            auto types = table->copy_types();
            data_chunk_t chunk(&env.resource, types, NUM_ROWS);
            chunk.set_cardinality(NUM_ROWS);
            for (uint64_t i = 0; i < NUM_ROWS; i++) {
                if (null_row(i)) {
                    chunk.data[0].set_null(i, true);
                    chunk.data[1].set_null(i, true);
                    chunk.data[2].set_null(i, true);
                } else {
                    chunk.set_value(0, i, spread_value(i));
                    chunk.set_value(1, i, std::vector<int64_t>{spread_value(i), spread_value(i + 1)});
                    const std::string text = "s_" + std::to_string(i);
                    chunk.set_value(2, i, std::string_view{text});
                }
                chunk.data[3].set_null(i, true);
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
        }

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header{};
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    const auto bytes = file_bytes(test_db_path());
    REQUIRE_FALSE(bytes.empty());
    const auto run = first_poison_run(bytes);
    INFO("file size " << bytes.size() << ", first run of never-written bytes at offset " << run);
    CHECK(run == bytes.size());

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                INFO("row " << row);
                REQUIRE(chunk.data[0].is_null(i) == null_row(row));
                REQUIRE(chunk.data[3].is_null(i));
                if (!null_row(row)) {
                    REQUIRE(chunk.data[0].get_value<int64_t>(i) == spread_value(row));
                    const auto text = "s_" + std::to_string(row);
                    REQUIRE(chunk.data[2].get_value<std::string_view>(i) == text);
                    REQUIRE(sequence_of(chunk.data[1], i).length == 2);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: a HUGEINT segment packed after an 8-byte segment is read through an aligned pointer") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    constexpr uint64_t ROWS = DEFAULT_VECTOR_CAPACITY;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    const complex_logical_type narrow_type{logical_type::BIGINT};
    const complex_logical_type wide_type{logical_type::HUGEINT};
    auto narrow = column_data_t::create_column(&env.resource, bm, 0, 0, narrow_type);
    auto wide = column_data_t::create_column(&env.resource, bm, 1, 0, wide_type);
    {
        auto append_pbm = partial_block_manager_t::for_checkpoint(bm);
        column_append_state narrow_state{&append_pbm};
        REQUIRE_FALSE(narrow->initialize_append(narrow_state).has_error());
        vector_t one(&env.resource, logical_type::BIGINT, 1);
        one.set_value(0, int64_t{42});
        REQUIRE_FALSE(narrow->append(narrow_state, one, 1).has_error());

        column_append_state wide_state{&append_pbm};
        REQUIRE_FALSE(wide->initialize_append(wide_state).has_error());
        vector_t values(&env.resource, logical_type::HUGEINT, ROWS);
        for (uint64_t i = 0; i < ROWS; i++) {
            values.set_value(i, int128_t(spread_value(i)));
        }
        REQUIRE_FALSE(wide->append(wide_state, values, ROWS).has_error());
        REQUIRE_FALSE(append_pbm.flush_partial_blocks().has_error());
    }

    auto pbm = partial_block_manager_t::for_checkpoint(bm);
    auto narrow_persistent = narrow->checkpoint(pbm);
    REQUIRE_FALSE(narrow_persistent.has_error());
    auto wide_persistent = wide->checkpoint(pbm);
    REQUIRE_FALSE(wide_persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());
    REQUIRE(wide_persistent.value().data_pointers.size() == 1);
    const auto& wide_dp = wide_persistent.value().data_pointers[0];
    REQUIRE(wide_dp.compression == compression::compression_type::UNCOMPRESSED);
    REQUIRE(wide_dp.block_pointer.block_id == narrow_persistent.value().data_pointers[0].block_pointer.block_id);
    CHECK(wide_dp.block_pointer.offset % alignof(int128_t) == 0);

    auto reloaded = column_data_t::create_column(&env.resource, bm, 1, 0, wide_type);
    REQUIRE_FALSE(reloaded->initialize_column(wide_persistent.value()).has_error());
    column_scan_state state;
    state.initialize(wide_type);
    reloaded->initialize_scan(state);
    vector_t result(&env.resource, wide_type, DEFAULT_VECTOR_CAPACITY);
    result.set_vector_type(vector_type::CONSTANT);
    REQUIRE(reloaded->scan(state, result, ROWS) == ROWS);
    REQUIRE_FALSE(state.has_error());
    const auto address = reinterpret_cast<std::uintptr_t>(result.data());
    INFO("in-block offset " << wide_dp.block_pointer.offset << ", pointer mod 16 = " << address % 16);
    CHECK(address % alignof(int128_t) == 0);
    for (uint64_t i = 0; i < ROWS; i++) {
        int128_t value;
        std::memcpy(&value, result.data() + i * sizeof(int128_t), sizeof(int128_t));
        REQUIRE(value == int128_t(spread_value(i)));
    }

    cleanup_test_file();
}

namespace {
    // The 16 bytes of int128_t(spread_value(row)); one bit pattern serves every 16-byte type.
    void write_wide(components::vector::vector_t& v, uint64_t i, uint64_t row) {
        const components::types::int128_t value(spread_value(row));
        std::memcpy(v.data() + i * sizeof(value), &value, sizeof(value));
    }

    bool wide_equals(const std::byte* p, uint64_t row) {
        const components::types::int128_t expected(spread_value(row));
        return std::memcmp(p, &expected, sizeof(expected)) == 0;
    }
} // namespace

TEST_CASE("checkpoint_load: NULL rows of HUGEINT, UHUGEINT, UUID and DECIMAL(38) are stored and read back") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    constexpr uint64_t ROWS = 300;

    test_env_t env;
    auto decimal = complex_logical_type::create_decimal(&env.resource, 38, 4);
    REQUIRE_FALSE(decimal.has_error());
    const std::vector<complex_logical_type> types{complex_logical_type{logical_type::HUGEINT},
                                                  complex_logical_type{logical_type::UHUGEINT},
                                                  complex_logical_type{logical_type::UUID},
                                                  decimal.value()};
    for (const auto& type : types) {
        INFO("logical type " << static_cast<int>(type.type()));
        cleanup_test_file();
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        auto column = column_data_t::create_column(&env.resource, bm, 0, 0, type);
        {
            auto append_pbm = partial_block_manager_t::for_checkpoint(bm);
            column_append_state state{&append_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            vector_t input(&env.resource, type, ROWS);
            for (uint64_t i = 0; i < ROWS; i++) {
                if (null_row(i)) {
                    input.set_null(i, true);
                } else {
                    write_wide(input, i, i);
                }
            }
            REQUIRE_FALSE(column->append(state, input, ROWS).has_error());
            REQUIRE_FALSE(append_pbm.flush_partial_blocks().has_error());
        }

        column_scan_state scan_state;
        scan_state.initialize(type);
        column->initialize_scan(scan_state);
        vector_t result(&env.resource, type, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(column->scan_count(scan_state, result, ROWS) == ROWS);
        REQUIRE_FALSE(scan_state.has_error());
        for (uint64_t i = 0; i < ROWS; i++) {
            INFO("row " << i);
            REQUIRE(result.is_null(i) == null_row(i));
            if (!null_row(i)) {
                REQUIRE(wide_equals(result.data() + i * sizeof(int128_t), i));
            }
        }
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: a HUGEINT segment placed 8 bytes off a 16-byte boundary by an older file reads back") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    constexpr uint64_t ROWS = DEFAULT_VECTOR_CAPACITY;
    constexpr uint32_t OLD_OFFSET = 8;
    constexpr uint64_t SHIFT = 5;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    const complex_logical_type type{logical_type::HUGEINT};
    auto column = column_data_t::create_column(&env.resource, bm, 0, 0, type);
    {
        auto append_pbm = partial_block_manager_t::for_checkpoint(bm);
        column_append_state state{&append_pbm};
        REQUIRE_FALSE(column->initialize_append(state).has_error());
        vector_t input(&env.resource, type, ROWS);
        for (uint64_t i = 0; i < ROWS; i++) {
            write_wide(input, i, i);
        }
        REQUIRE_FALSE(column->append(state, input, ROWS).has_error());
        REQUIRE_FALSE(append_pbm.flush_partial_blocks().has_error());
    }
    auto pbm = partial_block_manager_t::for_checkpoint(bm);
    auto persistent = column->checkpoint(pbm);
    REQUIRE_FALSE(persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());
    auto& p = persistent.value();
    REQUIRE(p.data_pointers.size() == 1);
    REQUIRE(p.data_pointers[0].compression == compression::compression_type::UNCOMPRESSED);

    // The same segment as an older file laid it out: at an 8-aligned offset that is not 16-aligned,
    // holding different values so the read provably comes from this block.
    const uint64_t old_block = bm.free_block_id();
    block_t image(&env.resource, old_block, static_cast<uint64_t>(bm.block_size()));
    std::memset(image.buffer(), 0, static_cast<size_t>(bm.block_size()));
    for (uint64_t i = 0; i < ROWS; i++) {
        const int128_t value(spread_value(i + SHIFT));
        std::memcpy(image.buffer() + OLD_OFFSET + i * sizeof(value), &value, sizeof(value));
    }
    REQUIRE_FALSE(bm.write(image, old_block).has_error());
    p.data_pointers[0].block_pointer = block_pointer_t(old_block, OLD_OFFSET);
    p.data_pointers[0].segment_size = ROWS * sizeof(int128_t);

    auto reloaded = column_data_t::create_column(&env.resource, bm, 0, 0, type);
    REQUIRE_FALSE(reloaded->initialize_column(p).has_error());
    column_scan_state state;
    state.initialize(type);
    reloaded->initialize_scan(state);
    vector_t result(&env.resource, type, DEFAULT_VECTOR_CAPACITY);
    result.set_vector_type(vector_type::CONSTANT);
    REQUIRE(reloaded->scan(state, result, ROWS) == ROWS);
    REQUIRE_FALSE(state.has_error());
    const auto address = reinterpret_cast<std::uintptr_t>(result.data());
    INFO("pointer mod 16 = " << address % 16);
    CHECK(address % alignof(int128_t) == 0);
    for (uint64_t i = 0; i < ROWS; i++) {
        INFO("row " << i);
        REQUIRE(wide_equals(result.data() + i * sizeof(int128_t), i + SHIFT));
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: every buffer payload the buffer manager hands out is 16-byte aligned") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());
    const auto aligned = [](const std::byte* p) { return reinterpret_cast<std::uintptr_t>(p) % alignof(int128_t) == 0; };

    auto tiny = env.buffer_manager.register_transient_memory(1024, bm.block_size());
    REQUIRE_FALSE(tiny.has_error());
    auto tiny_pin = env.buffer_manager.pin(tiny.value());
    REQUIRE_FALSE(tiny_pin.has_error());
    CHECK(aligned(tiny_pin.value().ptr()));

    auto managed = env.buffer_manager.allocate(memory_tag::TRANSIENT_TABLE, bm.block_size());
    REQUIRE_FALSE(managed.has_error());
    CHECK(aligned(managed.value().ptr()));

    const uint64_t block_id = bm.free_block_id();
    block_t image(&env.resource, block_id, static_cast<uint64_t>(bm.block_size()));
    CHECK(aligned(image.buffer()));
    std::memset(image.buffer(), 0, static_cast<size_t>(bm.block_size()));
    REQUIRE_FALSE(bm.write(image, block_id).has_error());
    auto loaded = bm.register_block(block_id);
    auto loaded_pin = env.buffer_manager.pin(loaded);
    REQUIRE_FALSE(loaded_pin.has_error());
    CHECK(aligned(loaded_pin.value().ptr()));

    cleanup_test_file();
}
