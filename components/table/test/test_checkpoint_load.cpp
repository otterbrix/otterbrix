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

#include <functional>
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
            table.finalize_append(state, transaction_data{0, 0});
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
                chunk.set_value(0, i, logical_value_t{resource, value_fn(offset + i)});
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
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
                chunk.set_value(0, i, logical_value_t{resource, value_fn(offset + i)});
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
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

    // write phase
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

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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

        // scan all rows
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto val = chunk.data[0].value(i);
                REQUIRE(val.value<int64_t>() == static_cast<int64_t>(scanned + i));
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

    // write phase
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
            table->finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }

        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
                // INT64
                auto id_val = chunk.data[0].value(i);
                REQUIRE(id_val.value<int64_t>() == static_cast<int64_t>(row));
                // STRING
                auto name_val = chunk.data[1].value(i);
                std::string expected_name = std::string("name_") + std::to_string(row);
                REQUIRE(*name_val.value<std::string*>() == expected_name);
                // DOUBLE
                auto score_val = chunk.data[2].value(i);
                REQUIRE(score_val.value<double>() == Catch::Approx(static_cast<double>(row) * 1.5));
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

    // write phase
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

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
    // DEFAULT_VECTOR_CAPACITY is 1024, row_group_size defaults to that
    // use enough rows to span multiple row groups
    constexpr uint64_t NUM_ROWS = DEFAULT_VECTOR_CAPACITY * 3 + 100;

    meta_block_pointer_t table_pointer;

    // write phase
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

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
                auto val = chunk.data[0].value(i);
                REQUIRE(val.value<int64_t>() == static_cast<int64_t>(scanned + i));
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

        // 100x1, 100x2, 100x3, 100x4, 100x5
        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t idx) {
            return static_cast<int64_t>(idx / 100 + 1);
        });
        REQUIRE(table->calculate_size() == NUM_ROWS);

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

        // cycle through 5 values: 1,2,3,4,5,1,2,3,...
        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t idx) {
            return static_cast<int64_t>(idx % 5 + 1);
        });
        REQUIRE(table->calculate_size() == NUM_ROWS);

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

        // all unique values: 0..499
        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

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
    constexpr uint64_t CONST_ROWS = DEFAULT_VECTOR_CAPACITY; // fills one row group
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

// ---------------------------------------------------------------------------
// REGRESSION: shared partial block corruption on REOPEN after table GROWS.
//
// The catalog table pg_attribute packs ~8 narrow, low-cardinality columns into
// ONE shared partial block at distinct offsets (col0 @0, the rest at increasing
// non-zero offsets). On REOPEN after the table GROWS and re-checkpoints, the
// shared block's in-memory buffer was clobbered at the packed (non-zero-offset)
// columns, so reads returned garbage.
//
// This mimics pg_attribute: 8 BIGINT columns, each very low cardinality so the
// checkpointer DICTIONARY/RLE/CONSTANT-compresses every segment to a few bytes,
// and the shared partial_block_manager packs all of them into one block. Then:
//   checkpoint #1 -> reopen #1 -> GROW (append rows) -> checkpoint #2 ->
//   reopen #2 -> scan and assert EVERY column reads back EXACTLY.
namespace {
    // Replicate the agent-level checkpoint sequence (manager_disk.cpp): flush the
    // table metadata, set the meta block, serialize the free list, and persist the
    // header WITH the free list. Persisting the free list across reopen is what
    // makes the block-recycling corruption deterministic.
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
        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        return table_pointer;
    }

    // pg_attribute-like value generator: column c at row r. Low cardinality per
    // column so the checkpointer compresses each segment; distinct per-column
    // patterns so a cross-column clobber is detectable.
    int64_t pg_attr_value(uint64_t col, uint64_t row) {
        switch (col) {
            case 0:
                return static_cast<int64_t>(row); // attrelid-like, RLE-ish runs
            case 1:
                return static_cast<int64_t>(20 + row % 3); // atttypid: {20,21,22}
            case 2:
                return static_cast<int64_t>(row % 8 + 1); // attnum: 1..8 cycling (DICTIONARY)
            case 3:
                return 1; // attnotnull: CONSTANT
            case 4:
                return 0; // atthasdefault: CONSTANT
            case 5:
                return 0; // attisdropped: CONSTANT 0 (the SSB symptom)
            case 6:
                return 1000; // added_at_commit_id: CONSTANT
            case 7:
                return 0; // dropped_at_commit_id: CONSTANT 0
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
                    chunk.set_value(c, i, logical_value_t{resource, pg_attr_value(c, r)});
                }
            }
            table_append_state state(resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
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
                    auto v = chunk.data[c].value(i);
                    INFO("col=" << c << " row=" << r);
                    REQUIRE(v.value<int64_t>() == pg_attr_value(c, r));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == total_rows);
    }

    // Pool large enough to hold the (tiny) working set resident: the corruption is an
    // append-into-loaded-segment bug independent of eviction pressure, so the large
    // pool keeps this guard focused on the data corruption, not pool exhaustion.
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
    constexpr uint64_t INITIAL_ROWS = 200; // small -> compressed segments pack into one block
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

    // Phase 1: create, append, checkpoint #1.
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = std::make_unique<data_table_t>(&env.resource, bm, make_columns(), "pg_attribute");
        append_pg_attr_rows(*table, &env.resource, NUM_COLS, 0, INITIAL_ROWS);
        REQUIRE(table->calculate_size() == INITIAL_ROWS);
        table_pointer = full_checkpoint(*table, bm);
    }

    // Phase 2: REOPEN #1, verify, then GROW + checkpoint #2.
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();
        REQUIRE(loaded->column_count() == NUM_COLS);

        // After reopen the loaded data still reads correctly.
        scan_and_verify_pg_attr(*loaded, NUM_COLS, INITIAL_ROWS);

        // GROW the table: append more rows, then re-checkpoint. This is where the
        // shared partial block gets freed/recycled and its packed (non-zero-offset)
        // columns clobbered.
        append_pg_attr_rows(*loaded, &env.resource, NUM_COLS, INITIAL_ROWS, GROW_ROWS);
        REQUIRE(loaded->calculate_size() == TOTAL_ROWS);
        table_pointer = full_checkpoint(*loaded, bm);
    }

    // Phase 3: REOPEN #2, scan EVERY column. The packed columns at non-zero
    // offsets (atttypid, attnum, attnotnull, atthasdefault, attisdropped,
    // added_at_commit_id, dropped_at_commit_id) must read back EXACTLY. On the
    // buggy code attisdropped/added_at_commit_id come back garbage (the SSB
    // symptom).
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

// Nested-column persistence: a LIST/ARRAY/STRUCT column keeps its payload in CHILD
// column_data_t nodes (elements, fields) plus a validity bitmap; only the top-level
// segments used to reach the checkpoint, so the reloaded table had typed columns with
// EMPTY children — the child scan asserted on a null segment (state.current). These
// three cases pin the full data round-trip through checkpoint + load_from_disk.

TEST_CASE("checkpoint_load: LIST column round-trips its child data") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 100;
    auto list_length = [](uint64_t row) { return row % 5; }; // includes empty lists

    meta_block_pointer_t table_pointer;

    // write phase
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
        table->finalize_append(state, transaction_data{0, 0});
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                auto lv = chunk.data[1].value(i);
                REQUIRE(lv.type().type() == logical_type::LIST);
                REQUIRE(lv.children().size() == list_length(row));
                for (uint64_t j = 0; j < list_length(row); j++) {
                    REQUIRE(lv.children()[j].value<uint64_t>() == row * 100 + j);
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

    // write phase
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
        table->finalize_append(state, transaction_data{0, 0});
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
                auto av = chunk.data[0].value(i);
                REQUIRE(av.type().type() == logical_type::ARRAY);
                REQUIRE(av.children().size() == ARRAY_SIZE);
                for (uint64_t j = 0; j < ARRAY_SIZE; j++) {
                    REQUIRE(av.children()[j].value<uint64_t>() == row * ARRAY_SIZE + j);
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

    // write phase
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
            std::vector<logical_value_t> members;
            members.emplace_back(&env.resource, static_cast<int64_t>(i * 7));
            members.emplace_back(&env.resource, std::string("row_") + std::to_string(i));
            chunk.set_value(0, i, logical_value_t::create_struct(&env.resource, struct_type, members));
        }
        {
            table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
                auto sv = chunk.data[0].value(i);
                REQUIRE(sv.type().type() == logical_type::STRUCT);
                REQUIRE(sv.children().size() == 2);
                REQUIRE(sv.children()[0].value<int64_t>() == static_cast<int64_t>(row * 7));
                const auto& name_value = sv.children()[1]; // named local: chunk values are temporaries
                REQUIRE(name_value.value<std::string_view>() == std::string("row_") + std::to_string(row));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

// NULL validity must round-trip through checkpoint + load, across MULTIPLE row groups,
// and the reload must leave every validity segment DISK-BACKED without allocating new
// blocks. Two distinct defects are pinned here:
//   1. the checkpoint never wrote the validity bitmap, so the reload manufactured an
//      all-valid one — every NULL silently became a present zero/empty value;
//   2. the manufactured bitmaps were written THROUGH to the data file on every load, so
//      merely REOPENING a table allocated fresh blocks and grew the file each time.
// With validity persisted, the reload registers the checkpointed blocks and allocates
// nothing.
TEST_CASE("checkpoint_load: NULL validity round-trips, reopen allocates no new blocks") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    // Three row groups (row group size == DEFAULT_VECTOR_CAPACITY == 1024): NULL bits must
    // survive past the first vector and the first row group.
    constexpr uint64_t NUM_ROWS = 3000;
    constexpr uint64_t NULL_STEP = 7;
    auto is_null_row = [](uint64_t row) { return row % NULL_STEP == 0; };

    meta_block_pointer_t table_pointer;
    uint64_t blocks_after_checkpoint = 0;

    // write phase
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
                    chunk.set_value(0, i, logical_value_t{&env.resource, nullptr});
                    chunk.set_value(1, i, logical_value_t{&env.resource, nullptr});
                } else {
                    chunk.set_value(0, i, logical_value_t{&env.resource, static_cast<int64_t>(row * 3)});
                    chunk.set_value(1, i, logical_value_t{&env.resource, std::string("r") + std::to_string(row)});
                }
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
        blocks_after_checkpoint = bm.total_blocks();
        REQUIRE(blocks_after_checkpoint > 0);
    }

    // read phase
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        REQUIRE(bm.total_blocks() == blocks_after_checkpoint);

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded_result = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded_result.has_error());
        auto& loaded = loaded_result.value();

        // Loading must REGISTER the persisted validity blocks, not manufacture + write
        // through fresh ones: the block count is exactly what the checkpoint left.
        CHECK(bm.total_blocks() == blocks_after_checkpoint);

        uint64_t scanned = 0;
        uint64_t nulls_seen = 0;
        otterbrix_test::scan_table_segment(*loaded, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = scanned + i;
                INFO("row " << row);
                const auto v = chunk.data[0].value(i);
                const auto s = chunk.data[1].value(i); // named local: chunk values are temporaries
                if (is_null_row(row)) {
                    CHECK(v.is_null());
                    CHECK(s.is_null());
                    nulls_seen += v.is_null() ? 1 : 0;
                } else {
                    REQUIRE_FALSE(v.is_null());
                    REQUIRE_FALSE(s.is_null());
                    REQUIRE(v.value<int64_t>() == static_cast<int64_t>(row * 3));
                    REQUIRE(s.value<std::string_view>() == std::string("r") + std::to_string(row));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
        CHECK(nulls_seen == (NUM_ROWS + NULL_STEP - 1) / NULL_STEP);
    }

    cleanup_test_file();
}

// compact() must preserve NULLs. Its rebuild scans the WHOLE table into ONE growing chunk
// (collection_scan_state::scan loops row groups into the same result), and the validity
// child's scan state never tracked the parent's result_offset: every vector's NULL bits
// landed at chunk offset 0. The rebuilt table then held the union of ALL row groups' NULL
// patterns folded into its first 1024 rows (bit = row mod 1024) and read every later row
// as non-NULL — and since the disk agent compacts before every checkpoint, this is what
// got persisted. Total NULL COUNT is preserved by the fold, which is what kept it silent.
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
                chunk.set_value(0, i, logical_value_t{&env.resource, nullptr});
            } else {
                chunk.set_value(0, i, logical_value_t{&env.resource, static_cast<int64_t>(row * 3)});
            }
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }
    REQUIRE(table->calculate_size() == NUM_ROWS);

    // The rebuild under test.
    REQUIRE(table->compact(0));
    REQUIRE(table->calculate_size() == NUM_ROWS);

    uint64_t scanned = 0;
    otterbrix_test::scan_table_segment(*table, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
        for (uint64_t i = 0; i < chunk.size(); i++) {
            const uint64_t row = scanned + i;
            INFO("row " << row);
            const auto v = chunk.data[0].value(i);
            CHECK(v.is_null() == is_null_row(row));
            if (!is_null_row(row)) {
                REQUIRE_FALSE(v.is_null());
                REQUIRE(v.value<int64_t>() == static_cast<int64_t>(row * 3));
            }
        }
        scanned += chunk.size();
    });
    REQUIRE(scanned == NUM_ROWS);

    cleanup_test_file();
}

// Same NULL round-trip, but appended in 100-ROW batches so appends CROSS row-group
// boundaries mid-call (the SQL INSERT path appends per-statement chunks that almost never
// align with the 1024-row group size). Pinned bug: an append that rolled into a fresh row
// group kept writing NULL bits through the PREVIOUS group's validity buffer, so the live
// disk table held a first-group bitmap with every later group's NULL pattern folded into
// it (bit = row mod 1024) and all-valid bitmaps for the later groups — which is exactly
// what the checkpoint then persisted. A 1024-aligned append (the test above) never hits
// this.
TEST_CASE("checkpoint_load: NULL validity survives boundary-crossing appends") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 3000;
    constexpr uint64_t BATCH = 100; // never aligned with the 1024-row group size
    constexpr uint64_t NULL_STEP = 100;
    // Row `r` is NULL when (r + 1) is a multiple of 100 — matching the integration test's
    // id % 100 == 0 pattern so the fold (row mod 1024) is observable and distinct per group.
    auto is_null_row = [](uint64_t row) { return (row + 1) % NULL_STEP == 0; };

    meta_block_pointer_t table_pointer;

    // write phase: 30 appends of 100 rows each
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
                    chunk.set_value(0, i, logical_value_t{&env.resource, nullptr});
                } else {
                    chunk.set_value(0, i, logical_value_t{&env.resource, static_cast<int64_t>(row * 3)});
                }
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data{0, 0});
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

        // The LIVE table must already read back the right NULLs (pre-checkpoint).
        uint64_t live_scanned = 0;
        otterbrix_test::scan_table_segment(*table, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = live_scanned + i;
                INFO("live row " << row);
                const auto v = chunk.data[0].value(i);
                REQUIRE(v.is_null() == is_null_row(row));
            }
            live_scanned += chunk.size();
        });
        REQUIRE(live_scanned == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // read phase
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
                const auto v = chunk.data[0].value(i);
                CHECK(v.is_null() == is_null_row(row));
                if (!is_null_row(row)) {
                    REQUIRE_FALSE(v.is_null());
                    REQUIRE(v.value<int64_t>() == static_cast<int64_t>(row * 3));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

// A LIST segment's RAW payload is one uint64 child-offset per row -- that is what the LIST
// legs of append / fixed_size_scan / finalize_append write and read. complex_logical_type::
// size() for LIST is sizeof(list_entry_t) == 16, twice that, and column_segment_t::type_size
// used to be initialised from it. Every raw-byte consumer therefore took the wrong width:
//
//   * the checkpoint's CONSTANT/RLE/DICTIONARY analysis walked 16 bytes per row, i.e. TWICE
//     the segment's real extent, folding whatever followed the offsets into the "values";
//   * the compressed scan wrote 16 bytes per row into the uint64 offset vector that
//     list_column_data_t::scan_count sizes at 8 bytes per row -- an 8 KiB heap overrun per
//     1024-row vector, reachable from a plain SELECT on any reloaded LIST column.
//
// It stayed invisible because the emitted BYTE STREAM round-tripped: the first half landed
// on the offsets correctly and only the run past the end of the buffer was wrong. It
// surfaced as a pmr-pool "pointer being freed was not allocated" abort in the LIST
// round-trip case above, but only for heap layouts where the trailing 8 KiB happened to
// cover pool metadata -- which is why it looked like a flake.
//
// All-empty lists make every stored offset zero, so the checkpoint picks CONSTANT and the
// persisted segment is exactly ONE stored element. That single number is the whole bug: 8
// with the correct physical width, 16 with the logical one.
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
        // The append state must not outlive the checkpoint: checkpointing re-points still-
        // managed child segments and drops the block_handle its pin refers to.
        column_append_state append_state;
        REQUIRE_FALSE(column->initialize_append(append_state).has_error());

        std::pmr::vector<complex_logical_type> types(&env.resource);
        types.emplace_back(list_type);
        data_chunk_t input(&env.resource, types, NUM_ROWS);
        input.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            input.set_value(0, i, std::vector<uint64_t>{}); // every list empty
        }
        REQUIRE_FALSE(column->append(append_state, input.data[0], NUM_ROWS).has_error());
    }

    partial_block_manager_t pbm(bm);
    auto persistent = column->checkpoint(pbm);
    REQUIRE_FALSE(persistent.has_error());
    REQUIRE_FALSE(pbm.flush_partial_blocks().has_error());

    // The LIST node's own data pointers are its offsets segments (its children are the
    // validity bitmap and the element column).
    REQUIRE(persistent.value().data_pointers.size() == 1);
    const auto& dp = persistent.value().data_pointers[0];
    REQUIRE(dp.tuple_count == NUM_ROWS);
    REQUIRE(dp.compression == compression::compression_type::CONSTANT);
    // RED before the fix: 16 == sizeof(list_entry_t).
    CHECK(dp.segment_size == sizeof(uint64_t));

    // ...and the data still round-trips through a reload.
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
        const auto cell = result.value(i); // named local: value() returns a temporary
        REQUIRE(cell.type().type() == logical_type::LIST);
        CHECK(cell.children().empty());
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: 4-byte CONSTANT segment must not misalign the segments packed after it") {
    // Layout regression (F8): the partial-block packer places segments back-to-back, and the
    // resulting offset is PERSISTED in the data pointer, so it survives restart. An INT32
    // column whose values are all identical flushes as a 4-byte CONSTANT segment; before the
    // packer aligned placements, everything packed after it — this column's validity bitmap
    // (read through uint64_t*) and the next column's UNCOMPRESSED BIGINT payload (handed to
    // the result vector as a raw int64 pointer by fixed_size_scan) — sat at offset 4 mod 8.
    // Scanning the reloaded table then performed misaligned uint64/int64 loads: undefined
    // behaviour, observable under -fsanitize=alignment. This test is the sanitizer repro and
    // the value-level round-trip check in one.
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
        columns.emplace_back("tag", logical_type::INTEGER);   // all-constant -> 4-byte segment
        columns.emplace_back("value", logical_type::BIGINT);  // varied -> uncompressed int64 payload
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
            table->finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
        REQUIRE(table->calculate_size() == NUM_ROWS);

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
