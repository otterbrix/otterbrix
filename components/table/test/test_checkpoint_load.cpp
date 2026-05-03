#include <catch2/catch.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/data_pointer.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <functional>
#include <unistd.h>

namespace {
    constexpr uint32_t ROW_GROUP_LAYOUTS_MAGIC = 0x31584150U; // "PAX1"
    constexpr uint32_t TABLE_LAYOUT_METADATA_FLAG = 1U << 31;

    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_checkpoint_load_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_test_file() { std::remove(test_db_path().c_str()); }

    struct test_env_t {
        std::pmr::synchronized_pool_resource resource;
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
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
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
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "test_table");

        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    // read phase
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        REQUIRE(loaded->table_name() == "test_table");
        REQUIRE(loaded->column_count() == 1);

        // scan all rows
        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
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
        bm.create_new_database();

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
                chunk.set_value(0, i, logical_value_t{&env.resource, static_cast<int64_t>(row)});
                chunk.set_value(1, i, logical_value_t{&env.resource, std::string("name_") + std::to_string(row)});
                chunk.set_value(2, i, logical_value_t{&env.resource, static_cast<double>(row) * 1.5});
            }
            table_append_state state(&env.resource);
            table->append_lock(state);
            table->initialize_append(state);
            table->append(chunk, state);
            table->finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }

        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    // read phase
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        REQUIRE(loaded->table_name() == "multi_col");
        REQUIRE(loaded->column_count() == 3);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
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
                REQUIRE(score_val.value<double>() == Approx(static_cast<double>(row) * 1.5));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: fixed integer columns are written as pax") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 300;

    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_table");

        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            auto batch = std::min<uint64_t>(NUM_ROWS - offset, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                auto row = offset + i;
                chunk.set_value(0, i, logical_value_t{&env.resource, static_cast<int64_t>(row)});
                chunk.set_value(1, i, logical_value_t{&env.resource, static_cast<uint32_t>(row * 10)});
                chunk.set_value(2, i, logical_value_t{&env.resource, std::string("row_") + std::to_string(row)});
            }
            table_append_state state(&env.resource);
            table->append_lock(state);
            table->initialize_append(state);
            table->append(chunk, state);
            table->finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);

        REQUIRE(reader.read_string() == "pax_table");
        REQUIRE(reader.read<uint32_t>() == 3);
        for (uint32_t i = 0; i < 3; i++) {
            (void) reader.read_string();
            (void) reader.read<uint8_t>();
            (void) reader.read<uint8_t>();
        }

        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & TABLE_LAYOUT_METADATA_FLAG) != 0);
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);

        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.columnar_data_pointers.size() == 3);
        REQUIRE(row_group.columnar_data_pointers[0].size() == 3);
        REQUIRE(row_group.columnar_data_pointers[1].size() == 3);
        REQUIRE_FALSE(row_group.columnar_data_pointers[2].empty());

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        auto layout_kind = static_cast<row_group_layout_kind>(reader.read<uint8_t>());
        REQUIRE(layout_kind == row_group_layout_kind::PAX_FIXED);

        auto pax_layout = pax_fixed_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.rows_per_page == 128);
        REQUIRE(pax_layout.pages.size() == 3);
        REQUIRE(pax_layout.pages[0].slices.size() == 2);
        REQUIRE(pax_layout.pages[0].slices[0].data_pointer.block_pointer.block_id ==
                pax_layout.pages[0].slices[1].data_pointer.block_pointer.block_id);
        REQUIRE(pax_layout.pages[0].slices[0].column_index == 0);
        REQUIRE(pax_layout.pages[0].slices[1].column_index == 1);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto row = scanned + i;
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(chunk.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 10));
                REQUIRE(*chunk.data[2].value(i).value<std::string*>() == std::string("row_") + std::to_string(row));
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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "empty_table");

        REQUIRE(table->calculate_size() == 0);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    // read phase
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "big_table");

        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    // read phase
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        REQUIRE(loaded->table_name() == "big_table");
        REQUIRE(loaded->column_count() == 1);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "const_table");

        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t) { return CONSTANT_VALUE; });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        REQUIRE(loaded->table_name() == "const_table");
        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == CONSTANT_VALUE);
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
        bm.create_new_database();

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
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t global_idx = scanned + i;
                int64_t expected = static_cast<int64_t>(global_idx / 100 + 1);
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == expected);
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
        bm.create_new_database();

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
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t global_idx = scanned + i;
                int64_t expected = static_cast<int64_t>(global_idx % 5 + 1);
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == expected);
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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "unique_table");

        // all unique values: 0..499
        append_int64_data(*table, &env.resource, NUM_ROWS);
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == static_cast<int64_t>(scanned + i));
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
        bm.create_new_database();

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
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, TOTAL_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                uint64_t global_idx = scanned + i;
                int64_t expected;
                if (global_idx < CONST_ROWS) {
                    expected = CONSTANT_VALUE;
                } else {
                    expected = static_cast<int64_t>(global_idx - CONST_ROWS);
                }
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == expected);
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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("score", logical_type::DOUBLE);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "double_table");

        append_double_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t) { return CONSTANT_DOUBLE; });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].value(i).value<double>() == Approx(CONSTANT_DOUBLE));
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
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "tiny_table");

        append_int64_data_with_fn(*table, &env.resource, NUM_ROWS, [](uint64_t) { return VALUE; });
        REQUIRE(table->calculate_size() == NUM_ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        REQUIRE(loaded->table_name() == "tiny_table");
        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == VALUE);
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}
