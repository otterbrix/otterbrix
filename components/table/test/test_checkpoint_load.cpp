#include <catch2/catch.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/data_pointer.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/serialization/deserializer.hpp>
#include <core/file/local_file_system.hpp>

#include <functional>
#include <optional>
#include <unistd.h>

namespace components::table {
    struct row_group_test_access_t {
        static bool try_scan_pax_generic_projected(row_group_t& row_group,
                                                   collection_scan_state& state,
                                                   vector::data_chunk_t& result) {
            return row_group.try_scan_pax_generic_projected(state, result);
        }
        static bool try_scan_pax_fixed_projected(row_group_t& row_group,
                                                 collection_scan_state& state,
                                                 vector::data_chunk_t& result) {
            return row_group.try_scan_pax_fixed_projected(state, result);
        }
    };
} // namespace components::table

namespace {
    constexpr uint32_t COLUMN_TYPES_METADATA_MAGIC = 0x31484353U; // "SCH1"
    constexpr uint32_t ROW_GROUP_LAYOUTS_MAGIC = 0x31584150U; // "PAX1"
    constexpr uint32_t TABLE_COLUMN_TYPES_METADATA_FLAG = 1U << 31;
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

    void append_fixed_integer_pair(components::table::data_table_t& table,
                                   std::pmr::memory_resource* resource,
                                   uint64_t count,
                                   std::function<int64_t(uint64_t)> left_value_fn,
                                   std::function<uint32_t(uint64_t)> right_value_fn) {
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
                const auto row = offset + i;
                chunk.set_value(0, i, logical_value_t{resource, left_value_fn(row)});
                chunk.set_value(1, i, logical_value_t{resource, right_value_fn(row)});
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void append_nullable_fixed_integer_pair(components::table::data_table_t& table,
                                            std::pmr::memory_resource* resource,
                                            uint64_t count,
                                            std::function<std::optional<int64_t>(uint64_t)> left_value_fn,
                                            std::function<std::optional<uint32_t>(uint64_t)> right_value_fn) {
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
                const auto row = offset + i;
                auto left_value = left_value_fn(row);
                auto right_value = right_value_fn(row);
                if (left_value.has_value()) {
                    chunk.set_value(0, i, logical_value_t{resource, *left_value});
                } else {
                    chunk.set_value(0, i, logical_value_t{resource, nullptr});
                }
                if (right_value.has_value()) {
                    chunk.set_value(1, i, logical_value_t{resource, *right_value});
                } else {
                    chunk.set_value(1, i, logical_value_t{resource, nullptr});
                }
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void append_nullable_string_data(components::table::data_table_t& table,
                                     std::pmr::memory_resource* resource,
                                     uint64_t count,
                                     std::function<std::optional<std::string>(uint64_t)> value_fn) {
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
                auto value = value_fn(offset + i);
                if (value.has_value()) {
                    chunk.set_value(0, i, logical_value_t{resource, *value});
                } else {
                    chunk.set_value(0, i, logical_value_t{resource, nullptr});
                }
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    std::string padded_name(uint64_t row) {
        auto digits = std::to_string(row);
        if (digits.size() < 4) {
            digits = std::string(4 - digits.size(), '0') + digits;
        }
        return "name_" + digits;
    }

    components::types::complex_logical_type make_person_struct_type() {
        using namespace components::types;

        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::BIGINT, "id");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        return complex_logical_type::create_struct("person", fields, "person_struct");
    }

    components::types::complex_logical_type make_nested_struct_type() {
        using namespace components::types;

        std::vector<complex_logical_type> nested_fields;
        nested_fields.emplace_back(logical_type::STRING_LITERAL, "name");
        nested_fields.emplace_back(logical_type::BIGINT, "score");
        auto meta_type = complex_logical_type::create_struct("meta", nested_fields, "meta_struct");

        std::vector<complex_logical_type> root_fields;
        root_fields.emplace_back(logical_type::BIGINT, "id");
        root_fields.push_back(meta_type);
        return complex_logical_type::create_struct("entry", root_fields, "entry_struct");
    }

    void append_struct_data(components::table::data_table_t& table,
                            std::pmr::memory_resource* resource,
                            uint64_t count,
                            std::function<components::types::logical_value_t(uint64_t)> value_fn) {
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
        REQUIRE(reader.read<uint32_t>() == 2);
        for (uint32_t i = 0; i < 2; i++) {
            (void) reader.read_string();
            (void) reader.read<uint8_t>();
            (void) reader.read<uint8_t>();
        }

        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & TABLE_LAYOUT_METADATA_FLAG) != 0);
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);

        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.columnar_data_pointers.size() == 2);
        REQUIRE(row_group.columnar_data_pointers[0].size() == 3);
        REQUIRE(row_group.columnar_data_pointers[1].size() == 3);

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
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: string columns are written as pax generic") {
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
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        columns.emplace_back("id", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_table");

        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            auto batch = std::min<uint64_t>(NUM_ROWS - offset, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const auto row = offset + i;
                if (row % 7 == 0) {
                    chunk.set_value(0, i, logical_value_t{&env.resource, nullptr});
                } else {
                    chunk.set_value(0, i, logical_value_t{&env.resource, std::string("name_") + std::to_string(row)});
                }
                chunk.set_value(1, i, logical_value_t{&env.resource, static_cast<int64_t>(row * 10)});
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

        REQUIRE(reader.read_string() == "pax_generic_table");
        REQUIRE(reader.read<uint32_t>() == 2);
        for (uint32_t i = 0; i < 2; i++) {
            (void) reader.read_string();
            (void) reader.read<uint8_t>();
            (void) reader.read<uint8_t>();
        }

        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & TABLE_LAYOUT_METADATA_FLAG) != 0);
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);

        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.columnar_data_pointers.size() == 2);
        REQUIRE(row_group.columnar_data_pointers[0].size() == 3);
        REQUIRE_FALSE(row_group.columnar_data_pointers[1].empty());

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        auto layout_kind = static_cast<row_group_layout_kind>(reader.read<uint8_t>());
        REQUIRE(layout_kind == row_group_layout_kind::PAX_GENERIC);

        auto pax_layout = pax_generic_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.rows_per_page == 128);
        REQUIRE(pax_layout.pages.size() == 3);
        REQUIRE(pax_layout.pages[0].slices.size() == 2);
        REQUIRE(pax_layout.pages[0].slices[0].slice_kind == pax_generic_slice_kind::STRING_VALUES);
        REQUIRE(pax_layout.pages[0].slices[0].codec_kind == pax_generic_codec_kind::STRING_SEGMENT);
        REQUIRE(pax_layout.pages[0].slices[1].slice_kind == pax_generic_slice_kind::VALIDITY);
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
                const auto row = scanned + i;
                REQUIRE(chunk.data[0].is_null(i) == (row % 7 == 0));
                if (row % 7 != 0) {
                    REQUIRE(*chunk.data[0].value(i).value<std::string*>() == std::string("name_") + std::to_string(row));
                }
                REQUIRE(chunk.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row * 10));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic string scan preserves null validity across pages") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 300;
    const auto table_path = test_db_path() + ".pax_generic_nulls";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto value_fn = [](uint64_t row) -> std::optional<std::string> {
        if (row < 128) {
            return (row % 5 == 0) ? std::optional<std::string>{}
                                  : std::optional<std::string>{std::string("left_") + std::to_string(row)};
        }
        if (row < 256) {
            return std::nullopt;
        }
        return std::string("tail_") + std::to_string(row);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_nulls");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                const auto expected = value_fn(row);
                REQUIRE(chunk.data[0].is_null(i) == !expected.has_value());
                if (expected.has_value()) {
                    REQUIRE(*chunk.data[0].value(i).value<std::string*>() == *expected);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic string scan restores overflow strings") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 130;
    const auto table_path = test_db_path() + ".pax_generic_overflow";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto value_fn = [](uint64_t row) -> std::optional<std::string> {
        if (row % 9 == 0) {
            return std::nullopt;
        }
        if (row % 3 == 0) {
            return std::string(6000 + row, static_cast<char>('a' + (row % 26)));
        }
        return std::string("short_") + std::to_string(row);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_overflow");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        REQUIRE(reader.read_string() == "pax_generic_overflow");
        REQUIRE(reader.read<uint32_t>() == 1);
        (void) reader.read_string();
        (void) reader.read<uint8_t>();
        (void) reader.read<uint8_t>();
        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);
        (void) row_group_pointer_t::deserialize(reader);
        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        REQUIRE(static_cast<row_group_layout_kind>(reader.read<uint8_t>()) == row_group_layout_kind::PAX_GENERIC);
        auto pax_layout = pax_generic_row_group_layout_t::deserialize(reader);
        bool found_overflow = false;
        for (const auto& page : pax_layout.pages) {
            for (const auto& slice : page.slices) {
                if (slice.slice_kind == pax_generic_slice_kind::STRING_VALUES && slice.payload.has_value() &&
                    !slice.payload->extra_block_ids.empty()) {
                    found_overflow = true;
                }
            }
        }
        REQUIRE(found_overflow);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                const auto expected = value_fn(row);
                REQUIRE(chunk.data[0].is_null(i) == !expected.has_value());
                if (expected.has_value()) {
                    REQUIRE(*chunk.data[0].value(i).value<std::string*>() == *expected);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic struct column restores fixed and string children") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 180;
    const auto table_path = test_db_path() + ".pax_generic_struct";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    auto struct_type = make_person_struct_type();

    auto value_fn = [&](uint64_t row) {
        if (row % 7 == 0) {
            return logical_value_t{&env.resource, nullptr};
        }

        std::vector<logical_value_t> fields;
        fields.emplace_back(&env.resource, row % 2 == 0);
        fields.emplace_back(&env.resource, static_cast<int64_t>(row * 11));
        if (row % 5 == 0) {
            fields.emplace_back(&env.resource, nullptr);
        } else {
            fields.emplace_back(&env.resource, padded_name(row));
        }
        return logical_value_t::create_struct(&env.resource, struct_type, fields);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("person", struct_type);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_struct");
        append_struct_data(*table, &env.resource, NUM_ROWS, value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        REQUIRE(reader.read_string() == "pax_generic_struct");
        auto column_count = reader.read<uint32_t>();
        REQUIRE((column_count & TABLE_COLUMN_TYPES_METADATA_FLAG) != 0);
        REQUIRE((column_count & ~TABLE_COLUMN_TYPES_METADATA_FLAG) == 1);
        (void) reader.read_string();
        (void) reader.read<uint8_t>();
        (void) reader.read<uint8_t>();
        REQUIRE(reader.read<uint32_t>() == COLUMN_TYPES_METADATA_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        {
            auto serialized_type = reader.read_string();
            std::pmr::string payload(serialized_type.begin(), serialized_type.end(), &env.resource);
            components::serializer::msgpack_deserializer_t deserializer(payload);
            auto persisted_type = complex_logical_type::deserialize(&env.resource, &deserializer);
            REQUIRE(persisted_type.type() == logical_type::STRUCT);
            REQUIRE(persisted_type.child_types().size() == 3);
            REQUIRE(persisted_type.child_types()[0].type() == logical_type::BOOLEAN);
            REQUIRE(persisted_type.child_types()[1].type() == logical_type::BIGINT);
            REQUIRE(persisted_type.child_types()[2].type() == logical_type::STRING_LITERAL);
        }
        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);
        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.columnar_data_pointers.size() == 1);
        REQUIRE(row_group.columnar_data_pointers[0].empty());

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        REQUIRE(static_cast<row_group_layout_kind>(reader.read<uint8_t>()) == row_group_layout_kind::PAX_GENERIC);
        auto pax_layout = pax_generic_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.version == 2);
        bool found_root_validity = false;
        bool found_flag_values = false;
        bool found_id_values = false;
        bool found_name_values = false;
        for (const auto& page : pax_layout.pages) {
            for (const auto& slice : page.slices) {
                if (slice.slice_kind == pax_generic_slice_kind::VALIDITY && slice.field_path.empty()) {
                    found_root_validity = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{0}) {
                    found_flag_values = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{1}) {
                    found_id_values = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::STRING_VALUES &&
                           slice.field_path == std::vector<uint16_t>{2}) {
                    found_name_values = true;
                }
            }
        }
        REQUIRE(found_root_validity);
        REQUIRE(found_flag_values);
        REQUIRE(found_id_values);
        REQUIRE(found_name_values);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                auto expected = value_fn(row);
                auto actual = chunk.data[0].value(i);
                REQUIRE(actual.is_null() == expected.is_null());
                if (expected.is_null()) {
                    continue;
                }
                REQUIRE(actual.type().type() == logical_type::STRUCT);
                REQUIRE(actual.children()[0].value<bool>() == expected.children()[0].value<bool>());
                REQUIRE(actual.children()[1].value<int64_t>() == expected.children()[1].value<int64_t>());
                REQUIRE(actual.children()[2].is_null() == expected.children()[2].is_null());
                if (!expected.children()[2].is_null()) {
                    REQUIRE(*actual.children()[2].value<std::string*>() == *expected.children()[2].value<std::string*>());
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic nested struct column restores overflow strings") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 140;
    const auto table_path = test_db_path() + ".pax_generic_nested_struct";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    auto struct_type = make_nested_struct_type();
    const auto& meta_type = struct_type.child_types()[1];

    auto value_fn = [&](uint64_t row) {
        if (row % 9 == 0) {
            return logical_value_t{&env.resource, nullptr};
        }

        std::vector<logical_value_t> meta_fields;
        if (row % 6 == 0) {
            meta_fields.emplace_back(&env.resource, nullptr);
        } else if (row % 4 == 0) {
            meta_fields.emplace_back(&env.resource, std::string(6000 + row, static_cast<char>('a' + (row % 26))));
        } else {
            meta_fields.emplace_back(&env.resource, padded_name(row));
        }
        meta_fields.emplace_back(&env.resource, static_cast<int64_t>(row * 100));

        std::vector<logical_value_t> fields;
        fields.emplace_back(&env.resource, static_cast<int64_t>(row));
        fields.emplace_back(logical_value_t::create_struct(&env.resource, meta_type, meta_fields));
        return logical_value_t::create_struct(&env.resource, struct_type, fields);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("entry", struct_type);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_nested_struct");
        append_struct_data(*table, &env.resource, NUM_ROWS, value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                auto expected = value_fn(row);
                auto actual = chunk.data[0].value(i);
                REQUIRE(actual.is_null() == expected.is_null());
                if (expected.is_null()) {
                    continue;
                }

                REQUIRE(actual.children()[0].value<int64_t>() == expected.children()[0].value<int64_t>());
                const auto& actual_meta = actual.children()[1];
                const auto& expected_meta = expected.children()[1];
                REQUIRE(actual_meta.children()[0].is_null() == expected_meta.children()[0].is_null());
                if (!expected_meta.children()[0].is_null()) {
                    REQUIRE(*actual_meta.children()[0].value<std::string*>() ==
                            *expected_meta.children()[0].value<std::string*>());
                }
                REQUIRE(actual_meta.children()[1].value<int64_t>() == expected_meta.children()[1].value<int64_t>());
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan uses fast path") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1300;
    const auto table_path = test_db_path() + ".pax_generic_projected";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_projected");
        append_nullable_string_data(*table,
                                    &env.resource,
                                    NUM_ROWS,
                                    [](uint64_t row) -> std::optional<std::string> { return padded_name(row); });

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        data_chunk_t first_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                        helper_state.table_state,
                                                                        first_chunk));
        REQUIRE(first_chunk.size() == DEFAULT_VECTOR_CAPACITY);
        for (uint64_t i = 0; i < first_chunk.size(); i++) {
            REQUIRE(*first_chunk.data[0].value(i).value<std::string*>() == padded_name(i));
        }

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, nullptr);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < result.size(); i++) {
                REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(scanned + i));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan supports simple filters") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 300;
    const auto table_path = test_db_path() + ".pax_generic_filters";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto value_fn = [](uint64_t row) -> std::optional<std::string> {
        if (row % 7 == 0) {
            return std::nullopt;
        }
        return padded_name(row);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_filters");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        std::pmr::vector<uint64_t> eq_filter_columns(&env.resource);
        eq_filter_columns.push_back(0);
        constant_filter_t eq_filter(components::expressions::compare_type::eq,
                                    logical_value_t{&env.resource, padded_name(111)},
                                    std::move(eq_filter_columns));

        table_scan_state eq_state(&env.resource);
        loaded->initialize_scan(eq_state, projected_indices, &eq_filter);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        data_chunk_t eq_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                        eq_state.table_state,
                                                                        eq_chunk));
        REQUIRE(eq_chunk.size() == 1);
        REQUIRE(*eq_chunk.data[0].value(0).value<std::string*>() == padded_name(111));

        std::pmr::vector<uint64_t> gt_filter_columns(&env.resource);
        gt_filter_columns.push_back(0);
        constant_filter_t gt_filter(components::expressions::compare_type::gt,
                                    logical_value_t{&env.resource, padded_name(250)},
                                    std::move(gt_filter_columns));

        table_scan_state gt_state(&env.resource);
        loaded->initialize_scan(gt_state, projected_indices, &gt_filter);
        data_chunk_t gt_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(gt_result, gt_state);

        uint64_t expected_gt_count = 0;
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            auto expected = value_fn(row);
            if (expected.has_value() && *expected > padded_name(250)) {
                expected_gt_count++;
            }
        }
        REQUIRE(gt_result.size() == expected_gt_count);
        for (uint64_t i = 0; i < gt_result.size(); i++) {
            REQUIRE(*gt_result.data[0].value(i).value<std::string*>() > padded_name(250));
        }

        std::pmr::vector<uint64_t> miss_filter_columns(&env.resource);
        miss_filter_columns.push_back(0);
        constant_filter_t miss_filter(components::expressions::compare_type::eq,
                                      logical_value_t{&env.resource, padded_name(112)},
                                      std::move(miss_filter_columns));

        table_scan_state miss_state(&env.resource);
        loaded->initialize_scan(miss_state, projected_indices, &miss_filter);
        data_chunk_t miss_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(miss_result, miss_state);
        REQUIRE(miss_result.size() == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan restores overflow strings") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 260;
    const auto table_path = test_db_path() + ".pax_generic_projected_overflow";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto value_fn = [](uint64_t row) -> std::optional<std::string> {
        if (row % 9 == 0) {
            return std::nullopt;
        }
        if (row % 5 == 0) {
            return std::string(6000 + row, static_cast<char>('a' + (row % 26))) + "_" + padded_name(row);
        }
        return padded_name(row);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_projected_overflow");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        data_chunk_t first_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                        helper_state.table_state,
                                                                        first_chunk));
        REQUIRE(first_chunk.size() == NUM_ROWS);
        for (uint64_t i = 0; i < first_chunk.size(); i++) {
            auto expected = value_fn(i);
            REQUIRE(first_chunk.data[0].is_null(i) == !expected.has_value());
            if (expected.has_value()) {
                REQUIRE(*first_chunk.data[0].value(i).value<std::string*>() == *expected);
            }
        }

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, nullptr);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < result.size(); i++) {
                auto expected = value_fn(scanned + i);
                REQUIRE(result.data[0].is_null(i) == !expected.has_value());
                if (expected.has_value()) {
                    REQUIRE(*result.data[0].value(i).value<std::string*>() == *expected);
                }
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan falls back for mixed projection") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 200;
    const auto table_path = test_db_path() + ".pax_generic_fallback";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        columns.emplace_back("id", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_fallback");

        auto types = table->copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            uint64_t batch = std::min(NUM_ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const auto row = offset + i;
                chunk.set_value(0, i, logical_value_t{&env.resource, padded_name(row)});
                chunk.set_value(1, i, logical_value_t{&env.resource, static_cast<int64_t>(row * 2)});
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
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE_FALSE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                              helper_state.table_state,
                                                                              helper_chunk));

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, nullptr);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++) {
                const auto row = scanned + i;
                REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(row));
                REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row * 2));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan uses fast path") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1300;
    const auto table_path = test_db_path() + ".pax_scan";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_scan");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  NUM_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 3); });

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        data_chunk_t first_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                      helper_state.table_state,
                                                                      first_chunk));
        REQUIRE(first_chunk.size() == DEFAULT_VECTOR_CAPACITY);
        for (uint64_t i = 0; i < first_chunk.size(); i++) {
            REQUIRE(first_chunk.data[0].value(i).value<int64_t>() == static_cast<int64_t>(i));
            REQUIRE(first_chunk.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(i * 3));
        }

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, nullptr);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < result.size(); i++) {
                const auto row = scanned + i;
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 3));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan supports simple filters") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1300;
    const auto table_path = test_db_path() + ".pax_filter";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_filter");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  NUM_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row + 100); });

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        std::pmr::vector<uint64_t> filter_columns(&env.resource);
        filter_columns.push_back(0);
        constant_filter_t eq_filter(components::expressions::compare_type::eq,
                                    logical_value_t{&env.resource, int64_t(777)},
                                    std::move(filter_columns));

        table_scan_state eq_state(&env.resource);
        loaded->initialize_scan(eq_state, projected_indices, &eq_filter);
        data_chunk_t eq_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(eq_result, eq_state);
        REQUIRE(eq_result.size() == 1);
        REQUIRE(eq_result.data[0].value(0).value<int64_t>() == 777);
        REQUIRE(eq_result.data[1].value(0).value<uint32_t>() == 877);

        std::pmr::vector<uint64_t> gt_filter_columns(&env.resource);
        gt_filter_columns.push_back(0);
        constant_filter_t gt_filter(components::expressions::compare_type::gt,
                                    logical_value_t{&env.resource, int64_t(1200)},
                                    std::move(gt_filter_columns));

        table_scan_state gt_state(&env.resource);
        loaded->initialize_scan(gt_state, projected_indices, &gt_filter);
        data_chunk_t gt_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(gt_result, gt_state);
        REQUIRE(gt_result.size() == NUM_ROWS - 1201);
        for (uint64_t i = 0; i < gt_result.size(); i++) {
            const auto row = 1201 + i;
            REQUIRE(gt_result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
            REQUIRE(gt_result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row + 100));
        }

        std::pmr::vector<uint64_t> miss_filter_columns(&env.resource);
        miss_filter_columns.push_back(0);
        constant_filter_t miss_filter(components::expressions::compare_type::gte,
                                      logical_value_t{&env.resource, int64_t(5000)},
                                      std::move(miss_filter_columns));

        table_scan_state miss_state(&env.resource);
        loaded->initialize_scan(miss_state, projected_indices, &miss_filter);
        data_chunk_t miss_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(miss_result, miss_state);
        REQUIRE(miss_result.size() == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan preserves null validity") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1300;
    const auto table_path = test_db_path() + ".pax_nulls";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto left_value_fn = [](uint64_t row) -> std::optional<int64_t> {
        if (row < 128) {
            return (row % 5 == 0) ? std::optional<int64_t>{} : std::optional<int64_t>{static_cast<int64_t>(row)};
        }
        if (row < 256) {
            return std::nullopt;
        }
        if (row < 384) {
            return static_cast<int64_t>(row);
        }
        return (row % 11 == 0) ? std::optional<int64_t>{} : std::optional<int64_t>{static_cast<int64_t>(row)};
    };

    auto right_value_fn = [](uint64_t row) -> std::optional<uint32_t> {
        if (row < 128) {
            return static_cast<uint32_t>(row * 7);
        }
        if (row < 256) {
            return (row % 2 == 0) ? std::optional<uint32_t>{} : std::optional<uint32_t>{static_cast<uint32_t>(row * 7)};
        }
        if (row < 384) {
            return std::nullopt;
        }
        return static_cast<uint32_t>(row * 7);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_nulls");
        append_nullable_fixed_integer_pair(*table, &env.resource, NUM_ROWS, left_value_fn, right_value_fn);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                      helper_state.table_state,
                                                                      helper_chunk));
        REQUIRE(helper_chunk.size() == DEFAULT_VECTOR_CAPACITY);

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, nullptr);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < result.size(); i++) {
                const auto row = scanned + i;
                const auto expected_left = left_value_fn(row);
                const auto expected_right = right_value_fn(row);

                REQUIRE(result.data[0].is_null(i) == !expected_left.has_value());
                if (expected_left.has_value()) {
                    REQUIRE(result.data[0].value(i).value<int64_t>() == *expected_left);
                }

                REQUIRE(result.data[1].is_null(i) == !expected_right.has_value());
                if (expected_right.has_value()) {
                    REQUIRE(result.data[1].value(i).value<uint32_t>() == *expected_right);
                }
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);

        std::pmr::vector<uint64_t> match_columns(&env.resource);
        match_columns.push_back(0);
        constant_filter_t match_filter(components::expressions::compare_type::eq,
                                       logical_value_t{&env.resource, int64_t(400)},
                                       std::move(match_columns));
        table_scan_state match_state(&env.resource);
        loaded->initialize_scan(match_state, projected_indices, &match_filter);
        data_chunk_t match_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(match_result, match_state);
        REQUIRE(match_result.size() == 1);
        REQUIRE_FALSE(match_result.data[0].is_null(0));
        REQUIRE(match_result.data[0].value(0).value<int64_t>() == 400);
        REQUIRE_FALSE(match_result.data[1].is_null(0));
        REQUIRE(match_result.data[1].value(0).value<uint32_t>() == 2800);

        std::pmr::vector<uint64_t> null_columns(&env.resource);
        null_columns.push_back(0);
        constant_filter_t null_filter(components::expressions::compare_type::eq,
                                      logical_value_t{&env.resource, int64_t(130)},
                                      std::move(null_columns));
        table_scan_state null_state(&env.resource);
        loaded->initialize_scan(null_state, projected_indices, &null_filter);
        data_chunk_t null_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(null_result, null_state);
        REQUIRE(null_result.size() == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan falls back for unsupported cases") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    const auto table_path = test_db_path() + ".pax_fallback";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fallback");

        auto types = table->copy_types();
        for (uint64_t offset = 0; offset < 64; offset += DEFAULT_VECTOR_CAPACITY) {
            const auto batch = std::min<uint64_t>(64 - offset, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const auto row = offset + i;
                chunk.set_value(0, i, logical_value_t{&env.resource, static_cast<int64_t>(row)});
                chunk.set_value(1, i, logical_value_t{&env.resource, static_cast<uint32_t>(row * 5)});
                chunk.set_value(2, i, logical_value_t{&env.resource, std::string("name_") + std::to_string(row)});
            }
            table_append_state state(&env.resource);
            table->append_lock(state);
            table->initialize_append(state);
            table->append(chunk, state);
            table->finalize_append(state, transaction_data{0, 0});
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
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        {
            std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(2)};
            std::vector<size_t> projected_cols{0, 2};
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, nullptr);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE_FALSE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                                state.table_state,
                                                                                result));
        }

        {
            std::vector<storage_index_t> projected_indices{storage_index_t(0)};
            std::vector<size_t> projected_cols{0};
            std::pmr::vector<uint64_t> filter_columns(&env.resource);
            filter_columns.push_back(1);
            constant_filter_t filter(components::expressions::compare_type::gt,
                                     logical_value_t{&env.resource, uint32_t(100)},
                                     std::move(filter_columns));
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, &filter);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE_FALSE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                                state.table_state,
                                                                                result));
        }
    }

    std::remove(table_path.c_str());
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
