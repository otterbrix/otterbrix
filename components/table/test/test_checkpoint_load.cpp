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

    struct persisted_column_definition_t {
        std::string name;
        components::types::logical_type type_id{components::types::logical_type::INVALID};
        bool not_null{false};
    };

    struct persisted_table_metadata_t {
        std::string name;
        bool has_column_type_metadata{false};
        std::vector<persisted_column_definition_t> columns;
        std::vector<std::string> column_type_payloads;
        bool has_layout_metadata{false};
        std::vector<components::table::storage::row_group_pointer_t> row_groups;
    };

    persisted_table_metadata_t
    read_persisted_table_metadata(components::table::storage::metadata_manager_t& meta_mgr,
                                  components::table::storage::meta_block_pointer_t table_pointer) {
        using namespace components::table::storage;

        metadata_reader_t reader(meta_mgr, table_pointer);
        persisted_table_metadata_t result;
        result.name = reader.read_string();

        const auto column_count_value = reader.read<uint32_t>();
        result.has_column_type_metadata = (column_count_value & TABLE_COLUMN_TYPES_METADATA_FLAG) != 0;
        const auto column_count = column_count_value & ~TABLE_COLUMN_TYPES_METADATA_FLAG;
        result.columns.reserve(column_count);
        for (uint32_t i = 0; i < column_count; i++) {
            persisted_column_definition_t column;
            column.name = reader.read_string();
            column.type_id = static_cast<components::types::logical_type>(reader.read<uint8_t>());
            column.not_null = reader.read<uint8_t>() != 0;
            result.columns.push_back(std::move(column));
        }

        if (result.has_column_type_metadata) {
            if (reader.read<uint32_t>() != COLUMN_TYPES_METADATA_MAGIC) {
                throw std::logic_error("unknown table column types metadata extension section");
            }
            const auto payload_count = reader.read<uint32_t>();
            if (payload_count != result.columns.size()) {
                throw std::logic_error("table column types metadata count mismatch");
            }
            result.column_type_payloads.reserve(payload_count);
            for (uint32_t i = 0; i < payload_count; i++) {
                result.column_type_payloads.push_back(reader.read_string());
            }
        }

        const auto row_group_count_value = reader.read<uint32_t>();
        result.has_layout_metadata = (row_group_count_value & TABLE_LAYOUT_METADATA_FLAG) != 0;
        const auto row_group_count = row_group_count_value & ~TABLE_LAYOUT_METADATA_FLAG;
        result.row_groups.reserve(row_group_count);
        for (uint32_t i = 0; i < row_group_count; i++) {
            result.row_groups.push_back(row_group_pointer_t::deserialize(reader));
        }

        if (result.has_layout_metadata) {
            if (reader.read<uint32_t>() != ROW_GROUP_LAYOUTS_MAGIC) {
                throw std::logic_error("unknown table metadata extension section");
            }
            const auto layout_count = reader.read<uint32_t>();
            if (layout_count != result.row_groups.size()) {
                throw std::logic_error("row group layout metadata count mismatch");
            }
            for (uint32_t i = 0; i < layout_count; i++) {
                auto layout_kind = static_cast<row_group_layout_kind>(reader.read<uint8_t>());
                result.row_groups[i].layout_kind = layout_kind;
                result.row_groups[i].pax_fixed_layout.reset();
                result.row_groups[i].pax_generic_layout.reset();
                if (layout_kind == row_group_layout_kind::PAX_FIXED) {
                    result.row_groups[i].pax_fixed_layout = pax_fixed_row_group_layout_t::deserialize(reader);
                } else if (layout_kind == row_group_layout_kind::PAX_GENERIC) {
                    result.row_groups[i].pax_generic_layout = pax_generic_row_group_layout_t::deserialize(reader);
                }
            }
        }

        return result;
    }

    components::table::storage::meta_block_pointer_t
    write_persisted_table_metadata(components::table::storage::metadata_manager_t& meta_mgr,
                                   const persisted_table_metadata_t& metadata) {
        using namespace components::table::storage;

        metadata_writer_t writer(meta_mgr);
        writer.write_string(metadata.name);

        auto column_count = static_cast<uint32_t>(metadata.columns.size());
        if (metadata.has_column_type_metadata) {
            column_count |= TABLE_COLUMN_TYPES_METADATA_FLAG;
        }
        writer.write<uint32_t>(column_count);
        for (const auto& column : metadata.columns) {
            writer.write_string(column.name);
            writer.write<uint8_t>(static_cast<uint8_t>(column.type_id));
            writer.write<uint8_t>(column.not_null ? 1 : 0);
        }

        if (metadata.has_column_type_metadata) {
            if (metadata.column_type_payloads.size() != metadata.columns.size()) {
                throw std::logic_error("column type payload count mismatch");
            }
            writer.write<uint32_t>(COLUMN_TYPES_METADATA_MAGIC);
            writer.write<uint32_t>(static_cast<uint32_t>(metadata.column_type_payloads.size()));
            for (const auto& payload : metadata.column_type_payloads) {
                writer.write_string(payload);
            }
        }

        auto row_group_count = static_cast<uint32_t>(metadata.row_groups.size());
        if (metadata.has_layout_metadata) {
            row_group_count |= TABLE_LAYOUT_METADATA_FLAG;
        }
        writer.write<uint32_t>(row_group_count);
        for (const auto& row_group : metadata.row_groups) {
            row_group.serialize(writer);
        }

        if (metadata.has_layout_metadata) {
            writer.write<uint32_t>(ROW_GROUP_LAYOUTS_MAGIC);
            writer.write<uint32_t>(static_cast<uint32_t>(metadata.row_groups.size()));
            for (const auto& row_group : metadata.row_groups) {
                writer.write<uint8_t>(static_cast<uint8_t>(row_group.layout_kind));
                if (row_group.layout_kind == row_group_layout_kind::PAX_FIXED) {
                    if (!row_group.pax_fixed_layout.has_value()) {
                        throw std::logic_error("missing pax_fixed layout metadata");
                    }
                    row_group.pax_fixed_layout->serialize(writer);
                } else if (row_group.layout_kind == row_group_layout_kind::PAX_GENERIC) {
                    if (!row_group.pax_generic_layout.has_value()) {
                        throw std::logic_error("missing pax_generic layout metadata");
                    }
                    row_group.pax_generic_layout->serialize(writer);
                }
            }
        }

        auto pointer = writer.get_block_pointer();
        writer.flush();
        return pointer;
    }

    components::table::storage::meta_block_pointer_t
    rewrite_pax_fixed_layout_version(components::table::storage::metadata_manager_t& meta_mgr,
                                     components::table::storage::meta_block_pointer_t table_pointer,
                                     uint16_t target_version) {
        using namespace components::table::storage;

        if (target_version != 1 && target_version != 2) {
            throw std::logic_error("unsupported legacy pax_fixed version rewrite target");
        }

        auto metadata = read_persisted_table_metadata(meta_mgr, table_pointer);
        bool found_pax_fixed = false;
        for (auto& row_group : metadata.row_groups) {
            if (row_group.layout_kind != row_group_layout_kind::PAX_FIXED) {
                continue;
            }
            if (!row_group.pax_fixed_layout.has_value()) {
                throw std::logic_error("missing pax_fixed layout during legacy rewrite");
            }
            found_pax_fixed = true;
            row_group.pax_fixed_layout->version = target_version;
            if (target_version == 1) {
                for (auto& page : row_group.pax_fixed_layout->pages) {
                    for (auto& slice : page.slices) {
                        if (slice.validity_kind != pax_fixed_validity_kind::ALL_VALID) {
                            throw std::logic_error("pax_fixed v1 rewrite requires all-valid slices");
                        }
                        slice.validity_data_pointer.reset();
                    }
                }
            }
        }
        if (!found_pax_fixed) {
            throw std::logic_error("expected pax_fixed layout metadata");
        }
        metadata.has_layout_metadata = true;
        return write_persisted_table_metadata(meta_mgr, metadata);
    }

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

    template<typename FillRowFn>
    void append_rows(components::table::data_table_t& table,
                     std::pmr::memory_resource* resource,
                     uint64_t count,
                     FillRowFn&& fill_row) {
        using namespace components::table;
        using namespace components::vector;

        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            const auto batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                fill_row(chunk, offset + i, i);
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    components::types::logical_value_t make_enum_entry(std::pmr::memory_resource* resource,
                                                       int32_t value,
                                                       std::string label) {
        auto entry = components::types::logical_value_t{resource, value};
        entry.set_alias(label);
        return entry;
    }

    components::types::complex_logical_type make_status_enum_type(std::pmr::memory_resource* resource) {
        using namespace components::types;

        std::vector<logical_value_t> entries;
        entries.emplace_back(make_enum_entry(resource, 0, "pending"));
        entries.emplace_back(make_enum_entry(resource, 1, "ready"));
        entries.emplace_back(make_enum_entry(resource, 2, "done"));
        return complex_logical_type::create_enum("status_enum", std::move(entries), "status_enum");
    }

    components::types::complex_logical_type
    make_extended_struct_type(std::pmr::memory_resource* resource) {
        using namespace components::types;

        auto enum_type = make_status_enum_type(resource);
        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::HUGEINT, "amount");
        fields.emplace_back(logical_type::DOUBLE, "ratio");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        fields.emplace_back(logical_type::TIMESTAMP_MS, "event_ts");
        fields.emplace_back(complex_logical_type::create_decimal(20, 4, "price"));
        fields.emplace_back(enum_type);
        fields.back().set_alias("status");
        return complex_logical_type::create_struct("extended", fields, "extended_struct");
    }

    components::types::complex_logical_type make_pax_union_type() {
        using namespace components::types;

        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::HUGEINT, "amount");
        fields.emplace_back(logical_type::STRING_LITERAL, "label");
        return complex_logical_type::create_union(fields, "pax_union");
    }

    components::types::int128_t signed_huge_value(uint64_t row) {
        using components::types::int128_t;

        const auto base = (int128_t{1} << 90) + static_cast<int64_t>(row * 97);
        return (row % 2 == 0) ? base : -base;
    }

    components::types::uint128_t unsigned_huge_value(uint64_t row) {
        using components::types::uint128_t;

        return (uint128_t{1} << 100) + static_cast<uint64_t>(row * 131 + 17);
    }

    components::types::int128_t uuid_like_value(uint64_t row) {
        using components::types::int128_t;

        return (int128_t{0x1234} << 96) + (int128_t{0x5678} << 64) + static_cast<int64_t>(row * 211 + 5);
    }

    components::types::complex_logical_type make_list_struct_type() {
        using namespace components::types;

        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BIGINT, "id");
        fields.emplace_back(logical_type::STRING_LITERAL, "label");
        return complex_logical_type::create_struct("list_entry", fields, "list_entry_struct");
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
        bm.file_sync();
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

TEST_CASE("checkpoint_load: explicit columnar-only root support matrix excludes variant and unsupported roots") {
    using namespace components::types;

    REQUIRE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type(logical_type::BLOB)));
    REQUIRE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type(logical_type::INTERVAL)));
    REQUIRE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type(logical_type::MAP)));
    REQUIRE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type::create_variant("payload")));

    REQUIRE_FALSE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type(logical_type::STRING_LITERAL)));
    REQUIRE_FALSE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type::create_struct("payload", {complex_logical_type(logical_type::BIGINT, "id")}, "payload")));
    REQUIRE_FALSE(components::table::detail::is_explicit_pax_columnar_only_root_type(make_pax_union_type()));
    REQUIRE(to_physical_type(logical_type::BLOB) == physical_type::INVALID);
    REQUIRE(to_physical_type(logical_type::INTERVAL) == physical_type::INVALID);
    REQUIRE(to_physical_type(logical_type::MAP) == physical_type::INVALID);

    const auto variant_type = complex_logical_type::create_variant("payload");
    REQUIRE(variant_type.child_types().size() == 4);
    REQUIRE(variant_type.child_types()[0].type() == logical_type::LIST);
    REQUIRE(variant_type.child_types()[1].type() == logical_type::LIST);
    REQUIRE(variant_type.child_types()[2].type() == logical_type::LIST);
    REQUIRE(variant_type.child_types()[3].type() == logical_type::BLOB);
}

TEST_CASE("checkpoint_load: columnar-only layout policy disables pax routing") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.set_layout_policy(row_group_layout_policy::COLUMNAR_ONLY);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "columnar_only_layout");
        append_nullable_string_data(*table,
                                    &env.resource,
                                    150,
                                    [](uint64_t row) -> std::optional<std::string> { return padded_name(row); });

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();
        auto metadata = read_persisted_table_metadata(meta_mgr, table_pointer);

        database_header_t header;
        header.initialize();
        bm.write_header(header);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::COLUMNAR);
        REQUIRE_FALSE(metadata.row_groups[0].pax_fixed_layout.has_value());
        REQUIRE_FALSE(metadata.row_groups[0].pax_generic_layout.has_value());
        REQUIRE(metadata.row_groups[0].columnar_data_pointers.size() == 1);
        REQUIRE_FALSE(metadata.row_groups[0].columnar_data_pointers[0].empty());
    }

    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed v1 metadata reloads integer roots") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 260;
    const auto table_path = test_db_path() + ".pax_fixed_v1_compat";
    std::remove(table_path.c_str());

    meta_block_pointer_t table_pointer;
    meta_block_pointer_t legacy_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("score", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_v1");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  NUM_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row * 9 + 1); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 7 + 3); });

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
        legacy_pointer = rewrite_pax_fixed_layout_version(meta_mgr, table_pointer, 1);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        const auto metadata = read_persisted_table_metadata(meta_mgr, legacy_pointer);
        REQUIRE(metadata.has_layout_metadata);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::PAX_FIXED);
        REQUIRE(metadata.row_groups[0].pax_fixed_layout.has_value());
        REQUIRE(metadata.row_groups[0].pax_fixed_layout->version == 1);
        for (const auto& page : metadata.row_groups[0].pax_fixed_layout->pages) {
            for (const auto& slice : page.slices) {
                REQUIRE(slice.validity_kind == pax_fixed_validity_kind::ALL_VALID);
                REQUIRE_FALSE(slice.validity_data_pointer.has_value());
            }
        }

        metadata_reader_t reader(meta_mgr, legacy_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (auto& vector : chunk.data) {
                vector.flatten(chunk.size());
            }
            auto* ids = chunk.data[0].data<int64_t>();
            auto* scores = chunk.data[1].data<uint32_t>();
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                REQUIRE_FALSE(chunk.data[0].is_null(i));
                REQUIRE_FALSE(chunk.data[1].is_null(i));
                REQUIRE(ids[i] == static_cast<int64_t>(row * 9 + 1));
                REQUIRE(scores[i] == static_cast<uint32_t>(row * 7 + 3));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed v2 metadata restores nullable integer roots") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 260;
    const auto table_path = test_db_path() + ".pax_fixed_v2_compat";
    std::remove(table_path.c_str());

    meta_block_pointer_t table_pointer;
    meta_block_pointer_t legacy_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("score", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_v2");
        append_nullable_fixed_integer_pair(
            *table,
            &env.resource,
            NUM_ROWS,
            [](uint64_t row) -> std::optional<int64_t> {
                if (row % 9 == 0) {
                    return std::nullopt;
                }
                return static_cast<int64_t>(row * 11 + 5);
            },
            [](uint64_t row) -> std::optional<uint32_t> {
                if (row % 7 == 0) {
                    return std::nullopt;
                }
                return static_cast<uint32_t>(row * 13 + 2);
            });

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
        legacy_pointer = rewrite_pax_fixed_layout_version(meta_mgr, table_pointer, 2);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        const auto metadata = read_persisted_table_metadata(meta_mgr, legacy_pointer);
        REQUIRE(metadata.has_layout_metadata);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::PAX_FIXED);
        REQUIRE(metadata.row_groups[0].pax_fixed_layout.has_value());
        REQUIRE(metadata.row_groups[0].pax_fixed_layout->version == 2);
        bool saw_legacy_validity_payload = false;
        for (const auto& page : metadata.row_groups[0].pax_fixed_layout->pages) {
            for (const auto& slice : page.slices) {
                if (slice.validity_kind == pax_fixed_validity_kind::BITMASK) {
                    REQUIRE(slice.validity_data_pointer.has_value());
                    saw_legacy_validity_payload = true;
                }
            }
        }
        REQUIRE(saw_legacy_validity_payload);

        metadata_reader_t reader(meta_mgr, legacy_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (auto& vector : chunk.data) {
                vector.flatten(chunk.size());
            }
            auto* ids = chunk.data[0].data<int64_t>();
            auto* scores = chunk.data[1].data<uint32_t>();
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                const bool id_is_null = (row % 9 == 0);
                const bool score_is_null = (row % 7 == 0);
                REQUIRE(chunk.data[0].is_null(i) == id_is_null);
                REQUIRE(chunk.data[1].is_null(i) == score_is_null);
                if (!id_is_null) {
                    REQUIRE(ids[i] == static_cast<int64_t>(row * 11 + 5));
                }
                if (!score_is_null) {
                    REQUIRE(scores[i] == static_cast<uint32_t>(row * 13 + 2));
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: extended fixed-width scalar roots are written as pax fixed v3") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 260;
    const auto table_path = test_db_path() + ".pax_fixed_extended";
    std::remove(table_path.c_str());

    meta_block_pointer_t table_pointer;
    auto status_enum = make_status_enum_type(&env.resource);

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("flag", logical_type::BOOLEAN);
        columns.emplace_back("signed_big", logical_type::HUGEINT);
        columns.emplace_back("unsigned_big", logical_type::UHUGEINT);
        columns.emplace_back("score_f", logical_type::FLOAT);
        columns.emplace_back("score_d", logical_type::DOUBLE);
        columns.emplace_back("ts_sec", logical_type::TIMESTAMP_SEC);
        columns.emplace_back("ts_ms", logical_type::TIMESTAMP_MS);
        columns.emplace_back("ts_us", logical_type::TIMESTAMP_US);
        columns.emplace_back("ts_ns", logical_type::TIMESTAMP_NS);
        columns.emplace_back("small_dec", complex_logical_type::create_decimal(4, 1, "small_dec"));
        columns.emplace_back("medium_dec", complex_logical_type::create_decimal(9, 2, "medium_dec"));
        columns.emplace_back("large_dec", complex_logical_type::create_decimal(18, 3, "large_dec"));
        columns.emplace_back("huge_dec", complex_logical_type::create_decimal(30, 4, "huge_dec"));
        columns.emplace_back("uuid_col", logical_type::UUID);
        columns.emplace_back("status", status_enum);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_extended");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            const bool is_null = row % 17 == 0;
            chunk.data[0].data<bool>()[row_in_chunk] = (row % 2) == 0;
            chunk.data[1].data<int128_t>()[row_in_chunk] = signed_huge_value(row);
            chunk.data[2].data<uint128_t>()[row_in_chunk] = unsigned_huge_value(row);
            chunk.data[3].data<float>()[row_in_chunk] = static_cast<float>(0.5f + static_cast<float>(row) * 0.25f);
            chunk.data[4].data<double>()[row_in_chunk] = 1.25 + static_cast<double>(row) * 1.125;
            chunk.data[5].data<int64_t>()[row_in_chunk] = static_cast<int64_t>(1000 + row * 3);
            chunk.data[6].data<int64_t>()[row_in_chunk] = static_cast<int64_t>(100000 + row * 7);
            chunk.data[7].data<int64_t>()[row_in_chunk] = static_cast<int64_t>(10000000 + row * 11);
            chunk.data[8].data<int64_t>()[row_in_chunk] = static_cast<int64_t>(1000000000 + row * 13);
            chunk.data[9].data<int16_t>()[row_in_chunk] = static_cast<int16_t>(100 + row);
            chunk.data[10].data<int32_t>()[row_in_chunk] = static_cast<int32_t>(100000 + row * 17);
            chunk.data[11].data<int64_t>()[row_in_chunk] = static_cast<int64_t>(1000000000000LL + row * 19);
            chunk.data[12].data<int128_t>()[row_in_chunk] = (int128_t{1} << 85) + static_cast<int64_t>(row * 23);
            chunk.data[13].data<int128_t>()[row_in_chunk] = uuid_like_value(row);
            chunk.data[14].data<int32_t>()[row_in_chunk] = static_cast<int32_t>(row % 3);

            if (is_null) {
                for (auto& vector : chunk.data) {
                    vector.validity().set(row_in_chunk, false);
                }
            }
        });

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

        REQUIRE(reader.read_string() == "pax_fixed_extended");
        auto column_count = reader.read<uint32_t>();
        REQUIRE((column_count & TABLE_COLUMN_TYPES_METADATA_FLAG) != 0);
        REQUIRE((column_count & ~TABLE_COLUMN_TYPES_METADATA_FLAG) == 15);
        for (uint32_t i = 0; i < 15; i++) {
            (void) reader.read_string();
            (void) reader.read<uint8_t>();
            (void) reader.read<uint8_t>();
        }
        REQUIRE(reader.read<uint32_t>() == COLUMN_TYPES_METADATA_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 15);
        for (uint32_t i = 0; i < 15; i++) {
            (void) reader.read_string();
        }

        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & TABLE_LAYOUT_METADATA_FLAG) != 0);
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);

        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.columnar_data_pointers.size() == 15);
        for (const auto& pointers : row_group.columnar_data_pointers) {
            REQUIRE(pointers.size() == 3);
        }

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        REQUIRE(static_cast<row_group_layout_kind>(reader.read<uint8_t>()) == row_group_layout_kind::PAX_FIXED);
        auto pax_layout = pax_fixed_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.version == 3);
        REQUIRE(pax_layout.rows_per_page == 128);
        REQUIRE(pax_layout.pages.size() == 3);

        const auto require_slice_type = [&](uint32_t column_index, pax_fixed_column_type expected_type) {
            for (const auto& page : pax_layout.pages) {
                const auto it =
                    std::find_if(page.slices.begin(), page.slices.end(), [&](const pax_fixed_slice_t& slice) {
                        return slice.column_index == column_index;
                    });
                REQUIRE(it != page.slices.end());
                REQUIRE(it->column_type == expected_type);
            }
        };

        require_slice_type(0, pax_fixed_column_type::BOOL);
        require_slice_type(1, pax_fixed_column_type::INT128);
        require_slice_type(2, pax_fixed_column_type::UINT128);
        require_slice_type(3, pax_fixed_column_type::FLOAT);
        require_slice_type(4, pax_fixed_column_type::DOUBLE);
        require_slice_type(5, pax_fixed_column_type::INT64);
        require_slice_type(6, pax_fixed_column_type::INT64);
        require_slice_type(7, pax_fixed_column_type::INT64);
        require_slice_type(8, pax_fixed_column_type::INT64);
        require_slice_type(9, pax_fixed_column_type::INT16);
        require_slice_type(10, pax_fixed_column_type::INT32);
        require_slice_type(11, pax_fixed_column_type::INT64);
        require_slice_type(12, pax_fixed_column_type::INT128);
        require_slice_type(13, pax_fixed_column_type::INT128);
        require_slice_type(14, pax_fixed_column_type::INT32);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (auto& vector : chunk.data) {
                vector.flatten(chunk.size());
            }

            auto* flag = chunk.data[0].data<bool>();
            auto* signed_big = chunk.data[1].data<int128_t>();
            auto* unsigned_big = chunk.data[2].data<uint128_t>();
            auto* score_f = chunk.data[3].data<float>();
            auto* score_d = chunk.data[4].data<double>();
            auto* ts_sec = chunk.data[5].data<int64_t>();
            auto* ts_ms = chunk.data[6].data<int64_t>();
            auto* ts_us = chunk.data[7].data<int64_t>();
            auto* ts_ns = chunk.data[8].data<int64_t>();
            auto* small_dec = chunk.data[9].data<int16_t>();
            auto* medium_dec = chunk.data[10].data<int32_t>();
            auto* large_dec = chunk.data[11].data<int64_t>();
            auto* huge_dec = chunk.data[12].data<int128_t>();
            auto* uuid_col = chunk.data[13].data<int128_t>();
            auto* status = chunk.data[14].data<int32_t>();

            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                const bool is_null = row % 17 == 0;
                for (const auto& vector : chunk.data) {
                    REQUIRE(vector.is_null(i) == is_null);
                }
                if (is_null) {
                    continue;
                }

                REQUIRE(flag[i] == ((row % 2) == 0));
                REQUIRE(signed_big[i] == signed_huge_value(row));
                REQUIRE(unsigned_big[i] == unsigned_huge_value(row));
                REQUIRE(score_f[i] == Approx(static_cast<float>(0.5f + static_cast<float>(row) * 0.25f)));
                REQUIRE(score_d[i] == Approx(1.25 + static_cast<double>(row) * 1.125));
                REQUIRE(ts_sec[i] == static_cast<int64_t>(1000 + row * 3));
                REQUIRE(ts_ms[i] == static_cast<int64_t>(100000 + row * 7));
                REQUIRE(ts_us[i] == static_cast<int64_t>(10000000 + row * 11));
                REQUIRE(ts_ns[i] == static_cast<int64_t>(1000000000 + row * 13));
                REQUIRE(small_dec[i] == static_cast<int16_t>(100 + row));
                REQUIRE(medium_dec[i] == static_cast<int32_t>(100000 + row * 17));
                REQUIRE(large_dec[i] == static_cast<int64_t>(1000000000000LL + row * 19));
                REQUIRE(huge_dec[i] == ((int128_t{1} << 85) + static_cast<int64_t>(row * 23)));
                REQUIRE(uuid_col[i] == uuid_like_value(row));
                REQUIRE(status[i] == static_cast<int32_t>(row % 3));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
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

TEST_CASE("checkpoint_load: pax generic struct column restores extended fixed children") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 150;
    const auto table_path = test_db_path() + ".pax_generic_extended_struct";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    auto struct_type = make_extended_struct_type(&env.resource);
    const auto& decimal_type = struct_type.child_types()[5];
    const auto& enum_type = struct_type.child_types()[6];

    auto value_fn = [&](uint64_t row) {
        if (row % 11 == 0) {
            return logical_value_t{&env.resource, nullptr};
        }

        std::vector<logical_value_t> fields;
        fields.emplace_back(&env.resource, row % 2 == 0);
        fields.emplace_back(&env.resource, signed_huge_value(row));
        fields.emplace_back(&env.resource, 2.5 + static_cast<double>(row) * 0.75);
        if (row % 5 == 0) {
            fields.emplace_back(&env.resource, nullptr);
        } else {
            fields.emplace_back(&env.resource, padded_name(row));
        }
        fields.emplace_back(&env.resource, std::chrono::milliseconds{static_cast<int64_t>(5000 + row * 9)});
        fields.emplace_back(logical_value_t::create_decimal(&env.resource,
                                                            decimal_type,
                                                            (int128_t{1} << 80) + static_cast<int64_t>(row * 29)));
        fields.emplace_back(logical_value_t::create_enum(&env.resource, enum_type, static_cast<int32_t>(row % 3)));
        return logical_value_t::create_struct(&env.resource, struct_type, fields);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("payload", struct_type);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_extended_struct");
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
        (void) reader.read_string();
        auto column_count = reader.read<uint32_t>();
        REQUIRE((column_count & TABLE_COLUMN_TYPES_METADATA_FLAG) != 0);
        REQUIRE((column_count & ~TABLE_COLUMN_TYPES_METADATA_FLAG) == 1);
        (void) reader.read_string();
        (void) reader.read<uint8_t>();
        (void) reader.read<uint8_t>();
        REQUIRE(reader.read<uint32_t>() == COLUMN_TYPES_METADATA_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        (void) reader.read_string();
        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & TABLE_LAYOUT_METADATA_FLAG) != 0);
        REQUIRE((row_group_count & ~TABLE_LAYOUT_METADATA_FLAG) == 1);
        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.columnar_data_pointers.size() == 1);
        REQUIRE(row_group.columnar_data_pointers[0].empty());

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        REQUIRE(static_cast<row_group_layout_kind>(reader.read<uint8_t>()) == row_group_layout_kind::PAX_GENERIC);
        auto pax_layout = pax_generic_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.version == 2);

        bool found_flag = false;
        bool found_huge = false;
        bool found_ratio = false;
        bool found_name = false;
        bool found_timestamp = false;
        bool found_decimal = false;
        bool found_enum = false;
        for (const auto& page : pax_layout.pages) {
            for (const auto& slice : page.slices) {
                if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                    slice.field_path == std::vector<uint16_t>{0} &&
                    slice.fixed_logical_type == logical_type::BOOLEAN) {
                    found_flag = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{1} &&
                           slice.fixed_logical_type == logical_type::HUGEINT) {
                    found_huge = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{2} &&
                           slice.fixed_logical_type == logical_type::DOUBLE) {
                    found_ratio = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::STRING_VALUES &&
                           slice.field_path == std::vector<uint16_t>{3}) {
                    found_name = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{4} &&
                           slice.fixed_logical_type == logical_type::TIMESTAMP_MS) {
                    found_timestamp = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{5} &&
                           slice.fixed_logical_type == logical_type::DECIMAL) {
                    found_decimal = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{6} &&
                           slice.fixed_logical_type == logical_type::ENUM) {
                    found_enum = true;
                }
            }
        }

        REQUIRE(found_flag);
        REQUIRE(found_huge);
        REQUIRE(found_ratio);
        REQUIRE(found_name);
        REQUIRE(found_timestamp);
        REQUIRE(found_decimal);
        REQUIRE(found_enum);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            auto& payload = chunk.data[0];
            auto& children = payload.entries();
            for (auto& child : children) {
                child->flatten(chunk.size());
            }

            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                auto expected = value_fn(row);
                REQUIRE(payload.is_null(i) == expected.is_null());
                if (expected.is_null()) {
                    continue;
                }

                REQUIRE(children[0]->data<bool>()[i] == expected.children()[0].value<bool>());
                REQUIRE(children[1]->data<int128_t>()[i] == expected.children()[1].value<int128_t>());
                REQUIRE(children[2]->data<double>()[i] == Approx(expected.children()[2].value<double>()));
                REQUIRE(children[3]->is_null(i) == expected.children()[3].is_null());
                if (!expected.children()[3].is_null()) {
                    REQUIRE(children[3]->data<std::string_view>()[i] == expected.children()[3].value<std::string_view>());
                }
                REQUIRE(children[4]->data<int64_t>()[i] == expected.children()[4].value<std::chrono::milliseconds>().count());
                REQUIRE(children[5]->data<int128_t>()[i] == expected.children()[5].value<int128_t>());
                REQUIRE(children[6]->data<int32_t>()[i] == expected.children()[6].value<int32_t>());
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic union column restores active member") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 170;
    const auto table_path = test_db_path() + ".pax_generic_union";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    auto union_type = make_pax_union_type();
    auto union_members = union_type.child_types();
    union_members.erase(union_members.begin());

    auto value_fn = [&](uint64_t row) {
        if (row % 8 == 0) {
            return logical_value_t{&env.resource, nullptr};
        }

        const auto tag = static_cast<uint8_t>(row % 3);
        switch (tag) {
            case 0:
                return logical_value_t::create_union(&env.resource,
                                                     union_members,
                                                     tag,
                                                     logical_value_t{&env.resource, row % 2 == 0});
            case 1:
                return logical_value_t::create_union(&env.resource,
                                                     union_members,
                                                     tag,
                                                     logical_value_t{&env.resource, signed_huge_value(row)});
            default:
                return logical_value_t::create_union(
                    &env.resource,
                    union_members,
                    tag,
                    logical_value_t{&env.resource,
                                    row % 5 == 0 ? std::string(5000 + row, static_cast<char>('a' + (row % 26)))
                                                 : padded_name(row)});
        }
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("payload", union_type);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_union");
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
        (void) reader.read_string();
        auto column_count = reader.read<uint32_t>();
        REQUIRE((column_count & TABLE_COLUMN_TYPES_METADATA_FLAG) != 0);
        REQUIRE((column_count & ~TABLE_COLUMN_TYPES_METADATA_FLAG) == 1);
        (void) reader.read_string();
        (void) reader.read<uint8_t>();
        (void) reader.read<uint8_t>();
        REQUIRE(reader.read<uint32_t>() == COLUMN_TYPES_METADATA_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        (void) reader.read_string();
        auto row_group_count = reader.read<uint32_t>();
        REQUIRE((row_group_count & TABLE_LAYOUT_METADATA_FLAG) != 0);
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
        bool found_tag = false;
        bool found_bool = false;
        bool found_huge = false;
        bool found_string = false;
        for (const auto& page : pax_layout.pages) {
            for (const auto& slice : page.slices) {
                if (slice.slice_kind == pax_generic_slice_kind::VALIDITY && slice.field_path.empty()) {
                    found_root_validity = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{0} &&
                           slice.fixed_logical_type == logical_type::UTINYINT) {
                    found_tag = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{1} &&
                           slice.fixed_logical_type == logical_type::BOOLEAN) {
                    found_bool = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                           slice.field_path == std::vector<uint16_t>{2} &&
                           slice.fixed_logical_type == logical_type::HUGEINT) {
                    found_huge = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::STRING_VALUES &&
                           slice.field_path == std::vector<uint16_t>{3}) {
                    found_string = true;
                }
            }
        }

        REQUIRE(found_root_validity);
        REQUIRE(found_tag);
        REQUIRE(found_bool);
        REQUIRE(found_huge);
        REQUIRE(found_string);
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

                const auto tag = expected.children()[0].value<uint8_t>();
                REQUIRE(actual.children()[0].value<uint8_t>() == tag);
                switch (tag) {
                    case 0:
                        REQUIRE(actual.children()[1].value<bool>() == expected.children()[1].value<bool>());
                        break;
                    case 1:
                        REQUIRE(actual.children()[2].value<int128_t>() == expected.children()[2].value<int128_t>());
                        break;
                    case 2:
                        REQUIRE(actual.children()[3].value<std::string_view>() ==
                                expected.children()[3].value<std::string_view>());
                        break;
                    default:
                        FAIL("unexpected union tag");
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic list column restores offsets and child strings") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 190;
    const auto table_path = test_db_path() + ".pax_generic_list_strings";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL, "tags");

    auto make_value = [&](uint64_t row) -> std::optional<std::vector<std::string>> {
        if (row % 13 == 0) {
            return std::nullopt;
        }
        std::vector<std::string> values;
        const auto count = row % 5 == 0 ? 0U : static_cast<uint32_t>(1 + (row % 4));
        values.reserve(count);
        for (uint32_t i = 0; i < count; i++) {
            if ((row + i) % 7 == 0) {
                values.emplace_back(std::string(5000 + row + i, static_cast<char>('a' + (row % 26))));
            } else {
                values.emplace_back(padded_name(row) + "_" + std::to_string(i));
            }
        }
        return values;
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("tags", list_type);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_list_strings");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            auto expected = make_value(row);
            if (!expected.has_value()) {
                chunk.data[0].set_null(row_in_chunk, true);
                return;
            }
            std::vector<logical_value_t> items;
            items.reserve(expected->size());
            for (const auto& value : *expected) {
                items.emplace_back(&env.resource, value);
            }
            chunk.set_value(0, row_in_chunk, logical_value_t::create_list(&env.resource, logical_type::STRING_LITERAL, items));
        });

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
        auto metadata = read_persisted_table_metadata(meta_mgr, table_pointer);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::PAX_GENERIC);
        REQUIRE(metadata.row_groups[0].pax_generic_layout.has_value());
        REQUIRE(metadata.row_groups[0].pax_generic_layout->version == 3);

        bool found_offsets = false;
        bool found_child_strings = false;
        for (const auto& page : metadata.row_groups[0].pax_generic_layout->pages) {
            for (const auto& slice : page.slices) {
                if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES && slice.field_path.empty() &&
                    slice.fixed_logical_type == logical_type::UBIGINT) {
                    found_offsets = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::STRING_VALUES &&
                           slice.field_path == std::vector<uint16_t>{0}) {
                    found_child_strings = true;
                }
            }
        }
        REQUIRE(found_offsets);
        REQUIRE(found_child_strings);
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
                const auto expected = make_value(row);
                REQUIRE(chunk.data[0].is_null(i) == !expected.has_value());
                if (!expected.has_value()) {
                    continue;
                }

                const auto actual = chunk.data[0].value(i);
                REQUIRE(actual.children().size() == expected->size());
                for (uint64_t child_index = 0; child_index < expected->size(); child_index++) {
                    REQUIRE(actual.children()[child_index].value<std::string_view>() == (*expected)[child_index]);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic array of struct restores nested child streams") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 96;
    constexpr uint64_t ARRAY_SIZE = 3;
    const auto table_path = test_db_path() + ".pax_generic_array_struct";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    const auto struct_type = make_list_struct_type();
    const auto array_type = complex_logical_type::create_array(struct_type, ARRAY_SIZE, "entries");

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("entries", array_type);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_array_struct");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            std::vector<logical_value_t> entries;
            entries.reserve(ARRAY_SIZE);
            for (uint64_t child_index = 0; child_index < ARRAY_SIZE; child_index++) {
                std::vector<logical_value_t> fields;
                fields.emplace_back(&env.resource, static_cast<int64_t>(row * 10 + child_index));
                fields.emplace_back(&env.resource, padded_name(row) + "_" + std::to_string(child_index));
                entries.emplace_back(logical_value_t::create_struct(&env.resource, struct_type, fields));
            }
            chunk.set_value(0, row_in_chunk, logical_value_t::create_array(&env.resource, struct_type, entries));
        });

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
        auto metadata = read_persisted_table_metadata(meta_mgr, table_pointer);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::PAX_GENERIC);
        REQUIRE(metadata.row_groups[0].pax_generic_layout.has_value());
        REQUIRE(metadata.row_groups[0].pax_generic_layout->version == 3);

        bool found_ids = false;
        bool found_labels = false;
        for (const auto& page : metadata.row_groups[0].pax_generic_layout->pages) {
            for (const auto& slice : page.slices) {
                if (slice.slice_kind == pax_generic_slice_kind::FIXED_VALUES &&
                    slice.field_path == std::vector<uint16_t>{0, 0} &&
                    slice.fixed_logical_type == logical_type::BIGINT) {
                    found_ids = true;
                } else if (slice.slice_kind == pax_generic_slice_kind::STRING_VALUES &&
                           slice.field_path == std::vector<uint16_t>{0, 1}) {
                    found_labels = true;
                }
            }
        }
        REQUIRE(found_ids);
        REQUIRE(found_labels);
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
                const auto actual = chunk.data[0].value(i);
                REQUIRE(actual.children().size() == ARRAY_SIZE);
                for (uint64_t child_index = 0; child_index < ARRAY_SIZE; child_index++) {
                    const auto& entry = actual.children()[child_index];
                    REQUIRE(entry.children()[0].value<int64_t>() == static_cast<int64_t>(row * 10 + child_index));
                    REQUIRE(entry.children()[1].value<std::string_view>() ==
                            padded_name(row) + "_" + std::to_string(child_index));
                }
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

TEST_CASE("checkpoint_load: pax generic projected scan supports non-projected filters and null predicates") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 220;
    const auto table_path = test_db_path() + ".pax_generic_filter_projection_split";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto category_fn = [](uint64_t row) -> std::optional<std::string> {
        if (row % 6 == 0) {
            return std::nullopt;
        }
        return (row % 2 == 0) ? std::optional<std::string>{"alpha"} : std::optional<std::string>{"beta"};
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        columns.emplace_back("category", logical_type::STRING_LITERAL);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_filter_projection_split");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, padded_name(row)});
            auto category = category_fn(row);
            if (!category.has_value()) {
                chunk.data[1].set_null(row_in_chunk, true);
            } else {
                chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, *category});
            }
        });

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

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};
        std::vector<uint64_t> alpha_rows;
        std::vector<uint64_t> null_rows;
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            auto category = category_fn(row);
            if (!category.has_value()) {
                null_rows.push_back(row);
            } else if (*category == "alpha") {
                alpha_rows.push_back(row);
            }
        }

        {
            std::pmr::vector<uint64_t> filter_columns(&env.resource);
            filter_columns.push_back(1);
            constant_filter_t filter(components::expressions::compare_type::eq,
                                     logical_value_t{&env.resource, std::string("alpha")},
                                     std::move(filter_columns));

            table_scan_state helper_state(&env.resource);
            loaded->initialize_scan(helper_state, projected_indices, &filter);
            data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                            helper_state.table_state,
                                                                            helper_chunk));

            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, &filter);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

            uint64_t scanned = 0;
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < result.size(); i++) {
                    REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(alpha_rows[scanned + i]));
                }
                scanned += result.size();
            }
            REQUIRE(scanned == alpha_rows.size());
        }

        {
            std::pmr::vector<uint64_t> filter_columns(&env.resource);
            filter_columns.push_back(1);
            is_null_filter_t filter(components::expressions::compare_type::is_null, std::move(filter_columns));

            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, &filter);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

            uint64_t scanned = 0;
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < result.size(); i++) {
                    REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(null_rows[scanned + i]));
                }
                scanned += result.size();
            }
            REQUIRE(scanned == null_rows.size());
        }
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

TEST_CASE("checkpoint_load: pax fixed projected scan supports extended projections and non-projected filters") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 320;
    const auto table_path = test_db_path() + ".pax_fixed_extended_projection";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("flag", logical_type::BOOLEAN);
        columns.emplace_back("score", logical_type::DOUBLE);
        columns.emplace_back("event_ts", logical_type::TIMESTAMP_MS);
        columns.emplace_back("uuid_col", logical_type::UUID);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_extended_projection");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, row % 2 == 0});
            chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, 10.0 + static_cast<double>(row) / 4.0});
            chunk.set_value(2, row_in_chunk,
                            logical_value_t{&env.resource, std::chrono::milliseconds(static_cast<int64_t>(1000 + row * 15))});
            chunk.data[3].data<int128_t>()[row_in_chunk] = uuid_like_value(row);
        });

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

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(2), storage_index_t(3)};
        std::vector<size_t> projected_cols{0, 2, 3};
        std::pmr::vector<uint64_t> filter_columns(&env.resource);
        filter_columns.push_back(1);
        constant_filter_t filter(components::expressions::compare_type::gt,
                                 logical_value_t{&env.resource, 60.0},
                                 std::move(filter_columns));

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, &filter);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                      helper_state.table_state,
                                                                      helper_chunk));

        std::vector<uint64_t> expected_rows;
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            if (10.0 + static_cast<double>(row) / 4.0 > 60.0) {
                expected_rows.push_back(row);
            }
        }

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, &filter);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++) {
                const auto row = expected_rows[scanned + i];
                REQUIRE(result.data[0].data<bool>()[i] == (row % 2 == 0));
                REQUIRE(result.data[2].data<int64_t>()[i] == static_cast<int64_t>(1000 + row * 15));
                REQUIRE(result.data[3].data<int128_t>()[i] == uuid_like_value(row));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan supports null predicates") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 260;
    const auto table_path = test_db_path() + ".pax_fixed_null_predicates";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("score", logical_type::DOUBLE);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_null_predicates");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row)});
            if (row % 7 == 0) {
                chunk.data[1].set_null(row_in_chunk, true);
            } else {
                chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, static_cast<double>(row) * 1.5});
            }
        });

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

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};
        std::pmr::vector<uint64_t> filter_columns(&env.resource);
        filter_columns.push_back(1);
        is_null_filter_t filter(components::expressions::compare_type::is_null, std::move(filter_columns));

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, &filter);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                      helper_state.table_state,
                                                                      helper_chunk));

        std::vector<uint64_t> expected_rows;
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            if (row % 7 == 0) {
                expected_rows.push_back(row);
            }
        }

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, &filter);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++) {
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(expected_rows[scanned + i]));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());
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
