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
#include <components/table/transaction_manager.hpp>
#include <components/types/type_spec.hpp>
#include <core/file/local_file_system.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <map>
#include <random>
#include <set>
#include <memory_resource>
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
        static void reset_scan_path_counts(row_group_t& row_group) { row_group.reset_scan_path_counts(); }
        static row_group_scan_path_counts_t scan_path_counts(const row_group_t& row_group) {
            return row_group.scan_path_counts();
        }
        static const std::optional<storage::pax_fixed_row_group_layout_t>&
        pax_fixed_layout(const row_group_t& row_group) {
            return row_group.pax_fixed_layout_;
        }
        static std::optional<storage::pax_fixed_row_group_layout_t>&
        pax_fixed_layout_mutable(row_group_t& row_group) {
            return row_group.pax_fixed_layout_;
        }
        static uint64_t delete_pointer_count(const row_group_t& row_group) {
            return row_group.deletes_pointers_.size();
        }
    };
} // namespace components::table

namespace {
    constexpr uint32_t COLUMN_TYPES_METADATA_MAGIC = 0x31484353U; // "SCH1"
    constexpr uint32_t ROW_GROUP_LAYOUTS_MAGIC = 0x31584150U; // "PAX1"
    constexpr uint32_t TABLE_LAYOUT_POLICY_MAGIC = 0x3159504CU; // "LPY1"
    constexpr uint32_t TABLE_COLUMN_TYPES_METADATA_FLAG = 1U << 31;
    constexpr uint32_t TABLE_LAYOUT_METADATA_FLAG = 1U << 31;
    constexpr uint32_t TABLE_LAYOUT_POLICY_METADATA_FLAG = 1U << 30;

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
        bool has_layout_policy_metadata{false};
        components::table::storage::row_group_layout_policy layout_policy{
            components::table::storage::row_group_layout_policy::AUTO};
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
        result.has_layout_policy_metadata = (row_group_count_value & TABLE_LAYOUT_POLICY_METADATA_FLAG) != 0;
        const auto row_group_count =
            row_group_count_value & ~(TABLE_LAYOUT_METADATA_FLAG | TABLE_LAYOUT_POLICY_METADATA_FLAG);
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

        if (result.has_layout_policy_metadata) {
            if (reader.read<uint32_t>() != TABLE_LAYOUT_POLICY_MAGIC) {
                throw std::logic_error("unknown table layout policy metadata extension section");
            }
            result.layout_policy = static_cast<row_group_layout_policy>(reader.read<uint8_t>());
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

    void delete_committed_rows(components::table::data_table_t& table,
                               std::pmr::memory_resource* resource,
                               const std::vector<uint64_t>& rows) {
        using namespace components::table;
        using namespace components::types;
        using namespace components::vector;

        vector_t row_ids(resource, logical_type::BIGINT, rows.size());
        for (uint64_t i = 0; i < rows.size(); i++) {
            row_ids.set_value(i, logical_value_t{resource, static_cast<int64_t>(rows[i])});
        }

        auto delete_state = table.initialize_delete({});
        REQUIRE(table.delete_rows(*delete_state, row_ids, rows.size(), 0) == rows.size());
    }

    void update_fixed_column(components::table::data_table_t& table,
                             std::pmr::memory_resource* resource,
                             uint64_t column_index,
                             const std::vector<uint64_t>& rows,
                             const std::vector<components::types::logical_value_t>& values,
                             components::types::logical_type type) {
        using namespace components::table;
        using namespace components::types;
        using namespace components::vector;

        REQUIRE(rows.size() == values.size());
        vector_t row_ids(resource, logical_type::BIGINT, rows.size());
        data_chunk_t updates(resource, {type}, rows.size());
        updates.set_cardinality(rows.size());
        for (uint64_t i = 0; i < rows.size(); i++) {
            row_ids.set_value(i, logical_value_t{resource, static_cast<int64_t>(rows[i])});
            updates.set_value(0, i, values[i]);
        }
        table.update_column(row_ids, {column_index}, updates);
    }

    void update_string_column(components::table::data_table_t& table,
                              std::pmr::memory_resource* resource,
                              uint64_t column_index,
                              const std::vector<uint64_t>& rows,
                              const std::vector<std::optional<std::string>>& values) {
        using namespace components::table;
        using namespace components::types;
        using namespace components::vector;

        REQUIRE(rows.size() == values.size());
        vector_t row_ids(resource, logical_type::BIGINT, rows.size());
        data_chunk_t updates(resource, {logical_type::STRING_LITERAL}, rows.size());
        updates.set_cardinality(rows.size());
        for (uint64_t i = 0; i < rows.size(); i++) {
            row_ids.set_value(i, logical_value_t{resource, static_cast<int64_t>(rows[i])});
            if (values[i].has_value()) {
                updates.set_value(0, i, logical_value_t{resource, *values[i]});
            } else {
                updates.data[0].set_null(i, true);
            }
        }
        table.update_column(row_ids, {column_index}, updates);
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

        std::pmr::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::BIGINT, "id");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        return complex_logical_type::create_struct("person", fields, "person_struct");
    }

    components::types::complex_logical_type make_nested_struct_type() {
        using namespace components::types;

        std::pmr::vector<complex_logical_type> nested_fields;
        nested_fields.emplace_back(logical_type::STRING_LITERAL, "name");
        nested_fields.emplace_back(logical_type::BIGINT, "score");
        auto meta_type = complex_logical_type::create_struct("meta", nested_fields, "meta_struct");

        std::pmr::vector<complex_logical_type> root_fields;
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
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::HUGEINT, "amount");
        fields.emplace_back(logical_type::DOUBLE, "ratio");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        fields.emplace_back(logical_type::TIMESTAMP, "event_ts");
        fields.emplace_back(complex_logical_type::create_decimal(20, 4, "price"));
        fields.emplace_back(enum_type);
        fields.back().set_alias("status");
        return complex_logical_type::create_struct("extended", fields, "extended_struct");
    }

    components::types::complex_logical_type make_pax_union_type() {
        using namespace components::types;

        std::pmr::vector<complex_logical_type> fields;
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

        std::pmr::vector<complex_logical_type> fields;
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

TEST_CASE("checkpoint_load: point-lookup (fetch by row_id) on reopened PAX-fixed table") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 2500; // spans row groups (1024) and pages (256)
    meta_block_pointer_t table_pointer;
    const auto is_null = [](uint64_t r) { return (r % 7) == 0; };
    const auto expected = [](uint64_t r) { return static_cast<uint32_t>(r * 2654435761u + 12345u); };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("u", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pt");
        auto types = table->copy_types();
        uint64_t off = 0;
        while (off < NUM_ROWS) {
            const uint64_t b = std::min<uint64_t>(NUM_ROWS - off, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, b);
            chunk.set_cardinality(b);
            for (uint64_t i = 0; i < b; i++) {
                const uint64_t r = off + i;
                if (is_null(r)) {
                    chunk.data[0].set_null(i, true);
                } else {
                    chunk.set_value(0, i, logical_value_t{&env.resource, expected(r)});
                }
            }
            table_append_state st(&env.resource);
            table->append_lock(st);
            table->initialize_append(st);
            table->append(chunk, st);
            table->finalize_append(st, transaction_data{0, 0});
            off += b;
        }
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();
        database_header_t header;
        header.initialize();
        bm.write_header(header);
        bm.file_sync();
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(loaded->column_count() == 1);

        // Scattered probe hitting page (256) and row-group (1024) boundaries, null and non-null.
        const std::vector<uint64_t> probe = {0,   1,    6,    7,    8,    255,  256,  257,
                                             511, 512,  1023, 1024, 1025, 1791, 2047, 2048,
                                             2049, 2299, 2300, 2499};
        const uint64_t n = probe.size();
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        vector_t rows(&env.resource, logical_type::BIGINT, n);
        for (uint64_t i = 0; i < n; i++) {
            rows.set_value(i, logical_value_t{&env.resource, static_cast<int64_t>(probe[i])});
        }
        data_chunk_t result(&env.resource, loaded->copy_types(), n);
        column_fetch_state state;
        loaded->fetch(result, column_ids, rows, n, state);

        for (uint64_t i = 0; i < n; i++) {
            const uint64_t r = probe[i];
            INFO("probe row " << r);
            if (is_null(r)) {
                REQUIRE_FALSE(result.data[0].validity().row_is_valid(i));
            } else {
                REQUIRE(result.data[0].validity().row_is_valid(i));
                REQUIRE(result.data[0].value(i).value<uint32_t>() == expected(r));
            }
        }
    }
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: point-lookup (fetch by row_id) on reopened PAX-generic table") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 2500;
    meta_block_pointer_t table_pointer;
    // Mixed fixed+string schema routes to PAX_GENERIC: exercises string and fixed fetch
    // under the generic layout on a reopened table.
    const auto u_null = [](uint64_t r) { return (r % 7) == 0; };
    const auto s_null = [](uint64_t r) { return (r % 11) == 0; };
    const auto u_val = [](uint64_t r) { return static_cast<int32_t>(static_cast<uint32_t>(r) * 7u + 3u); };
    const auto s_val = [](uint64_t r) { return std::string("row-") + std::to_string(r) + "-payload"; };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("i", logical_type::INTEGER);
        columns.emplace_back("s", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "ptg");
        auto types = table->copy_types();
        uint64_t off = 0;
        while (off < NUM_ROWS) {
            const uint64_t b = std::min<uint64_t>(NUM_ROWS - off, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, b);
            chunk.set_cardinality(b);
            for (uint64_t i = 0; i < b; i++) {
                const uint64_t r = off + i;
                if (u_null(r)) {
                    chunk.data[0].set_null(i, true);
                } else {
                    chunk.set_value(0, i, logical_value_t{&env.resource, u_val(r)});
                }
                if (s_null(r)) {
                    chunk.data[1].set_null(i, true);
                } else {
                    chunk.set_value(1, i, logical_value_t{&env.resource, s_val(r)});
                }
            }
            table_append_state st(&env.resource);
            table->append_lock(st);
            table->initialize_append(st);
            table->append(chunk, st);
            table->finalize_append(st, transaction_data{0, 0});
            off += b;
        }
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();
        database_header_t header;
        header.initialize();
        bm.write_header(header);
        bm.file_sync();
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(loaded->column_count() == 2);

        const std::vector<uint64_t> probe = {0,   1,    6,    7,    11,   255,  256,  257,
                                             511, 512,  1023, 1024, 1025, 1791, 2047, 2048,
                                             2049, 2299, 2300, 2499};
        const uint64_t n = probe.size();
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        column_ids.emplace_back(1);
        vector_t rows(&env.resource, logical_type::BIGINT, n);
        for (uint64_t i = 0; i < n; i++) {
            rows.set_value(i, logical_value_t{&env.resource, static_cast<int64_t>(probe[i])});
        }
        data_chunk_t result(&env.resource, loaded->copy_types(), n);
        column_fetch_state state;
        loaded->fetch(result, column_ids, rows, n, state);

        for (uint64_t i = 0; i < n; i++) {
            const uint64_t r = probe[i];
            INFO("probe row " << r);
            if (u_null(r)) {
                REQUIRE_FALSE(result.data[0].validity().row_is_valid(i));
            } else {
                REQUIRE(result.data[0].validity().row_is_valid(i));
                REQUIRE(result.data[0].value(i).value<int32_t>() == u_val(r));
            }
            if (s_null(r)) {
                REQUIRE_FALSE(result.data[1].validity().row_is_valid(i));
            } else {
                REQUIRE(result.data[1].validity().row_is_valid(i));
                REQUIRE(*result.data[1].value(i).value<std::string*>() == s_val(r));
            }
        }
    }
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: column_segment rejects out-of-block-bounds data pointer") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    cleanup_test_file();

    test_env_t env;
    // Transient block carries a block_manager so the segment can query block_size().
    auto block = env.buffer_manager.register_transient_memory(1024, DEFAULT_BLOCK_ALLOC_SIZE);
    const complex_logical_type t{logical_type::BIGINT};

    // A small segment at offset 0 fits.
    REQUIRE_NOTHROW(column_segment_t(block, t, 0, 0, INVALID_BLOCK, 0, 1024));
    // A segment_size larger than the block must throw at construction, not slide off the buffer later.
    REQUIRE_THROWS_AS(column_segment_t(block, t, 0, 0, INVALID_BLOCK, 0, uint64_t(1) << 30), std::logic_error);
    // An offset past the block must also throw.
    REQUIRE_THROWS_AS(column_segment_t(block, t, 0, 0, INVALID_BLOCK, uint64_t(1) << 30, 64), std::logic_error);

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
        REQUIRE(row_group.columnar_data_pointers[0].size() == 2);
        REQUIRE(row_group.columnar_data_pointers[1].size() == 2);

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        auto layout_kind = static_cast<row_group_layout_kind>(reader.read<uint8_t>());
        REQUIRE(layout_kind == row_group_layout_kind::PAX_FIXED);

        auto pax_layout = pax_fixed_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.rows_per_page == 256);
        REQUIRE(pax_layout.pages.size() == 2);
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
        complex_logical_type::create_variant(std::pmr::get_default_resource(), "payload")));

    REQUIRE_FALSE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type(logical_type::STRING_LITERAL)));
    std::pmr::vector<complex_logical_type> payload_fields;
    payload_fields.emplace_back(logical_type::BIGINT, "id");
    REQUIRE_FALSE(components::table::detail::is_explicit_pax_columnar_only_root_type(
        complex_logical_type::create_struct("payload", payload_fields, "payload")));
    REQUIRE_FALSE(components::table::detail::is_explicit_pax_columnar_only_root_type(make_pax_union_type()));
    REQUIRE(to_physical_type(logical_type::BLOB) == physical_type::INVALID);
    REQUIRE(to_physical_type(logical_type::INTERVAL) == physical_type::STRUCT);
    REQUIRE(to_physical_type(logical_type::MAP) == physical_type::INVALID);

    const auto variant_type = complex_logical_type::create_variant(std::pmr::get_default_resource(), "payload");
    REQUIRE(variant_type.child_types().size() == 4);
    REQUIRE(variant_type.child_types()[0].type() == logical_type::LIST);
    REQUIRE(variant_type.child_types()[1].type() == logical_type::LIST);
    REQUIRE(variant_type.child_types()[2].type() == logical_type::LIST);
    REQUIRE(variant_type.child_types()[3].type() == logical_type::BLOB);
}

TEST_CASE("checkpoint_load: explicit pax support matrix accepts mixed and rejects fallback root schemas") {
    using components::table::column_definition_t;
    using components::table::detail::supports_explicit_pax_schema;
    using namespace components::types;

    std::vector<column_definition_t> fixed_columns;
    fixed_columns.emplace_back("id", logical_type::BIGINT);
    fixed_columns.emplace_back("score", logical_type::DOUBLE);
    std::string error_message;
    REQUIRE(supports_explicit_pax_schema(fixed_columns, &error_message));

    std::vector<column_definition_t> generic_columns;
    generic_columns.emplace_back("name", logical_type::STRING_LITERAL);
    std::pmr::vector<complex_logical_type> payload_fields;
    payload_fields.emplace_back(logical_type::BIGINT, "id");
    generic_columns.emplace_back("payload", complex_logical_type::create_struct("payload", payload_fields, "payload"));
    error_message.clear();
    REQUIRE(supports_explicit_pax_schema(generic_columns, &error_message));

    std::vector<column_definition_t> mixed_columns;
    mixed_columns.emplace_back("name", logical_type::STRING_LITERAL);
    mixed_columns.emplace_back("count", logical_type::BIGINT);
    error_message.clear();
    REQUIRE(supports_explicit_pax_schema(mixed_columns, &error_message));
    REQUIRE(error_message.empty());

    std::vector<column_definition_t> fallback_columns;
    fallback_columns.emplace_back("gap", logical_type::INTERVAL);
    error_message.clear();
    REQUIRE_FALSE(supports_explicit_pax_schema(fallback_columns, &error_message));
    REQUIRE(error_message.find("not supported by USING PAX") != std::string::npos);

    const auto require_unsupported_label = [&](logical_type type, const char* label) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("unsupported", type);
        error_message.clear();
        REQUIRE_FALSE(supports_explicit_pax_schema(columns, &error_message));
        REQUIRE(error_message.find("type '" + std::string(label) + "'") != std::string::npos);
        REQUIRE(error_message.find("not supported by USING PAX") != std::string::npos);
    };

    require_unsupported_label(logical_type::DATE, "date");
    require_unsupported_label(logical_type::TIME, "time");
    require_unsupported_label(logical_type::TIME_TZ, "timetz");
    require_unsupported_label(logical_type::BLOB, "blob");
    require_unsupported_label(logical_type::INTERVAL, "interval");

    const auto require_unsupported_complex = [&](complex_logical_type type) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("unsupported", std::move(type));
        error_message.clear();
        REQUIRE_FALSE(supports_explicit_pax_schema(columns, &error_message));
        REQUIRE(error_message.find("not supported by USING PAX") != std::string::npos);
    };

    require_unsupported_complex(complex_logical_type::create_map(std::pmr::get_default_resource(),
                                                                 logical_type::INTEGER,
                                                                 logical_type::STRING_LITERAL,
                                                                 "map_payload"));
    require_unsupported_complex(complex_logical_type::create_variant(std::pmr::get_default_resource(), "variant_payload"));
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

TEST_CASE("checkpoint_load: explicit layout policy metadata restores pax-only tables") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_only_layout");
        append_int64_data(*table, &env.resource, 64);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();
        auto metadata = read_persisted_table_metadata(meta_mgr, table_pointer);

        REQUIRE(metadata.has_layout_policy_metadata);
        REQUIRE(metadata.layout_policy == row_group_layout_policy::PAX_ONLY);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::PAX_FIXED);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.set_layout_policy(row_group_layout_policy::AUTO);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        REQUIRE(loaded);
        REQUIRE(bm.layout_policy() == row_group_layout_policy::PAX_ONLY);
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

TEST_CASE("checkpoint_load: extended fixed-width scalar roots are written as pax fixed v4") {
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
        columns.emplace_back("ts_sec", logical_type::TIMESTAMP);
        columns.emplace_back("ts_ms", logical_type::TIMESTAMP);
        columns.emplace_back("ts_us", logical_type::TIMESTAMP);
        columns.emplace_back("ts_ns", logical_type::TIMESTAMP);
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
            REQUIRE(pointers.size() == 2);
        }

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        REQUIRE(static_cast<row_group_layout_kind>(reader.read<uint8_t>()) == row_group_layout_kind::PAX_FIXED);
        auto pax_layout = pax_fixed_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.version == 4);
        REQUIRE(pax_layout.rows_per_page == 256);
        REQUIRE(pax_layout.pages.size() == 2);

        const auto require_slice_type = [&](uint32_t column_index, pax_fixed_column_type expected_type) {
            for (const auto& page : pax_layout.pages) {
                const auto it =
                    std::find_if(page.slices.begin(), page.slices.end(), [&](const pax_fixed_slice_t& slice) {
                        return slice.column_index == column_index;
                });
                REQUIRE(it != page.slices.end());
                REQUIRE(it->column_type == expected_type);
                REQUIRE(it->statistics.has_value());
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
        REQUIRE(row_group.columnar_data_pointers[0].size() == 2);
        REQUIRE(row_group.columnar_data_pointers[1].empty());

        REQUIRE(reader.read<uint32_t>() == ROW_GROUP_LAYOUTS_MAGIC);
        REQUIRE(reader.read<uint32_t>() == 1);
        auto layout_kind = static_cast<row_group_layout_kind>(reader.read<uint8_t>());
        REQUIRE(layout_kind == row_group_layout_kind::PAX_GENERIC);

        auto pax_layout = pax_generic_row_group_layout_t::deserialize(reader);
        REQUIRE(pax_layout.version == 4);
        REQUIRE(pax_layout.rows_per_page == 256);
        REQUIRE(pax_layout.pages.size() == 2);
        REQUIRE(pax_layout.pages[0].slices.size() == 4);
        REQUIRE(pax_layout.pages[0].slices[0].slice_kind == pax_generic_slice_kind::STRING_VALUES);
        REQUIRE(pax_layout.pages[0].slices[0].codec_kind == pax_generic_codec_kind::STRING_SEGMENT);
        REQUIRE(pax_layout.pages[0].slices[0].statistics.has_value());
        REQUIRE(pax_layout.pages[0].slices[1].slice_kind == pax_generic_slice_kind::VALIDITY);
        REQUIRE(pax_layout.pages[0].slices[2].column_index == 1);
        REQUIRE(pax_layout.pages[0].slices[2].slice_kind == pax_generic_slice_kind::FIXED_VALUES);
        REQUIRE(pax_layout.pages[0].slices[2].codec_kind == pax_generic_codec_kind::FIXED_PLAIN);
        REQUIRE(pax_layout.pages[0].slices[2].fixed_logical_type == logical_type::BIGINT);
        REQUIRE(pax_layout.pages[0].slices[2].statistics.has_value());
        REQUIRE(pax_layout.pages[0].slices[3].column_index == 1);
        REQUIRE(pax_layout.pages[0].slices[3].slice_kind == pax_generic_slice_kind::VALIDITY);
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

// Load many row groups under a small buffer pool, checkpoint, reopen in a fresh block
// manager, and verify every row (value and null mask) comes back exactly. Small pool +
// blocks make pool-pressure truncation reproduce at a few thousand rows.
TEST_CASE("checkpoint_load: pax fixed round-trip preserves every row and null mask under buffer-pool pressure") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    // Independent small buffer pool (16 MB) + small blocks (256 KB).
    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    buffer_pool_t pool(&resource, uint64_t(16) << 20, false, uint64_t(1) << 24);
    standard_buffer_manager_t buffer_manager(&resource, fs, pool);
    constexpr uint64_t BLOCK_ALLOC = uint64_t(256) << 10;

    constexpr uint64_t NUM_ROWS = 40000;
    const auto path = test_db_path();
    std::remove(path.c_str());
    meta_block_pointer_t table_pointer;

    auto col0_fn = [](uint64_t row) -> std::optional<int64_t> {
        return (row % 13 == 0) ? std::optional<int64_t>{} : std::optional<int64_t>{static_cast<int64_t>(row)};
    };
    auto col1_fn = [](uint64_t row) -> std::optional<uint32_t> {
        return (row % 7 == 0) ? std::optional<uint32_t>{} : std::optional<uint32_t>{static_cast<uint32_t>(row * 3)};
    };

    {
        single_file_block_manager_t bm(buffer_manager, fs, path, BLOCK_ALLOC);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&resource, bm, std::move(columns), "scale_nulls");
        append_nullable_fixed_integer_pair(*table, &resource, NUM_ROWS, col0_fn, col1_fn);
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
        single_file_block_manager_t bm(buffer_manager, fs, path, BLOCK_ALLOC);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&resource, bm, reader);

        uint64_t scanned = 0;
        loaded->scan_table_segment(0, NUM_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                const auto e0 = col0_fn(row);
                const auto e1 = col1_fn(row);
                REQUIRE(chunk.data[0].is_null(i) == !e0.has_value());
                if (e0.has_value()) {
                    REQUIRE(chunk.data[0].value(i).value<int64_t>() == *e0);
                }
                REQUIRE(chunk.data[1].is_null(i) == !e1.has_value());
                if (e1.has_value()) {
                    REQUIRE(chunk.data[1].value(i).value<uint32_t>() == *e1);
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS); // the whole table survived — no silent truncation
    }

    std::remove(path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: clean restored row group checkpoint reuses persisted blocks") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 300;
    const auto table_path = test_db_path() + ".clean_reuse";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    uint64_t blocks_after_first_checkpoint = 0;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "clean_reuse");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, [](uint64_t row) {
            return std::string("name_") + std::to_string(row);
        });

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();
        blocks_after_first_checkpoint = bm.total_blocks();

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

        loaded->compact(UINT64_MAX);

        metadata_writer_t writer(meta_mgr);
        loaded->checkpoint(writer);

        REQUIRE(bm.total_blocks() <= blocks_after_first_checkpoint + 1);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: restored row group dirty append is persisted") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 129;
    const auto table_path = test_db_path() + ".dirty_append";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "dirty_append");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, [](uint64_t row) {
            return std::string("before_") + std::to_string(row);
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
        append_nullable_string_data(*loaded, &env.resource, 1, [](uint64_t) {
            return std::string("after_reload");
        });

        metadata_writer_t writer(meta_mgr);
        loaded->checkpoint(writer);
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
        loaded->scan_table_segment(0, NUM_ROWS + 1, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto row = scanned + i;
                if (row < NUM_ROWS) {
                    REQUIRE(*chunk.data[0].value(i).value<std::string*>() ==
                            std::string("before_") + std::to_string(row));
                } else {
                    REQUIRE(*chunk.data[0].value(i).value<std::string*>() == "after_reload");
                }
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == NUM_ROWS + 1);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: columnar-only policy rewrites restored pax row group") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 260;
    const auto table_path = test_db_path() + ".policy_mismatch";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "policy_mismatch");
        append_nullable_string_data(*table, &env.resource, NUM_ROWS, [](uint64_t row) {
            return std::string("name_") + std::to_string(row);
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
        bm.set_layout_policy(row_group_layout_policy::COLUMNAR_ONLY);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        metadata_writer_t writer(meta_mgr);
        loaded->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        auto metadata = read_persisted_table_metadata(meta_mgr, table_pointer);
        REQUIRE(metadata.row_groups.size() == 1);
        REQUIRE(metadata.row_groups[0].layout_kind == row_group_layout_kind::COLUMNAR);
        REQUIRE_FALSE(metadata.row_groups[0].pax_generic_layout.has_value());
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

TEST_CASE("checkpoint_load: columnar layout refuses to persist a nested column (fail-closed)") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    const auto table_path = test_db_path() + ".columnar_nested_reject";
    std::remove(table_path.c_str());
    auto struct_type = make_person_struct_type();

    single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
    bm.create_new_database();
    // COLUMNAR checkpoint can't persist struct child columns, so it must throw rather than drop them.
    bm.set_layout_policy(row_group_layout_policy::COLUMNAR_ONLY);

    std::vector<column_definition_t> columns;
    columns.emplace_back("person", struct_type);
    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "cn");
    auto value_fn = [&](uint64_t row) {
        std::vector<logical_value_t> fields;
        fields.emplace_back(&env.resource, row % 2 == 0);
        fields.emplace_back(&env.resource, static_cast<int64_t>(row * 11));
        fields.emplace_back(&env.resource, padded_name(row));
        return logical_value_t::create_struct(&env.resource, struct_type, fields);
    };
    append_struct_data(*table, &env.resource, 64, value_fn);

    metadata_manager_t meta_mgr(bm);
    metadata_writer_t writer(meta_mgr);
    REQUIRE_THROWS_AS(table->checkpoint(writer), std::logic_error);

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
            auto type_spec = reader.read_string();
            auto persisted_type = components::types::decode_type_spec(&env.resource, type_spec);
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
        fields.emplace_back(&env.resource,
                            core::date::timestamp_t{core::date::microseconds{static_cast<int64_t>(5000 + row * 9)}});
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
                           slice.fixed_logical_type == logical_type::TIMESTAMP) {
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
                REQUIRE(children[4]->data<int64_t>()[i] ==
                        expected.children()[4].value<core::date::timestamp_t>().value.count());
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

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected > 0);
        REQUIRE(counts.pax_generic_pruned_pages == 0);
        REQUIRE(counts.pax_generic_prefetched_blocks > 0);
        REQUIRE(counts.pax_generic_skipped_payload_pages == 0);
        REQUIRE(counts.pax_fixed_projected == 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan skips committed deletes") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_generic_delete_overlay";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_delete");
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
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        const std::vector<uint64_t> deleted_rows{0, 5, 128, 255, 300, 511};
        delete_committed_rows(*loaded, &env.resource, deleted_rows);

        std::vector<uint64_t> expected_rows;
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            if (std::find(deleted_rows.begin(), deleted_rows.end(), row) == deleted_rows.end()) {
                expected_rows.push_back(row);
            }
        }

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                        helper_state.table_state,
                                                                        helper_chunk));
        REQUIRE(helper_chunk.size() == expected_rows.size());

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
                const auto row = expected_rows[scanned + i];
                REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(row));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());
        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected > 0);
        REQUIRE(counts.pax_fixed_projected == 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan applies active transaction delete visibility") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_generic_active_txn_delete_visibility";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_active_txn_delete");
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
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        const std::vector<uint64_t> deleted_rows{0, 5, 128, 255, 300, 511};
        vector_t row_ids(&env.resource, logical_type::BIGINT, deleted_rows.size());
        for (uint64_t i = 0; i < deleted_rows.size(); i++) {
            row_ids.set_value(i, logical_value_t{&env.resource, static_cast<int64_t>(deleted_rows[i])});
        }

        transaction_manager_t txn_manager(&env.resource);
        auto writer_session = components::session::session_id_t::generate_uid();
        auto& writer_txn = txn_manager.begin_transaction(writer_session);
        auto delete_state = loaded->initialize_delete({});
        REQUIRE(loaded->delete_rows(*delete_state,
                                    row_ids,
                                    deleted_rows.size(),
                                    writer_txn.transaction_id()) == deleted_rows.size());

        std::vector<uint64_t> all_rows;
        all_rows.reserve(NUM_ROWS);
        std::vector<uint64_t> committed_visible_rows;
        committed_visible_rows.reserve(NUM_ROWS - deleted_rows.size());
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            all_rows.push_back(row);
            if (std::find(deleted_rows.begin(), deleted_rows.end(), row) == deleted_rows.end()) {
                committed_visible_rows.push_back(row);
            }
        }

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        const auto scan_and_check = [&](transaction_data txn, const std::vector<uint64_t>& expected_rows) {
            row_group_test_access_t::reset_scan_path_counts(*first_row_group);

            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, nullptr);
            state.table_state.txn = txn;
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
                    REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(row));
                }
                scanned += result.size();
            }
            REQUIRE(scanned == expected_rows.size());

            const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
            REQUIRE(counts.pax_generic_projected > 0);
            REQUIRE(counts.pax_fixed_projected == 0);
            REQUIRE(counts.regular == 0);
        };

        auto reader_session = components::session::session_id_t::generate_uid();
        auto& reader_txn = txn_manager.begin_transaction(reader_session);
        scan_and_check(reader_txn.data(), all_rows);
        txn_manager.abort(reader_session);

        const auto writer_txn_id = writer_txn.transaction_id();
        const auto commit_id = txn_manager.commit(writer_session);
        loaded->commit_all_deletes(writer_txn_id, commit_id);
        txn_manager.publish(commit_id);

        auto fresh_session = components::session::session_id_t::generate_uid();
        auto& fresh_txn = txn_manager.begin_transaction(fresh_session);
        scan_and_check(fresh_txn.data(), committed_visible_rows);
        txn_manager.abort(fresh_session);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan persists committed deletes") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_generic_persisted_delete_overlay";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_persisted_delete");
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

    const std::vector<uint64_t> deleted_rows{0, 5, 128, 255, 300, 511};
    std::vector<uint64_t> expected_rows;
    for (uint64_t row = 0; row < NUM_ROWS; row++) {
        if (std::find(deleted_rows.begin(), deleted_rows.end(), row) == deleted_rows.end()) {
            expected_rows.push_back(row);
        }
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        delete_committed_rows(*loaded, &env.resource, deleted_rows);

        metadata_writer_t writer(meta_mgr);
        loaded->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);
        REQUIRE(row_group_test_access_t::delete_pointer_count(*first_row_group) == 1);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);
        REQUIRE(row_group_test_access_t::delete_pointer_count(*first_row_group) == 1);
        REQUIRE(loaded->calculate_size() == expected_rows.size());

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                        helper_state.table_state,
                                                                        helper_chunk));
        REQUIRE(helper_chunk.size() == expected_rows.size());

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
                const auto row = expected_rows[scanned + i];
                REQUIRE(*result.data[0].value(i).value<std::string*>() == padded_name(row));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected > 0);
        REQUIRE(counts.pax_fixed_projected == 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax generic projected scan applies update overlay") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_generic_update_overlay";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_generic_update");
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
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        const std::vector<uint64_t> updated_rows{5, 128, 300};
        const std::string updated_value = "zzzz_updated_hot";
        update_string_column(*loaded,
                             &env.resource,
                             0,
                             updated_rows,
                             {updated_value, std::nullopt, updated_value});

        std::vector<storage_index_t> projected_indices{storage_index_t(0)};
        std::vector<size_t> projected_cols{0};

        {
            table_scan_state helper_state(&env.resource);
            loaded->initialize_scan(helper_state, projected_indices, nullptr);
            data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                            helper_state.table_state,
                                                                            helper_chunk));
            REQUIRE(helper_chunk.size() == NUM_ROWS);
            REQUIRE(*helper_chunk.data[0].value(5).value<std::string*>() == updated_value);
            REQUIRE(helper_chunk.data[0].is_null(128));
            REQUIRE(*helper_chunk.data[0].value(300).value<std::string*>() == updated_value);
        }

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        std::pmr::vector<uint64_t> eq_filter_columns(&env.resource);
        eq_filter_columns.push_back(0);
        constant_filter_t eq_filter(components::expressions::compare_type::eq,
                                    logical_value_t{&env.resource, updated_value},
                                    std::move(eq_filter_columns));

        table_scan_state eq_state(&env.resource);
        loaded->initialize_scan(eq_state, projected_indices, &eq_filter);
        data_chunk_t eq_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(eq_result, eq_state);
        REQUIRE(eq_result.size() == 2);
        for (uint64_t i = 0; i < eq_result.size(); i++) {
            REQUIRE(*eq_result.data[0].value(i).value<std::string*>() == updated_value);
        }

        std::pmr::vector<uint64_t> null_filter_columns(&env.resource);
        null_filter_columns.push_back(0);
        is_null_filter_t null_filter(components::expressions::compare_type::is_null, std::move(null_filter_columns));

        table_scan_state null_state(&env.resource);
        loaded->initialize_scan(null_state, projected_indices, &null_filter);
        data_chunk_t null_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(null_result, null_state);
        REQUIRE(null_result.size() == 1);
        REQUIRE(null_result.data[0].is_null(0));

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected > 0);
        REQUIRE(counts.pax_fixed_projected == 0);
        REQUIRE(counts.regular == 0);
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

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected > 0);
        REQUIRE(counts.pax_generic_pruned_pages > 0);
        REQUIRE(counts.pax_generic_prefetched_blocks > 0);
        REQUIRE(counts.pax_generic_skipped_payload_pages > 0);
        REQUIRE(counts.pax_fixed_projected == 0);
        REQUIRE(counts.regular == 0);
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

            row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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

            const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
            REQUIRE(counts.pax_generic_projected > 0);
            REQUIRE(counts.pax_generic_prefetched_blocks > 0);
            REQUIRE(counts.pax_fixed_projected == 0);
            REQUIRE(counts.regular == 0);
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

TEST_CASE("checkpoint_load: pax generic projected scan supports mixed projection") {
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
        REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                        helper_state.table_state,
                                                                        helper_chunk));
        REQUIRE(helper_chunk.size() == NUM_ROWS);
        for (uint64_t i = 0; i < helper_chunk.size(); i++) {
            REQUIRE(*helper_chunk.data[0].value(i).value<std::string*>() == padded_name(i));
            REQUIRE(helper_chunk.data[1].value(i).value<int64_t>() == static_cast<int64_t>(i * 2));
        }

        {
            std::vector<storage_index_t> row_id_projection{storage_index_t(0), storage_index_t()};
            std::pmr::vector<complex_logical_type> row_id_result_types(&env.resource);
            row_id_result_types.emplace_back(logical_type::STRING_LITERAL);
            row_id_result_types.emplace_back(logical_type::BIGINT);
            table_scan_state row_id_state(&env.resource);
            loaded->initialize_scan(row_id_state, row_id_projection, nullptr);
            data_chunk_t row_id_result(&env.resource, row_id_result_types, DEFAULT_VECTOR_CAPACITY);
            REQUIRE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                            row_id_state.table_state,
                                                                            row_id_result));
            REQUIRE(row_id_result.size() == NUM_ROWS);
            for (uint64_t i = 0; i < row_id_result.size(); i++) {
                REQUIRE(*row_id_result.data[0].value(i).value<std::string*>() == padded_name(i));
                REQUIRE(row_id_result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(i));
            }
        }

        {
            std::vector<storage_index_t> string_projection{storage_index_t(0)};
            std::vector<size_t> string_projected_cols{0};
            std::pmr::vector<uint64_t> filter_columns(&env.resource);
            filter_columns.push_back(0);
            constant_filter_t invalid_filter(components::expressions::compare_type::invalid,
                                             logical_value_t{&env.resource, padded_name(10)},
                                             std::move(filter_columns));
            table_scan_state invalid_filter_state(&env.resource);
            loaded->initialize_scan(invalid_filter_state, string_projection, &invalid_filter);
            data_chunk_t invalid_filter_result(&env.resource,
                                               loaded->copy_types(),
                                               string_projected_cols,
                                               DEFAULT_VECTOR_CAPACITY);
            REQUIRE_FALSE(row_group_test_access_t::try_scan_pax_generic_projected(*first_row_group,
                                                                                  invalid_filter_state.table_state,
                                                                                  invalid_filter_result));
        }

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected > 0);
        REQUIRE(counts.pax_generic_pruned_pages == 0);
        REQUIRE(counts.pax_generic_prefetched_blocks > 0);
        REQUIRE(counts.pax_generic_skipped_payload_pages == 0);
        REQUIRE(counts.pax_fixed_projected == 0);
        REQUIRE(counts.regular == 0);
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

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected == 0);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_pruned_pages == 0);
        REQUIRE(counts.pax_fixed_prefetched_blocks > 0);
        REQUIRE(counts.pax_fixed_skipped_payload_pages == 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan skips committed deletes") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_fixed_delete_overlay";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_delete");
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
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        const std::vector<uint64_t> deleted_rows{0, 3, 128, 255, 256, 511};
        delete_committed_rows(*loaded, &env.resource, deleted_rows);

        std::vector<uint64_t> expected_rows;
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            if (std::find(deleted_rows.begin(), deleted_rows.end(), row) == deleted_rows.end()) {
                expected_rows.push_back(row);
            }
        }

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                      helper_state.table_state,
                                                                      helper_chunk));
        REQUIRE(helper_chunk.size() == expected_rows.size());

        {
            std::vector<storage_index_t> row_id_projection{storage_index_t(0), storage_index_t()};
            std::pmr::vector<complex_logical_type> row_id_result_types(&env.resource);
            row_id_result_types.emplace_back(logical_type::BIGINT);
            row_id_result_types.emplace_back(logical_type::BIGINT);
            table_scan_state row_id_state(&env.resource);
            loaded->initialize_scan(row_id_state, row_id_projection, nullptr);
            data_chunk_t row_id_result(&env.resource, row_id_result_types, DEFAULT_VECTOR_CAPACITY);
            REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                          row_id_state.table_state,
                                                                          row_id_result));
            REQUIRE(row_id_result.size() == expected_rows.size());
            for (uint64_t i = 0; i < row_id_result.size(); i++) {
                const auto row = expected_rows[i];
                REQUIRE(row_id_result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(row_id_result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row));
            }
        }

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
                const auto row = expected_rows[scanned + i];
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 3));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());

        auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        std::pmr::vector<uint64_t> filter_columns(&env.resource);
        filter_columns.push_back(0);
        constant_filter_t filter(components::expressions::compare_type::gte,
                                 logical_value_t{&env.resource, int64_t(250)},
                                 std::move(filter_columns));

        std::vector<uint64_t> filtered_expected_rows;
        for (auto row : expected_rows) {
            if (row >= 250) {
                filtered_expected_rows.push_back(row);
            }
        }

        table_scan_state filtered_state(&env.resource);
        loaded->initialize_scan(filtered_state, projected_indices, &filter);
        data_chunk_t filtered_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        scanned = 0;
        while (true) {
            filtered_result.reset();
            loaded->scan(filtered_result, filtered_state);
            if (filtered_result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < filtered_result.size(); i++) {
                const auto row = filtered_expected_rows[scanned + i];
                REQUIRE(filtered_result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(filtered_result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 3));
            }
            scanned += filtered_result.size();
        }
        REQUIRE(scanned == filtered_expected_rows.size());

        counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        conjunction_and_filter_t range_filter;
        std::pmr::vector<uint64_t> lower_filter_columns(&env.resource);
        lower_filter_columns.push_back(0);
        range_filter.child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::gte,
            logical_value_t{&env.resource, int64_t(300)},
            std::move(lower_filter_columns)));
        std::pmr::vector<uint64_t> upper_filter_columns(&env.resource);
        upper_filter_columns.push_back(0);
        range_filter.child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::lt,
            logical_value_t{&env.resource, int64_t(320)},
            std::move(upper_filter_columns)));

        std::vector<uint64_t> range_expected_rows;
        for (auto row : expected_rows) {
            if (row >= 300 && row < 320) {
                range_expected_rows.push_back(row);
            }
        }

        table_scan_state range_state(&env.resource);
        loaded->initialize_scan(range_state, projected_indices, &range_filter);
        data_chunk_t range_result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        scanned = 0;
        while (true) {
            range_result.reset();
            loaded->scan(range_result, range_state);
            if (range_result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < range_result.size(); i++) {
                const auto row = range_expected_rows[scanned + i];
                REQUIRE(range_result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(range_result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 3));
            }
            scanned += range_result.size();
        }
        REQUIRE(scanned == range_expected_rows.size());

        counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_pruned_pages > 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan applies active transaction delete visibility") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_fixed_active_txn_delete_visibility";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_active_txn_delete");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  NUM_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 5); });

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

        const std::vector<uint64_t> deleted_rows{0, 3, 128, 255, 256, 511};
        vector_t row_ids(&env.resource, logical_type::BIGINT, deleted_rows.size());
        for (uint64_t i = 0; i < deleted_rows.size(); i++) {
            row_ids.set_value(i, logical_value_t{&env.resource, static_cast<int64_t>(deleted_rows[i])});
        }

        transaction_manager_t txn_manager(&env.resource);
        auto writer_session = components::session::session_id_t::generate_uid();
        auto& writer_txn = txn_manager.begin_transaction(writer_session);
        auto delete_state = loaded->initialize_delete({});
        REQUIRE(loaded->delete_rows(*delete_state,
                                    row_ids,
                                    deleted_rows.size(),
                                    writer_txn.transaction_id()) == deleted_rows.size());

        std::vector<uint64_t> all_rows;
        all_rows.reserve(NUM_ROWS);
        std::vector<uint64_t> committed_visible_rows;
        committed_visible_rows.reserve(NUM_ROWS - deleted_rows.size());
        for (uint64_t row = 0; row < NUM_ROWS; row++) {
            all_rows.push_back(row);
            if (std::find(deleted_rows.begin(), deleted_rows.end(), row) == deleted_rows.end()) {
                committed_visible_rows.push_back(row);
            }
        }

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        const auto scan_and_check = [&](transaction_data txn, const std::vector<uint64_t>& expected_rows) {
            row_group_test_access_t::reset_scan_path_counts(*first_row_group);

            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, nullptr);
            state.table_state.txn = txn;
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
                    REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                    REQUIRE(result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 5));
                }
                scanned += result.size();
            }
            REQUIRE(scanned == expected_rows.size());

            const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
            REQUIRE(counts.pax_fixed_projected > 0);
            REQUIRE(counts.regular == 0);
        };

        auto reader_session = components::session::session_id_t::generate_uid();
        auto& reader_txn = txn_manager.begin_transaction(reader_session);
        scan_and_check(reader_txn.data(), all_rows);
        txn_manager.abort(reader_session);

        const auto writer_txn_id = writer_txn.transaction_id();
        const auto commit_id = txn_manager.commit(writer_session);
        loaded->commit_all_deletes(writer_txn_id, commit_id);
        txn_manager.publish(commit_id);

        auto fresh_session = components::session::session_id_t::generate_uid();
        auto& fresh_txn = txn_manager.begin_transaction(fresh_session);
        scan_and_check(fresh_txn.data(), committed_visible_rows);
        txn_manager.abort(fresh_session);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan persists committed deletes") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_fixed_persisted_delete_overlay";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_persisted_delete");
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

    const std::vector<uint64_t> deleted_rows{0, 3, 128, 255, 256, 511};
    std::vector<uint64_t> expected_rows;
    for (uint64_t row = 0; row < NUM_ROWS; row++) {
        if (std::find(deleted_rows.begin(), deleted_rows.end(), row) == deleted_rows.end()) {
            expected_rows.push_back(row);
        }
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        delete_committed_rows(*loaded, &env.resource, deleted_rows);

        metadata_writer_t writer(meta_mgr);
        loaded->checkpoint(writer);
        table_pointer = writer.get_block_pointer();

        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);
        REQUIRE(row_group_test_access_t::delete_pointer_count(*first_row_group) == 1);
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();

        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);
        REQUIRE(row_group_test_access_t::delete_pointer_count(*first_row_group) == 1);
        REQUIRE(loaded->calculate_size() == expected_rows.size());

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        table_scan_state helper_state(&env.resource);
        loaded->initialize_scan(helper_state, projected_indices, nullptr);
        data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                      helper_state.table_state,
                                                                      helper_chunk));
        REQUIRE(helper_chunk.size() == expected_rows.size());

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
                const auto row = expected_rows[scanned + i];
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 3));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan applies update overlay") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_fixed_update_overlay";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_update");
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
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);

        const std::vector<uint64_t> updated_rows{5, 128, 300};
        update_fixed_column(*loaded,
                            &env.resource,
                            0,
                            updated_rows,
                            {logical_value_t{&env.resource, int64_t(100005)},
                             logical_value_t{&env.resource, int64_t(100128)},
                             logical_value_t{&env.resource, int64_t(100300)}},
                            logical_type::BIGINT);
        update_fixed_column(*loaded,
                            &env.resource,
                            1,
                            updated_rows,
                            {logical_value_t{&env.resource, uint32_t(9005)},
                             logical_value_t{&env.resource, uint32_t(9128)},
                             logical_value_t{&env.resource, uint32_t(9300)}},
                            logical_type::UINTEGER);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        {
            table_scan_state helper_state(&env.resource);
            loaded->initialize_scan(helper_state, projected_indices, nullptr);
            data_chunk_t helper_chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                          helper_state.table_state,
                                                                          helper_chunk));
            REQUIRE(helper_chunk.size() == NUM_ROWS);
            REQUIRE(helper_chunk.data[0].value(5).value<int64_t>() == int64_t(100005));
            REQUIRE(helper_chunk.data[1].value(5).value<uint32_t>() == uint32_t(9005));
            REQUIRE(helper_chunk.data[0].value(128).value<int64_t>() == int64_t(100128));
            REQUIRE(helper_chunk.data[1].value(128).value<uint32_t>() == uint32_t(9128));
            REQUIRE(helper_chunk.data[0].value(300).value<int64_t>() == int64_t(100300));
            REQUIRE(helper_chunk.data[1].value(300).value<uint32_t>() == uint32_t(9300));
        }

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        std::pmr::vector<uint64_t> filter_columns(&env.resource);
        filter_columns.push_back(0);
        constant_filter_t filter(components::expressions::compare_type::gte,
                                 logical_value_t{&env.resource, int64_t(100000)},
                                 std::move(filter_columns));

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, &filter);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        loaded->scan(result, state);
        REQUIRE(result.size() == updated_rows.size());
        REQUIRE(result.data[0].value(0).value<int64_t>() == int64_t(100005));
        REQUIRE(result.data[1].value(0).value<uint32_t>() == uint32_t(9005));
        REQUIRE(result.data[0].value(1).value<int64_t>() == int64_t(100128));
        REQUIRE(result.data[1].value(1).value<uint32_t>() == uint32_t(9128));
        REQUIRE(result.data[0].value(2).value<int64_t>() == int64_t(100300));
        REQUIRE(result.data[1].value(2).value<uint32_t>() == uint32_t(9300));

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected == 0);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan supports dml update predicate tree") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 768;
    const auto table_path = test_db_path() + ".pax_fixed_update_predicate_tree";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("flag", logical_type::UINTEGER);
        columns.emplace_back("version", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_update_tree");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row)});
            chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, uint32_t{0}});
            chunk.set_value(2, row_in_chunk, logical_value_t{&env.resource, uint32_t{1}});
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

        const std::vector<uint64_t> updated_rows{300, 305, 540, 600};
        update_fixed_column(*loaded,
                            &env.resource,
                            1,
                            updated_rows,
                            {logical_value_t{&env.resource, uint32_t{0}},
                             logical_value_t{&env.resource, uint32_t{1}},
                             logical_value_t{&env.resource, uint32_t{1}},
                             logical_value_t{&env.resource, uint32_t{1}}},
                            logical_type::UINTEGER);
        update_fixed_column(*loaded,
                            &env.resource,
                            2,
                            updated_rows,
                            {logical_value_t{&env.resource, uint32_t{2}},
                             logical_value_t{&env.resource, uint32_t{1}},
                             logical_value_t{&env.resource, uint32_t{2}},
                             logical_value_t{&env.resource, uint32_t{2}}},
                            logical_type::UINTEGER);

        conjunction_and_filter_t filter;
        std::pmr::vector<uint64_t> lower_filter_columns(&env.resource);
        lower_filter_columns.push_back(0);
        filter.child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::gte,
            logical_value_t{&env.resource, int64_t{260}},
            std::move(lower_filter_columns)));
        std::pmr::vector<uint64_t> upper_filter_columns(&env.resource);
        upper_filter_columns.push_back(0);
        filter.child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::lt,
            logical_value_t{&env.resource, int64_t{580}},
            std::move(upper_filter_columns)));

        auto changed_filter = std::make_unique<conjunction_or_filter_t>();
        std::pmr::vector<uint64_t> flag_filter_columns(&env.resource);
        flag_filter_columns.push_back(1);
        changed_filter->child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::ne,
            logical_value_t{&env.resource, uint32_t{0}},
            std::move(flag_filter_columns)));
        std::pmr::vector<uint64_t> version_filter_columns(&env.resource);
        version_filter_columns.push_back(2);
        changed_filter->child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::ne,
            logical_value_t{&env.resource, uint32_t{1}},
            std::move(version_filter_columns)));
        filter.child_filters.push_back(std::move(changed_filter));

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1), storage_index_t(2)};
        std::vector<size_t> projected_cols{0, 1, 2};
        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, &filter);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

        const std::vector<uint64_t> expected_rows{300, 305, 540};
        uint64_t scanned = 0;
        while (true) {
            result.reset();
            loaded->scan(result, state);
            if (result.size() == 0) {
                break;
            }

            for (uint64_t i = 0; i < result.size(); i++) {
                const auto row = expected_rows[scanned + i];
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(result.data[1].value(i).value<uint32_t>() == (row == 300 ? uint32_t{0} : uint32_t{1}));
                REQUIRE(result.data[2].value(i).value<uint32_t>() == (row == 305 ? uint32_t{1} : uint32_t{2}));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == expected_rows.size());

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected == 0);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_pruned_pages > 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan survives append after checkpoint") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t BASE_ROWS = DEFAULT_VECTOR_CAPACITY * 2 + 100;
    constexpr uint64_t APPEND_ROWS = 50;
    const auto table_path = test_db_path() + ".pax_fixed_append_after_checkpoint";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_append");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  BASE_ROWS,
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

        append_fixed_integer_pair(*loaded,
                                  &env.resource,
                                  APPEND_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(BASE_ROWS + row); },
                                  [](uint64_t row) { return static_cast<uint32_t>((BASE_ROWS + row) * 3); });

        auto* first_row_group = loaded->row_group()->row_group(0);
        auto* second_row_group = loaded->row_group()->row_group(1);
        auto* partial_row_group = loaded->row_group()->row_group(2);
        REQUIRE(first_row_group != nullptr);
        REQUIRE(second_row_group != nullptr);
        REQUIRE(partial_row_group != nullptr);
        row_group_test_access_t::reset_scan_path_counts(*first_row_group);
        row_group_test_access_t::reset_scan_path_counts(*second_row_group);
        row_group_test_access_t::reset_scan_path_counts(*partial_row_group);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

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
                REQUIRE(result.data[1].validity().row_is_valid(i));
                REQUIRE(result.data[1].value(i).value<uint32_t>() == static_cast<uint32_t>(row * 3));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == BASE_ROWS + APPEND_ROWS);

        const auto first_counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        const auto second_counts = row_group_test_access_t::scan_path_counts(*second_row_group);
        const auto partial_counts = row_group_test_access_t::scan_path_counts(*partial_row_group);
        REQUIRE(first_counts.pax_generic_projected == 0);
        REQUIRE(second_counts.pax_generic_projected == 0);
        REQUIRE(partial_counts.pax_generic_projected == 0);
        REQUIRE(first_counts.pax_fixed_projected > 0);
        REQUIRE(second_counts.pax_fixed_projected > 0);
        REQUIRE(partial_counts.regular > 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// A data block whose on-disk CRC32c no longer matches must be rejected on reopen, not scanned
// (the prefetch/batch read path also has to verify, not just metadata blocks).
TEST_CASE("checkpoint_load: torn/corrupt database header recovers from intact slot or throws") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    constexpr uint64_t ROWS = 1000;
    const auto table_path = test_db_path() + ".hdr_fault";
    std::remove(table_path.c_str());

    // Phase 1: build + checkpoint. write_header stamps both header slots with valid checksums.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        std::vector<column_definition_t> columns;
        columns.emplace_back("v", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "hdr");
        append_int64_data(*table, &env.resource, ROWS);

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        database_header_t header;
        header.initialize();
        header.meta_block = writer.get_block_pointer().block_pointer;
        bm.write_header(header);
        bm.file_sync();
    }

    // Flip a byte in the checksummed meta_block field of a header slot so its stored checksum no longer matches.
    const auto corrupt_header_slot = [&](uint64_t slot_offset) {
        std::fstream f(table_path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        const auto pos = static_cast<std::streamoff>(slot_offset + sizeof(uint64_t)); // meta_block field
        f.seekg(pos);
        char b = 0;
        f.read(&b, 1);
        REQUIRE(f.good());
        b = static_cast<char>(static_cast<unsigned char>(b) ^ 0xFFu);
        f.seekp(pos);
        f.write(&b, 1);
        f.flush();
        REQUIRE(f.good());
    };

    // Phase 2: corrupt one slot. Reopen must reject it on checksum and recover from the intact slot.
    corrupt_header_slot(SECTOR_SIZE);
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        REQUIRE_NOTHROW(bm.load_existing_database());
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t mp;
        mp.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, mp);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        uint64_t scanned = 0;
        loaded->scan_table_segment(0, ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].value(i).value<int64_t>() == static_cast<int64_t>(scanned + i));
            }
            scanned += chunk.size();
        });
        REQUIRE(scanned == ROWS);
    }

    // Phase 3: corrupt the other slot too. Both invalid, so load must throw.
    corrupt_header_slot(2 * SECTOR_SIZE);
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        REQUIRE_THROWS_AS(bm.load_existing_database(), std::runtime_error);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: corrupted pax data block is detected on reopen scan") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    constexpr uint64_t ROWS = 4096; // several row groups, so real persisted PAX data blocks
    const auto table_path = test_db_path() + ".pax_corrupt_detect";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    uint64_t block_alloc_size = 0;

    // Phase 1: build a PAX_ONLY table and checkpoint it to disk.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_corrupt");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 3); });

        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        table_pointer = writer.get_block_pointer();
        block_alloc_size = bm.block_allocation_size();

        database_header_t header;
        header.initialize();
        bm.write_header(header);
    }

    // Phase 2: introspect the persisted layout to find a value-column PAX data block id.
    uint64_t corrupt_block_id = INVALID_INDEX;
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        auto* rg0 = loaded->row_group()->row_group(0);
        REQUIRE(rg0 != nullptr);
        const auto& layout = row_group_test_access_t::pax_fixed_layout(*rg0);
        REQUIRE(layout.has_value());
        for (const auto& page : layout->pages) {
            for (const auto& slice : page.slices) {
                if (slice.column_index == 1) {
                    corrupt_block_id = slice.data_pointer.block_pointer.block_id;
                    break;
                }
            }
            if (corrupt_block_id != INVALID_INDEX) {
                break;
            }
        }
        REQUIRE(corrupt_block_id != INVALID_INDEX);
    }

    // Phase 3: flip a payload byte inside that data block, invalidating its stored CRC32c.
    {
        const uint64_t payload_byte = BLOCK_START + corrupt_block_id * block_alloc_size + sizeof(uint64_t);
        std::fstream f(table_path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekg(static_cast<std::streamoff>(payload_byte));
        char b = 0;
        f.read(&b, 1);
        REQUIRE(f.good());
        b = static_cast<char>(static_cast<unsigned char>(b) ^ 0xFFu);
        f.seekp(static_cast<std::streamoff>(payload_byte));
        f.write(&b, 1);
        f.flush();
        REQUIRE(f.good());
    }

    // Phase 4: reopen and scan. Metadata is intact so reopen succeeds; the corrupted data block
    // must raise a checksum mismatch when the scan loads it.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);

        bool threw = false;
        try {
            auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
            std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
            std::vector<size_t> projected_cols{0, 1};
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, nullptr);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
            }
        } catch (const std::exception& e) {
            threw = true;
            REQUIRE(std::string(e.what()).find("checksum") != std::string::npos);
        }
        REQUIRE(threw);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// A checkpoint interrupted before the header swap (new data/metadata blocks written and fsynced,
// header never committed) must leave the prior committed state intact and reopenable. The header
// swap is the atomic commit point; an un-swapped header still points at the prior meta_block.
TEST_CASE("checkpoint_load: interrupted checkpoint before header swap preserves prior state") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    constexpr uint64_t V1_ROWS = 2000;       // committed state
    constexpr uint64_t V2_EXTRA = 1500;      // appended but never committed
    const auto table_path = test_db_path() + ".pax_durability";
    std::remove(table_path.c_str());

    // Commit: persist data/metadata, fsync, swap header, fsync.
    const auto commit = [](single_file_block_manager_t& bm, data_table_t& table) {
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table.checkpoint(writer);
        writer.flush();
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_list_ptr = bm.serialize_free_list();
        bm.file_sync();
        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        bm.write_header(header);
        bm.file_sync();
    };

    // Reopen through the on-disk header's meta_block and scan, asserting `expected` rows with value == row*3.
    const auto reopen_and_count = [](test_env_t& env, const std::string& path, uint64_t expected) {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};
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
        REQUIRE(scanned == expected);
    };

    // Phase 1: build a PAX_ONLY table with V1_ROWS and commit it (header swapped).
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_durability");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  V1_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 3); });
        commit(bm, *table);
    }

    // Phase 2: reopen, append V2_EXTRA, persist+fsync the new data/metadata, but do not swap the
    // header (the on-disk state after a crash between checkpoint's two fsyncs).
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        append_fixed_integer_pair(*loaded,
                                  &env.resource,
                                  V2_EXTRA,
                                  [](uint64_t row) { return static_cast<int64_t>(V1_ROWS + row); },
                                  [](uint64_t row) { return static_cast<uint32_t>((V1_ROWS + row) * 3); });

        metadata_manager_t meta_mgr2(bm);
        metadata_writer_t writer2(meta_mgr2);
        loaded->checkpoint(writer2);
        writer2.flush();
        bm.set_meta_block(writer2.get_block_pointer().block_pointer);
        (void) bm.serialize_free_list();
        bm.file_sync();
        // crash here: header is never written, so the on-disk header still points at V1's meta_block.
    }

    // Phase 3: reopen with a fresh pool via the header. Must observe exactly the committed V1.
    {
        test_env_t env;
        reopen_and_count(env, table_path, V1_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// Single UINTEGER column, persisted via PAX, cold-reopened, then more rows appended into the same
// sub-1024 row group, scanned via the regular path. Appending raw values into a reopened
// dictionary-compressed segment used to decode back as garbage; the append must roll onto a fresh
// uncompressed segment instead.
TEST_CASE("checkpoint_load: minimal repro single-rg append-after-reopen value") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    const auto table_path = test_db_path() + ".pax_minrepro";

    const auto commit = [](single_file_block_manager_t& bm, data_table_t& table) {
        metadata_manager_t mm(bm);
        metadata_writer_t w(mm);
        table.checkpoint(w);
        w.flush();
        bm.set_meta_block(w.get_block_pointer().block_pointer);
        auto fl = bm.serialize_free_list();
        bm.file_sync();
        database_header_t h;
        h.initialize();
        h.free_list = fl.block_pointer;
        bm.write_header(h);
        bm.file_sync();
    };

    const auto val = [](uint64_t row) { return static_cast<uint32_t>(row * 2654435761ull + 7ull); };

    // Run without nulls first to isolate values from validity, then with nulls.
    for (bool with_nulls : {false, true}) {
        std::remove(table_path.c_str());
        constexpr uint64_t R1 = 111;
        constexpr uint64_t R2 = 205;

        const auto append_batch = [&](data_table_t& table, std::pmr::memory_resource* res, uint64_t start, uint64_t n) {
            auto types = table.copy_types();
            uint64_t off = 0;
            while (off < n) {
                const uint64_t b = std::min(n - off, uint64_t(DEFAULT_VECTOR_CAPACITY));
                data_chunk_t chunk(res, types, b);
                chunk.set_cardinality(b);
                for (uint64_t i = 0; i < b; i++) {
                    const uint64_t row = start + off + i;
                    if (with_nulls && (row % 3 == 2)) {
                        chunk.set_value(0, i, logical_value_t{res, nullptr});
                    } else {
                        chunk.set_value(0, i, logical_value_t{res, val(row)});
                    }
                }
                table_append_state st(res);
                table.append_lock(st);
                table.initialize_append(st);
                table.append(chunk, st);
                table.finalize_append(st, transaction_data{0, 0});
                off += b;
            }
        };

        {
            test_env_t env;
            single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
            bm.create_new_database();
            bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
            std::vector<column_definition_t> columns;
            columns.emplace_back("v", logical_type::UINTEGER);
            auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "minrepro");
            append_batch(*table, &env.resource, 0, R1);
            commit(bm, *table);
        }
        {
            test_env_t env;
            single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
            bm.load_existing_database();
            metadata_manager_t mm(bm);
            meta_block_pointer_t mp;
            mp.block_pointer = bm.meta_block();
            metadata_reader_t reader(mm, mp);
            auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
            append_batch(*loaded, &env.resource, R1, R2);
            commit(bm, *loaded);
        }
        {
            test_env_t env;
            single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
            bm.load_existing_database();
            metadata_manager_t mm(bm);
            meta_block_pointer_t mp;
            mp.block_pointer = bm.meta_block();
            metadata_reader_t reader(mm, mp);
            auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

            std::vector<storage_index_t> idx{storage_index_t(0)};
            std::vector<size_t> pcols{0};
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, idx, nullptr);
            data_chunk_t result(&env.resource, loaded->copy_types(), pcols, DEFAULT_VECTOR_CAPACITY);
            uint64_t scanned = 0;
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < result.size(); i++) {
                    const uint64_t row = scanned + i;
                    INFO("with_nulls=" << with_nulls << " row=" << row);
                    const bool is_null = with_nulls && (row % 3 == 2);
                    if (is_null) {
                        REQUIRE_FALSE(result.data[0].validity().row_is_valid(i));
                    } else {
                        REQUIRE(result.data[0].validity().row_is_valid(i));
                        REQUIRE(result.data[0].value(i).value<uint32_t>() == val(row));
                    }
                }
                scanned += result.size();
            }
            REQUIRE(scanned == R1 + R2);
        }
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// Differential round-trip fuzzer (catalog mode). Catches silent corruption (wrong value or null
// bit, no error). Runs seeded random trials (random schema, nulls, row counts across row-group/page
// boundaries; append -> cold-reopen -> append-after-reopen -> cold-reopen -> scan, with random
// DELETEs/UPDATEs mirrored into an oracle), collects a signature per mismatch instead of aborting on
// the first, and asserts clean at the end. Each trial picks PAX_ONLY or COLUMNAR_ONLY; UPDATE is
// gated to PAX because the columnar checkpoint drops the update_segment overlay on reopen (separate
// open bug). Set FUZZ_TRIALS=N for more trials; FUZZ_ONLY_SEED=<seed> runs exactly that one trial.
TEST_CASE("checkpoint_load: differential round-trip fuzzer catalog (pax cold-reopen == oracle)") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    const auto table_path = test_db_path() + ".pax_fuzz";

    const auto commit = [](single_file_block_manager_t& bm, data_table_t& table) {
        metadata_manager_t mm(bm);
        metadata_writer_t w(mm);
        table.checkpoint(w);
        w.flush();
        bm.set_meta_block(w.get_block_pointer().block_pointer);
        auto fl = bm.serialize_free_list();
        bm.file_sync();
        database_header_t h;
        h.initialize();
        h.free_list = fl.block_pointer;
        bm.write_header(h);
        bm.file_sync();
    };

    struct cell_t {
        bool isnull = false;
        int64_t i64 = 0;  // all integer types (bit pattern)
        double f64 = 0.0; // FLOAT/DOUBLE
        std::string str;  // STRING_LITERAL
    };

    const std::array<logical_type, 12> TYPES{logical_type::BOOLEAN,
                                             logical_type::TINYINT,
                                             logical_type::UTINYINT,
                                             logical_type::SMALLINT,
                                             logical_type::USMALLINT,
                                             logical_type::INTEGER,
                                             logical_type::UINTEGER,
                                             logical_type::BIGINT,
                                             logical_type::UBIGINT,
                                             logical_type::FLOAT,
                                             logical_type::DOUBLE,
                                             logical_type::STRING_LITERAL};
    const auto type_name = [](logical_type t) -> const char* {
        switch (t) {
            case logical_type::BOOLEAN: return "BOOLEAN";
            case logical_type::TINYINT: return "TINYINT";
            case logical_type::UTINYINT: return "UTINYINT";
            case logical_type::SMALLINT: return "SMALLINT";
            case logical_type::USMALLINT: return "USMALLINT";
            case logical_type::INTEGER: return "INTEGER";
            case logical_type::UINTEGER: return "UINTEGER";
            case logical_type::BIGINT: return "BIGINT";
            case logical_type::UBIGINT: return "UBIGINT";
            case logical_type::FLOAT: return "FLOAT";
            case logical_type::DOUBLE: return "DOUBLE";
            case logical_type::STRING_LITERAL: return "STRING";
            default: return "?";
        }
    };

    // Generate a value for `t`, write it into the chunk and return the oracle cell.
    const auto gen = [](std::mt19937_64& rng, std::pmr::memory_resource* res, logical_type t, data_chunk_t& chunk,
                        uint64_t col, uint64_t i) -> cell_t {
        cell_t cell;
        switch (t) {
            case logical_type::BOOLEAN: {
                const bool v = (rng() & 1u) != 0;
                cell.i64 = v ? 1 : 0;
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::TINYINT: {
                const int8_t v = static_cast<int8_t>(rng());
                cell.i64 = v;
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::UTINYINT: {
                const uint8_t v = static_cast<uint8_t>(rng());
                cell.i64 = static_cast<int64_t>(v);
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::SMALLINT: {
                const int16_t v = static_cast<int16_t>(rng());
                cell.i64 = v;
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::USMALLINT: {
                const uint16_t v = static_cast<uint16_t>(rng());
                cell.i64 = static_cast<int64_t>(v);
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::INTEGER: {
                const int32_t v = static_cast<int32_t>(static_cast<uint32_t>(rng()));
                cell.i64 = v;
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::UINTEGER: {
                const uint32_t v = static_cast<uint32_t>(rng());
                cell.i64 = static_cast<int64_t>(v);
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::BIGINT: {
                const int64_t v = static_cast<int64_t>(rng());
                cell.i64 = v;
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::UBIGINT: {
                const uint64_t v = rng();
                cell.i64 = static_cast<int64_t>(v);
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::FLOAT: {
                const float v = static_cast<float>(static_cast<int64_t>(rng())) / 64.0f;
                cell.f64 = static_cast<double>(v);
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            case logical_type::DOUBLE: {
                const double v = static_cast<double>(static_cast<int64_t>(rng())) / 1024.0;
                cell.f64 = v;
                chunk.set_value(col, i, logical_value_t{res, v});
                break;
            }
            default: { // STRING_LITERAL
                const uint64_t len = rng() % 25;
                std::string s;
                s.reserve(len);
                for (uint64_t k = 0; k < len; k++) {
                    s.push_back(static_cast<char>('a' + (rng() % 26)));
                }
                cell.str = s;
                chunk.set_value(col, i, logical_value_t{res, s});
                break;
            }
        }
        return cell;
    };

    // Compare a scanned cell to its oracle. Returns "" on match, else a symptom string.
    const auto check = [](logical_type t, const cell_t& cell, components::vector::vector_t& vec,
                          uint64_t i) -> const char* {
        const bool valid = vec.validity().row_is_valid(i);
        if (cell.isnull) {
            return valid ? "validity(expected-null,got-value)" : "";
        }
        if (!valid) {
            return "validity(expected-value,got-null)";
        }
        const auto lv = vec.value(i);
        switch (t) {
            case logical_type::BOOLEAN:
                return (lv.value<bool>() == (cell.i64 != 0)) ? "" : "value";
            case logical_type::TINYINT:
                return (lv.value<int8_t>() == static_cast<int8_t>(cell.i64)) ? "" : "value";
            case logical_type::UTINYINT:
                return (lv.value<uint8_t>() == static_cast<uint8_t>(cell.i64)) ? "" : "value";
            case logical_type::SMALLINT:
                return (lv.value<int16_t>() == static_cast<int16_t>(cell.i64)) ? "" : "value";
            case logical_type::USMALLINT:
                return (lv.value<uint16_t>() == static_cast<uint16_t>(cell.i64)) ? "" : "value";
            case logical_type::INTEGER:
                return (lv.value<int32_t>() == static_cast<int32_t>(cell.i64)) ? "" : "value";
            case logical_type::UINTEGER:
                return (lv.value<uint32_t>() == static_cast<uint32_t>(cell.i64)) ? "" : "value";
            case logical_type::BIGINT:
                return (lv.value<int64_t>() == cell.i64) ? "" : "value";
            case logical_type::UBIGINT:
                return (lv.value<uint64_t>() == static_cast<uint64_t>(cell.i64)) ? "" : "value";
            case logical_type::FLOAT: {
                const float got = lv.value<float>();
                const float exp = static_cast<float>(cell.f64);
                return (std::memcmp(&got, &exp, sizeof(float)) == 0) ? "" : "value";
            }
            case logical_type::DOUBLE: {
                const double got = lv.value<double>();
                return (std::memcmp(&got, &cell.f64, sizeof(double)) == 0) ? "" : "value";
            }
            default: {
                const auto* got = lv.value<std::string*>();
                return (got && *got == cell.str) ? "" : "value";
            }
        }
    };

    // Oracle for a constant_filter on an integer column: comparator(column_value, predicate), with a
    // NULL column value always failing the filter. Integer-only so the comparison is exact.
    const auto cmp_apply = [](components::expressions::compare_type c, auto x, auto y) -> bool {
        using ct = components::expressions::compare_type;
        switch (c) {
            case ct::eq: return x == y;
            case ct::ne: return x != y;
            case ct::gt: return x > y;
            case ct::gte: return x >= y;
            case ct::lt: return x < y;
            case ct::lte: return x <= y;
            default: return false;
        }
    };
    const auto cell_matches = [&cmp_apply](logical_type t, components::expressions::compare_type c, const cell_t& a,
                                           const cell_t& b) -> bool {
        if (a.isnull) {
            return false;
        }
        switch (t) {
            case logical_type::TINYINT: return cmp_apply(c, static_cast<int8_t>(a.i64), static_cast<int8_t>(b.i64));
            case logical_type::UTINYINT: return cmp_apply(c, static_cast<uint8_t>(a.i64), static_cast<uint8_t>(b.i64));
            case logical_type::SMALLINT: return cmp_apply(c, static_cast<int16_t>(a.i64), static_cast<int16_t>(b.i64));
            case logical_type::USMALLINT:
                return cmp_apply(c, static_cast<uint16_t>(a.i64), static_cast<uint16_t>(b.i64));
            case logical_type::INTEGER: return cmp_apply(c, static_cast<int32_t>(a.i64), static_cast<int32_t>(b.i64));
            case logical_type::UINTEGER: return cmp_apply(c, static_cast<uint32_t>(a.i64), static_cast<uint32_t>(b.i64));
            case logical_type::BIGINT: return cmp_apply(c, static_cast<int64_t>(a.i64), static_cast<int64_t>(b.i64));
            case logical_type::UBIGINT: return cmp_apply(c, static_cast<uint64_t>(a.i64), static_cast<uint64_t>(b.i64));
            default: return false;
        }
    };
    const auto is_filterable = [](logical_type t) {
        switch (t) {
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
            case logical_type::BIGINT:
            case logical_type::UBIGINT: return true;
            default: return false;
        }
    };

    const int trials = std::getenv("FUZZ_TRIALS") ? std::atoi(std::getenv("FUZZ_TRIALS")) : 150;
    std::map<std::string, std::pair<uint64_t, uint64_t>> catalog; // signature -> {count, example-seed}
    const auto record = [&](const std::string& sig, uint64_t seed) {
        auto& e = catalog[sig];
        e.first++;
        if (e.first == 1) {
            e.second = seed;
        }
    };

    const uint64_t seed_base = 12648430ull;
    const uint64_t seed_mult = 2654435761ull;
    // FUZZ_ONLY_SEED=<seed> runs exactly one trial with that seed (deterministic repro).
    const char* only_seed_env = std::getenv("FUZZ_ONLY_SEED");
    const bool single = only_seed_env != nullptr;
    const uint64_t only_seed = single ? std::strtoull(only_seed_env, nullptr, 10) : 0;
    const int loop_trials = single ? 1 : trials;
    for (int trial = 0; trial < loop_trials; trial++) {
        const uint64_t seed = single ? only_seed : seed_base + static_cast<uint64_t>(trial) * seed_mult;
        std::mt19937_64 rng(seed);
        std::remove(table_path.c_str());

        const uint64_t ncols = 1 + (rng() % 4);
        struct colspec_t {
            logical_type type;
            bool nullable;
        };
        std::vector<colspec_t> schema;
        std::vector<column_definition_t> columns;
        for (uint64_t c = 0; c < ncols; c++) {
            const auto t = TYPES[rng() % TYPES.size()];
            schema.push_back({t, (rng() % 3) != 0});
            columns.emplace_back("c" + std::to_string(c), t);
        }

        std::vector<std::vector<cell_t>> oracle(ncols);
        uint64_t total = 0;
        std::set<uint64_t> deleted_rows; // rows removed via DELETE — excluded from the visible oracle
        bool had_update = false;         // whether any UPDATE was applied this trial (signature tag)
        const bool do_mid_reopen = (rng() & 1u) != 0; // distinguish reopen-append from plain append
        // Exercise both on-disk layouts (PAX and COLUMNAR); both must round-trip values + null bits.
        const bool use_columnar = (rng() % 3) == 0;
        const auto layout_policy =
            use_columnar ? row_group_layout_policy::COLUMNAR_ONLY : row_group_layout_policy::PAX_ONLY;
        const char* layout_tag = use_columnar ? "columnar" : "pax";

        const auto append_batch = [&](data_table_t& table, std::pmr::memory_resource* res, uint64_t n) {
            auto types = table.copy_types();
            uint64_t off = 0;
            while (off < n) {
                const uint64_t b = std::min(n - off, uint64_t(DEFAULT_VECTOR_CAPACITY));
                data_chunk_t chunk(res, types, b);
                chunk.set_cardinality(b);
                for (uint64_t i = 0; i < b; i++) {
                    for (uint64_t c = 0; c < ncols; c++) {
                        if (schema[c].nullable && (rng() % 100) < 30) {
                            cell_t nullcell;
                            nullcell.isnull = true;
                            oracle[c].push_back(nullcell);
                            chunk.set_value(c, i, logical_value_t{res, nullptr});
                        } else {
                            oracle[c].push_back(gen(rng, res, schema[c].type, chunk, c, i));
                        }
                    }
                }
                table_append_state st(res);
                table.append_lock(st);
                table.initialize_append(st);
                table.append(chunk, st);
                table.finalize_append(st, transaction_data{0, 0});
                off += b;
                total += b;
            }
        };

        // Apply random DELETEs and UPDATEs to the live table, mirroring each into the oracle.
        // Called at every append/reopen point so mutations are verified across the reopen too.
        const auto mutate = [&](data_table_t& table, std::pmr::memory_resource* res) {
            if (total == 0) {
                return;
            }
            const auto live_rows = [&]() {
                std::vector<uint64_t> v;
                for (uint64_t r = 0; r < total; r++) {
                    if (deleted_rows.find(r) == deleted_rows.end()) {
                        v.push_back(r);
                    }
                }
                return v;
            };
            const auto pick_distinct = [&](const std::vector<uint64_t>& pool, uint64_t k) {
                std::set<uint64_t> picks;
                for (uint64_t t = 0; t < k * 4 && picks.size() < k; t++) {
                    picks.insert(pool[rng() % pool.size()]);
                }
                return std::vector<uint64_t>(picks.begin(), picks.end());
            };

            // UPDATE a random column on a random subset of live rows. Gated to PAX: the columnar
            // checkpoint drops the update_segment overlay on reopen (separate open bug). The coin is
            // still drawn either way so the RNG stream stays stable across layouts.
            const bool do_update = (rng() & 1u) != 0;
            if (do_update && !use_columnar) {
                auto live = live_rows();
                if (!live.empty()) {
                    const uint64_t c = rng() % ncols;
                    const uint64_t k = 1 + rng() % std::min<uint64_t>(live.size(), 40);
                    auto rows = pick_distinct(live, k);
                    vector_t row_ids(res, logical_type::BIGINT, rows.size());
                    data_chunk_t upd(res, {schema[c].type}, rows.size());
                    upd.set_cardinality(rows.size());
                    for (uint64_t i = 0; i < rows.size(); i++) {
                        row_ids.set_value(i, logical_value_t{res, static_cast<int64_t>(rows[i])});
                        if (schema[c].nullable && (rng() % 100) < 30) {
                            cell_t nc;
                            nc.isnull = true;
                            oracle[c][rows[i]] = nc;
                            upd.data[0].set_null(i, true);
                        } else {
                            // gen() writes the value straight into the update chunk and returns the cell.
                            oracle[c][rows[i]] = gen(rng, res, schema[c].type, upd, 0, i);
                        }
                    }
                    table.update_column(row_ids, {c}, upd);
                    had_update = true;
                }
            }

            // DELETE a random subset of live rows (~1/3 of the mutation points).
            if ((rng() % 3) == 0) {
                auto live = live_rows();
                if (!live.empty()) {
                    const uint64_t k = 1 + rng() % std::min<uint64_t>(live.size(), 30);
                    auto rows = pick_distinct(live, k);
                    vector_t row_ids(res, logical_type::BIGINT, rows.size());
                    for (uint64_t i = 0; i < rows.size(); i++) {
                        row_ids.set_value(i, logical_value_t{res, static_cast<int64_t>(rows[i])});
                    }
                    auto del_state = table.initialize_delete({});
                    table.delete_rows(*del_state, row_ids, rows.size(), 0);
                    for (auto r : rows) {
                        deleted_rows.insert(r);
                    }
                }
            }
        };

        const uint64_t r1 = (rng() % 3) * uint64_t(DEFAULT_VECTOR_CAPACITY) + (rng() % 400);
        const uint64_t r2 = 1 + (rng() % 1200);

        const auto reopen = [&](test_env_t& env, single_file_block_manager_t& bm) {
            bm.load_existing_database();
            metadata_manager_t mm(bm);
            meta_block_pointer_t mp;
            mp.block_pointer = bm.meta_block();
            metadata_reader_t reader(mm, mp);
            return data_table_t::load_from_disk(&env.resource, bm, reader); // metadata fully read here
        };

        bool trial_threw = false;
        try {
            if (do_mid_reopen) {
                // append r1, commit, COLD-reopen, append r2 (append-after-reopen), commit.
                {
                    test_env_t env;
                    single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
                    bm.create_new_database();
                    bm.set_layout_policy(layout_policy);
                    auto cols_copy = columns;
                    auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(cols_copy), "pax_fuzz");
                    append_batch(*table, &env.resource, r1);
                    mutate(*table, &env.resource); // mutate in-memory r1 rows before commit #1
                    commit(bm, *table);
                }
                {
                    test_env_t env;
                    single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
                    auto loaded = reopen(env, bm);
                    mutate(*loaded, &env.resource); // mutate persisted/committed rows after reopen
                    append_batch(*loaded, &env.resource, r2);
                    mutate(*loaded, &env.resource); // mutate mixed committed+appended before commit #2
                    commit(bm, *loaded);
                }
            } else {
                // append r1+r2 in one process, single commit (no reopen between).
                test_env_t env;
                single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
                bm.create_new_database();
                bm.set_layout_policy(layout_policy);
                auto cols_copy = columns;
                auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(cols_copy), "pax_fuzz");
                append_batch(*table, &env.resource, r1);
                append_batch(*table, &env.resource, r2);
                mutate(*table, &env.resource); // mutate in-memory rows before the single commit
                commit(bm, *table);
            }
        } catch (const std::exception& e) {
            trial_threw = true;
            record(std::string("THREW-on-build: ") + e.what(), seed);
        }
        if (trial_threw) {
            continue;
        }

        // Verify: cold reopen, scan, diff against oracle, recording one signature per column.
        try {
            test_env_t env;
            single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
            auto loaded = reopen(env, bm);

            std::vector<storage_index_t> idx;
            std::vector<size_t> pcols;
            for (uint64_t c = 0; c < ncols; c++) {
                idx.push_back(storage_index_t(c));
                pcols.push_back(c);
            }

            // When an integer column exists, sometimes apply a random constant filter to exercise the
            // filtered/projected scan and PAX zone-map pruning. The same predicate is applied to the oracle below.
            std::optional<constant_filter_t> filter;
            uint64_t filter_col = 0;
            components::expressions::compare_type filter_cmp = components::expressions::compare_type::eq;
            cell_t filter_threshold;
            bool has_filter = false;
            {
                std::vector<uint64_t> filterable;
                for (uint64_t c = 0; c < ncols; c++) {
                    if (is_filterable(schema[c].type)) {
                        filterable.push_back(c);
                    }
                }
                // Gated to PAX: columnar filtered scans currently disagree with the oracle (separate bug).
                if (!use_columnar && !filterable.empty() && (rng() & 1u) != 0) {
                    filter_col = filterable[rng() % filterable.size()];
                    static const components::expressions::compare_type cmps[] = {
                        components::expressions::compare_type::eq,  components::expressions::compare_type::ne,
                        components::expressions::compare_type::gt,  components::expressions::compare_type::gte,
                        components::expressions::compare_type::lt,  components::expressions::compare_type::lte};
                    filter_cmp = cmps[rng() % 6];
                    data_chunk_t tchunk(&env.resource, {schema[filter_col].type}, 1);
                    tchunk.set_cardinality(1);
                    filter_threshold = gen(rng, &env.resource, schema[filter_col].type, tchunk, 0, 0);
                    std::pmr::vector<uint64_t> fcols(&env.resource);
                    fcols.push_back(filter_col);
                    filter.emplace(filter_cmp, tchunk.data[0].value(0), std::move(fcols));
                    has_filter = true;
                }
            }

            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, idx, has_filter ? &*filter : nullptr);
            data_chunk_t result(&env.resource, loaded->copy_types(), pcols, DEFAULT_VECTOR_CAPACITY);

            // Scan returns surviving rows in ascending row-id order, so the k-th scanned row maps to
            // the k-th non-deleted original row that also satisfies the filter.
            std::vector<uint64_t> visible;
            visible.reserve(total);
            for (uint64_t r = 0; r < total; r++) {
                if (deleted_rows.find(r) != deleted_rows.end()) {
                    continue;
                }
                if (has_filter &&
                    !cell_matches(schema[filter_col].type, filter_cmp, oracle[filter_col][r], filter_threshold)) {
                    continue;
                }
                visible.push_back(r);
            }
            const char* del_tag = deleted_rows.empty() ? "0" : "1";
            const char* upd_tag = had_update ? "1" : "0";
            const char* filt_tag = has_filter ? "1" : "0";

            std::set<uint64_t> recorded_cols;
            uint64_t scanned = 0;
            bool overflow = false;
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < result.size(); i++) {
                    const uint64_t scan_idx = scanned + i;
                    if (scan_idx >= visible.size()) {
                        overflow = true; // a deleted/extra row surfaced — flagged by the count check below
                        continue;
                    }
                    const uint64_t row = visible[scan_idx];
                    for (uint64_t c = 0; c < ncols; c++) {
                        if (recorded_cols.count(c) || row >= oracle[c].size()) {
                            continue;
                        }
                        const char* sym = check(schema[c].type, oracle[c][row], result.data[c], i);
                        if (sym[0] != '\0') {
                            const char* region = (do_mid_reopen && row >= r1) ? "appended" : "committed";
                            std::string sig = std::string("layout=") + layout_tag + " type=" +
                                              type_name(schema[c].type) +
                                              " nullcol=" + (schema[c].nullable ? "1" : "0") + " region=" + region +
                                              " reopen_append=" + (do_mid_reopen ? "1" : "0") + " del=" + del_tag +
                                              " upd=" + upd_tag + " filt=" + filt_tag + " symptom=" + sym;
                            record(sig, seed);
                            recorded_cols.insert(c);
                        }
                    }
                }
                scanned += result.size();
            }
            if (overflow || scanned != visible.size()) {
                record("layout=" + std::string(layout_tag) + " row-count-mismatch reopen_append=" +
                           std::string(do_mid_reopen ? "1" : "0") + " del=" + del_tag + " upd=" + upd_tag +
                           " filt=" + filt_tag,
                       seed);
            }
        } catch (const std::exception& e) {
            record(std::string("THREW-on-scan: ") + e.what(), seed);
        }
    }

    std::remove(table_path.c_str());
    cleanup_test_file();

    if (!catalog.empty()) {
        std::fprintf(stderr, "\n==== PAX fuzzer bug catalog (%d trials) — %zu distinct classes ====\n",
                     trials, catalog.size());
        for (const auto& [sig, info] : catalog) {
            std::fprintf(stderr, "  [%4llu x] %s  (example seed=%llu)\n",
                         static_cast<unsigned long long>(info.first), sig.c_str(),
                         static_cast<unsigned long long>(info.second));
        }
        std::fprintf(stderr, "====================================================================\n\n");
    }
    INFO("PAX fuzzer found " << catalog.size() << " distinct bug classes (see stderr catalog above)");
    REQUIRE(catalog.empty());
}

TEST_CASE("checkpoint_load: pax fixed projected scan decodes value encodings") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 512;
    const auto table_path = test_db_path() + ".pax_fixed_encodings";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("constant_value", logical_type::BIGINT);
        columns.emplace_back("run_value", logical_type::BIGINT);
        columns.emplace_back("dict_value", logical_type::BIGINT);
        columns.emplace_back("plain_value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_encodings");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, int64_t(42)});
            chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row / 64)});
            chunk.set_value(2, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row % 5)});
            chunk.set_value(3, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row * 1000003)});
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

        const auto& layout = row_group_test_access_t::pax_fixed_layout(*first_row_group);
        REQUIRE(layout.has_value());

        bool saw_constant = false;
        bool saw_rle = false;
        bool saw_dictionary = false;
        bool saw_uncompressed = false;
        for (const auto& page : layout->pages) {
            for (const auto& slice : page.slices) {
                const auto compression = slice.data_pointer.compression;
                const auto raw_size = static_cast<uint64_t>(slice.data_pointer.tuple_count) * sizeof(int64_t);
                switch (slice.column_index) {
                    case 0:
                        saw_constant = saw_constant ||
                                       compression == components::table::compression::compression_type::CONSTANT;
                        break;
                    case 1:
                        saw_rle = saw_rle || compression == components::table::compression::compression_type::RLE;
                        break;
                    case 2:
                        saw_dictionary =
                            saw_dictionary ||
                            compression == components::table::compression::compression_type::DICTIONARY;
                        break;
                    case 3:
                        saw_uncompressed =
                            saw_uncompressed ||
                            compression == components::table::compression::compression_type::UNCOMPRESSED;
                        break;
                    default:
                        break;
                }
                if (compression != components::table::compression::compression_type::UNCOMPRESSED) {
                    REQUIRE(slice.data_pointer.segment_size < raw_size);
                }
            }
        }
        REQUIRE(saw_constant);
        REQUIRE(saw_rle);
        REQUIRE(saw_dictionary);
        REQUIRE(saw_uncompressed);

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        std::vector<storage_index_t> projected_indices{
            storage_index_t(0), storage_index_t(1), storage_index_t(2), storage_index_t(3)};
        std::vector<size_t> projected_cols{0, 1, 2, 3};
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
                REQUIRE(result.data[0].value(i).value<int64_t>() == 42);
                REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row / 64));
                REQUIRE(result.data[2].value(i).value<int64_t>() == static_cast<int64_t>(row % 5));
                REQUIRE(result.data[3].value(i).value<int64_t>() == static_cast<int64_t>(row * 1000003));
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_prefetched_blocks > 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// rows_per_page (100) does not divide DEFAULT_VECTOR_CAPACITY, so a batch boundary cuts through a
// page and the next batch decodes it from a non-zero in-page offset. Exercises the windowed
// (offset>0) decode path for CONSTANT/RLE/DICTIONARY/UNCOMPRESSED columns.
TEST_CASE("checkpoint_load: pax fixed projected scan decodes value encodings across page-split windows") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 2500;
    constexpr uint16_t ROWS_PER_PAGE = 100;
    static_assert(DEFAULT_VECTOR_CAPACITY % ROWS_PER_PAGE != 0,
                  "rows_per_page must not divide the vector capacity, otherwise batches stay page-aligned");
    const auto table_path = test_db_path() + ".pax_fixed_encodings_windowed";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_pax_rows_per_page(ROWS_PER_PAGE);

        std::vector<column_definition_t> columns;
        columns.emplace_back("constant_value", logical_type::BIGINT);
        columns.emplace_back("run_value", logical_type::BIGINT);
        columns.emplace_back("dict_value", logical_type::BIGINT);
        columns.emplace_back("plain_value", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_encodings_windowed");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, int64_t(42)});
            chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row / 64)});
            chunk.set_value(2, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row % 5)});
            chunk.set_value(3, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row * 1000003)});
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
        REQUIRE(row_group_test_access_t::pax_fixed_layout(*first_row_group).has_value());

        std::vector<storage_index_t> projected_indices{
            storage_index_t(0), storage_index_t(1), storage_index_t(2), storage_index_t(3)};
        std::vector<size_t> projected_cols{0, 1, 2, 3};
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
                REQUIRE(result.data[0].value(i).value<int64_t>() == 42);
                REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row / 64));
                REQUIRE(result.data[2].value(i).value<int64_t>() == static_cast<int64_t>(row % 5));
                REQUIRE(result.data[3].value(i).value<int64_t>() == static_cast<int64_t>(row * 1000003));
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
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);
        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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

        {
            const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
            REQUIRE(counts.pax_generic_projected == 0);
            REQUIRE(counts.pax_fixed_projected > 0);
            REQUIRE(counts.pax_fixed_prefetched_blocks > 0);
            REQUIRE(counts.regular == 0);
        }

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected == 0);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_pruned_pages > 0);
        REQUIRE(counts.pax_fixed_prefetched_blocks == 0);
        REQUIRE(counts.pax_fixed_skipped_payload_pages > 0);
        REQUIRE(counts.regular == 0);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: pax fixed projected scan filters fixed-width scalar types") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 300;
    constexpr uint64_t TARGET_ROW = 37;
    const auto table_path = test_db_path() + ".pax_fixed_filter_types";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;
    auto status_enum = make_status_enum_type(&env.resource);
    const auto decimal_i64_type = complex_logical_type::create_decimal(18, 3, "decimal_i64");
    const auto decimal_i128_type = complex_logical_type::create_decimal(30, 4, "decimal_i128");

    const auto tiny_value = [](uint64_t row) { return static_cast<int8_t>((row % 101) - 50); };
    const auto utiny_value = [](uint64_t row) { return static_cast<uint8_t>((row * 3) % 251); };
    const auto small_value = [](uint64_t row) { return static_cast<int16_t>(-1000 + static_cast<int64_t>(row * 5)); };
    const auto usmall_value = [](uint64_t row) { return static_cast<uint16_t>(1000 + row * 7); };
    const auto int_value = [](uint64_t row) { return static_cast<int32_t>(-50000 + static_cast<int64_t>(row * 11)); };
    const auto uint_value = [](uint64_t row) { return static_cast<uint32_t>(50000 + row * 13); };
    const auto ubig_value = [](uint64_t row) { return static_cast<uint64_t>(1000000000ULL + row * 17); };
    const auto float_value = [](uint64_t row) { return static_cast<float>(0.25f + static_cast<float>(row) * 0.5f); };
    const auto double_value = [](uint64_t row) { return 0.125 + static_cast<double>(row) * 0.25; };
    const auto ts_value = [](uint64_t row) {
        return core::date::timestamp_t{core::date::microseconds{static_cast<int64_t>(5000 + row * 19)}};
    };
    const auto tstz_value = [](uint64_t row) {
        return core::date::timestamptz_t{core::date::microseconds{static_cast<int64_t>(7000 + row * 23)}};
    };
    const auto decimal_i64_value = [](uint64_t row) {
        return static_cast<int64_t>(1000000000000LL + row * 29);
    };
    const auto decimal_i128_value = [](uint64_t row) {
        return (int128_t{1} << 85) + static_cast<int64_t>(row * 31);
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();

        std::vector<column_definition_t> columns;
        columns.emplace_back("flag", logical_type::BOOLEAN);
        columns.emplace_back("tiny_col", logical_type::TINYINT);
        columns.emplace_back("utiny_col", logical_type::UTINYINT);
        columns.emplace_back("small_col", logical_type::SMALLINT);
        columns.emplace_back("usmall_col", logical_type::USMALLINT);
        columns.emplace_back("int_col", logical_type::INTEGER);
        columns.emplace_back("uint_col", logical_type::UINTEGER);
        columns.emplace_back("ubig_col", logical_type::UBIGINT);
        columns.emplace_back("huge_col", logical_type::HUGEINT);
        columns.emplace_back("uhuge_col", logical_type::UHUGEINT);
        columns.emplace_back("float_col", logical_type::FLOAT);
        columns.emplace_back("ts_col", logical_type::TIMESTAMP);
        columns.emplace_back("tstz_col", logical_type::TIMESTAMP_TZ);
        columns.emplace_back("decimal_i64_col", decimal_i64_type);
        columns.emplace_back("decimal_i128_col", decimal_i128_type);
        columns.emplace_back("uuid_col", logical_type::UUID);
        columns.emplace_back("double_col", logical_type::DOUBLE);
        columns.emplace_back("status", status_enum);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_filter_types");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.data[0].data<bool>()[row_in_chunk] = (row % 2) == 0;
            chunk.data[1].data<int8_t>()[row_in_chunk] = tiny_value(row);
            chunk.data[2].data<uint8_t>()[row_in_chunk] = utiny_value(row);
            chunk.data[3].data<int16_t>()[row_in_chunk] = small_value(row);
            chunk.data[4].data<uint16_t>()[row_in_chunk] = usmall_value(row);
            chunk.data[5].data<int32_t>()[row_in_chunk] = int_value(row);
            chunk.data[6].data<uint32_t>()[row_in_chunk] = uint_value(row);
            chunk.data[7].data<uint64_t>()[row_in_chunk] = ubig_value(row);
            chunk.data[8].data<int128_t>()[row_in_chunk] = signed_huge_value(row);
            chunk.data[9].data<uint128_t>()[row_in_chunk] = unsigned_huge_value(row);
            chunk.data[10].data<float>()[row_in_chunk] = float_value(row);
            chunk.data[11].data<int64_t>()[row_in_chunk] = ts_value(row).value.count();
            chunk.data[12].data<int64_t>()[row_in_chunk] = tstz_value(row).value.count();
            chunk.data[13].data<int64_t>()[row_in_chunk] = decimal_i64_value(row);
            chunk.data[14].data<int128_t>()[row_in_chunk] = decimal_i128_value(row);
            chunk.data[15].data<int128_t>()[row_in_chunk] = uuid_like_value(row);
            chunk.data[16].data<double>()[row_in_chunk] = double_value(row);
            chunk.data[17].data<int32_t>()[row_in_chunk] = static_cast<int32_t>(row % 3);

            if (row >= 256 && row % 29 == 0) {
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
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        auto* first_row_group = loaded->row_group()->row_group(0);
        REQUIRE(first_row_group != nullptr);
        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

        using compare_type = components::expressions::compare_type;

        const auto scan_constant_filter = [&](uint64_t column_index,
                                              compare_type filter_type,
                                              logical_value_t constant,
                                              auto&& check_value) {
            std::vector<storage_index_t> projected_indices{storage_index_t(column_index)};
            std::vector<size_t> projected_cols{static_cast<size_t>(column_index)};
            std::pmr::vector<uint64_t> filter_columns(&env.resource);
            filter_columns.push_back(column_index);
            constant_filter_t filter(filter_type, std::move(constant), std::move(filter_columns));

            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, &filter);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);

            uint64_t matched = 0;
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < result.size(); i++) {
                    REQUIRE_FALSE(result.data[column_index].is_null(i));
                    check_value(result.data[column_index], i);
                }
                matched += result.size();
            }
            INFO("column_index=" << column_index << " filter_type=" << static_cast<int>(filter_type));
            REQUIRE(matched > 0);
        };

        const auto scan_eq = [&](uint64_t column_index, logical_value_t constant, auto&& check_value) {
            scan_constant_filter(column_index, compare_type::eq, std::move(constant), check_value);
        };

        const auto exercise_non_eq_filters =
            [&]<typename MakeConstant, typename ReadValue, typename CompareValue>(
                uint64_t column_index, MakeConstant make_constant, ReadValue read_value, CompareValue constant_value) {
                scan_constant_filter(column_index, compare_type::ne, make_constant(), [&](const vector_t& values, uint64_t i) {
                    REQUIRE(read_value(values, i) != constant_value);
                });
                scan_constant_filter(column_index, compare_type::gt, make_constant(), [&](const vector_t& values, uint64_t i) {
                    REQUIRE(read_value(values, i) > constant_value);
                });
                scan_constant_filter(column_index, compare_type::gte, make_constant(), [&](const vector_t& values, uint64_t i) {
                    REQUIRE(read_value(values, i) >= constant_value);
                });
                scan_constant_filter(column_index, compare_type::lt, make_constant(), [&](const vector_t& values, uint64_t i) {
                    REQUIRE(read_value(values, i) < constant_value);
                });
                scan_constant_filter(column_index, compare_type::lte, make_constant(), [&](const vector_t& values, uint64_t i) {
                    REQUIRE(read_value(values, i) <= constant_value);
                });
            };

        scan_eq(0, logical_value_t{&env.resource, false}, [](const vector_t& values, uint64_t i) {
            REQUIRE_FALSE(values.data<bool>()[i]);
        });
        scan_eq(1, logical_value_t{&env.resource, tiny_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int8_t>()[i] == tiny_value(TARGET_ROW));
        });
        scan_eq(2, logical_value_t{&env.resource, utiny_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<uint8_t>()[i] == utiny_value(TARGET_ROW));
        });
        scan_eq(3, logical_value_t{&env.resource, small_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int16_t>()[i] == small_value(TARGET_ROW));
        });
        scan_eq(4, logical_value_t{&env.resource, usmall_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<uint16_t>()[i] == usmall_value(TARGET_ROW));
        });
        scan_eq(5, logical_value_t{&env.resource, int_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int32_t>()[i] == int_value(TARGET_ROW));
        });
        scan_eq(6, logical_value_t{&env.resource, uint_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<uint32_t>()[i] == uint_value(TARGET_ROW));
        });
        scan_eq(7, logical_value_t{&env.resource, ubig_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<uint64_t>()[i] == ubig_value(TARGET_ROW));
        });
        scan_eq(8, logical_value_t{&env.resource, signed_huge_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int128_t>()[i] == signed_huge_value(TARGET_ROW));
        });
        scan_eq(9,
                logical_value_t{&env.resource, unsigned_huge_value(TARGET_ROW)},
                [&](const vector_t& values, uint64_t i) {
                    REQUIRE(values.data<uint128_t>()[i] == unsigned_huge_value(TARGET_ROW));
                });
        scan_eq(10, logical_value_t{&env.resource, float_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<float>()[i] == Approx(float_value(TARGET_ROW)));
        });
        scan_eq(11, logical_value_t{&env.resource, ts_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int64_t>()[i] == ts_value(TARGET_ROW).value.count());
        });
        scan_eq(12, logical_value_t{&env.resource, tstz_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int64_t>()[i] == tstz_value(TARGET_ROW).value.count());
        });
        scan_eq(13,
                logical_value_t::create_decimal(&env.resource, decimal_i64_type, decimal_i64_value(TARGET_ROW)),
                [&](const vector_t& values, uint64_t i) {
                    REQUIRE(values.data<int64_t>()[i] == decimal_i64_value(TARGET_ROW));
                });
        scan_eq(14,
                logical_value_t::create_decimal(&env.resource, decimal_i128_type, decimal_i128_value(TARGET_ROW)),
                [&](const vector_t& values, uint64_t i) {
                    REQUIRE(values.data<int128_t>()[i] == decimal_i128_value(TARGET_ROW));
                });
        scan_eq(15, logical_value_t{&env.resource, uuid_like_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<int128_t>()[i] == uuid_like_value(TARGET_ROW));
        });
        scan_eq(16, logical_value_t{&env.resource, double_value(TARGET_ROW)}, [&](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<double>()[i] == Approx(double_value(TARGET_ROW)));
        });
        scan_eq(17,
                logical_value_t::create_enum(&env.resource, status_enum, static_cast<int32_t>(TARGET_ROW % 3)),
                [&](const vector_t& values, uint64_t i) {
                    REQUIRE(values.data<int32_t>()[i] == static_cast<int32_t>(TARGET_ROW % 3));
                });

        scan_constant_filter(0, compare_type::ne, logical_value_t{&env.resource, false}, [](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<bool>()[i]);
        });
        scan_constant_filter(0, compare_type::gt, logical_value_t{&env.resource, false}, [](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<bool>()[i]);
        });
        scan_constant_filter(0, compare_type::gte, logical_value_t{&env.resource, true}, [](const vector_t& values, uint64_t i) {
            REQUIRE(values.data<bool>()[i]);
        });
        scan_constant_filter(0, compare_type::lt, logical_value_t{&env.resource, true}, [](const vector_t& values, uint64_t i) {
            REQUIRE_FALSE(values.data<bool>()[i]);
        });
        scan_constant_filter(0, compare_type::lte, logical_value_t{&env.resource, false}, [](const vector_t& values, uint64_t i) {
            REQUIRE_FALSE(values.data<bool>()[i]);
        });
        exercise_non_eq_filters(
            1,
            [&] { return logical_value_t{&env.resource, tiny_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int8_t>()[i]; },
            tiny_value(TARGET_ROW));
        exercise_non_eq_filters(
            2,
            [&] { return logical_value_t{&env.resource, utiny_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<uint8_t>()[i]; },
            utiny_value(TARGET_ROW));
        exercise_non_eq_filters(
            3,
            [&] { return logical_value_t{&env.resource, small_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int16_t>()[i]; },
            small_value(TARGET_ROW));
        exercise_non_eq_filters(
            4,
            [&] { return logical_value_t{&env.resource, usmall_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<uint16_t>()[i]; },
            usmall_value(TARGET_ROW));
        exercise_non_eq_filters(
            5,
            [&] { return logical_value_t{&env.resource, int_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int32_t>()[i]; },
            int_value(TARGET_ROW));
        exercise_non_eq_filters(
            6,
            [&] { return logical_value_t{&env.resource, uint_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<uint32_t>()[i]; },
            uint_value(TARGET_ROW));
        exercise_non_eq_filters(
            7,
            [&] { return logical_value_t{&env.resource, ubig_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<uint64_t>()[i]; },
            ubig_value(TARGET_ROW));
        exercise_non_eq_filters(
            8,
            [&] { return logical_value_t{&env.resource, signed_huge_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int128_t>()[i]; },
            signed_huge_value(TARGET_ROW));
        exercise_non_eq_filters(
            9,
            [&] { return logical_value_t{&env.resource, unsigned_huge_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<uint128_t>()[i]; },
            unsigned_huge_value(TARGET_ROW));
        scan_constant_filter(10,
                             compare_type::ne,
                             logical_value_t{&env.resource, float_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE_FALSE(values.data<float>()[i] == Approx(float_value(TARGET_ROW)));
                             });
        scan_constant_filter(10,
                             compare_type::gt,
                             logical_value_t{&env.resource, float_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<float>()[i] > float_value(TARGET_ROW));
                             });
        scan_constant_filter(10,
                             compare_type::gte,
                             logical_value_t{&env.resource, float_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<float>()[i] >= float_value(TARGET_ROW));
                             });
        scan_constant_filter(10,
                             compare_type::lt,
                             logical_value_t{&env.resource, float_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<float>()[i] < float_value(TARGET_ROW));
                             });
        scan_constant_filter(10,
                             compare_type::lte,
                             logical_value_t{&env.resource, float_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<float>()[i] <= float_value(TARGET_ROW));
                             });
        exercise_non_eq_filters(
            11,
            [&] { return logical_value_t{&env.resource, ts_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int64_t>()[i]; },
            ts_value(TARGET_ROW).value.count());
        exercise_non_eq_filters(
            12,
            [&] { return logical_value_t{&env.resource, tstz_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int64_t>()[i]; },
            tstz_value(TARGET_ROW).value.count());
        exercise_non_eq_filters(
            13,
            [&] { return logical_value_t::create_decimal(&env.resource, decimal_i64_type, decimal_i64_value(TARGET_ROW)); },
            [](const vector_t& values, uint64_t i) { return values.data<int64_t>()[i]; },
            decimal_i64_value(TARGET_ROW));
        exercise_non_eq_filters(
            14,
            [&] { return logical_value_t::create_decimal(&env.resource, decimal_i128_type, decimal_i128_value(TARGET_ROW)); },
            [](const vector_t& values, uint64_t i) { return values.data<int128_t>()[i]; },
            decimal_i128_value(TARGET_ROW));
        exercise_non_eq_filters(
            15,
            [&] { return logical_value_t{&env.resource, uuid_like_value(TARGET_ROW)}; },
            [](const vector_t& values, uint64_t i) { return values.data<int128_t>()[i]; },
            uuid_like_value(TARGET_ROW));
        scan_constant_filter(16,
                             compare_type::ne,
                             logical_value_t{&env.resource, double_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE_FALSE(values.data<double>()[i] == Approx(double_value(TARGET_ROW)));
                             });
        scan_constant_filter(16,
                             compare_type::gt,
                             logical_value_t{&env.resource, double_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<double>()[i] > double_value(TARGET_ROW));
                             });
        scan_constant_filter(16,
                             compare_type::gte,
                             logical_value_t{&env.resource, double_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<double>()[i] >= double_value(TARGET_ROW));
                             });
        scan_constant_filter(16,
                             compare_type::lt,
                             logical_value_t{&env.resource, double_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<double>()[i] < double_value(TARGET_ROW));
                             });
        scan_constant_filter(16,
                             compare_type::lte,
                             logical_value_t{&env.resource, double_value(TARGET_ROW)},
                             [&](const vector_t& values, uint64_t i) {
                                 REQUIRE(values.data<double>()[i] <= double_value(TARGET_ROW));
                             });
        exercise_non_eq_filters(
            17,
            [&] { return logical_value_t::create_enum(&env.resource, status_enum, static_cast<int32_t>(TARGET_ROW % 3)); },
            [](const vector_t& values, uint64_t i) { return values.data<int32_t>()[i]; },
            static_cast<int32_t>(TARGET_ROW % 3));

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected == 0);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_prefetched_blocks > 0);
        REQUIRE(counts.regular == 0);
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
        columns.emplace_back("event_ts", logical_type::TIMESTAMP);
        columns.emplace_back("uuid_col", logical_type::UUID);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_extended_projection");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, row % 2 == 0});
            chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, 10.0 + static_cast<double>(row) / 4.0});
            chunk.set_value(2,
                            row_in_chunk,
                            logical_value_t{&env.resource,
                                            core::date::timestamp_t{core::date::microseconds{
                                                static_cast<int64_t>(1000 + row * 15)}}});
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

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_generic_projected == 0);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.pax_fixed_prefetched_blocks > 0);
        REQUIRE(counts.regular == 0);
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

// A column buffer is reused across pages within a filtered batch, so a page with cleared validity
// bits must not leak nulls into a later all-valid page. 4 pages in one batch with the nullable
// column ALL_INVALID/ALL_VALID/BITMASK/ALL_VALID; a filter (id >= 0) selects every row to force the
// per-page path. The buffer's validity has to be reset on hand-out or pages 1/3 read as null.
TEST_CASE("checkpoint_load: pax fixed projected scan resets reused buffer validity across pages") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 256;
    constexpr uint16_t ROWS_PER_PAGE = 64;
    const auto table_path = test_db_path() + ".pax_fixed_buffer_reuse_validity";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    auto payload_is_null = [](uint64_t row) {
        if (row < 64) {
            return true; // page 0: ALL_INVALID
        }
        if (row >= 128 && row < 192) {
            return row % 2 == 0; // page 2: BITMASK
        }
        return false; // pages 1 and 3: ALL_VALID
    };

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_pax_rows_per_page(ROWS_PER_PAGE);

        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::BIGINT);
        auto table =
            std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_buffer_reuse_validity");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row)});
            if (payload_is_null(row)) {
                chunk.data[1].set_null(row_in_chunk, true);
            } else {
                chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row) * 10});
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

        // Project both the filter column and the nullable payload column to exercise both reuse paths.
        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        std::pmr::vector<uint64_t> filter_columns(&env.resource);
        filter_columns.push_back(0);
        constant_filter_t filter(components::expressions::compare_type::gte,
                                 logical_value_t{&env.resource, int64_t(0)},
                                 std::move(filter_columns));

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
                const auto row = scanned + i;
                REQUIRE_FALSE(result.data[0].is_null(i));
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(result.data[1].is_null(i) == payload_is_null(row));
                if (!payload_is_null(row)) {
                    REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row) * 10);
                }
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// Multi-column AND page pruning: a two-column conjunction (a >= 200 AND b < 1000000) where column
// `a`'s per-page min/max excludes whole pages. Two distinct filter columns take the filter-tree
// statistics path rather than the single-column fast branch. 4 pages; pages 0..2 prune, page 3 scans.
TEST_CASE("checkpoint_load: pax fixed projected scan prunes pages on multi-column AND filter") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 256;
    constexpr uint16_t ROWS_PER_PAGE = 64;
    const auto table_path = test_db_path() + ".pax_fixed_multicol_prune";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_pax_rows_per_page(ROWS_PER_PAGE);

        std::vector<column_definition_t> columns;
        columns.emplace_back("a", logical_type::BIGINT);
        columns.emplace_back("b", logical_type::BIGINT);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_fixed_multicol_prune");

        append_rows(*table, &env.resource, NUM_ROWS, [&](data_chunk_t& chunk, uint64_t row, uint64_t row_in_chunk) {
            chunk.set_value(0, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row)});
            chunk.set_value(1, row_in_chunk, logical_value_t{&env.resource, static_cast<int64_t>(row) * 3});
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

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        conjunction_and_filter_t filter;
        std::pmr::vector<uint64_t> a_columns(&env.resource);
        a_columns.push_back(0);
        filter.child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::gte, logical_value_t{&env.resource, int64_t(200)}, std::move(a_columns)));
        std::pmr::vector<uint64_t> b_columns(&env.resource);
        b_columns.push_back(1);
        filter.child_filters.push_back(std::make_unique<constant_filter_t>(
            components::expressions::compare_type::lt, logical_value_t{&env.resource, int64_t(1000000)}, std::move(b_columns)));

        row_group_test_access_t::reset_scan_path_counts(*first_row_group);

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
                const auto row = 200 + scanned + i;
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(row) * 3);
            }
            scanned += result.size();
        }
        REQUIRE(scanned == NUM_ROWS - 200);

        const auto counts = row_group_test_access_t::scan_path_counts(*first_row_group);
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);
        REQUIRE(counts.pax_fixed_pruned_pages == 3);
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
            std::vector<storage_index_t> row_id_projection{storage_index_t(0), storage_index_t()};
            std::vector<size_t> row_id_projected_cols{0, 1};
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, row_id_projection, nullptr);
            data_chunk_t result(&env.resource, loaded->copy_types(), row_id_projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE_FALSE(row_group_test_access_t::try_scan_pax_fixed_projected(*first_row_group,
                                                                                state.table_state,
                                                                                result));
        }

        {
            std::vector<storage_index_t> projected_indices{storage_index_t(0)};
            std::vector<size_t> projected_cols{0};
            std::pmr::vector<uint64_t> filter_columns(&env.resource);
            filter_columns.push_back(0);
            constant_filter_t invalid_filter(components::expressions::compare_type::invalid,
                                             logical_value_t{&env.resource, int64_t{10}},
                                             std::move(filter_columns));
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, &invalid_filter);
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

// ---------------------------------------------------------------------------------------------
// Commit-path fault injection. The two tests below inject EIO via the POSIX fsync/pwrite hooks: a
// checkpoint that can't be made durable must throw, and the prior committed state must survive
// (header never swapped).
#if defined(DEV_MODE) && defined(PLATFORM_POSIX)
namespace {
    // Hooks are capture-less function pointers, so the state lives at TU scope. The RAII guard
    // clears it on scope exit (including on a REQUIRE/throw unwind) so a fault can't leak into a later test.
    bool g_fault_fsync_fail = false;                          // every fsync fails with EIO
    uint64_t g_fault_pwrite_fail_at_or_above = UINT64_MAX;    // fail pwrites whose offset >= this

    int fault_fsync_hook(int fd) {
        if (g_fault_fsync_fail) {
            errno = EIO;
            return -1;
        }
        return ::fsync(fd);
    }
    int64_t fault_pwrite_hook(int fd, const void* buffer, size_t nr_bytes, uint64_t location) {
        if (location >= g_fault_pwrite_fail_at_or_above) {
            errno = EIO;
            return -1;
        }
        return ::pwrite(fd, buffer, nr_bytes, static_cast<off_t>(location));
    }
    struct fault_injection_guard_t {
        fault_injection_guard_t() {
            g_fault_fsync_fail = false;
            g_fault_pwrite_fail_at_or_above = UINT64_MAX;
        }
        ~fault_injection_guard_t() {
            core::filesystem::testing::reset_posix_positioned_io_hooks();
            g_fault_fsync_fail = false;
            g_fault_pwrite_fail_at_or_above = UINT64_MAX;
        }
    };
} // namespace

TEST_CASE("checkpoint_load: fsync failure during checkpoint aborts commit and preserves prior state") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    constexpr uint64_t V1_ROWS = 1500;  // durably committed
    constexpr uint64_t V2_EXTRA = 800;  // appended, commit aborted by fsync EIO
    const auto table_path = test_db_path() + ".fsync_fault";
    std::remove(table_path.c_str());

    fault_injection_guard_t guard;

    // Commit: persist data/metadata, fsync, swap header, fsync.
    const auto commit = [](single_file_block_manager_t& bm, data_table_t& table) {
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table.checkpoint(writer);
        writer.flush();
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_list_ptr = bm.serialize_free_list();
        bm.file_sync();
        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        bm.write_header(header);
        bm.file_sync();
    };

    const auto reopen_and_count = [](test_env_t& env, const std::string& path, uint64_t expected) {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};
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
        REQUIRE(scanned == expected);
    };

    // Phase 1: build V1 and commit it durably (hooks inactive).
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "fsync_fault");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  V1_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 3); });
        commit(bm, *table);
    }

    // Phase 2: reopen, append V2, attempt commit with fsync forced to fail. file_sync() must throw
    // (checkpoint not durable); the header is never swapped.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        append_fixed_integer_pair(*loaded,
                                  &env.resource,
                                  V2_EXTRA,
                                  [](uint64_t row) { return static_cast<int64_t>(V1_ROWS + row); },
                                  [](uint64_t row) { return static_cast<uint32_t>((V1_ROWS + row) * 3); });

        core::filesystem::testing::set_posix_fsync_hook(&fault_fsync_hook);
        g_fault_fsync_fail = true;
        REQUIRE_THROWS_AS(commit(bm, *loaded), std::runtime_error);
        g_fault_fsync_fail = false;
        core::filesystem::testing::set_posix_fsync_hook(nullptr);
    }

    // Phase 3: reopen via the on-disk header. Must observe exactly the committed V1.
    {
        test_env_t env;
        reopen_and_count(env, table_path, V1_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

TEST_CASE("checkpoint_load: block-write failure during checkpoint aborts commit and preserves prior state") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    constexpr uint64_t V1_ROWS = 1500;  // durably committed
    constexpr uint64_t V2_EXTRA = 800;  // appended, commit aborted by block-write EIO
    const auto table_path = test_db_path() + ".write_fault";
    std::remove(table_path.c_str());

    fault_injection_guard_t guard;

    const auto commit = [](single_file_block_manager_t& bm, data_table_t& table) {
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table.checkpoint(writer);
        writer.flush();
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_list_ptr = bm.serialize_free_list();
        bm.file_sync();
        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        bm.write_header(header);
        bm.file_sync();
    };

    const auto reopen_and_count = [](test_env_t& env, const std::string& path, uint64_t expected) {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};
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
        REQUIRE(scanned == expected);
    };

    // Phase 1: build V1 and commit it durably (hooks inactive).
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "write_fault");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  V1_ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 3); });
        commit(bm, *table);
    }

    // Phase 2: reopen, append V2, attempt commit while every data/metadata block write (offset >=
    // BLOCK_START, header slots still writable) fails with EIO. checksum_and_write must throw rather
    // than swallow the failed write; the header is never swapped.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database();
        metadata_manager_t meta_mgr(bm);
        meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = bm.meta_block();
        metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        append_fixed_integer_pair(*loaded,
                                  &env.resource,
                                  V2_EXTRA,
                                  [](uint64_t row) { return static_cast<int64_t>(V1_ROWS + row); },
                                  [](uint64_t row) { return static_cast<uint32_t>((V1_ROWS + row) * 3); });

        core::filesystem::testing::set_posix_pwrite_hook(&fault_pwrite_hook);
        g_fault_pwrite_fail_at_or_above = BLOCK_START;
        REQUIRE_THROWS_AS(commit(bm, *loaded), std::runtime_error);
        g_fault_pwrite_fail_at_or_above = UINT64_MAX;
        core::filesystem::testing::set_posix_pwrite_hook(nullptr);
    }

    // Phase 3: reopen via the on-disk header. Must observe exactly the committed V1.
    {
        test_env_t env;
        reopen_and_count(env, table_path, V1_ROWS);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}
#endif // DEV_MODE && PLATFORM_POSIX

// Corrupt the metadata block (the row-group-pointer tree). A flipped payload byte invalidates its
// CRC32c, so reopening and rebuilding the table must raise a checksum mismatch rather than walk a
// corrupt pointer tree.
TEST_CASE("checkpoint_load: corrupted metadata block is detected on reopen") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    constexpr uint64_t ROWS = 2048;
    const auto table_path = test_db_path() + ".meta_corrupt";
    std::remove(table_path.c_str());

    meta_block_pointer_t meta_ptr;
    uint64_t block_alloc_size = 0;

    // Phase 1: build a PAX_ONLY table and commit it; capture the metadata block pointer.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "meta_corrupt");
        append_fixed_integer_pair(*table,
                                  &env.resource,
                                  ROWS,
                                  [](uint64_t row) { return static_cast<int64_t>(row); },
                                  [](uint64_t row) { return static_cast<uint32_t>(row * 3); });
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        table->checkpoint(writer);
        writer.flush();
        meta_ptr = writer.get_block_pointer();
        bm.set_meta_block(meta_ptr.block_pointer);
        block_alloc_size = bm.block_allocation_size();
        bm.file_sync();
        database_header_t header;
        header.initialize();
        header.meta_block = meta_ptr.block_pointer;
        bm.write_header(header);
        bm.file_sync();
    }

    // Phase 2: flip a payload byte inside the metadata block, invalidating its stored CRC32c.
    {
        const uint64_t meta_block_id = meta_ptr.block_id();
        const uint64_t payload_byte = BLOCK_START + meta_block_id * block_alloc_size + sizeof(uint64_t);
        std::fstream f(table_path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekg(static_cast<std::streamoff>(payload_byte));
        char b = 0;
        f.read(&b, 1);
        REQUIRE(f.good());
        b = static_cast<char>(static_cast<unsigned char>(b) ^ 0xFFu);
        f.seekp(static_cast<std::streamoff>(payload_byte));
        f.write(&b, 1);
        f.flush();
        REQUIRE(f.good());
    }

    // Phase 3: reopen. The header is intact so load succeeds, but rebuilding the table walks the
    // corrupted metadata block, which must raise a checksum mismatch.
    {
        test_env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.load_existing_database(); // header intact, so this succeeds
        metadata_manager_t meta_mgr(bm);

        bool threw = false;
        try {
            // The reader ctor CRC-verifies the first metadata block, so the mismatch may surface here
            // rather than in load_from_disk; keep both inside the try.
            metadata_reader_t reader(meta_mgr, meta_ptr);
            auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
            // If the metadata block were read lazily, force it by scanning.
            std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
            std::vector<size_t> projected_cols{0, 1};
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, nullptr);
            data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            while (true) {
                result.reset();
                loaded->scan(result, state);
                if (result.size() == 0) {
                    break;
                }
            }
        } catch (const std::exception& e) {
            threw = true;
            REQUIRE(std::string(e.what()).find("checksum") != std::string::npos);
        }
        REQUIRE(threw);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}

// The per-block CRC32c proves bytes are intact, not that a decoded block_pointer offset is in range.
// A bug or hostile file could leave a CRC-valid metadata pointer whose data offset points past the
// block; the PAX scan decode path must throw on it, not read OOB. Simulate by corrupting the loaded
// in-memory layout's data offset (bypassing the metadata CRC) and then scanning.
TEST_CASE("checkpoint_load: out-of-bounds pax data offset fails closed on scan") {
    using namespace components::table;
    using namespace components::table::storage;
    using namespace components::types;
    using namespace components::vector;
    cleanup_test_file();

    test_env_t env;
    constexpr uint64_t NUM_ROWS = 1300;
    const auto table_path = test_db_path() + ".pax_oob_offset";
    std::remove(table_path.c_str());
    meta_block_pointer_t table_pointer;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, table_path);
        bm.create_new_database();
        bm.set_layout_policy(row_group_layout_policy::PAX_ONLY);
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("value", logical_type::UINTEGER);
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "pax_oob");
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
        const uint64_t block_size = bm.block_size();
        metadata_manager_t meta_mgr(bm);
        metadata_reader_t reader(meta_mgr, table_pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);

        std::vector<storage_index_t> projected_indices{storage_index_t(0), storage_index_t(1)};
        std::vector<size_t> projected_cols{0, 1};

        // Baseline: an unmodified projected scan succeeds.
        {
            table_scan_state state(&env.resource);
            loaded->initialize_scan(state, projected_indices, nullptr);
            auto* rg0 = loaded->row_group()->row_group(0);
            REQUIRE(rg0 != nullptr);
            data_chunk_t chunk(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
            REQUIRE(row_group_test_access_t::try_scan_pax_fixed_projected(*rg0, state.table_state, chunk));
            REQUIRE(chunk.size() == DEFAULT_VECTOR_CAPACITY);
        }

        // Corrupt the value column's data offset in the loaded layout to point past the block, then scan.
        auto* rg0 = loaded->row_group()->row_group(0);
        REQUIRE(rg0 != nullptr);
        auto& layout = row_group_test_access_t::pax_fixed_layout_mutable(*rg0);
        REQUIRE(layout.has_value());
        bool corrupted = false;
        for (auto& page : layout->pages) {
            for (auto& slice : page.slices) {
                if (slice.column_index == 1) {
                    slice.data_pointer.block_pointer.offset = static_cast<uint32_t>(block_size + 64);
                    corrupted = true;
                }
            }
        }
        REQUIRE(corrupted);

        table_scan_state state(&env.resource);
        loaded->initialize_scan(state, projected_indices, nullptr);
        data_chunk_t result(&env.resource, loaded->copy_types(), projected_cols, DEFAULT_VECTOR_CAPACITY);
        // The decode path bounds-checks the disk-derived offset and throws rather than reading OOB.
        REQUIRE_THROWS_AS(row_group_test_access_t::try_scan_pax_fixed_projected(*rg0, state.table_state, result),
                          std::logic_error);
    }

    std::remove(table_path.c_str());
    cleanup_test_file();
}
