// column_fetch_state fields (result_outlives_pins, fetch_error) live on the CHILD state for a
// nested column; a default-constructed child silently drops pin ownership and swallows errors.

#include <catch2/catch_test_macros.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <cstring>
#include <string>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    const std::string& nested_fetch_db_path() {
        static const std::string path = "/tmp/test_otterbrix_nested_fetch_channel_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());
        return path;
    }

    struct nested_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;
        // A real disk manager over a scratch file: what these tests need is a column without a
        // catalog, not a storage layer that cannot do I/O.
        tstorage::single_file_block_manager_t block_manager;

        nested_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, nested_fetch_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~nested_env_t() { std::remove(nested_fetch_db_path().c_str()); }
    };

    enum class shape_t
    {
        STRUCT_OF_STRING,
        STRUCT_OF_STRUCT_STRING,
        STRUCT_OF_LIST_STRING
    };

    // Every field needs an alias: struct_column_data_t refuses an unnamed struct field.
    complex_logical_type field_type(nested_env_t& env, shape_t shape) {
        switch (shape) {
            case shape_t::STRUCT_OF_STRING:
                return complex_logical_type(logical_type::STRING_LITERAL, "payload");
            case shape_t::STRUCT_OF_STRUCT_STRING: {
                std::pmr::vector<complex_logical_type> inner_fields(&env.resource);
                inner_fields.emplace_back(logical_type::STRING_LITERAL, "payload");
                return complex_logical_type::create_struct("payload_box", inner_fields, "inner");
            }
            case shape_t::STRUCT_OF_LIST_STRING:
                return complex_logical_type::create_list(complex_logical_type{logical_type::STRING_LITERAL}, "items");
        }
        return complex_logical_type{logical_type::NA};
    }

    complex_logical_type outer_struct_type(nested_env_t& env, shape_t shape) {
        std::pmr::vector<complex_logical_type> fields(&env.resource);
        fields.push_back(field_type(env, shape));
        return complex_logical_type::create_struct("row_box", fields);
    }

    logical_value_t one_row_value(nested_env_t& env, shape_t shape, const std::string& big) {
        auto outer = outer_struct_type(env, shape);
        auto field = field_type(env, shape);
        std::vector<logical_value_t> members;
        switch (shape) {
            case shape_t::STRUCT_OF_STRING:
                members.emplace_back(&env.resource, big);
                break;
            case shape_t::STRUCT_OF_STRUCT_STRING: {
                std::vector<logical_value_t> inner_members;
                inner_members.emplace_back(&env.resource, big);
                members.push_back(logical_value_t::create_struct(&env.resource, field, inner_members));
                break;
            }
            case shape_t::STRUCT_OF_LIST_STRING: {
                std::vector<logical_value_t> elements;
                elements.emplace_back(&env.resource, big);
                members.push_back(logical_value_t::create_list(&env.resource,
                                                               complex_logical_type{logical_type::STRING_LITERAL},
                                                               elements));
                break;
            }
        }
        return logical_value_t::create_struct(&env.resource, outer, members);
    }

    struct built_table_t {
        std::unique_ptr<data_table_t> table;
        column_segment_t* leaf_segment{nullptr};
    };

    built_table_t build(nested_env_t& env, tstorage::block_manager_t& bm, shape_t shape, const std::string& big) {
        built_table_t out;
        std::vector<column_definition_t> columns;
        columns.emplace_back("s", outer_struct_type(env, shape));
        out.table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "nested_fetch");

        auto types = out.table->copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, one_row_value(env, shape, big));

        table_append_state state(&env.resource);
        REQUIRE_FALSE(out.table->append_lock(state).has_error());
        REQUIRE_FALSE(out.table->initialize_append(state).has_error());
        REQUIRE_FALSE(out.table->append(chunk, state).has_error());

        REQUIRE(state.append_state.states != nullptr);
        auto& struct_append = state.append_state.states[0];
        REQUIRE(struct_append.current == nullptr); // struct owns no segments; non-null means reading the wrong one
        REQUIRE(struct_append.child_appends.size() == 2);
        auto* field_append = &struct_append.child_appends[1];
        if (shape != shape_t::STRUCT_OF_STRING) {
            REQUIRE(field_append->child_appends.size() == 2); // nested field: leaf sits through child_appends[1] again
            if (shape == shape_t::STRUCT_OF_STRUCT_STRING) {
                REQUIRE(field_append->current == nullptr);
            }
            field_append = &field_append->child_appends[1];
        }
        out.leaf_segment = field_append->current;
        REQUIRE(out.leaf_segment != nullptr);
        REQUIRE(out.leaf_segment->type.to_physical_type() == physical_type::STRING);
        out.table->finalize_append(state, transaction_data{0, 0});
        return out;
    }

    // [uint32 dict_size][uint32 dict_end] at the segment start, one 16-byte (block id, offset) marker
    // packed at dict_end - dict_size; the REQUIREs turn a layout change into a loud failure, not corruption.
    void overwrite_only_overflow_marker(nested_env_t& env, column_segment_t& segment, uint64_t new_block_id) {
        auto pinned = env.buffer_manager.pin(segment.block);
        REQUIRE_FALSE(pinned.has_error());
        auto* base = pinned.value().ptr() + segment.block_offset();
        uint32_t dict_size = 0;
        uint32_t dict_end = 0;
        std::memcpy(&dict_size, base, sizeof(uint32_t));
        std::memcpy(&dict_end, base + sizeof(uint32_t), sizeof(uint32_t));
        REQUIRE(dict_size == 16);
        auto* marker = base + dict_end - dict_size;
        uint64_t named_block = 0;
        std::memcpy(&named_block, marker, sizeof(uint64_t));
        REQUIRE(named_block >= tstorage::MAXIMUM_BLOCK);
        std::memcpy(marker, &new_block_id, sizeof(uint64_t));
    }

    // Simulates what the buffer pool may do to an unpinned block: reuse its memory. A copied view
    // won't notice, a borrowed view reads the poison -- a deterministic stand-in for a flaky eviction race.
    void poison_overflow_blocks(nested_env_t& env, column_segment_t& segment) {
        auto* raw_state = segment.segment_state();
        REQUIRE(raw_state != nullptr);
        auto& string_state = raw_state->cast<uncompressed_string_segment_state>();
        REQUIRE_FALSE(string_state.overflow_blocks.empty());
        for (auto& entry : string_state.overflow_blocks) {
            REQUIRE(entry.second != nullptr);
            auto pinned = env.buffer_manager.pin(entry.second->block);
            REQUIRE_FALSE(pinned.has_error());
            std::memset(pinned.value().ptr(), 0x5A, entry.second->size);
        }
    }

    std::string_view leaf_view(data_chunk_t& out, shape_t shape) {
        auto& struct_vec = out.data[0];
        auto& field = *struct_vec.entries()[0];
        switch (shape) {
            case shape_t::STRUCT_OF_STRING:
                return field.data<std::string_view>()[0];
            case shape_t::STRUCT_OF_STRUCT_STRING:
                return field.entries()[0]->data<std::string_view>()[0];
            case shape_t::STRUCT_OF_LIST_STRING:
                return field.entry().data<std::string_view>()[0];
        }
        return {};
    }

    core::result_wrapper_t<bool> fetch_row_zero(nested_env_t& env, data_table_t& table, data_chunk_t& out) {
        components::storage::table_storage_adapter_t adapter(table, &env.resource);
        components::storage::storage_t& storage = adapter;
        vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
        row_ids.data<int64_t>()[0] = 0;
        return storage.fetch(out, row_ids, 1, {}, transaction_data{}, fetch_visibility_t::SNAPSHOT);
    }

} // namespace

// row_group_t judges only the top-level column_scan_state, so a struct child's scan_error had no reader.
TEST_CASE("nested scan: a data_corruption raised under a struct stops the scan") {
    nested_env_t env;
    auto& bm = env.block_manager;
    const std::string big(5000, 's');

    shape_t shape = shape_t::STRUCT_OF_STRING;
    SECTION("STRUCT(payload STRING)") { shape = shape_t::STRUCT_OF_STRING; }
    SECTION("STRUCT(inner STRUCT(payload STRING))") { shape = shape_t::STRUCT_OF_STRUCT_STRING; }
    SECTION("STRUCT(items LIST(STRING))") { shape = shape_t::STRUCT_OF_LIST_STRING; }

    auto built = build(env, bm, shape, big);
    components::storage::table_storage_adapter_t adapter(*built.table, &env.resource);
    components::storage::storage_t& storage = adapter;

    {
        std::pmr::vector<data_chunk_t> batches(&env.resource);
        auto ok = storage.scan_batched(batches, nullptr, -1, nullptr, transaction_data{});
        REQUIRE_FALSE(ok.has_error());
        REQUIRE(batches.size() == 1);
        REQUIRE(batches.front().size() == 1);
        REQUIRE(leaf_view(batches.front(), shape) == big);
    }

    overwrite_only_overflow_marker(env, *built.leaf_segment, tstorage::MAXIMUM_BLOCK + 424242);

    std::pmr::vector<data_chunk_t> batches(&env.resource);
    auto scanned = storage.scan_batched(batches, nullptr, -1, nullptr, transaction_data{});
    REQUIRE(scanned.has_error());
    REQUIRE(scanned.error().type == core::error_code_t::data_corruption);
}

// The child's fetch_error needs a reader, or the adapter reports success over an empty field.
TEST_CASE("nested fetch: a data_corruption raised under a struct reaches the statement") {
    nested_env_t env;
    auto& bm = env.block_manager;
    const std::string big(5000, 'n');

    shape_t shape = shape_t::STRUCT_OF_STRING;
    SECTION("STRUCT(payload STRING)") { shape = shape_t::STRUCT_OF_STRING; }
    SECTION("STRUCT(inner STRUCT(payload STRING))") { shape = shape_t::STRUCT_OF_STRUCT_STRING; }
    SECTION("STRUCT(items LIST(STRING))") { shape = shape_t::STRUCT_OF_LIST_STRING; }

    auto built = build(env, bm, shape, big);

    {
        auto types = built.table->copy_types();
        data_chunk_t out(&env.resource, types, 1);
        auto ok = fetch_row_zero(env, *built.table, out);
        REQUIRE_FALSE(ok.has_error());
        REQUIRE(out.size() == 1);
        REQUIRE(leaf_view(out, shape) == big);
    }

    overwrite_only_overflow_marker(env, *built.leaf_segment, tstorage::MAXIMUM_BLOCK + 424242);

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    auto fetch_r = fetch_row_zero(env, *built.table, out);
    REQUIRE(fetch_r.has_error());
    REQUIRE(fetch_r.error().type == core::error_code_t::data_corruption);
}

// LIST is skipped: its element always copies via string_scan_partial -> fetch_string_owned, flag or not.
TEST_CASE("nested fetch: a big string in a struct field outlives the pins that read it") {
    nested_env_t env;
    auto& bm = env.block_manager;
    const std::string big(5000, 'p');

    shape_t shape = shape_t::STRUCT_OF_STRING;
    SECTION("STRUCT(payload STRING)") { shape = shape_t::STRUCT_OF_STRING; }
    SECTION("STRUCT(inner STRUCT(payload STRING))") { shape = shape_t::STRUCT_OF_STRUCT_STRING; }

    auto built = build(env, bm, shape, big);

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    auto fetch_r = fetch_row_zero(env, *built.table, out);
    REQUIRE_FALSE(fetch_r.has_error());
    REQUIRE(out.size() == 1);

    // Every pin from the fetch is already gone, so whatever the chunk still points at is fair game for the pool.
    poison_overflow_blocks(env, *built.leaf_segment);

    // Checked as a bool so a failure reports "false" instead of dumping poison bytes into the log.
    const auto view = leaf_view(out, shape);
    const bool intact = view == std::string_view(big);
    INFO("field length " << view.size() << ", first byte '" << (view.empty() ? '?' : view.front()) << "'");
    CHECK(intact);
}
