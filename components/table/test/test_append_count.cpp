#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/data_pointer.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/types/logical_value.hpp>
#include <core/file/local_file_system.hpp>

#include <algorithm>
#include <cstdio>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    struct env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        env_t()
            : buffer_pool(&resource, uint64_t(1) << 22, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::vector<tstorage::buffer_handle_t> exhaust_pool(env_t& env) {
        std::vector<tstorage::buffer_handle_t> held;
        while (true) {
            auto allocated =
                env.buffer_manager.allocate(tstorage::memory_tag::BASE_TABLE, env.buffer_manager.block_size(), true);
            if (allocated.has_error()) {
                break;
            }
            held.push_back(std::move(allocated.value()));
            REQUIRE(held.size() < 1024);
        }
        return held;
    }

    void checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table.checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
    }

    std::vector<tstorage::row_group_pointer_t> read_pointers(env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        reader.read_string();
        const auto col_count = reader.read<uint32_t>();
        std::pmr::vector<std::byte> spec(&env.resource);
        for (uint32_t i = 0; i < col_count; i++) {
            reader.read_string();
            const auto spec_size = reader.read<uint32_t>();
            spec.resize(spec_size);
            reader.read_data(spec.data(), spec_size);
            reader.read<uint8_t>();
            reader.read<uint32_t>();
            reader.read<uint64_t>();
            reader.read<uint64_t>();
        }
        const auto rg_count = reader.read<uint32_t>();
        std::vector<tstorage::row_group_pointer_t> out;
        for (uint32_t i = 0; i < rg_count; i++) {
            out.push_back(tstorage::row_group_pointer_t::deserialize(reader));
        }
        REQUIRE_FALSE(reader.has_error());
        return out;
    }

    uint64_t segment_rows(const tstorage::column_data_pointers_t& node) {
        uint64_t total = 0;
        for (const auto& segment : node.segments) {
            total += segment.tuple_count;
        }
        return total;
    }

    std::string describe(const tstorage::column_data_pointers_t& node) {
        std::string out = "{count=" + std::to_string(node.count) + "/segments=" + std::to_string(segment_rows(node));
        for (const auto& child : node.children) {
            out += " " + describe(child);
        }
        return out + "}";
    }

    void require_consistent(const tstorage::column_data_pointers_t& node, uint64_t rows) {
        INFO("node " << describe(node) << " expected rows " << rows);
        CHECK(node.count == rows);
        if (!node.segments.empty()) {
            CHECK(segment_rows(node) == rows);
        }
    }

    core::result_wrapper_t<std::unique_ptr<data_table_t>> reload_table(env_t& env,
                                                                       tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        return data_table_t::load_from_disk(&env.resource, bm, reader);
    }

    struct row_t {
        int64_t k;
        logical_value_t v;
    };

    uint64_t scan_rows(data_table_t& table, env_t& env, std::vector<row_t>* cells = nullptr) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, transaction_data::committed(), nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (chunk.size() == 0) {
                break;
            }
            if (cells) {
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    cells->push_back(row_t{chunk.value(0, i).value<int64_t>(), chunk.value(1, i)});
                }
            }
            seen += chunk.size();
        }
        return seen;
    }

    const column_data_t& column_of(data_table_t& table, uint64_t index) {
        auto* row_group = table.row_group()->row_group_tree()->segment_at(0);
        REQUIRE(row_group != nullptr);
        const auto* column = row_group->column_identity(index);
        REQUIRE(column != nullptr);
        return *column;
    }

    enum class shape_t
    {
        STRING,
        LIST,
        STRUCT,
        ARRAY
    };

    const char* shape_name(shape_t shape) {
        switch (shape) {
            case shape_t::STRING:
                return "string";
            case shape_t::LIST:
                return "list";
            case shape_t::STRUCT:
                return "struct";
            case shape_t::ARRAY:
                return "array";
        }
        return "?";
    }

    complex_logical_type column_type(env_t& env, shape_t shape) {
        switch (shape) {
            case shape_t::STRING:
                return complex_logical_type{logical_type::STRING_LITERAL};
            case shape_t::LIST:
                return complex_logical_type::create_list(logical_type::STRING_LITERAL);
            case shape_t::STRUCT: {
                std::pmr::vector<complex_logical_type> fields(&env.resource);
                fields.emplace_back(logical_type::STRING_LITERAL, "s");
                return complex_logical_type::create_struct("rec", fields, "rec_t");
            }
            case shape_t::ARRAY:
                return complex_logical_type::create_array(logical_type::STRING_LITERAL, 1);
        }
        return complex_logical_type{logical_type::STRING_LITERAL};
    }

    data_chunk_t one_row(env_t& env, data_table_t& table, shape_t shape, const std::string& payload, int64_t k) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, k);
        switch (shape) {
            case shape_t::STRING:
                chunk.set_value(1, 0, std::string_view{payload});
                break;
            case shape_t::LIST:
            case shape_t::ARRAY:
                chunk.set_value(1, 0, std::vector<std::string_view>{std::string_view{payload}});
                break;
            case shape_t::STRUCT: {
                std::vector<logical_value_t> fields;
                fields.emplace_back(&env.resource, payload);
                chunk.set_value(1, 0, logical_value_t::create_struct(&env.resource, types[1], fields));
                break;
            }
        }
        return chunk;
    }

    std::string cell_string(const logical_value_t& cell, shape_t shape) {
        switch (shape) {
            case shape_t::STRING:
                return std::string{cell.value<std::string_view>()};
            case shape_t::LIST:
            case shape_t::ARRAY:
                REQUIRE(cell.children().size() == 1);
                return *cell.children()[0].value<std::string*>();
            case shape_t::STRUCT:
                REQUIRE(cell.children().size() == 1);
                return std::string{cell.children()[0].value<std::string_view>()};
        }
        return {};
    }

    std::unique_ptr<data_table_t> make_table(env_t& env, tstorage::single_file_block_manager_t& bm, shape_t shape) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("k", logical_type::BIGINT);
        columns.emplace_back("v", column_type(env, shape));
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "append_count");
    }

    constexpr int64_t REFUSED_K = 7;
    constexpr int64_t GOOD_K = 8;

    core::error_t refused_append(env_t& env, data_table_t& table, shape_t shape, const std::string& payload) {
        auto chunk = one_row(env, table, shape, payload, REFUSED_K);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        auto held = exhaust_pool(env);
        INFO("held whole-block pins: " << held.size());
        auto appended = table.append(chunk, state);
        REQUIRE(appended.has_error());
        return appended.error();
    }

    void good_append(env_t& env, data_table_t& table, shape_t shape, const std::string& payload) {
        auto chunk = one_row(env, table, shape, payload, GOOD_K);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    std::string db_path(const char* tag) {
        return "/tmp/test_otterbrix_append_count_" + std::string{tag} + "_" + std::to_string(::getpid()) + ".otbx";
    }

    void run_shape(shape_t shape, bool retry) {
        const std::string path = db_path(shape_name(shape));
        std::remove(path.c_str());
        env_t env;
        const std::string big(5000, 'x');
        const uint64_t expected_rows = retry ? 1 : 0;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, shape);

            auto refusal = refused_append(env, *table, shape, big);
            INFO("refusal: " << refusal.what);
            CHECK(refusal.type == core::error_code_t::out_of_memory);
            if (retry) {
                good_append(env, *table, shape, "ok");
            }

            CHECK(column_of(*table, 1).count() == expected_rows);
            std::vector<row_t> cells;
            REQUIRE(scan_rows(*table, env, &cells) == expected_rows);
            if (retry) {
                CHECK(cells[0].k == GOOD_K);
                CHECK(cell_string(cells[0].v, shape) == "ok");
            }

            checkpoint_production(bm, *table);
            auto pointers = read_pointers(env, bm);
            REQUIRE(pointers.size() == 1);
            REQUIRE(pointers[0].data_pointers.size() == 2);
            const auto& node = pointers[0].data_pointers[1];
            INFO("persisted: row group rows " << pointers[0].tuple_count << ", column v " << describe(node));
            CHECK(pointers[0].tuple_count == expected_rows);
            require_consistent(node, expected_rows);
            for (const auto& child : node.children) {
                require_consistent(child, expected_rows);
            }
        }
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.load_existing_database().has_error());
            auto loaded = reload_table(env, bm);
            INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
            REQUIRE_FALSE(loaded.has_error());
            std::vector<row_t> cells;
            REQUIRE(scan_rows(*loaded.value(), env, &cells) == expected_rows);
            if (retry) {
                CHECK(cells[0].k == GOOD_K);
                CHECK(cell_string(cells[0].v, shape) == "ok");
            }
        }
        std::remove(path.c_str());
    }

} // namespace

TEST_CASE("append_count: STRING, refused append then checkpoint", "[append_count][string]") {
    run_shape(shape_t::STRING, false);
}
TEST_CASE("append_count: STRING, refused append then a good row", "[append_count][string]") {
    run_shape(shape_t::STRING, true);
}
TEST_CASE("append_count: LIST<STRING>, refused append then checkpoint", "[append_count][list]") {
    run_shape(shape_t::LIST, false);
}
TEST_CASE("append_count: LIST<STRING>, refused append then a good row", "[append_count][list]") {
    run_shape(shape_t::LIST, true);
}
TEST_CASE("append_count: STRUCT<STRING>, refused append then checkpoint", "[append_count][struct]") {
    run_shape(shape_t::STRUCT, false);
}
TEST_CASE("append_count: STRUCT<STRING>, refused append then a good row", "[append_count][struct]") {
    run_shape(shape_t::STRUCT, true);
}
TEST_CASE("append_count: ARRAY<STRING>, refused append then checkpoint", "[append_count][array]") {
    run_shape(shape_t::ARRAY, false);
}
TEST_CASE("append_count: ARRAY<STRING>, refused append then a good row", "[append_count][array]") {
    run_shape(shape_t::ARRAY, true);
}

namespace {

    struct held_small_t {
        std::shared_ptr<tstorage::block_handle_t> block;
        tstorage::buffer_handle_t pin;
    };

    std::vector<held_small_t> top_up_pool(env_t& env, uint64_t size, uint64_t keep) {
        std::vector<held_small_t> held;
        while (true) {
            auto block = env.buffer_manager.register_small_memory(tstorage::memory_tag::BASE_TABLE, size);
            if (block.has_error()) {
                break;
            }
            auto pinned = env.buffer_manager.pin(block.value());
            if (pinned.has_error()) {
                break;
            }
            held.push_back(held_small_t{std::move(block.value()), std::move(pinned.value())});
            REQUIRE(held.size() < 4096);
        }
        REQUIRE(held.size() >= keep);
        held.resize(held.size() - keep);
        return held;
    }

} // namespace

TEST_CASE("append_count: STRING, a chunk spanning three segments refused at its tail",
          "[append_count][string][boundary]") {
    const std::string path = db_path("boundary");
    std::remove(path.c_str());
    env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm, shape_t::STRING);
        // 1000-byte strings fill a STRING segment in ~32 rows, so 70 of them span three segments.
        const uint64_t segment_size =
            std::min<uint64_t>(bm.block_size(), DEFAULT_VECTOR_CAPACITY * column_type(env, shape_t::STRING).size());
        const uint64_t small_rows = 70;
        {
            std::vector<std::string> payloads(small_rows, std::string(1000, 'y'));
            payloads.emplace_back(5000, 'x');
            auto types = table->copy_types();
            data_chunk_t chunk(&env.resource, types, payloads.size());
            chunk.set_cardinality(payloads.size());
            for (uint64_t i = 0; i < payloads.size(); i++) {
                chunk.set_value(0, i, static_cast<int64_t>(i));
                chunk.set_value(1, i, std::string_view{payloads[i]});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            auto held = exhaust_pool(env);
            auto small = top_up_pool(env, segment_size, 3);
            INFO("held whole-block pins: " << held.size() << ", segment-sized pins: " << small.size());
#ifdef DEV_MODE
            const auto freed_before = bm.dev_freed_ids().size();
#endif
            auto appended = table->append(chunk, state);
            REQUIRE(appended.has_error());
            INFO("refusal: " << appended.error().what);
            CHECK(appended.error().type == core::error_code_t::out_of_memory);
#ifdef DEV_MODE
            // The written-through segments' blocks go back to the file, and none of them is still named.
            const auto& freed = bm.dev_freed_ids();
            CHECK(freed.size() > freed_before);
            std::pmr::vector<uint64_t> live(&env.resource);
            table->row_group()->collect_disk_block_ids(live);
            for (auto it = freed.begin() + static_cast<std::ptrdiff_t>(freed_before); it != freed.end(); ++it) {
                CHECK(std::find(live.begin(), live.end(), *it) == live.end());
            }
#endif
        }
        auto infos = table->get_column_segment_info();
        uint64_t string_segments = 0;
        uint64_t string_rows = 0;
        for (const auto& info : infos) {
            if (info.column_id == 1) {
                string_segments++;
                string_rows += info.segment_count;
            }
        }
        INFO("STRING column: " << string_segments << " segment(s), " << string_rows << " row(s), count "
                               << column_of(*table, 1).count());
        CHECK(column_of(*table, 1).count() == 0);
        CHECK(string_rows == 0);

        good_append(env, *table, shape_t::STRING, "ok");
        std::vector<row_t> cells;
        REQUIRE(scan_rows(*table, env, &cells) == 1);
        CHECK(cells[0].k == GOOD_K);
        CHECK(cell_string(cells[0].v, shape_t::STRING) == "ok");

        checkpoint_production(bm, *table);
        auto pointers = read_pointers(env, bm);
        REQUIRE(pointers.size() == 1);
        const auto& node = pointers[0].data_pointers[1];
        INFO("persisted: row group rows " << pointers[0].tuple_count << ", column v " << describe(node));
        require_consistent(node, 1);
        for (const auto& child : node.children) {
            require_consistent(child, 1);
        }
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(loaded.has_error());
        std::vector<row_t> cells;
        REQUIRE(scan_rows(*loaded.value(), env, &cells) == 1);
        CHECK(cells[0].k == GOOD_K);
        CHECK(cell_string(cells[0].v, shape_t::STRING) == "ok");
    }
    std::remove(path.c_str());
}

TEST_CASE("append_count: STRING, a chunk crossing a row-group boundary refused in the next row group",
          "[append_count][string][row_group]") {
    const std::string path = db_path("row_group");
    std::remove(path.c_str());
    env_t env;
    const uint64_t kept_rows = DEFAULT_VECTOR_CAPACITY - 2;
    auto string_chunk = [&](data_table_t& table, const std::vector<std::string>& payloads, int64_t first_k) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, payloads.size());
        chunk.set_cardinality(payloads.size());
        for (uint64_t i = 0; i < payloads.size(); i++) {
            chunk.set_value(0, i, first_k + static_cast<int64_t>(i));
            chunk.set_value(1, i, std::string_view{payloads[i]});
        }
        return chunk;
    };
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm, shape_t::STRING);
        REQUIRE(table->row_group()->row_group_size() == DEFAULT_VECTOR_CAPACITY);
        {
            auto chunk = string_chunk(*table, std::vector<std::string>(kept_rows, "kept"), 0);
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            REQUIRE_FALSE(table->append(chunk, state).has_error());
            table->finalize_append(state, transaction_data::committed());
        }
        {
            auto chunk = string_chunk(*table, {"a", "b", std::string(5000, 'x')}, 1000000);
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            auto held = exhaust_pool(env);
            auto appended = table->append(chunk, state);
            REQUIRE(appended.has_error());
            INFO("refusal: " << appended.error().what);
            CHECK(appended.error().type == core::error_code_t::out_of_memory);
        }
        REQUIRE(table->row_group()->row_group_tree()->segment_at(1) == nullptr);
        CHECK(column_of(*table, 0).count() == kept_rows);
        CHECK(column_of(*table, 1).count() == kept_rows);
        {
            std::vector<row_t> cells;
            REQUIRE(scan_rows(*table, env, &cells) == kept_rows);
            CHECK(cell_string(cells.back().v, shape_t::STRING) == "kept");
        }

        good_append(env, *table, shape_t::STRING, "ok");
        std::vector<row_t> cells;
        REQUIRE(scan_rows(*table, env, &cells) == kept_rows + 1);
        CHECK(cells.back().k == GOOD_K);
        CHECK(cell_string(cells.back().v, shape_t::STRING) == "ok");

        checkpoint_production(bm, *table);
        auto pointers = read_pointers(env, bm);
        REQUIRE(pointers.size() == 1);
        CHECK(pointers[0].tuple_count == kept_rows + 1);
        for (const auto& node : pointers[0].data_pointers) {
            INFO("persisted: " << describe(node));
            require_consistent(node, kept_rows + 1);
            for (const auto& child : node.children) {
                require_consistent(child, kept_rows + 1);
            }
        }
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(loaded.has_error());
        std::vector<row_t> cells;
        REQUIRE(scan_rows(*loaded.value(), env, &cells) == kept_rows + 1);
        CHECK(cells.back().k == GOOD_K);
        CHECK(cell_string(cells.back().v, shape_t::STRING) == "ok");
    }
    std::remove(path.c_str());
}
