#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/types/logical_value.hpp>
#include <core/file/local_file_system.hpp>

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
            auto allocated = env.buffer_manager.allocate(tstorage::memory_tag::BASE_TABLE,
                                                         env.buffer_manager.block_size(),
                                                         true);
            if (allocated.has_error()) {
                break;
            }
            held.push_back(std::move(allocated.value()));
            REQUIRE(held.size() < 1024);
        }
        return held;
    }

    struct held_small_t {
        std::shared_ptr<tstorage::block_handle_t> block;
        tstorage::buffer_handle_t pin;
    };

    // Fills what the whole-block pins left with `size`-byte pins, then frees `keep` of them.
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

    struct row_t {
        int64_t k;
        logical_value_t v;
    };

    // Returns the rows seen; the scan's error (if any) lands in *error.
    uint64_t scan_rows(data_table_t& table, env_t& env, std::vector<row_t>* cells, core::error_t* error) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, transaction_data::committed(), nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (state.table_state.has_error()) {
                if (error) {
                    *error = state.table_state.scan_error;
                }
                break;
            }
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

    std::unique_ptr<data_table_t>
    make_table(env_t& env, tstorage::single_file_block_manager_t& bm, complex_logical_type v_type) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("k", logical_type::BIGINT);
        columns.emplace_back("v", std::move(v_type));
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "unwind_limits");
    }

    data_chunk_t
    string_chunk(env_t& env, data_table_t& table, const std::vector<std::string>& payloads, int64_t first_k) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, payloads.size());
        chunk.set_cardinality(payloads.size());
        for (uint64_t i = 0; i < payloads.size(); i++) {
            chunk.set_value(0, i, first_k + static_cast<int64_t>(i));
            chunk.set_value(1, i, std::string_view{payloads[i]});
        }
        return chunk;
    }

    void committed_append(data_table_t& table, data_chunk_t& chunk, env_t& env) {
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    std::string db_path(const char* tag) {
        return "/tmp/test_otterbrix_unwind_limits_" + std::string{tag} + "_" + std::to_string(::getpid()) + ".otbx";
    }

    // 1022 committed rows, then a 4-row chunk that crosses into the next row group while the pool
    // keeps only `free_small_pins` 4 KiB pins. The append is refused and the kept rows stay.
    void crossing_chunk_refused(const char* tag, uint64_t free_small_pins) {
        const std::string path = db_path(tag);
        std::remove(path.c_str());
        env_t env;
        const uint64_t kept_rows = DEFAULT_VECTOR_CAPACITY - 2;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, complex_logical_type{logical_type::STRING_LITERAL});
            {
                auto chunk = string_chunk(env, *table, std::vector<std::string>(kept_rows, "kept"), 0);
                committed_append(*table, chunk, env);
            }
            {
                auto chunk = string_chunk(env, *table, {"a", "b", "c", "d"}, 1000000);
                table_append_state state(&env.resource);
                REQUIRE_FALSE(table->append_lock(state).has_error());
                REQUIRE_FALSE(table->initialize_append(state).has_error());
                auto held = exhaust_pool(env);
                auto small = top_up_pool(env, uint64_t(1) << 12, free_small_pins);
                auto appended = table->append(chunk, state);
                REQUIRE(appended.has_error());
                CHECK(appended.error().type == core::error_code_t::out_of_memory);
            }
            CHECK(table->row_group()->row_group_tree()->segment_at(1) == nullptr);
            CHECK(column_of(*table, 0).count() == kept_rows);
            CHECK(column_of(*table, 1).count() == kept_rows);
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            const auto seen = scan_rows(*table, env, &cells, &err);
            CHECK_FALSE(err.contains_error());
            CHECK(seen == kept_rows);
        }
        std::remove(path.c_str());
    }

} // namespace

// L0. The next row group cannot open its append: the pool refuses its transient segments. The
// unwind ran from under the row-group lock the append held and took it again (1800f017: hang).
TEST_CASE("unwind_limits: L0 a crossing chunk whose next row group cannot open its append", "[unwind_limits][l0]") {
    crossing_chunk_refused("l0", 0);
}

// L0b. Two 4 KiB pins free: the next row group opens, re-pointing the filled one at the disk is
// refused; 4 and more let the whole append through. Same unwind under the same lock.
TEST_CASE("unwind_limits: L0b a crossing chunk whose filled row group cannot be re-pointed at the disk",
          "[unwind_limits][l0b]") {
    crossing_chunk_refused("l0b", 2);
}
