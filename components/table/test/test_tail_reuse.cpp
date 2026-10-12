// A shared per-collection packer reuses tail blocks across appends. What it must keep true: a tail
// block is never rewritten once a root names it, a resident copy of a reused block serves the rows
// written after it was loaded, and a crash mid-rewrite loses only the rows the durable root never had.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <filesystem>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include "block_reachability_walker.hpp"
#include "fault_injection_file.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string tr_db_path(const char* tag) {
        return "/tmp/test_otterbrix_tail_reuse_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct tr_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        tr_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::string payload_of(uint64_t row) {
        std::string s = "tail_reuse_row_" + std::to_string(row) + "_";
        while (s.size() < 64) {
            s.push_back(static_cast<char>('a' + (row + s.size()) % 26));
        }
        return s;
    }

    std::unique_ptr<data_table_t> make_table(tr_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "tail_reuse");
    }

    // 50-row statements, as INSERT ... VALUES appends.
    core::result_wrapper_t<bool> append_rows(data_table_t& table, tr_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            const uint64_t batch = std::min<uint64_t>(count - offset, 50);
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            std::vector<std::string> values;
            values.reserve(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = start + offset + i;
                values.push_back(payload_of(row));
                chunk.set_value(0, i, static_cast<int64_t>(row));
                chunk.set_value(1, i, std::string_view{values.back()});
            }
            table_append_state state(&env.resource);
            if (auto r = table.append_lock(state); r.has_error()) {
                return r;
            }
            if (auto r = table.initialize_append(state); r.has_error()) {
                return r;
            }
            if (auto r = table.append(chunk, state); r.has_error()) {
                return r;
            }
            table.finalize_append(state, transaction_data::committed());
            offset += batch;
        }
        return true;
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
    }

    std::unique_ptr<data_table_t> reload_table(tr_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    uint64_t verify_rows(data_table_t& table, tr_env_t& env) {
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
                INFO("scan error: " << state.table_state.scan_error.what.c_str());
                REQUIRE_FALSE(state.table_state.has_error());
            }
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto id_cell = chunk.value(0, i);
                auto payload_cell = chunk.value(1, i);
                REQUIRE(id_cell.value<int64_t>() == static_cast<int64_t>(seen));
                REQUIRE(payload_cell.value<std::string_view>() == payload_of(seen));
                seen++;
            }
        }
        return seen;
    }

    // Block ids of the payload column's disk-backed segments, in segment order.
    std::vector<uint64_t> payload_blocks(data_table_t& table) {
        std::vector<uint64_t> out;
        for (auto& info : table.get_column_segment_info()) {
            if (info.column_path == "[1]" && info.segment_type == "PERSISTENT") {
                out.push_back(info.block_id);
            }
        }
        return out;
    }

} // namespace

TEST_CASE("tail_reuse: segments filled by successive appends share one block until a root names it", "[tailreuse]") {
    const auto path = tr_db_path("shared");
    remove_file(path);
    tr_env_t env;
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_table(env, bm);

    // ~240 rows fill a 16 KiB string segment: 2000 rows = 8 filled segments over 40 statements.
    REQUIRE_FALSE(append_rows(*table, env, 0, 2000).has_error());
    const auto before = payload_blocks(*table);
    REQUIRE(before.size() >= 6);
    std::set<uint64_t> distinct(before.begin(), before.end());
    REQUIRE(distinct.size() <= 2);

    checkpoint_production(bm, *table);
    const std::set<uint64_t> named(before.begin(), before.end());

    // After the root, the next filled segment must land in a block the root does not name.
    REQUIRE_FALSE(append_rows(*table, env, 2000, 600).has_error());
    const auto after = payload_blocks(*table);
    REQUIRE(after.size() > before.size());
    for (size_t i = before.size(); i < after.size(); i++) {
        INFO("segment " << i << " block " << after[i]);
        REQUIRE(named.count(after[i]) == 0);
    }
    REQUIRE(verify_rows(*table, env) == 2600);

    checkpoint_production(bm, *table);
    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    REQUIRE(report.unexplained.empty());
    REQUIRE(report.reachable_free_overlap.empty());
    table.reset();
    {
        tr_env_t env2;
        tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm2.load_existing_database().has_error());
        auto reloaded = reload_table(env2, bm2);
        REQUIRE(verify_rows(*reloaded, env2) == 2600);
    }
    remove_file(path);
}

TEST_CASE("tail_reuse: a resident copy of a reused block serves the rows appended after it was loaded", "[tailreuse]") {
    const auto path = tr_db_path("resident");
    remove_file(path);
    tr_env_t env;
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_table(env, bm);

    REQUIRE_FALSE(append_rows(*table, env, 0, 300).has_error());
    REQUIRE(payload_blocks(*table).size() == 1);
    REQUIRE(verify_rows(*table, env) == 300); // loads the tail block
    REQUIRE_FALSE(append_rows(*table, env, 300, 600).has_error());
    const auto blocks = payload_blocks(*table);
    REQUIRE(blocks.size() >= 3);
    REQUIRE(blocks[0] == blocks[1]);
    REQUIRE(verify_rows(*table, env) == 900); // the resident copy must carry the new segments

    // And so must the disk image once the copy is evicted.
    REQUIRE_FALSE(env.buffer_pool.set_limit(uint64_t(1) << 20).has_error());
    REQUIRE(verify_rows(*table, env) == 900);
    remove_file(path);
}

TEST_CASE("tail_reuse: a crash while a tail block is being rewritten loses only rows past the durable root",
          "[tailreuse]") {
    const auto path = tr_db_path("crash");
    remove_file(path);
    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);
    tr_env_t env;
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_table(env, bm);

    REQUIRE_FALSE(append_rows(*table, env, 0, 1000).has_error());
    checkpoint_production(bm, *table);
    const auto root_blocks = payload_blocks(*table);

    // Rows past the root fill a fresh tail block; the second statement that fills a segment rewrites it.
    REQUIRE_FALSE(append_rows(*table, env, 1000, 300).has_error());
    const auto after_first = payload_blocks(*table);
    REQUIRE(after_first.size() > root_blocks.size());
    const uint64_t tail = after_first.back();
    const std::set<uint64_t> root_set(root_blocks.begin(), root_blocks.end());
    REQUIRE(root_set.count(tail) == 0);

    // The next block write is the tail's rewrite: tear it and crash.
    plan.torn_at_write = plan.writes_seen + 1;
    auto second = append_rows(*table, env, 1300, 300);
    REQUIRE(second.has_error());
    SECTION("the OS lost everything after the last fsync") { scope.last()->crash_revert(); }
    SECTION("the torn half landed on disk") { plan.crashed = true; }

    table.reset();
    {
        otterbrix_test::fault_plan_t plan2;
        otterbrix_test::fault_injection_scope_t scope2(plan2);
        tr_env_t env2;
        tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm2.load_existing_database().has_error());
        auto reloaded = reload_table(env2, bm2);
        REQUIRE(verify_rows(*reloaded, env2) == 1000);
        // The root's copy of the open tail (checkpoint pbm) differs from the live re-point; only the torn id matters.
        const auto reloaded_blocks = payload_blocks(*reloaded);
        REQUIRE(reloaded_blocks.size() == root_blocks.size());
        for (auto id : reloaded_blocks) {
            REQUIRE(id != tail);
        }
        // Life goes on: the torn block is reissued or ignored, never read as data.
        REQUIRE_FALSE(append_rows(*reloaded, env2, 1000, 700).has_error());
        checkpoint_production(bm2, *reloaded);
        auto report = otterbrix_test::walk_blocks(bm2, path, &env2.resource);
        REQUIRE(report.ok);
        REQUIRE(report.unexplained.empty());
        REQUIRE(report.reachable_free_overlap.empty());
        REQUIRE(verify_rows(*reloaded, env2) == 1700);
    }
    {
        tr_env_t env3;
        tstorage::single_file_block_manager_t bm3(env3.buffer_manager, env3.fs, path);
        REQUIRE_FALSE(bm3.load_existing_database().has_error());
        auto reloaded = reload_table(env3, bm3);
        REQUIRE(verify_rows(*reloaded, env3) == 1700);
    }
    remove_file(path);
}
