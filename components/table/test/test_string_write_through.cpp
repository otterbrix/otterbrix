// Write-through re-points a filled segment to a real file block on append; STRING was excluded,
// so its payload stayed pinned and every round re-copied it (measured: 942 of 942 non-evictable).
// Losing the adopted segment's overflow-block registration corrupts every read of a big string, not a rare race.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <algorithm>
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

    std::string wt_db_path(const char* tag) {
        return "/tmp/test_otterbrix_string_wt_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct wt_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        wt_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    int64_t mixed_seed(uint64_t row) {
        uint64_t x = row + 0x9E3779B97F4A7C15ull;
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ull;
        x ^= x >> 27;
        return static_cast<int64_t>(x & 0x7FFFFFFFFFFFFFFFull);
    }

    // Every 17th row is a BIG string (>= 4096), so the overflow path stays on for every table below.
    std::string wt_payload(uint64_t row) {
        const bool big = (row % 17) == 0;
        const size_t target = big ? 8192 : 4074;
        std::string s = "row_" + std::to_string(row) + "_";
        s.reserve(target);
        const char* alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        uint64_t x = static_cast<uint64_t>(mixed_seed(row));
        while (s.size() < target) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            s.push_back(alphabet[x % 36]);
        }
        return s;
    }

    std::unique_ptr<data_table_t> make_string_table(wt_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "string_wt");
    }

    void append_rows(data_table_t& table, wt_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                auto payload = wt_payload(row);
                chunk.set_value(1, i, std::string_view{payload});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
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

    std::unique_ptr<data_table_t> reload_table(wt_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    uint64_t verify_rows(data_table_t& table, wt_env_t& env) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, nullptr);
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
                const auto id = id_cell.value<int64_t>();
                REQUIRE(id == static_cast<int64_t>(seen));
                const auto payload = payload_cell.value<std::string_view>();
                REQUIRE(payload == wt_payload(static_cast<uint64_t>(id)));
                seen++;
            }
        }
        return seen;
    }

    struct string_segment_census_t {
        uint64_t total{0};
        uint64_t persistent{0};
        std::vector<uint64_t> overflow_blocks;
    };

    // The payload column reports its own segments under column_path "[1]".
    string_segment_census_t census_payload_segments(data_table_t& table) {
        string_segment_census_t out;
        for (auto& info : table.get_column_segment_info()) {
            if (info.column_path != "[1]") {
                continue;
            }
            out.total++;
            if (info.segment_type == "PERSISTENT") {
                out.persistent++;
                for (auto id : info.additional_blocks) {
                    out.overflow_blocks.push_back(id);
                }
            }
        }
        return out;
    }

    uint64_t file_size_of(const std::string& path) {
        std::error_code ec;
        auto s = std::filesystem::file_size(path, ec);
        return ec ? 0 : static_cast<uint64_t>(s);
    }

} // namespace

TEST_CASE("string_write_through: filled string segments become disk-backed during append", "[stringwt]") {
    const auto path = wt_db_path("evictable");
    remove_file(path);
    wt_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_string_table(env, bm);

    constexpr uint64_t ROWS = 600;
    append_rows(*table, env, 0, ROWS);

    auto census = census_payload_segments(*table);
    WARN("[stringwt gate E] payload segments: total=" << census.total << " persistent=" << census.persistent
                                                      << " overflow_blocks=" << census.overflow_blocks.size());
    REQUIRE(census.total > 1);
    // Everything but the open (appendable) tail is sealed by the fill and must be on disk.
    REQUIRE(census.persistent + 1 >= census.total);
    REQUIRE(census.persistent > 0);

    REQUIRE(verify_rows(*table, env) == ROWS);
    remove_file(path);
}

// probe B of test_checkpoint_in_place measured the residual this closes: one superseded string generation per round.
TEST_CASE("string_write_through: an unchanged string table is named, not copied, by a round", "[stringwt]") {
    const auto path = wt_db_path("named");
    remove_file(path);
    wt_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_string_table(env, bm);

    constexpr uint64_t ROWS = 400;
    append_rows(*table, env, 0, ROWS);

    checkpoint_production(bm, *table);
    checkpoint_production(bm, *table);
    const auto root_b = bm.dev_durable_root_data_snapshot();

    bm.dev_reset_tracking();
    checkpoint_production(bm, *table);
    const auto root_c = bm.dev_durable_root_data_snapshot();

    WARN("[stringwt gate F] root_b=" << root_b.size() << " root_c=" << root_c.size()
                                     << " issued_by_round_c=" << bm.dev_issued_ids().size());
    REQUIRE(root_b == root_c);
    for (auto issued : bm.dev_issued_ids()) {
        REQUIRE(root_c.count(issued) == 0);
    }

    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    REQUIRE(report.unexplained.empty());
    REQUIRE(report.reachable_free_overlap.empty());

    REQUIRE(verify_rows(*table, env) == ROWS);

    table.reset();
    {
        wt_env_t env2;
        tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm2.load_existing_database().has_error());
        auto reloaded = reload_table(env2, bm2);
        REQUIRE(verify_rows(*reloaded, env2) == ROWS);
    }
    remove_file(path);
}

TEST_CASE("string_write_through: transitioned segments survive eviction, big strings included", "[stringwt]") {
    const auto path = wt_db_path("evict");
    remove_file(path);
    wt_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_string_table(env, bm);

    constexpr uint64_t ROWS = 600;
    append_rows(*table, env, 0, ROWS);

    auto census = census_payload_segments(*table);
    REQUIRE(census.persistent > 0);
    REQUIRE_FALSE(census.overflow_blocks.empty());

    // 4 MiB: below ~2.4 MB of payload across ~150 packed 16 KiB segments (forces eviction) but
    // above the scan's own working set -- 2 MiB starves the scan itself with a genuine pin OOM.
    REQUIRE_FALSE(env.buffer_pool.set_limit(uint64_t(1) << 22).has_error());
    REQUIRE(verify_rows(*table, env) == ROWS);
    remove_file(path);
}

// Rollback frees issued_since_root_ minus registry-alive ids, so every block the write-through's
// string leg allocates must be registry-alive by the time the transition returns.
TEST_CASE("string_write_through: a failed round's rollback keeps the live string blocks", "[stringwt]") {
    const auto path = wt_db_path("rollback");
    remove_file(path);
    wt_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_string_table(env, bm);

    constexpr uint64_t ROWS = 600;
    append_rows(*table, env, 0, ROWS);

    auto census = census_payload_segments(*table);
    REQUIRE(census.persistent > 0);
    REQUIRE_FALSE(census.overflow_blocks.empty());

    {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        plan.fail_after_writes = plan.writes_seen; // the NEXT write (the header) fails
        auto committed = bm.write_header(header);
        plan.fail_after_writes = 0;
        REQUIRE(committed.has_error());
        REQUIRE_FALSE(bm.degraded());
    }

    const auto reusable = bm.dev_reusable_snapshot();
    const auto pending = bm.dev_pending_free_snapshot();
    uint64_t checked = 0;
    for (auto id : census.overflow_blocks) {
        INFO("overflow block " << id);
        CHECK(bm.registry_alive(id));
        CHECK(reusable.count(id) == 0);
        CHECK(pending.count(id) == 0);
        checked++;
    }
    WARN("[stringwt gate H] overflow blocks kept through the rollback: " << checked);

    REQUIRE(verify_rows(*table, env) == ROWS);
    checkpoint_production(bm, *table);
    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    REQUIRE(report.unexplained.empty());
    REQUIRE(report.reachable_free_overlap.empty());

    table.reset();
    {
        wt_env_t env2;
        tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm2.load_existing_database().has_error());
        auto reloaded = reload_table(env2, bm2);
        REQUIRE(verify_rows(*reloaded, env2) == ROWS);
    }
    remove_file(path);
}

TEST_CASE("string_write_through: PROBE evictability and per-round cost", "[stringwt][probe]") {
    const auto path = wt_db_path("probe");
    remove_file(path);
    wt_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_string_table(env, bm);

    constexpr uint64_t ROWS = 4000;
    constexpr uint64_t DELTA = 16;
    append_rows(*table, env, 0, ROWS);
    uint64_t next_row = ROWS;

    auto census = census_payload_segments(*table);
    WARN("[stringwt probe] after append: payload segments total=" << census.total << " persistent=" << census.persistent
                                                                  << " (was 0 of all before the fix)");

    checkpoint_production(bm, *table);
    WARN("[stringwt probe] initial checkpoint: blocks=" << bm.total_blocks() << " free=" << bm.free_blocks()
                                                        << " file=" << file_size_of(path)
                                                        << " bytes/row=" << file_size_of(path) / next_row);
    for (int round = 1; round <= 3; ++round) {
        append_rows(*table, env, next_row, DELTA);
        next_row += DELTA;
        bm.dev_reset_tracking();
        checkpoint_production(bm, *table);
        WARN("[stringwt probe] same-process round "
             << round << ": blocks=" << bm.total_blocks() << " free=" << bm.free_blocks()
             << " issued=" << bm.dev_issued_ids().size() << " file=" << file_size_of(path)
             << " bytes/row=" << file_size_of(path) / next_row);
    }
    REQUIRE(verify_rows(*table, env) == next_row);
    remove_file(path);
}
