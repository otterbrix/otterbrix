// In-place checkpointing: a segment whose payload already lives in a real FILE block is
// read-only (appends start a fresh transient, updates ride the overlay the checkpoint refuses
// to serialize), so its bytes cannot have changed since the round that wrote them. The new
// root may therefore NAME the existing block instead of copying it — the shadow-paging
// machinery already supports blocks shared between the live tree and the root under
// construction (reclaim_superseded_root skips pending_root_data_, serialize_free_list never
// publishes a block the root names).
//
// Without this, every checkpoint rewrites the WHOLE table into fresh blocks and the previous
// round's copies go to the free list: measured offline on a 4090-byte-per-row layout, 113 of
// 294 blocks (29.6 MB of a 77 MB file) were free-listed garbage — one full superseded
// generation, ~2851 bytes per row, held forever because the file is never truncated.
//
// The gates below pin the fix; the [probe] cases print the before/after numbers.

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
#include <vector>
#include <unistd.h>

#include "block_reachability_walker.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string inplace_db_path(const char* tag) {
        return "/tmp/test_otterbrix_ckpt_inplace_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct inplace_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        inplace_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // Deliberately incompressible (splitmix64): the analysis must DECLINE every compressed
    // form, so the copy the in-place reference replaces would have been byte-identical.
    int64_t fixed_value(uint64_t row) {
        uint64_t x = row + 0x9E3779B97F4A7C15ull;
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ull;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBull;
        x ^= x >> 31;
        return static_cast<int64_t>(x & 0x7FFFFFFFFFFFFFFFull);
    }

    // The measured layout: ~4090 bytes per row (inline strings, just under the big-string
    // limit); every 17th row is a BIG string (>= 4096) so the overflow path is exercised too.
    std::string string_payload(uint64_t row) {
        const bool big = (row % 17) == 0;
        const size_t target = big ? 8192 : 4074;
        std::string s = "row_" + std::to_string(row) + "_";
        s.reserve(target);
        const char* alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        uint64_t x = static_cast<uint64_t>(fixed_value(row));
        while (s.size() < target) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            s.push_back(alphabet[x % 36]);
        }
        return s;
    }

    std::unique_ptr<data_table_t>
    make_fixed_table(inplace_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "inplace_fixed");
    }

    std::unique_ptr<data_table_t>
    make_string_table(inplace_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "inplace_strings");
    }

    void append_fixed_rows(data_table_t& table, inplace_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, fixed_value(start + offset + i));
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void append_string_rows(data_table_t& table, inplace_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                auto payload = string_payload(row);
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

    // The exact sequence of table_storage_t::checkpoint (services/disk/manager_disk.cpp).
    void checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table.checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    std::unique_ptr<data_table_t> reload_table(inplace_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    uint64_t verify_fixed_rows(data_table_t& table, inplace_env_t& env) {
        std::vector<storage_index_t> column_ids{storage_index_t(0)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            REQUIRE_FALSE(state.table_state.has_error());
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto cell = chunk.value(0, i);
                REQUIRE(cell.value<int64_t>() == fixed_value(seen));
                seen++;
            }
        }
        return seen;
    }

    uint64_t verify_string_rows(data_table_t& table, inplace_env_t& env) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            REQUIRE_FALSE(state.table_state.has_error());
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto id_cell = chunk.value(0, i);
                auto payload_cell = chunk.value(1, i);
                const auto id = id_cell.value<int64_t>();
                REQUIRE(id == static_cast<int64_t>(seen));
                const auto payload = payload_cell.value<std::string_view>();
                REQUIRE(payload == string_payload(static_cast<uint64_t>(id)));
                seen++;
            }
        }
        return seen;
    }

    uint64_t file_size_of(const std::string& path) {
        std::error_code ec;
        auto s = std::filesystem::file_size(path, ec);
        return ec ? 0 : static_cast<uint64_t>(s);
    }

    template<typename set_t>
    uint64_t count_missing(const set_t& needles, const set_t& haystack) {
        uint64_t missing = 0;
        for (auto id : needles) {
            if (haystack.count(id) == 0) {
                missing++;
            }
        }
        return missing;
    }

} // namespace

// ---------------------------------------------------------------------------------------
// GATE 1 — two committed rounds over an UNCHANGED fixed-size table name the SAME data
// blocks: after one round has landed every segment on disk, the next round has nothing to
// rewrite, so its root differs only in metadata chains.
// ---------------------------------------------------------------------------------------
TEST_CASE("checkpoint_in_place: unchanged fixed-size segments are named, not copied", "[inplace]") {
    const auto path = inplace_db_path("fixed");
    remove_file(path);
    inplace_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = make_fixed_table(env, bm);
    constexpr uint64_t ROWS = 300000;
    append_fixed_rows(*table, env, 0, ROWS);

    // Round A: first checkpoint. Fresh segments legitimately get written; the round also
    // re-points the live tail to disk-backed blocks, so from here on EVERY segment of this
    // table is disk-backed.
    checkpoint_production(bm, *table);
    // Round B: the round whose in-place references settle the root onto the live tree's own
    // blocks (round A's root still named the copies it made of segments that were not yet
    // disk-backed when it pinned them).
    checkpoint_production(bm, *table);
    const auto root_b = bm.dev_durable_root_data_snapshot();

    // Round C over the unchanged table: nothing may be rewritten.
    bm.dev_reset_tracking();
    checkpoint_production(bm, *table);
    const auto root_c = bm.dev_durable_root_data_snapshot();

    REQUIRE(root_b == root_c);

    // Every block the round issued is metadata (table-metadata or free-list chain), never data.
    for (auto issued : bm.dev_issued_ids()) {
        REQUIRE(root_c.count(issued) == 0);
    }

    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    REQUIRE(report.unexplained.empty());
    REQUIRE(report.reachable_free_overlap.empty());

    REQUIRE(verify_fixed_rows(*table, env) == ROWS);

    // The proof that matters: a FRESH process reads the un-copied blocks back correctly.
    table.reset();
    {
        inplace_env_t env2;
        tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm2.load_existing_database().has_error());
        auto reloaded = reload_table(env2, bm2);
        REQUIRE(verify_fixed_rows(*reloaded, env2) == ROWS);
    }
    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// GATE 2 — the measured workload: a LOADED table (strings just under the big-string limit,
// plus big-string overflow rows) takes a small delta. The round must keep every loaded
// block in the new root — the whole point: live data is not rewritten, so no superseded
// generation of it lands in the free list.
// ---------------------------------------------------------------------------------------
TEST_CASE("checkpoint_in_place: a loaded string table keeps its blocks across a delta round", "[inplace]") {
    const auto path = inplace_db_path("strings");
    remove_file(path);
    constexpr uint64_t ROWS = 2000;
    constexpr uint64_t DELTA = 8;

    {
        inplace_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_string_table(env, bm);
        append_string_rows(*table, env, 0, ROWS);
        checkpoint_production(bm, *table);
    }

    {
        inplace_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        // What the durable root names, as adopted by the loader itself — segment blocks AND
        // big-string overflow blocks.
        const auto loaded_root = bm.dev_durable_root_data_snapshot();
        REQUIRE_FALSE(loaded_root.empty());

        append_string_rows(*table, env, ROWS, DELTA);
        checkpoint_production(bm, *table);
        const auto new_root = bm.dev_durable_root_data_snapshot();

        // Not one loaded block was replaced by a copy.
        REQUIRE(count_missing(loaded_root, new_root) == 0);

        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        REQUIRE(report.unexplained.empty());
        REQUIRE(report.reachable_free_overlap.empty());

        REQUIRE(verify_string_rows(*table, env) == ROWS + DELTA);
    }

    // Reopen from the committed root and verify every payload byte-exactly.
    {
        inplace_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        REQUIRE(verify_string_rows(*table, env) == ROWS + DELTA);
    }
    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// PROBE (measurement, not a gate) — the shape the offline parse measured: rows of ~4090
// bytes, repeated delta-append + checkpoint rounds, then the same with a process restart
// between rounds. Prints blocks/free/file-size so the before/after cost is a number.
// ---------------------------------------------------------------------------------------
TEST_CASE("checkpoint_in_place: PROBE steady-state garbage per round", "[inplace][probe]") {
    constexpr uint64_t ROWS = 4000;
    constexpr uint64_t DELTA = 16;

    // A: the measured workload — a LOADED string table taking small deltas, one process
    // restart per round. Every string segment is disk-backed after the load, so this is where
    // the in-place references carry the whole table.
    {
        const auto path = inplace_db_path("probe_reopen");
        remove_file(path);
        uint64_t next_row = 0;
        {
            inplace_env_t env;
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE_FALSE(bm.create_new_database().has_error());
            auto table = make_string_table(env, bm);
            append_string_rows(*table, env, 0, ROWS);
            next_row = ROWS;
            checkpoint_production(bm, *table);
            WARN("[probe A] initial checkpoint: rows=" << next_row << " blocks=" << bm.total_blocks()
                                                       << " free=" << bm.free_blocks() << " file=" << file_size_of(path)
                                                       << " bytes/row=" << file_size_of(path) / next_row);
        }
        for (int round = 1; round <= 3; ++round) {
            inplace_env_t env;
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE_FALSE(bm.load_existing_database().has_error());
            auto table = reload_table(env, bm);
            append_string_rows(*table, env, next_row, DELTA);
            next_row += DELTA;
            checkpoint_production(bm, *table);
            // published = what the durable root's own free list names — the metric an offline
            // parse of the file sees (in-memory free_blocks() misses blocks that are only
            // published for the NEXT open, e.g. superseded loaded blocks the registry pins).
            auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
            REQUIRE(report.ok);
            WARN("[probe A] reopened round " << round << ": blocks=" << bm.total_blocks()
                                             << " free=" << bm.free_blocks()
                                             << " published=" << report.free_list_content.size()
                                             << " file=" << file_size_of(path)
                                             << " bytes/row=" << file_size_of(path) / next_row);
        }
        {
            inplace_env_t env;
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE_FALSE(bm.load_existing_database().has_error());
            auto table = reload_table(env, bm);
            REQUIRE(verify_string_rows(*table, env) == next_row);
        }
        remove_file(path);
    }

    // B: the residual — string segments born in THIS process stay managed (the write-through
    // transition excludes STRING), so a same-process round still copies every one of them and
    // the free list keeps one superseded generation of the strings until a restart or compact.
    {
        const auto path = inplace_db_path("probe_sameproc");
        remove_file(path);
        inplace_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_string_table(env, bm);
        append_string_rows(*table, env, 0, ROWS);
        uint64_t next_row = ROWS;
        checkpoint_production(bm, *table);
        for (int round = 1; round <= 3; ++round) {
            append_string_rows(*table, env, next_row, DELTA);
            next_row += DELTA;
            checkpoint_production(bm, *table);
            WARN("[probe B] same-process string round " << round << ": blocks=" << bm.total_blocks()
                                                        << " free=" << bm.free_blocks()
                                                        << " file=" << file_size_of(path)
                                                        << " bytes/row=" << file_size_of(path) / next_row);
        }
        REQUIRE(verify_string_rows(*table, env) == next_row);
        remove_file(path);
    }

    // C: fixed-size data converges in the SAME process — after one round every segment is
    // disk-backed, so later rounds issue only metadata chains.
    {
        const auto path = inplace_db_path("probe_fixed");
        remove_file(path);
        inplace_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_fixed_table(env, bm);
        append_fixed_rows(*table, env, 0, 300000);
        checkpoint_production(bm, *table);
        WARN("[probe C] fixed-size initial: blocks=" << bm.total_blocks() << " free=" << bm.free_blocks()
                                                     << " file=" << file_size_of(path));
        for (int round = 1; round <= 3; ++round) {
            bm.dev_reset_tracking();
            checkpoint_production(bm, *table);
            WARN("[probe C] fixed-size round " << round << ": blocks=" << bm.total_blocks()
                                               << " free=" << bm.free_blocks() << " issued="
                                               << bm.dev_issued_ids().size() << " file=" << file_size_of(path));
        }
        REQUIRE(verify_fixed_rows(*table, env) == 300000);
        remove_file(path);
    }
}
