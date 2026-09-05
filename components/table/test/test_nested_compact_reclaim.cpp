// compact must reclaim the disk blocks of NESTED columns' children.
//
// collect_disk_block_ids is data_table_t::compact's ONLY source of "what the outgoing collection
// owned". standard_column_data_t overrides it to add its validity child; without their own
// overrides STRUCT/LIST/ARRAY fall through to the base implementation, which walks only the node's
// OWN data_ tree — a tree STRUCT and ARRAY do not even populate. Since the checkpoint_children
// hooks make every child column (validity is children[0], nested payloads follow) a real persisted
// column, a RELOADED nested column's children sit on real .otbx blocks — and when compact tears
// the collection down those blocks are returned to nobody.
//
// Two things conspire to hide the leak, and this file is shaped to defeat both:
//
//   * PARTIAL-BLOCK PACKING. A block shared by a child segment and any TOP-LEVEL-owned segment (a
//     flat column's data, a LIST's offsets) is reported through the top-level walk by accident,
//     and freeing a packed block once frees all of it, so a nested column sitting NEXT TO a flat
//     column can leak nothing at all. The gates below isolate each kind: STRUCT-only and
//     ARRAY-only tables own NO top-level segments (without the overrides compact collects
//     NOTHING), and the LIST table's elements are fat enough (> 0.8 * block, the
//     partial_block_manager_t::FULL_THRESHOLD) that its child segments take DEDICATED blocks no
//     offsets segment can share.
//
//   * THE ROOT FORMULA — and only a judged REOPENED file tells the truth about it. Children own
//     disk blocks only after a LOAD (write-through never descends into nested children), and
//     compact straight after the load is still covered by the formula: the loaded blocks are
//     durable_root_data_, and reclaim_superseded_root sweeps them once the collection is dead. One
//     checkpoint BEFORE the compact moves the durable root past the load root (the children's
//     blocks survive that reclaim only through their registry entries) — the compact then destroys
//     the registry entries, and after the NEXT checkpoint the load-time child blocks are named by
//     no root, no free list and no registry: orphaned durably.
//
// Gates, each judged per nested kind:
//   1. (unit-shaped) on a freshly loaded table, collect_disk_block_ids must cover every block the
//      loader registered — the loader's registry IS the collection's ownership;
//   2. (walker) checkpoint -> delete -> compact -> checkpoint leaves ZERO unexplained blocks in
//      the durable file;
//   3. (closed cycle) reopen/checkpoint/compact/checkpoint rounds do not grow the file.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <unistd.h>

#include "block_reachability_walker.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t NESTED_ROWS = 6000;
    // 40 * 8 B * 1024 rows = 320 KiB of child payload per row group: past FULL_THRESHOLD,
    // so LIST/ARRAY child segments take dedicated blocks that no top-level segment shares.
    constexpr uint64_t ARRAY_WIDTH = 40;
    constexpr uint64_t LIST_LENGTH = 40;
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    enum class nested_kind_t
    {
        STRUCT,
        LIST,
        ARRAY
    };

    const char* kind_name(nested_kind_t kind) {
        switch (kind) {
            case nested_kind_t::STRUCT:
                return "struct";
            case nested_kind_t::LIST:
                return "list";
            case nested_kind_t::ARRAY:
                return "array";
        }
        return "?";
    }

    std::string nested_db_path(const char* tag) {
        return "/tmp/test_otterbrix_nested_reclaim_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    uint64_t file_size_of(const std::string& path) {
        std::error_code ec;
        auto s = std::filesystem::file_size(path, ec);
        return ec ? 0 : static_cast<uint64_t>(s);
    }

    struct nested_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        nested_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    complex_logical_type nested_struct_type(nested_env_t& env) {
        std::pmr::vector<complex_logical_type> fields(&env.resource);
        fields.emplace_back(logical_type::BIGINT, "num");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        return complex_logical_type::create_struct("pair", fields);
    }

    // ONE column of ONE nested kind, no flat column: for STRUCT and ARRAY the top-level node
    // owns no segments at all, so the base-walk reclaim source is provably EMPTY, and no
    // packing accident can hand a child block to compact through someone else's walk.
    std::unique_ptr<data_table_t>
    make_nested_table(nested_env_t& env, tstorage::single_file_block_manager_t& bm, nested_kind_t kind) {
        std::vector<column_definition_t> columns;
        switch (kind) {
            case nested_kind_t::STRUCT:
                columns.emplace_back("s", nested_struct_type(env));
                break;
            case nested_kind_t::LIST:
                columns.emplace_back("l", complex_logical_type::create_list(logical_type::UBIGINT));
                break;
            case nested_kind_t::ARRAY:
                columns.emplace_back("a", complex_logical_type::create_array(logical_type::UBIGINT, ARRAY_WIDTH));
                break;
        }
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "nested_reclaim_table");
    }

    std::string row_name(uint64_t row) { return "nested_row_payload_padding_" + std::to_string(row); }

    void append_nested_rows(data_table_t& table,
                            nested_env_t& env,
                            nested_kind_t kind,
                            uint64_t start,
                            uint64_t count) {
        auto struct_type = nested_struct_type(env);
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const uint64_t row = start + offset + i;
                switch (kind) {
                    case nested_kind_t::STRUCT: {
                        std::vector<logical_value_t> members;
                        members.emplace_back(&env.resource, static_cast<int64_t>(row * 7));
                        members.emplace_back(&env.resource, row_name(row));
                        chunk.set_value(0, i, logical_value_t::create_struct(&env.resource, struct_type, members));
                        break;
                    }
                    case nested_kind_t::LIST: {
                        std::vector<uint64_t> list;
                        list.reserve(LIST_LENGTH);
                        for (uint64_t j = 0; j < LIST_LENGTH; j++) {
                            list.emplace_back(row * 100 + j);
                        }
                        chunk.set_value(0, i, list);
                        break;
                    }
                    case nested_kind_t::ARRAY: {
                        std::vector<uint64_t> arr;
                        arr.reserve(ARRAY_WIDTH);
                        for (uint64_t j = 0; j < ARRAY_WIDTH; j++) {
                            arr.emplace_back(row * 1000 + j);
                        }
                        chunk.set_value(0, i, arr);
                        break;
                    }
                }
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // The EXACT sequence of table_storage_t::checkpoint (services/disk/manager_disk.cpp).
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
        REQUIRE_FALSE(bm.file_sync().has_error());
    }

    std::unique_ptr<data_table_t> reload_table(nested_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded.has_error());
        return std::move(loaded.value());
    }

    // Content-addressed verification: each row is identified by its FIRST payload value (the
    // row number is recoverable from it), so the check survives compact rewriting row
    // positions AND proves that a freed-and-reissued block never held data anyone needed.
    uint64_t scan_and_verify(data_table_t& table, nested_env_t& env, nested_kind_t kind) {
        std::vector<storage_index_t> column_ids{storage_index_t(0)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto cell = chunk.data[0].value(i);
                switch (kind) {
                    case nested_kind_t::STRUCT: {
                        REQUIRE(cell.type().type() == logical_type::STRUCT);
                        REQUIRE(cell.children().size() == 2);
                        const auto row = static_cast<uint64_t>(cell.children()[0].value<int64_t>()) / 7;
                        REQUIRE(cell.children()[0].value<int64_t>() == static_cast<int64_t>(row * 7));
                        const auto& name_value = cell.children()[1]; // named local: cells are temporaries
                        REQUIRE(name_value.value<std::string_view>() == row_name(row));
                        break;
                    }
                    case nested_kind_t::LIST: {
                        REQUIRE(cell.type().type() == logical_type::LIST);
                        REQUIRE(cell.children().size() == LIST_LENGTH);
                        const auto row = cell.children()[0].value<uint64_t>() / 100;
                        for (uint64_t j = 0; j < LIST_LENGTH; j++) {
                            REQUIRE(cell.children()[j].value<uint64_t>() == row * 100 + j);
                        }
                        break;
                    }
                    case nested_kind_t::ARRAY: {
                        REQUIRE(cell.type().type() == logical_type::ARRAY);
                        REQUIRE(cell.children().size() == ARRAY_WIDTH);
                        const auto row = cell.children()[0].value<uint64_t>() / 1000;
                        for (uint64_t j = 0; j < ARRAY_WIDTH; j++) {
                            REQUIRE(cell.children()[j].value<uint64_t>() == row * 1000 + j);
                        }
                        break;
                    }
                }
                seen++;
            }
        }
        return seen;
    }

    void delete_first_rows(data_table_t& table, nested_env_t& env, uint64_t delete_count) {
        transaction_manager_t mgr(&env.resource);
        auto session = components::session::session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);
        auto txn_id = txn.data().transaction_id;
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        uint64_t deleted = 0;
        while (deleted < delete_count) {
            uint64_t batch = std::min(delete_count - deleted, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t row_ids_chunk(&env.resource, id_type, batch);
            for (uint64_t i = 0; i < batch; i++) {
                row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(deleted + i));
            }
            row_ids_chunk.set_cardinality(batch);
            table_delete_state del_state(&env.resource);
            table.delete_rows(del_state, row_ids_chunk.data[0], batch, txn_id);
            deleted += batch;
        }
        auto commit_id = mgr.commit(session);
        mgr.publish(commit_id);
        table.commit_all_deletes(txn_id, commit_id);
    }

    std::string id_set_to_string(const std::set<uint64_t>& ids) {
        std::string s = "{";
        for (auto id : ids) {
            s += std::to_string(id) + ",";
        }
        s += "}";
        return s;
    }

    // GATE 1 + GATE 2 for one nested kind, on its own database file.
    void run_walker_gates(nested_kind_t kind) {
        const auto path = nested_db_path(kind_name(kind));
        remove_file(path);
        nested_env_t env;

        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_nested_table(env, bm, kind);
            append_nested_rows(*table, env, kind, 0, NESTED_ROWS);
            checkpoint_production(bm, *table);
        }

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);

        // GATE 1. The loader registered one block_handle_t per loaded segment plus the
        // big-string overflow blocks — for EVERY node of every column tree, children
        // included. That registry is precisely what this fresh collection owns, so compact's
        // reclaim source must cover all of it. (Scoped so the counted copy dies before
        // compact — a held copy would keep the outgoing collection alive.)
        {
            std::pmr::vector<uint64_t> collected{&env.resource};
            table->row_group()->collect_disk_block_ids(collected);
            std::set<uint64_t> collected_set(collected.begin(), collected.end());
            std::set<uint64_t> missing;
            for (auto id : bm.dev_live_registry_ids()) {
                if (collected_set.count(id) == 0) {
                    missing.insert(id);
                }
            }
            INFO(kind_name(kind) << ": registered by the loader but invisible to compact's reclaim: "
                                 << id_set_to_string(missing));
            CHECK(missing.empty());
        }

        // Move the durable root past the load root while the live tree still points at the
        // load-time blocks. Under root N+1 those blocks survive reclaim only through their
        // registry entries — which the compact below destroys.
        checkpoint_production(bm, *table);

        // A real compacting round: drop half the table, rebuild, commit root N+2.
        delete_first_rows(*table, env, NESTED_ROWS / 2);
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);

        // GATE 2. Every block of the file must be explained by the durable root, the free
        // list, or the live registry. On HEAD the children's load-time blocks are in none.
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        INFO(kind_name(kind) << ": unexplained (leaked) blocks: " << id_set_to_string(report.unexplained));
        CHECK(report.unexplained.empty());
        CHECK(report.reachable_free_overlap.empty());

        // The other side of the hazard: nothing that was freed may still be needed. Reopen
        // fresh and prove every surviving row's nested payload intact, then run one more
        // compacting round (which REUSES the freed ids) and prove it again.
        table.reset();
        {
            tstorage::single_file_block_manager_t bm2(env.buffer_manager, env.fs, path);
            REQUIRE(!bm2.load_existing_database().has_error());
            auto reloaded = reload_table(env, bm2);
            REQUIRE(scan_and_verify(*reloaded, env, kind) == NESTED_ROWS / 2);
            REQUIRE(reloaded->compact(WATERMARK));
            checkpoint_production(bm2, *reloaded);
            REQUIRE(scan_and_verify(*reloaded, env, kind) == NESTED_ROWS / 2);
        }

        remove_file(path);
    }

} // namespace

// ---------------------------------------------------------------------------------------
// GATES 1 + 2 — ownership coverage on a loaded table, and a fully explained durable
// file after a compacting round, per nested kind.
//
// The defect: gate 1 reports every child block (validity, STRUCT fields, LIST/ARRAY
// elements) as registered-but-uncollected — for STRUCT and ARRAY the collected set is
// EMPTY; gate 2 reports the same blocks as UNEXPLAINED — named by no root, no free list,
// no registry — i.e. leaked durably, invisible until a reopened file is judged.
// ---------------------------------------------------------------------------------------
TEST_CASE("nested_compact_reclaim: STRUCT children blocks are collected and the walk stays explained", "[f6]") {
    run_walker_gates(nested_kind_t::STRUCT);
}

TEST_CASE("nested_compact_reclaim: LIST children blocks are collected and the walk stays explained", "[f6]") {
    run_walker_gates(nested_kind_t::LIST);
}

TEST_CASE("nested_compact_reclaim: ARRAY children blocks are collected and the walk stays explained", "[f6]") {
    run_walker_gates(nested_kind_t::ARRAY);
}

// ---------------------------------------------------------------------------------------
// GATE 3 — the closed-cycle property, restated for nested columns across process
// lifetimes: reopen / checkpoint / compact / checkpoint must not grow the file once warm.
// STRUCT-only shape: the whole table is child payload, so pre-fix compact reclaims zero.
//
// The defect: every cycle orphans the child blocks the load created, so the pool comes
// up short by that amount each cycle and the file grows without bound.
// ---------------------------------------------------------------------------------------
TEST_CASE("nested_compact_reclaim: reopen+compact rounds do not grow the file", "[f6]") {
    const auto path = nested_db_path("steady");
    remove_file(path);
    nested_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_nested_table(env, bm, nested_kind_t::STRUCT);
        append_nested_rows(*table, env, nested_kind_t::STRUCT, 0, NESTED_ROWS);
        checkpoint_production(bm, *table);
    }

    // One cycle = one process lifetime: load, supersede the load root, compact, commit.
    auto run_cycle = [&](uint64_t& blocks_out) {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        checkpoint_production(bm, *table);
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);
        blocks_out = bm.total_blocks();
    };

    // Warm-up: the first compacting cycles legitimately raise the high-water mark (double
    // occupancy until the superseding root is durable, then the first spend of the
    // reclaimed space). From there on the file must be a closed cycle.
    uint64_t steady_blocks = 0;
    for (int warmup = 0; warmup < 3; ++warmup) {
        run_cycle(steady_blocks);
    }

    const uint64_t steady_size = file_size_of(path);
    for (int round = 0; round < 3; ++round) {
        uint64_t blocks_now = 0;
        run_cycle(blocks_now);
        INFO("round " << round << ": block_count " << steady_blocks << " -> " << blocks_now << ", file_size "
                      << steady_size << " -> " << file_size_of(path));
        CHECK(blocks_now == steady_blocks);
        CHECK(file_size_of(path) == steady_size);
    }

    // The data survived every round of reuse.
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        REQUIRE(scan_and_verify(*table, env, nested_kind_t::STRUCT) == NESTED_ROWS);
    }

    remove_file(path);
}
