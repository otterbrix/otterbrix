// A rolled-back transaction's rows leave through collection_t::revert_append. The packer shares a
// tail block between row groups, so the row groups past the revert row name blocks that still carry
// committed rows of the row groups before them, and the segments erased inside the kept row group
// name blocks of their own. After the revert every block must have exactly one owner: the table
// (a surviving segment) or the free list, never both and never neither.

#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
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
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include "block_reachability_walker.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string db_path(const char* tag) {
        return "/tmp/test_otterbrix_txn_revert_blocks_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    struct env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::string payload_of(uint64_t row, uint64_t width) {
        std::string s = "row_" + std::to_string(row) + "_";
        while (s.size() < width) {
            s.push_back(static_cast<char>('a' + (row * 7 + s.size()) % 26));
        }
        return s;
    }

    std::unique_ptr<data_table_t> make_table(env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("k", logical_type::BIGINT);
        columns.emplace_back("v", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "txn_revert");
    }

    // One statement: a single append state fed 1024-row chunks, finalized with `txn`.
    void
    append_rows(data_table_t& table, env_t& env, uint64_t start, uint64_t count, uint64_t width, transaction_data txn) {
        auto types = table.copy_types();
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        uint64_t offset = 0;
        while (offset < count) {
            const uint64_t n = std::min<uint64_t>(count - offset, DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(&env.resource, types, n);
            chunk.set_cardinality(n);
            std::vector<std::string> values;
            values.reserve(n);
            for (uint64_t i = 0; i < n; i++) {
                const uint64_t row = start + offset + i;
                values.push_back(payload_of(row, width));
                chunk.set_value(0, i, static_cast<int64_t>(row));
                chunk.set_value(1, i, std::string_view{values.back()});
            }
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            offset += n;
        }
        table.finalize_append(state, txn);
    }

    // Production-shaped (table_storage_t::checkpoint): metadata, free list, fsync, header, fsync. A refusal
    // is returned, not required: a latched block-accounting error is one of the outcomes under test.
    core::error_t checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (auto r = table.checkpoint(writer); r.has_error()) {
            return r.error();
        }
        if (auto r = writer.flush(); r.has_error()) {
            return r.error();
        }
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        if (free_ptr.has_error()) {
            return free_ptr.error();
        }
        if (auto r = bm.file_sync(); r.has_error()) {
            return r.error();
        }
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        if (auto r = bm.write_header(header); r.has_error()) {
            return r.error();
        }
        if (auto r = bm.file_sync(); r.has_error()) {
            return r.error();
        }
        return core::error_t::no_error();
    }

    std::unique_ptr<data_table_t> reload_table(env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    // Rows 0..n-1 in order, each with its payload.
    uint64_t verified_rows(data_table_t& table, env_t& env, uint64_t width) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, transaction_data::committed(), nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t rows = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto k = chunk.value(0, i).value<int64_t>();
                const auto cell = chunk.value(1, i);
                const std::string v{cell.value<std::string_view>()};
                INFO("row #" << rows << ": k=" << k << " v=" << v.substr(0, 32));
                REQUIRE(k == static_cast<int64_t>(rows));
                REQUIRE(v == payload_of(rows, width));
                rows++;
            }
        }
        return rows;
    }

    std::set<uint64_t> live_blocks(data_table_t& table, env_t& env) {
        std::pmr::vector<uint64_t> live(&env.resource);
        table.row_group()->collect_disk_block_ids(live);
        return std::set<uint64_t>(live.begin(), live.end());
    }

    std::string ids_of(const std::set<uint64_t>& s) {
        std::string out = "{";
        for (auto id : s) {
            out += std::to_string(id) + ",";
        }
        return out + "}";
    }

    constexpr uint64_t COMMITTED = 1500; // row group 0 full, 476 rows into row group 1
    constexpr uint64_t TXN_ROWS = 2000;  // closes row groups 1 and 2, opens row group 3
    constexpr uint64_t LATER = 100;

    // committed rows; a transaction appends and is rolled back; another commit; checkpoint. Returns the
    // blocks the revert freed while a surviving segment still named them.
    std::set<uint64_t>
    rolled_back_table(env_t& env, tstorage::single_file_block_manager_t& bm, data_table_t& table, uint64_t width) {
        append_rows(table, env, 0, COMMITTED, width, transaction_data::committed());
        append_rows(table, env, COMMITTED, TXN_ROWS, width, transaction_data{7, 7});
        const auto freed_before = bm.dev_freed_ids().size();
        auto reverted = table.revert_append(static_cast<int64_t>(COMMITTED), TXN_ROWS);
        INFO("revert: " << (reverted.has_error() ? reverted.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(reverted.has_error());
        const std::set<uint64_t> freed(bm.dev_freed_ids().begin() + static_cast<std::ptrdiff_t>(freed_before),
                                       bm.dev_freed_ids().end());
        const auto live = live_blocks(table, env);
        std::set<uint64_t> freed_live;
        for (auto id : freed) {
            if (live.count(id) != 0) {
                freed_live.insert(id);
            }
        }
        INFO("rollback freed " << ids_of(freed) << ", the table still names " << ids_of(live));
        CHECK(freed_live.empty());
        append_rows(table, env, COMMITTED, LATER, width, transaction_data::committed());
        REQUIRE(verified_rows(table, env, width) == COMMITTED + LATER);
        return freed_live;
    }

    void require_one_owner_each(tstorage::single_file_block_manager_t& bm, const std::string& path, env_t& env) {
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        INFO("blocks: root " << ids_of(report.root_data) << " free list " << ids_of(report.free_list_content)
                             << " unexplained " << ids_of(report.unexplained) << " named and free "
                             << ids_of(report.reachable_free_overlap));
        CHECK(report.reachable_free_overlap.empty());
        CHECK(report.unexplained.empty());
    }

} // namespace

// 64-byte strings: three row groups fit the packer's two open tails, so the rolled-back row groups share
// their only block with the committed rows. The freed id is reissued to the next write-through, which
// overwrites those rows; the restart then reads none of them.
TEST_CASE("txn_revert_blocks: a rolled-back append must not free the tail its committed rows share",
          "[txn_revert_blocks][shared_tail]") {
    const std::string path = db_path("shared_tail");
    std::remove(path.c_str());
    env_t env;
    const uint64_t width = 64;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        rolled_back_table(env, bm, *table, width);
        REQUIRE_FALSE(checkpoint_production(bm, *table).contains_error());
        require_one_owner_each(bm, path, env);

        append_rows(*table, env, COMMITTED + LATER, TXN_ROWS, width, transaction_data::committed());
        CHECK_FALSE(bm.degraded());
        auto checkpointed = checkpoint_production(bm, *table);
        INFO("checkpoint after the reuse: " << checkpointed.what);
        REQUIRE_FALSE(checkpointed.contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        REQUIRE(verified_rows(*loaded, env, width) == COMMITTED + LATER + TXN_ROWS);
    }
    std::remove(path.c_str());
}

// Same table, reissued after a restart: the reload registers the block again, so the allocation is
// refused and latched instead, and every later checkpoint of the file with it.
TEST_CASE("txn_revert_blocks: a freed tail the root names must not be reissued after a restart",
          "[txn_revert_blocks][shared_tail][restart]") {
    const std::string path = db_path("shared_tail_restart");
    std::remove(path.c_str());
    env_t env;
    const uint64_t width = 64;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        rolled_back_table(env, bm, *table, width);
        REQUIRE_FALSE(checkpoint_production(bm, *table).contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        REQUIRE(verified_rows(*loaded, env, width) == COMMITTED + LATER);
        append_rows(*loaded, env, COMMITTED + LATER, TXN_ROWS, width, transaction_data::committed());
        INFO("allocation: " << (bm.has_allocation_error() ? bm.allocation_error().what : std::pmr::string{"ok"}));
        CHECK_FALSE(bm.degraded());
        auto checkpointed = checkpoint_production(bm, *loaded);
        INFO("checkpoint after the restart: " << checkpointed.what);
        REQUIRE_FALSE(checkpointed.contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        REQUIRE(verified_rows(*loaded, env, width) == COMMITTED + LATER + TXN_ROWS);
    }
    std::remove(path.c_str());
}

// 1000-byte strings: 16 rows per STRING segment, so the kept row group's segments past the revert row
// fill blocks of their own. Nobody frees them, and the packer's open tail among the freed blocks keeps
// taking the next commit's segments.
TEST_CASE("txn_revert_blocks: the segments erased inside the kept row group give their blocks back",
          "[txn_revert_blocks][leak]") {
    const std::string path = db_path("leak");
    std::remove(path.c_str());
    env_t env;
    const uint64_t width = 1000;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        rolled_back_table(env, bm, *table, width);
        REQUIRE_FALSE(checkpoint_production(bm, *table).contains_error());
        require_one_owner_each(bm, path, env);
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        REQUIRE(verified_rows(*loaded, env, width) == COMMITTED + LATER);
    }
    std::remove(path.c_str());
}
