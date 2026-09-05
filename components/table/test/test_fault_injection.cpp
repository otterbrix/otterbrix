// Smoke tests for the fault-injection seam: it can force (1) a write failure after N writes,
// (2) a torn write, (3) loss of everything after the last fsync (kill -9 semantics), and
// the killed state can be REOPENED through the normal load path — no hand-laid files.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <filesystem>
#include <string>
#include <unistd.h>

#include "block_reachability_walker.hpp"
#include "fault_injection_file.hpp"
#include "table_segment_scan.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string fault_db_path() {
        static std::string path =
            "/tmp/test_otterbrix_fault_injection_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_fault_files() {
        std::remove(fault_db_path().c_str());
        std::remove((fault_db_path() + ".crashcopy").c_str());
    }

    struct fault_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        fault_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_table(fault_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "fault_table");
    }

    void append_rows(data_table_t& table, fault_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, static_cast<int64_t>(start + offset + i));
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // Same, for the APPEND path. An append made while the device is failing REPORTS io_error —
    // column_data_t::transition_to_disk forwards partial_block_manager_t::flush_partial_blocks
    // rather than returning success over a block that never reached the file. The two cases
    // below stand INSIDE that window on purpose: they assert what the DURABLE HEADER does, not
    // that an append survives a dead disk. They use this variant so the append's honest failure
    // is observed rather than aborting the case.
    bool try_append_rows(data_table_t& table, fault_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, static_cast<int64_t>(start + offset + i));
            }
            table_append_state state(&env.resource);
            if (table.append_lock(state).has_error() || table.initialize_append(state).has_error() ||
                table.append(chunk, state).has_error()) {
                return false;
            }
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
        return true;
    }

    // Production-shaped checkpoint that REPORTS failure instead of asserting: fault tests
    // exist to observe the failure path.
    bool try_checkpoint(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (table.checkpoint(writer).has_error()) {
            return false;
        }
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        // The pre-header barrier can legitimately fail here — that is what these cases inject —
        // so bind its answer rather than dropping it: a failed barrier means the checkpoint did
        // not happen, which is exactly this helper's return value.
        if (auto barrier = bm.file_sync(); barrier.has_error()) {
            return false;
        }
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        // write_header writes AND fsyncs the header slot and reports both. That result IS
        // whether the checkpoint happened, so it is this helper's answer.
        if (bm.write_header(header).has_error()) {
            return false;
        }
        return true;
    }

    uint64_t scan_rows(data_table_t& table, uint64_t upper_bound) {
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(table, 0, upper_bound, [&](data_chunk_t& chunk) { scanned += chunk.size(); });
        return scanned;
    }

} // namespace

TEST_CASE("fault_injection: write failure after N writes does not advance the durable header") {
    cleanup_fault_files();
    fault_env_t env;
    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, fault_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 3000);

        // Let the first checkpoint through, then record its durable iteration.
        REQUIRE(try_checkpoint(bm, *table));
        tstorage::database_header_t before;
        REQUIRE(otterbrix_test::read_active_durable_header(fault_db_path(), before));

        // Fail every write from here on: the next checkpoint must not advance the header.
        plan.fail_after_writes = plan.writes_seen;
        // Unobserved on purpose: with every write failing, the write-through leg of this append
        // correctly reports io_error. What this case is about is the header below.
        try_append_rows(*table, env, 3000, 100);
        // Return deliberately unobserved: HEAD reports success even when the writes were
        // dropped on the floor — the DURABLE check below is the one that matters.
        try_checkpoint(bm, *table);
        tstorage::database_header_t after;
        REQUIRE(otterbrix_test::read_active_durable_header(fault_db_path(), after));
        CHECK(after.iteration == before.iteration);
        CHECK(after.meta_block == before.meta_block);
    }
    cleanup_fault_files();
}

TEST_CASE("fault_injection: torn header write leaves the previous durable state loadable") {
    cleanup_fault_files();
    fault_env_t env;
    otterbrix_test::fault_plan_t plan;
    {
        otterbrix_test::fault_injection_scope_t scope(plan);
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, fault_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 3000);
        REQUIRE(try_checkpoint(bm, *table));

        // Tear the very next write (wherever the second checkpoint round lands first) and
        // fail the rest: a mid-checkpoint power cut.
        plan.torn_at_write = plan.writes_seen + 1;
        // Unobserved for the same reason as the case above.
        try_append_rows(*table, env, 3000, 100);
        // Return unobserved for the same reason as above.
        try_checkpoint(bm, *table);
    }
    {
        // Reopen WITHOUT fault injection: the file must load, and must show the first
        // checkpoint's state (3000 rows) — the torn round never became durable.
        fault_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, fault_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env2.resource, bm, reader);
        REQUIRE(!loaded.has_error());
        CHECK(scan_rows(*loaded.value(), 4000) == 3000);
    }
    cleanup_fault_files();
}

TEST_CASE("fault_injection: crash_revert loses everything after the last fsync, state reopens") {
    cleanup_fault_files();
    fault_env_t env;
    otterbrix_test::fault_plan_t plan;
    std::string copy_path = fault_db_path() + ".crashcopy";
    {
        otterbrix_test::fault_injection_scope_t scope(plan);
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, fault_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 2000);
        REQUIRE(try_checkpoint(bm, *table)); // durable state A: 2000 rows

        // More rows and ANOTHER checkpoint, but kill before its final fsync could matter:
        // crash_revert() rolls the file back to the last successful fsync boundary.
        append_rows(*table, env, 2000, 2000);
        REQUIRE(scope.last() != nullptr);
        // Sabotage the tail: revert everything written since the last sync, then verify
        // the on-disk state equals durable state A. (The final fsync of checkpoint A was
        // the last sync; the write-through of the extra rows is exactly what a kill loses.)
        scope.last()->crash_revert();

        // The killed state is reopened through a filesystem COPY under a fresh
        // environment — the "открыть как после kill" mechanism, no hand-laid files.
        std::filesystem::copy_file(fault_db_path(), copy_path,
                                   std::filesystem::copy_options::overwrite_existing);
    }
    {
        fault_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, copy_path);
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env2.resource, bm, reader);
        REQUIRE(!loaded.has_error());
        CHECK(scan_rows(*loaded.value(), 5000) == 2000);
    }
    cleanup_fault_files();
}
