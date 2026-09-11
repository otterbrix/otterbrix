// A failed header write strands the round's allocations in issued_since_root_ -- measured ~655 KB/round on
// a 7.8 MB table. Freeing all of them is corruption, so the rollback discriminates via registry_alive(id).

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
#include <limits>
#include <set>
#include <string>
#include <unistd.h>

#include "block_reachability_walker.hpp"
#include "fault_injection_file.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t ROLLBACK_ROWS = 12000;
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    std::string rollback_db_path(const char* tag) {
        return "/tmp/test_otterbrix_failed_round_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct rollback_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        rollback_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_table(rollback_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "rollback_table");
    }

    std::string row_name(uint64_t row) { return "rollback_row_payload_padding_" + std::to_string(row); }

    void append_rows(data_table_t& table, rollback_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                auto name = row_name(row);
                chunk.set_value(1, i, std::string_view{name});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // Mirrors table_storage_t::checkpoint; only the header write fails so DATA writes don't also latch degraded().
    struct round_result_t {
        bool committed{false};
        core::error_t error{core::error_t::no_error()};
    };

    round_result_t checkpoint_round(tstorage::single_file_block_manager_t& bm,
                                    data_table_t& table,
                                    otterbrix_test::fault_plan_t* plan,
                                    bool header_write_fails) {
        round_result_t out;
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (auto cp = table.checkpoint(writer); cp.has_error()) {
            out.error = cp.error();
            return out;
        }
        if (auto flushed = writer.flush(); flushed.has_error()) {
            out.error = flushed.error();
            return out;
        }
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        if (free_ptr.has_error()) {
            out.error = free_ptr.error();
            return out;
        }
        if (auto barrier = bm.file_sync(); barrier.has_error()) {
            out.error = barrier.error();
            return out;
        }
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        if (header_write_fails) {
            REQUIRE(plan != nullptr);
            plan->fail_after_writes = plan->writes_seen;
        }
        auto committed = bm.write_header(header);
        if (header_write_fails) {
            plan->fail_after_writes = 0;
        }
        if (committed.has_error()) {
            out.error = committed.error();
            return out;
        }
        out.committed = true;
        return out;
    }

    std::unique_ptr<data_table_t> reload_table(rollback_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded.has_error());
        return std::move(loaded.value());
    }

    uint64_t file_size_of(const std::string& path) {
        std::error_code ec;
        auto s = std::filesystem::file_size(path, ec);
        return ec ? 0 : static_cast<uint64_t>(s);
    }

    uint64_t scan_and_count(data_table_t& table, rollback_env_t& env) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
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
                auto id_cell = chunk.value(0, i);
                auto name_cell = chunk.value(1, i);
                const auto id = id_cell.value<int64_t>();
                const auto name = name_cell.value<std::string_view>();
                REQUIRE(name == row_name(static_cast<uint64_t>(id)));
                seen++;
            }
        }
        return seen;
    }

    struct steady_state_t {
        std::unique_ptr<data_table_t> table;
        uint64_t blocks{0};
        uint64_t size{0};
    };

    steady_state_t reach_steady_state(rollback_env_t& env,
                                      tstorage::single_file_block_manager_t& bm,
                                      const std::string& path,
                                      otterbrix_test::fault_plan_t* plan) {
        steady_state_t out;
        out.table = make_table(env, bm);
        append_rows(*out.table, env, 0, ROLLBACK_ROWS);
        for (int warmup = 0; warmup < 3; ++warmup) {
            REQUIRE(out.table->compact(WATERMARK));
            REQUIRE(checkpoint_round(bm, *out.table, plan, false).committed);
        }
        out.blocks = bm.total_blocks();
        out.size = file_size_of(path);
        return out;
    }

} // namespace

// PROBE (measurement, not a gate): per-round cost of a persistent header-write failure, and what the walker sees.
TEST_CASE("failed_round: PROBE the residual of a persistent header-write failure", "[a7.7][probe]") {
    const auto path = rollback_db_path("probe");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);
    WARN("[probe] steady state: block_count=" << steady.blocks << " file_size=" << steady.size);

    for (int round = 1; round <= 5; ++round) {
        const uint64_t before_blocks = bm.total_blocks();
        const uint64_t before_size = file_size_of(path);
        bm.dev_reset_tracking();
        auto r = checkpoint_round(bm, *steady.table, &plan, true);
        CHECK_FALSE(r.committed);
        CHECK_FALSE(bm.degraded());

        std::set<uint64_t> issued(bm.dev_issued_ids().begin(), bm.dev_issued_ids().end());
        std::set<uint64_t> still_orphaned;
        for (auto id : issued) {
            if (!bm.registry_alive(id) && bm.dev_free_list_snapshot().count(id) == 0) {
                still_orphaned.insert(id);
            }
        }
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        WARN("[probe] failed round " << round << ": block_count " << before_blocks << " -> " << bm.total_blocks()
                                     << " file_size " << before_size << " -> " << file_size_of(path) << " (+"
                                     << (file_size_of(path) - before_size) << " B)"
                                     << " issued=" << issued.size() << " orphaned_after_round=" << still_orphaned.size()
                                     << " | walker: durable_block_count=" << report.block_count << " chain="
                                     << report.chain_blocks.size() << " durable_data=" << report.durable_data.size()
                                     << " registry=" << report.registry_live.size() << " freelist="
                                     << report.free_list_content.size() << " unexplained=" << report.unexplained.size()
                                     << " overlap=" << report.reachable_free_overlap.size());
    }
    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

TEST_CASE("failed_round: repeated header-write failures do not grow the file", "[a7.7]") {
    const auto path = rollback_db_path("nogrowth");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);

    for (int round = 1; round <= 6; ++round) {
        auto r = checkpoint_round(bm, *steady.table, &plan, true);
        REQUIRE_FALSE(r.committed);
        REQUIRE_FALSE(bm.degraded());
        INFO("failed round " << round << ": block_count " << steady.blocks << " -> " << bm.total_blocks()
                             << ", file_size " << steady.size << " -> " << file_size_of(path));
        CHECK(bm.total_blocks() == steady.blocks);
        CHECK(file_size_of(path) == steady.size);
    }

    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

// Every issued id lands on one side: registry-alive ids are KEPT; the rest go to reusable_, never pending_free_.
TEST_CASE("failed_round: the rollback gives back only what the live tree does not hold", "[a7.7]") {
    const auto path = rollback_db_path("discriminate");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);

    const uint64_t blocks_before_round = bm.total_blocks();
    // Reset BEFORE compact, so the tracking also captures compact's own allocations, not just the checkpoint's.
    bm.dev_reset_tracking();
    REQUIRE(steady.table->compact(WATERMARK));
    auto r = checkpoint_round(bm, *steady.table, &plan, true);
    REQUIRE_FALSE(r.committed);
    REQUIRE_FALSE(bm.degraded());

    std::set<uint64_t> issued(bm.dev_issued_ids().begin(), bm.dev_issued_ids().end());
    REQUIRE_FALSE(issued.empty());

    const auto reusable = bm.dev_reusable_snapshot();
    const auto pending = bm.dev_pending_free_snapshot();
    const uint64_t high_water = bm.total_blocks();
    size_t kept = 0;
    size_t reissuable = 0;
    size_t past_mark = 0;
    for (auto id : issued) {
        INFO("issued block " << id);
        if (bm.registry_alive(id)) {
            CHECK(reusable.count(id) == 0);
            CHECK(pending.count(id) == 0);
            CHECK(id < high_water);
            kept++;
        } else if (id >= high_water) {
            CHECK(reusable.count(id) == 0);
            CHECK(pending.count(id) == 0);
            past_mark++;
        } else {
            CHECK(reusable.count(id) != 0);
            CHECK(pending.count(id) == 0);
            reissuable++;
        }
    }
    WARN("[a7.7 gate 2] issued=" << issued.size() << " kept(live)=" << kept << " back_in_reusable=" << reissuable
                                 << " dropped_past_high_water=" << past_mark << " block_count " << blocks_before_round
                                 << " -> " << high_water);
    CHECK(kept > 0);
    CHECK(reissuable + past_mark > 0);
    CHECK(bm.total_blocks() <= blocks_before_round);

    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

// pending_free_.erase in the rollback is load-bearing: a later round's compaction can release ids an
// earlier failed round still holds in issued_since_root_ -- two failed compacting rounds is the smallest repro.
TEST_CASE("failed_round: a later round's compaction releases ids the round journal still holds", "[a7.7]") {
    const auto path = rollback_db_path("premise");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);

    bm.dev_reset_tracking();

    REQUIRE(steady.table->compact(WATERMARK));
    REQUIRE_FALSE(checkpoint_round(bm, *steady.table, &plan, true).committed);
    REQUIRE_FALSE(bm.degraded());

    REQUIRE(steady.table->compact(WATERMARK));
    const std::set<uint64_t> issued_since_commit(bm.dev_issued_ids().begin(), bm.dev_issued_ids().end());
    const auto pending_after_compact = bm.dev_pending_free_snapshot();
    std::set<uint64_t> overlap;
    for (auto id : issued_since_commit) {
        if (pending_after_compact.count(id) != 0) {
            overlap.insert(id);
        }
    }
    WARN("[a7.7 gate 2b] issued_since_commit=" << issued_since_commit.size() << " pending_free="
                                               << pending_after_compact.size() << " intersection=" << overlap.size());
    CHECK_FALSE(overlap.empty());

    REQUIRE_FALSE(checkpoint_round(bm, *steady.table, &plan, true).committed);
    const auto reusable_after = bm.dev_reusable_snapshot();
    const auto pending_after = bm.dev_pending_free_snapshot();
    const uint64_t high_water = bm.total_blocks();
    for (auto id : overlap) {
        INFO("id " << id << " was both issued-since-commit and pending-free");
        CHECK((reusable_after.count(id) != 0) + (pending_after.count(id) != 0) <= 1);
        if (bm.registry_alive(id)) {
            CHECK(reusable_after.count(id) == 0);
        } else if (id < high_water) {
            CHECK(pending_after.count(id) == 0);
            CHECK(reusable_after.count(id) != 0);
        }
    }

    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

// Without the rollback, failed-round allocations would push the high-water mark past the durable root's block_count.
TEST_CASE("failed_round: the walker reports zero unexplained after a failed round", "[a7.7]") {
    const auto path = rollback_db_path("walker");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);

    for (int round = 1; round <= 6; ++round) {
        auto r = checkpoint_round(bm, *steady.table, &plan, true);
        REQUIRE_FALSE(r.committed);
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        INFO("failed round " << round << ": durable block_count=" << report.block_count
                             << " high_water=" << bm.total_blocks() << " chain=" << report.chain_blocks.size()
                             << " durable_data=" << report.durable_data.size() << " registry="
                             << report.registry_live.size() << " freelist=" << report.free_list_content.size()
                             << " unexplained=" << report.unexplained.size());
        CHECK(report.unexplained.empty());
        // Durably reachable AND published as free would be reissued over live data.
        CHECK(report.reachable_free_overlap.empty());
        for (auto id : report.chain_blocks) {
            INFO("durable chain block " << id << " past high-water " << bm.total_blocks());
            CHECK(id < bm.total_blocks());
        }
        for (auto id : report.durable_data) {
            INFO("durable data block " << id << " past high-water " << bm.total_blocks());
            CHECK(id < bm.total_blocks());
        }
        for (auto id : report.registry_live) {
            INFO("registry-live block " << id << " past high-water " << bm.total_blocks());
            CHECK(id < bm.total_blocks());
        }
    }
    remove_file(path);
}

TEST_CASE("failed_round: a transient header-write failure still recovers", "[a7.7]") {
    const auto path = rollback_db_path("transient");
    remove_file(path);
    rollback_env_t env;

    uint64_t iteration_after_failure = 0;
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto steady = reach_steady_state(env, bm, path, &plan);

        tstorage::database_header_t before{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, before));

        REQUIRE_FALSE(checkpoint_round(bm, *steady.table, &plan, true).committed);
        REQUIRE_FALSE(bm.degraded());
        tstorage::database_header_t during{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, during));
        CHECK(during.iteration == before.iteration);

        REQUIRE(steady.table->compact(WATERMARK));
        REQUIRE(checkpoint_round(bm, *steady.table, &plan, false).committed);
        CHECK(file_size_of(path) == steady.size);

        tstorage::database_header_t after{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, after));
        CHECK(after.iteration == before.iteration + 1);
        iteration_after_failure = after.iteration;
    }

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::database_header_t header{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, header));
        CHECK(header.iteration == iteration_after_failure);
        auto table = reload_table(env, bm);
        REQUIRE(scan_and_count(*table, env) == ROLLBACK_ROWS);
    }
    remove_file(path);
}

// When the header write leaves the durable root INDETERMINATE, rollback must refuse outright: a torn write
// can look CRC-valid on the device even though write() reports failure, and a crash could recover that root.
TEST_CASE("failed_round: an INDETERMINATE header write releases nothing", "[a7.7]") {
    const auto path = rollback_db_path("indeterminate");
    remove_file(path);
    rollback_env_t env;

    std::set<uint64_t> issued;
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto steady = reach_steady_state(env, bm, path, &plan);
        const uint64_t blocks_before_round = bm.total_blocks();

        bm.dev_reset_tracking();
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(steady.table->checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());

        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        plan.torn_at_write = plan.writes_seen + 1;
        plan.fail_syncs_from = plan.syncs_seen + 1;
        auto committed = bm.write_header(header);
        plan.torn_at_write = 0;
        plan.fail_after_writes = 0;
        plan.fail_syncs_from = 0;
        REQUIRE(committed.has_error());
        // Case 3 latches: the manager refuses every later checkpoint on this file.
        CHECK(bm.degraded());

        issued.insert(bm.dev_issued_ids().begin(), bm.dev_issued_ids().end());
        REQUIRE_FALSE(issued.empty());
        const auto reusable = bm.dev_reusable_snapshot();
        for (auto id : issued) {
            INFO("issued block " << id << " must NOT have been released");
            CHECK(reusable.count(id) == 0);
        }
        CHECK(bm.total_blocks() >= blocks_before_round);

        const uint64_t rolled_back = bm.roll_back_uncommitted_round();
        CHECK(rolled_back == 0);
    }

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        REQUIRE(scan_and_count(*table, env) == ROLLBACK_ROWS);
    }
    remove_file(path);
}
