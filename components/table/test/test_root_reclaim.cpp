// Reclaim root N once root N+1 is durable; without this, checkpointing an UNCHANGED table
// extends the file every round, forever.
//
//     free = {blocks of root N} u {metadata chain of N} u {free-list chain of N}
//            - {blocks of root N+1} - {ids live in the block registry}
//
// Reachability is defined by the LOADER, not a second chain walk of this file's own invention.

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

    constexpr uint64_t RECLAIM_ROWS = 12000;
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    std::string reclaim_db_path(const char* tag) {
        return "/tmp/test_otterbrix_root_reclaim_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct reclaim_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        reclaim_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_table(reclaim_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "reclaim_table");
    }

    std::string row_name(uint64_t row) { return "reclaim_row_payload_padding_" + std::to_string(row); }

    void append_rows(data_table_t& table, reclaim_env_t& env, uint64_t start, uint64_t count) {
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

    std::unique_ptr<data_table_t> reload_table(reclaim_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded.has_error());
        return std::move(loaded.value());
    }

    template<typename container_t>
    std::string dump(const container_t& ids) {
        std::string s = "{";
        for (auto id : ids) {
            s += std::to_string(id) + ",";
        }
        s += "}";
        return s;
    }

    uint64_t file_size_of(const std::string& path) {
        std::error_code ec;
        auto s = std::filesystem::file_size(path, ec);
        return ec ? 0 : static_cast<uint64_t>(s);
    }

    uint64_t scan_and_count(data_table_t& table, reclaim_env_t& env) {
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

} // namespace

TEST_CASE("root_reclaim: PROBE measure per-round growth and load-time allocation", "[a7.3][probe]") {
    const auto path = reclaim_db_path("probe");
    remove_file(path);
    reclaim_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, RECLAIM_ROWS);
        checkpoint_production(bm, *table);
        WARN("[probe] after first checkpoint: block_count=" << bm.total_blocks()
                                                            << " file_size=" << file_size_of(path));
    }

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        bm.dev_reset_tracking();
        const uint64_t blocks_before = bm.total_blocks();
        auto table = reload_table(env, bm);
        WARN("[probe] load allocated issued=" << bm.dev_issued_ids().size()
                                              << " block_count " << blocks_before << " -> " << bm.total_blocks());
        REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);
        WARN("[probe] after a full scan of the freshly loaded table: issued="
             << bm.dev_issued_ids().size() << " block_count=" << bm.total_blocks());
    }

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        for (int round = 1; round <= 4; ++round) {
            const uint64_t before_blocks = bm.total_blocks();
            const uint64_t before_size = file_size_of(path);
            bm.dev_reset_tracking();
            REQUIRE(table->compact(WATERMARK));
            bm.dev_reset_tracking();
            checkpoint_production(bm, *table);
            auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
            REQUIRE(report.ok);
            WARN("[probe] round " << round << ": block_count " << before_blocks << " -> " << bm.total_blocks()
                                  << " (+" << (bm.total_blocks() - before_blocks) << ")"
                                  << " file_size " << before_size << " -> " << file_size_of(path)
                                  << " (+" << (file_size_of(path) - before_size) << " bytes)"
                                  << " reclaimed_this_round=" << bm.dev_freed_ids().size()
                                  << " chain=" << report.chain_blocks.size()
                                  << " durable_data=" << report.durable_data.size()
                                  << " registry=" << report.registry_live.size()
                                  << " freelist=" << report.free_list_content.size()
                                  << " unexplained=" << report.unexplained.size());
        }
        REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);
    }

    remove_file(path);
}

// Without this, +5 blocks EVERY round, forever, on a table nobody touched (measured on the
// PROBE above).
TEST_CASE("root_reclaim: an unchanged database does not grow round to round", "[a7.3]") {
    const auto path = reclaim_db_path("steady");
    remove_file(path);
    reclaim_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, RECLAIM_ROWS);
        checkpoint_production(bm, *table);
    }

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.load_existing_database().has_error());
    auto table = reload_table(env, bm);

    for (int warmup = 0; warmup < 2; ++warmup) {
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);
    }

    const uint64_t steady_blocks = bm.total_blocks();
    const uint64_t steady_size = file_size_of(path);
    for (int round = 0; round < 4; ++round) {
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);
        INFO("round " << round << ": block_count " << steady_blocks << " -> " << bm.total_blocks()
                      << ", file_size " << steady_size << " -> " << file_size_of(path));
        CHECK(bm.total_blocks() == steady_blocks);
        CHECK(file_size_of(path) == steady_size);
    }
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

TEST_CASE("root_reclaim: nothing reclaimed is reachable from the new root or live", "[a7.3]") {
    const auto path = reclaim_db_path("walker");
    remove_file(path);
    reclaim_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto table = make_table(env, bm);
    append_rows(*table, env, 0, RECLAIM_ROWS);

    for (int round = 1; round <= 4; ++round) {
        bm.dev_reset_tracking();
        REQUIRE(table->compact(WATERMARK));
        std::set<uint64_t> released;
        for (auto id : bm.dev_freed_ids()) {
            released.insert(id);
        }
        checkpoint_production(bm, *table);
        for (auto id : bm.dev_freed_ids()) {
            released.insert(id);
        }

        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        INFO("round " << round << " chain=" << report.chain_blocks.size()
                      << " durable_data=" << report.durable_data.size()
                      << " registry=" << report.registry_live.size()
                      << " freelist=" << report.free_list_content.size()
                      << " unexplained=" << report.unexplained.size());
        CHECK(report.reachable_free_overlap.empty());
        for (auto id : released) {
            INFO("released block " << id);
            CHECK(report.chain_blocks.count(id) == 0);
            CHECK(report.durable_data.count(id) == 0);
            CHECK(report.registry_live.count(id) == 0);
            CHECK_FALSE(bm.registry_alive(id));
        }
        CHECK(report.unexplained.empty());
    }
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

// The reclaim files ids into pending_free_, which free_block_id never draws from.
TEST_CASE("root_reclaim: crash between reclaim and header write leaves root N readable", "[a7.3]") {
    const auto path = reclaim_db_path("crash");
    remove_file(path);
    reclaim_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, RECLAIM_ROWS);
        checkpoint_production(bm, *table);
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);
    }

    uint64_t iteration_before = 0;
    {
        tstorage::database_header_t header{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, header));
        iteration_before = header.iteration;
    }

    // No file bytes are placed by hand: the fault interposer reverts everything since the last fsync.
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        REQUIRE(table->compact(WATERMARK));

        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table->checkpoint(writer).has_error()); // <- the reclaim happens in here
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert(); // power cut before write_header
    }

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::database_header_t header{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, header));
        CHECK(header.iteration == iteration_before);
        auto table = reload_table(env, bm);
        REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);
    }

    remove_file(path);
}

// The latch is sticky: a DEGRADED manager must stop the rebuild after one transient EIO/ENOSPC.
TEST_CASE("root_reclaim: one transient fsync failure does not grow the file without bound", "[a7.3]") {
    const auto path = reclaim_db_path("degraded");
    remove_file(path);
    reclaim_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto table = make_table(env, bm);
    append_rows(*table, env, 0, RECLAIM_ROWS);

    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    const uint64_t healthy_blocks = bm.total_blocks();
    CHECK_FALSE(bm.degraded());

    plan.fail_syncs_from = plan.syncs_seen + 1;
    {
        REQUIRE(table->compact(WATERMARK));
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        auto cp = table->checkpoint(writer);
        auto flushed = writer.flush();
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        auto barrier = bm.file_sync();
        INFO("the round that hit the transient error: checkpoint="
             << cp.has_error() << " flush=" << flushed.has_error() << " free_list=" << free_ptr.has_error()
             << " barrier=" << barrier.has_error());
        CHECK(barrier.has_error()); // the failing fsync is the pre-header barrier
    }
    plan.fail_syncs_from = 0; // transient: the device is healthy again
    CHECK(bm.degraded());

    const uint64_t after_failure = bm.total_blocks();
    // A degraded manager must make this loop cost NOTHING, not a full copy of the table per round.
    for (int round = 0; round < 5; ++round) {
        if (table->compact(WATERMARK)) {
            tstorage::metadata_manager_t meta_mgr(bm);
            tstorage::metadata_writer_t writer(meta_mgr);
            auto cp = table->checkpoint(writer);
            if (!cp.has_error()) {
                auto flushed = writer.flush();
                if (!flushed.has_error()) {
                    bm.set_meta_block(writer.get_block_pointer().block_pointer);
                    auto free_ptr = bm.serialize_free_list();
                    if (!free_ptr.has_error()) {
                        auto barrier = bm.file_sync();
                        if (!barrier.has_error()) {
                            tstorage::database_header_t header{};
                            header.initialize();
                            header.free_list = free_ptr.value().block_pointer;
                            auto committed = bm.write_header(header);
                            CHECK(committed.has_error()); // the latch refuses to commit
                        }
                    }
                }
            }
        }
        INFO("round " << round << ": healthy=" << healthy_blocks << " after_failure=" << after_failure
                      << " now=" << bm.total_blocks());
        CHECK(bm.total_blocks() == after_failure);
    }

    // Loud is NOT fatal: the degraded table still serves reads.
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

// The candidate list is DISK BYTES: block_manager_t::unregister_block's
// `assert(id < MAXIMUM_BLOCK)` does NOTHING under NDEBUG, letting the id wrap onto a live block.
TEST_CASE("root_reclaim: a transient-domain candidate is dropped and latched, not asserted", "[a7.3]") {
    const auto path = reclaim_db_path("domain");
    remove_file(path);
    reclaim_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());

    std::pmr::vector<uint64_t> poisoned(&env.resource);
    poisoned.push_back(tstorage::MAXIMUM_BLOCK + 7);
    bm.adopt_durable_root_data_blocks(poisoned);

    std::pmr::vector<uint64_t> new_root(&env.resource); // the root under construction names nothing
    auto reclaimed = bm.reclaim_superseded_root(new_root);
    REQUIRE_FALSE(reclaimed.has_error());
    CHECK(reclaimed.value() == 0); // it was refused, not reclaimed

    CHECK(bm.dev_reusable_snapshot().count(tstorage::MAXIMUM_BLOCK + 7) == 0);
    CHECK(bm.dev_pending_free_snapshot().count(tstorage::MAXIMUM_BLOCK + 7) == 0);
    REQUIRE(bm.has_allocation_error());
    CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);

    tstorage::database_header_t header{};
    header.initialize();
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());

    remove_file(path);
}

// Propagating a reclaim read error without latching leaves every health gate blind, so the
// next round rebuilds and fails the same walk, forever. DECISION: a failed reclaim LATCHES and
// does NOT roll allocations back -- see the long note at reclaim_superseded_root.
TEST_CASE("root_reclaim: a failed reclaim latches degraded and stops the file growing", "[a7.3][item_a]") {
    const auto path = reclaim_db_path("reclaim_read_error");
    remove_file(path);
    reclaim_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, RECLAIM_ROWS);
        checkpoint_production(bm, *table);
    }

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.load_existing_database().has_error());
    auto table = reload_table(env, bm);

    for (int warmup = 0; warmup < 2; ++warmup) {
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);
    }
    const uint64_t healthy_blocks = bm.total_blocks();
    CHECK_FALSE(bm.degraded());

    // The poison targets the DURABLE root's metadata chain, located via the file's own header.
    tstorage::database_header_t durable{};
    REQUIRE(otterbrix_test::read_active_durable_header(path, durable));
    REQUIRE(durable.meta_block != tstorage::INVALID_INDEX);
    const uint64_t meta_block_id = durable.meta_block / tstorage::META_SUB_BLOCKS_PER_BLOCK;
    plan.fail_reads_at_location = tstorage::BLOCK_START + meta_block_id * bm.block_allocation_size();

    REQUIRE(table->compact(WATERMARK));
    {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        auto cp = table->checkpoint(writer);
        INFO("the round whose reclaim could not read root N: checkpoint=" << cp.has_error()
             << " reads_failed=" << plan.reads_failed);
        CHECK(cp.has_error()); // the reclaim could not account for root N
    }
    CHECK(plan.reads_failed > 0);
    // The block stays rotten on purpose: the engine can't tell it apart from a transient EIO.

    CHECK(bm.degraded());

    const uint64_t after_failure = bm.total_blocks();
    for (int round = 0; round < 5; ++round) {
        if (table->compact(WATERMARK)) {
            tstorage::metadata_manager_t meta_mgr(bm);
            tstorage::metadata_writer_t writer(meta_mgr);
            auto cp = table->checkpoint(writer);
            if (!cp.has_error()) {
                auto flushed = writer.flush();
                if (!flushed.has_error()) {
                    bm.set_meta_block(writer.get_block_pointer().block_pointer);
                    auto free_ptr = bm.serialize_free_list();
                    if (!free_ptr.has_error()) {
                        auto barrier = bm.file_sync();
                        if (!barrier.has_error()) {
                            tstorage::database_header_t header{};
                            header.initialize();
                            header.free_list = free_ptr.value().block_pointer;
                            auto committed = bm.write_header(header);
                            CHECK(committed.has_error()); // the latch refuses to commit
                        }
                    }
                }
            }
        }
        INFO("round " << round << ": healthy=" << healthy_blocks << " after_failure=" << after_failure
                      << " now=" << bm.total_blocks());
        CHECK(bm.total_blocks() == after_failure);
    }

    // Loud is NOT fatal: the degraded table still serves reads.
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

// row_group() hands out COUNTED collection copies BY VALUE, so a held copy outlives compact();
// its destructor erasing blocks_[id] by ID after reissue would take the LIVE handle's slot with it.
TEST_CASE("root_reclaim: a collection held across compact does not strip a reused block's registry entry",
          "[a7.3][item_c]") {
    const auto path = reclaim_db_path("stale_holder");
    remove_file(path);
    reclaim_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto table = make_table(env, bm);
    append_rows(*table, env, 0, RECLAIM_ROWS);
    checkpoint_production(bm, *table);

    auto stale = table->row_group();
    REQUIRE(stale);

    // Round 1 releases the stale ids into pending_free_; round 2 is the first that may reissue them.
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);

    // The premise, asserted rather than assumed: at least one stale-held id is now live-registered.
    std::pmr::vector<uint64_t> stale_ids{&env.resource};
    stale->collect_disk_block_ids(stale_ids);
    std::sort(stale_ids.begin(), stale_ids.end());
    stale_ids.erase(std::unique(stale_ids.begin(), stale_ids.end()), stale_ids.end());
    std::set<uint64_t> reused;
    for (auto id : stale_ids) {
        if (bm.registry_alive(id)) {
            reused.insert(id);
        }
    }
    INFO("stale collection ids " << dump(stale_ids) << ", reused by the live table " << dump(reused));
    REQUIRE_FALSE(reused.empty());

    stale.reset();

    for (auto id : reused) {
        INFO("reused block " << id << " must still be live table state");
        CHECK(bm.registry_alive(id));
    }

    // The reclaim on top of this must not have freed anything the table is still reading.
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    INFO("chain=" << report.chain_blocks.size() << " durable_data=" << report.durable_data.size()
                  << " registry=" << report.registry_live.size() << " freelist="
                  << report.free_list_content.size() << " unexplained=" << dump(report.unexplained));
    CHECK(report.reachable_free_overlap.empty());
    CHECK(report.unexplained.empty());
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}
