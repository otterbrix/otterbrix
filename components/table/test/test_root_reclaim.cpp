// A7.3 — reclaim root N once root N+1 is durable.
//
// A7.1 made the previous root real (two slots, live CRC, winner among valid slots) and A7.2
// stopped handing out a block the durable root still names (pending_free_ / reusable_). What
// neither did is the other direction: NOTHING ever frees the SUPERSEDED root, so a checkpoint
// of an UNCHANGED table extends the file every single round, forever.
//
// The freeing formula this file gates:
//
//     free = {blocks of root N} u {metadata chain of N} u {free-list chain of N}
//            - {blocks of root N+1} - {ids live in the block registry}
//
// Reachability is defined by the LOADER, never by a second chain walk of this file's own
// invention: block_reachability_walker.hpp SCRATCH-LOADS a table from the durable root and
// takes the registry delta, so the walker and the engine cannot disagree about what a root
// references. Every gate below is phrased against that walker.

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

    // The EXACT sequence of table_storage_t::checkpoint (services/disk/manager_disk.cpp),
    // reproduced so a unit test stands in the same window production does.
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

// ---------------------------------------------------------------------------------------
// PROBE (measurement, not a gate): the numbers this task is required to report.
// ---------------------------------------------------------------------------------------
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

    // LOADING IS WRITING? Re-measure: how many blocks does simply OPENING the table allocate?
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

    // Round-to-round growth of an UNCHANGED database.
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
            // Round 1 is THE COMPACTING ROUND the task asks to be measured: the file holds two
            // copies of the table at once from here on, because root N's blocks are only
            // reusable after root N+1's header is on the device.
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

// ---------------------------------------------------------------------------------------
// A7.3 GATE 1 — the plan's gate: on an UNCHANGED database the file does not grow round to
// round.
//
// RED on HEAD: every checkpoint writes a fresh metadata chain, a fresh free-list chain and a
// fresh set of packed data blocks, and nothing frees the superseded root's copies of any of
// the three. Measured on the probe above: +5 blocks EVERY round, forever, on a table nobody
// touched.
// ---------------------------------------------------------------------------------------
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

    // Two warm-up rounds: the FIRST compacting round legitimately doubles the occupancy (the
    // superseded root's blocks are only reusable once the new header is durable, so both
    // copies live in the same file at once), and the second is the first that can spend that
    // reclaimed space. From round three on, an unchanged table must be a closed cycle.
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

// ---------------------------------------------------------------------------------------
// A7.3 GATE 2 — the walker reports ZERO unexplained blocks after a compacting checkpoint,
// and every id the round reclaimed is provably neither reachable from the NEW root nor live
// in the registry (the formula's two subtractions, asserted directly rather than assumed).
// ---------------------------------------------------------------------------------------
TEST_CASE("root_reclaim: nothing reclaimed is reachable from the new root or live", "[a7.3]") {
    const auto path = reclaim_db_path("walker");
    remove_file(path);
    reclaim_env_t env;

    // ONE session, and every checkpoint is preceded by a compact -- the exact shape
    // agent_disk_t::checkpoint_inner runs. That matters for the ZERO below: the engine keeps
    // two physical copies of a table at all times (the live write-through tree, uncompressed,
    // and the checkpoint's packed compressed copy), and it is compact() that releases the
    // previous live tree. A session that appends and then closes WITHOUT compacting strands
    // its live tree -- no root ever named those blocks, so no root reclaim can find them
    // either. That one-off cost per unclean session is measured in the PROBE above and is a
    // different defect from the one A7.3 fixes; it is not smuggled into this gate by choosing
    // a session shape that hides it.
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto table = make_table(env, bm);
    append_rows(*table, env, 0, RECLAIM_ROWS);

    for (int round = 1; round <= 4; ++round) {
        bm.dev_reset_tracking();
        REQUIRE(table->compact(WATERMARK));
        // Everything mark_as_free'd during the round: compact's release of the outgoing
        // collection AND A7.3's reclaim of the superseded root.
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
        // Subtraction 1: nothing released is reachable from the NEW durable root (its
        // metadata/free-list chains or the data blocks a scratch LOAD of that root registers).
        // Subtraction 2: nothing released is live in the block registry.
        for (auto id : released) {
            INFO("released block " << id);
            CHECK(report.chain_blocks.count(id) == 0);
            CHECK(report.durable_data.count(id) == 0);
            CHECK(report.registry_live.count(id) == 0);
            CHECK_FALSE(bm.registry_alive(id));
        }
        // The gate itself: after the reclaim there is no block below the high-water mark that
        // nothing accounts for.
        CHECK(report.unexplained.empty());
    }
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// A7.3 GATE 3 — a crash BETWEEN the reclaim and the header write leaves root N intact and
// READABLE. The reclaim files ids into pending_free_, which free_block_id never draws from,
// so nothing root N reads can have been overwritten by the round that died.
// ---------------------------------------------------------------------------------------
TEST_CASE("root_reclaim: crash between reclaim and header write leaves root N readable", "[a7.3]") {
    const auto path = reclaim_db_path("crash");
    remove_file(path);
    reclaim_env_t env;

    // Two durable rounds, so the root that must survive is itself a superseding root.
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
        tstorage::database_header_t header;
        REQUIRE(otterbrix_test::read_active_durable_header(path, header));
        iteration_before = header.iteration;
    }

    // Round N+1 runs up to (and including) the reclaim and the pre-header barrier, then the
    // power goes out. No file bytes are placed by hand: the T3 interposer reverts everything
    // written since the last successful fsync and kills the handle.
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

    // Reopen: the durable root must still be root N, and its DATA must read back exactly.
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        tstorage::database_header_t header;
        REQUIRE(otterbrix_test::read_active_durable_header(path, header));
        CHECK(header.iteration == iteration_before);
        auto table = reload_table(env, bm);
        REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);
    }

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// ITEM 2 — one TRANSIENT I/O error must not make the file grow without bound.
//
// Both latches (durability_error_, allocation_error_) make write_header return BEFORE
// promote_durable_root(), and both are sticky by design. So after a single transient
// EIO/ENOSPC pending_free_ never drains again for the life of the manager: free_block_id
// draws only from reusable_, reusable_ stays empty, and every rebuild extends the file.
// Meanwhile nothing upstream looks at the latch — agent_disk_t::checkpoint_inner keeps
// calling compact() every round — so the table grows by its own full size EVERY round,
// forever.
//
// RED on HEAD: block_count climbs by a full copy per round after ONE failed fsync.
// The chosen fix is that a DEGRADED manager stops the rebuild: data_table_t::compact refuses
// (the round is skipped exactly like the MVCC-gate skip), table_storage_t::checkpoint
// refuses with the latched error, and agent_disk_t::checkpoint_inner logs and defers. Reads
// keep working — loud is not fatal.
// ---------------------------------------------------------------------------------------
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

    // Two healthy rounds to reach the steady state.
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    const uint64_t healthy_blocks = bm.total_blocks();
    CHECK_FALSE(bm.degraded());

    // ONE transient fsync failure, then the device recovers.
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
    // Every later round takes the shape agent_disk_t::checkpoint_inner runs: compact, and only
    // if compact accepted the round, checkpoint. A degraded manager must make that loop cost
    // NOTHING — not a full copy of the table per round.
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
                            tstorage::database_header_t header;
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

    // Loud is NOT fatal (rule 6): the degraded table still serves reads.
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// A7.3, rule 19: the candidate list is DISK BYTES, so it must not reach an assert().
//
// The reclaim's candidates come from two disk-fed places: the durable root's data blocks,
// which data_table_t::load_from_disk collects out of data_pointer_t::block_pointer.block_id
// (a full uint64 read straight off the file, with no domain check between the reader and
// here), and the two chain walks. block_manager_t::unregister_block guards its input with
// `assert(id < MAXIMUM_BLOCK)` — an abort in a debug build, on the agent thread inside the
// checkpoint coroutine (rule 9), and NOTHING under NDEBUG, where a transient-domain id would
// then be fed to block_location and wrapped onto a real live block.
//
// A candidate outside the addressable domain must be dropped and latched, in every build.
// ---------------------------------------------------------------------------------------
TEST_CASE("root_reclaim: a transient-domain candidate is dropped and latched, not asserted", "[a7.3]") {
    const auto path = reclaim_db_path("domain");
    remove_file(path);
    reclaim_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());

    // What a corrupt row_group_pointer_t stream hands the manager as "root N's data blocks".
    std::pmr::vector<uint64_t> poisoned(&env.resource);
    poisoned.push_back(tstorage::MAXIMUM_BLOCK + 7);
    bm.adopt_durable_root_data_blocks(poisoned);

    std::pmr::vector<uint64_t> new_root(&env.resource); // the root under construction names nothing
    auto reclaimed = bm.reclaim_superseded_root(new_root);
    REQUIRE_FALSE(reclaimed.has_error());
    CHECK(reclaimed.value() == 0); // it was refused, not reclaimed

    // It never entered either half of the free pool...
    CHECK(bm.dev_reusable_snapshot().count(tstorage::MAXIMUM_BLOCK + 7) == 0);
    CHECK(bm.dev_pending_free_snapshot().count(tstorage::MAXIMUM_BLOCK + 7) == 0);
    // ...and the refusal is on the error channel, in every build.
    REQUIRE(bm.has_allocation_error());
    CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);

    // A checkpoint standing on accounting known to be corrupt must not become the durable root.
    tstorage::database_header_t header;
    header.initialize();
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// ITEM A — a FAILED RECLAIM must not fail every checkpoint forever while the file grows.
//
// reclaim_superseded_root walks root N's chains through metadata_manager_t::chain_blocks,
// which pins each sub-block via single_file_block_manager_t::read(). read() reports
// io_error / data_corruption but latched NOTHING, so the checkpoint failed while degraded()
// stayed false. Every health gate of the previous round keys exclusively on degraded()
// (data_table_t::compact, table_storage_t::checkpoint, agent_disk_t::checkpoint_inner), so
// none of them fired: the next round compacted again, rebuilt the whole collection into
// freshly extended blocks (compact's release goes to pending_free_, which only a COMMITTED
// header drains), failed the same walk, and did it again. One rotten bit in root N's
// metadata chain therefore cost a full copy of the table PER ROUND, forever, with every
// health indicator reporting the file healthy.
//
// DECISION, written down: a failed reclaim LATCHES (allocation_error_ — it is exactly a
// "this manager can no longer account for its blocks" failure), and the failed round does
// NOT roll its allocations back. See the long note at reclaim_superseded_root for why the
// rollback would be the more dangerous half.
//
// RED on HEAD: degraded() == false after the failed round, and block_count climbs every
// round after it.
// ---------------------------------------------------------------------------------------
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

    // Two healthy rounds: the steady state where an unchanged table is a closed cycle.
    for (int warmup = 0; warmup < 2; ++warmup) {
        REQUIRE(table->compact(WATERMARK));
        checkpoint_production(bm, *table);
    }
    const uint64_t healthy_blocks = bm.total_blocks();
    CHECK_FALSE(bm.degraded());

    // One rotten block: the DURABLE root's table-metadata chain. Its location is derived from
    // the file's own double header, so the poison lands on exactly the block the reclaim is
    // about to walk — no file bytes are placed by hand.
    tstorage::database_header_t durable;
    REQUIRE(otterbrix_test::read_active_durable_header(path, durable));
    REQUIRE(durable.meta_block != tstorage::INVALID_INDEX);
    const uint64_t meta_block_id = durable.meta_block / tstorage::META_SUB_BLOCKS_PER_BLOCK;
    plan.fail_reads_at_location = tstorage::BLOCK_START + meta_block_id * bm.block_allocation_size();

    // The round that hits it, in the shape services/disk/manager_disk.cpp runs.
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
    // The block stays rotten — that is the point. A bad bit in root N's metadata chain does
    // not heal, and the engine cannot tell it apart from a transient EIO on the same block;
    // either way root N is the root a crash recovers, so a file whose last durable root cannot
    // be read is not a file a later round may keep building on.

    // GATE 1: the failure is visible to whatever decides to compact. Nothing in the engine
    // reads "the checkpoint returned an error" — every gate keys on degraded().
    CHECK(bm.degraded());

    // GATE 2: the file stops growing. The failed round's own allocations are a ONE-TIME cost
    // (they are live in-memory table state and must not be freed); every round after it costs
    // nothing at all.
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
                            tstorage::database_header_t header;
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

    // Loud is NOT fatal (rule 6): the degraded table still serves reads.
    REQUIRE(scan_and_count(*table, env) == RECLAIM_ROWS);

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// ITEM C — a collection held across compact must not strip the registry entry of a block id
// that has since been REUSED by the live table.
//
// data_table_t::row_group() hands out shared_ptr<collection_t> copies BY VALUE, so the
// replaced collection can outlive compact(). compact() mark_as_free's + unregister_block(id)'s
// the outgoing collection's ids while that collection's segments still own handles for them;
// the ids go to pending_free_, a committed header promotes them to reusable_, and the next
// round hands one back out and register_block()s a FRESH handle for it. When the stale holder
// finally lets go, the old handle's destructor used to erase blocks_[id] by ID — taking the
// LIVE handle's slot with it. registry_alive(id) then reads false while a live segment is
// still reading the block, and registry_alive is the subtraction that stops
// reclaim_superseded_root from freeing live table state.
//
// RED on HEAD: the reused ids lose their registry entry the moment the stale collection is
// released, and the next compacting checkpoint frees blocks the live table is reading (the
// walker reports them as unexplained and the scan stops matching).
// ---------------------------------------------------------------------------------------
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

    // The holder agent_disk_t::maybe_cleanup_inner used to keep across its compact() call.
    auto stale = table->row_group();
    REQUIRE(stale);

    // Round 1 releases the stale collection's ids into pending_free_ and commits a root that
    // does not name them; round 2 is the first that may hand them back out.
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);
    REQUIRE(table->compact(WATERMARK));
    checkpoint_production(bm, *table);

    // The premise, asserted rather than assumed: at least one id the stale collection still
    // holds a handle for is now registered to the LIVE table.
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

    // The stale holder finally lets go, AFTER the reuse.
    stale.reset();

    for (auto id : reused) {
        INFO("reused block " << id << " must still be live table state");
        CHECK(bm.registry_alive(id));
    }

    // And the reclaim that runs on top of it must still account for every block, and must not
    // have freed anything the table is reading.
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
