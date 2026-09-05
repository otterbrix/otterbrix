// A7.7 — give back what a FAILED round took, and NOTHING else.
//
// A7.1 gave the file a real two-slot header, A7.2 split the free pool so a block the durable
// root still names can never be reissued, and A7.3 took the SUPERSEDED root down so an
// UNCHANGED table became a closed cycle. What none of them covers is the round that FAILS.
//
// A checkpoint round allocates three kinds of block before it commits:
//   * the PACKED COPY the checkpointer writes through partial_block_manager_t (a fresh copy of
//     every column segment, referenced only by the row_group_pointer_t stream);
//   * the TABLE-METADATA chain (metadata_manager_t inside table_storage_t::checkpoint);
//   * the FREE-LIST chain (a second metadata_manager_t inside serialize_free_list).
// None of the three is registered in the block registry — metadata_manager_t and
// partial_block_manager_t hold raw block_t buffers and never call register_block — so when the
// header write fails, no root names them and nothing in memory reaches them either. They sit in
// used_blocks_ / issued_since_root_ forever. Measured on this tree: ~655 KB per round on a
// 7.8 MB table, for as long as the failure persists, while storage_degraded() stays false
// (reconcile_failed_header_write case 2 deliberately does not latch, so a transient ENOSPC can
// still recover).
//
// The naive fix is CORRUPTION and the code says so at single_file_block_manager.cpp "WHY NOT
// ROLL BACK": the same round ALSO allocates blocks the in-memory tree now depends on —
// data_table_t::compact swaps row_groups_ to a rebuilt collection whose write-through allocated
// ids, and column_data_t::checkpoint re-points the still-managed live tail segments onto blocks
// from a fresh partial_block_manager. Both of those go through
// column_data_t::transition_segment_to_disk, which DOES register_block() the id and hands the
// handle to the live column_segment_t. So the discriminator is exactly the one A7.3's formula
// already uses: registry_alive(id).
//
//   releasable = issued_since_root_ - {ids live in the block registry}
//
// and the destination is reusable_, NOT pending_free_ — see the long note at the definition of
// roll_back_uncommitted_round().

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

    // The EXACT sequence of table_storage_t::checkpoint (services/disk/manager_disk.cpp),
    // reproduced so a unit test stands in the same window production does. `header_write_fails`
    // arms the fault interposer for ONE write — the header write — and disarms it again, which
    // is the only shape that reaches reconcile_failed_header_write case 2 without anything
    // latching. Failing the whole tail of the round instead would fail DATA writes too, and
    // those latch durability_error_, so the existing degraded() gate would hide the defect.
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
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        if (header_write_fails) {
            REQUIRE(plan != nullptr);
            plan->fail_after_writes = plan->writes_seen; // the NEXT write (the header) fails
        }
        auto committed = bm.write_header(header);
        if (header_write_fails) {
            plan->fail_after_writes = 0; // one bad write only: the device is otherwise healthy
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

    // Build a table, drive it to the A7.3 steady state (an unchanged table is a closed cycle
    // from the third round on) and leave the manager open at that point.
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

// ---------------------------------------------------------------------------------------
// PROBE (measurement, not a gate): what a persistent header-write failure costs per round,
// and what the reachability walker can and cannot see about it.
// ---------------------------------------------------------------------------------------
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

    // Every later round takes the shape agent_disk_t::checkpoint_inner runs after a failure:
    // the compact is GATED OFF (table_storage_t::last_checkpoint_failed_) but the checkpoint is
    // still attempted, so a transient error can recover.
    for (int round = 1; round <= 5; ++round) {
        const uint64_t before_blocks = bm.total_blocks();
        const uint64_t before_size = file_size_of(path);
        bm.dev_reset_tracking();
        auto r = checkpoint_round(bm, *steady.table, &plan, true);
        CHECK_FALSE(r.committed);
        CHECK_FALSE(bm.degraded()); // case 2 does not latch: that is the whole point

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
                                     << " issued=" << issued.size() << " orphaned_after_round="
                                     << still_orphaned.size() << " | walker: durable_block_count="
                                     << report.block_count << " chain=" << report.chain_blocks.size()
                                     << " durable_data=" << report.durable_data.size()
                                     << " registry=" << report.registry_live.size()
                                     << " freelist=" << report.free_list_content.size()
                                     << " unexplained=" << report.unexplained.size()
                                     << " overlap=" << report.reachable_free_overlap.size());
    }
    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// A7.7 GATE 1 — N injected header-write failures in a row do not grow the file.
//
// RED before the rollback: each failed round spends a fresh packed copy + metadata chain +
// free-list chain and releases none of it, so once the free pool a committed round left behind
// runs dry the file extends by a full round's worth EVERY round, forever, with degraded()
// false the whole time.
// ---------------------------------------------------------------------------------------
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
        // The failure this gate is about is the UNLATCHED one: reconcile case 2 does not
        // degrade the manager, so no existing health gate covers it.
        REQUIRE_FALSE(bm.degraded());
        INFO("failed round " << round << ": block_count " << steady.blocks << " -> " << bm.total_blocks()
                             << ", file_size " << steady.size << " -> " << file_size_of(path));
        CHECK(bm.total_blocks() == steady.blocks);
        CHECK(file_size_of(path) == steady.size);
    }

    // Loud is not fatal: the table the round failed on still serves its DATA.
    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// A7.7 GATE 2 — THE DISCRIMINATION. Every id a failed round issued ends up on exactly one of
// two sides, and which side is decided by the block registry:
//   * registry-alive  -> KEPT (the rebuilt collection, a re-pointed live tail segment);
//   * not registry-alive -> RELEASED, and into reusable_, never pending_free_.
//
// The pool matters as much as the set. pending_free_ only drains through a COMMITTED header,
// and the whole scenario here is that no header commits — routing a failed round's own
// allocations there would leave the file growing at exactly the old rate.
// ---------------------------------------------------------------------------------------
TEST_CASE("failed_round: the rollback gives back only what the live tree does not hold", "[a7.7]") {
    const auto path = rollback_db_path("discriminate");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);

    // A round that also COMPACTS, so the failed round really does allocate live table state:
    // data_table_t::compact swaps row_groups_ to a rebuilt collection whose write-through
    // registered its blocks, and column_data_t::checkpoint re-points the live tail. Those ids
    // are in issued_since_root_ exactly like the packed copy is, and freeing them would be the
    // corruption the "WHY NOT ROLL BACK" note refuses.
    const uint64_t blocks_before_round = bm.total_blocks();
    // The journal is reset BEFORE the compaction, not after it. issued_since_root_ — the set
    // this gate is about — starts at the last COMMITTED header, and the compaction runs after
    // that header and before this round's checkpoint, so its write-through allocations are in
    // it. Resetting after the compact measured only the checkpoint's own allocations, i.e.
    // strictly LESS than "every id a failed round issued", which is what the gate's name
    // promises and what roll_back_uncommitted_round actually walks.
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
            // Live table state: it must NOT be in either free pool, or the next allocation
            // overwrites a block a live segment is still reading, with a valid CRC on top. And
            // it must still be INSIDE the file, or the high-water descent has cut into it.
            CHECK(reusable.count(id) == 0);
            CHECK(pending.count(id) == 0);
            CHECK(id < high_water);
            kept++;
        } else if (id >= high_water) {
            // Released AND past the walked-down high-water mark: the round extended the file,
            // and the rollback took the extension back. Such an id must not stay in the pool —
            // issuing it would put a block beyond the block_count the next header records.
            CHECK(reusable.count(id) == 0);
            CHECK(pending.count(id) == 0);
            past_mark++;
        } else {
            // Named by no root and reachable from nothing in memory: back in the pool
            // free_block_id draws from RIGHT NOW — reusable_, not the quarantined half.
            CHECK(reusable.count(id) != 0);
            CHECK(pending.count(id) == 0);
            reissuable++;
        }
    }
    WARN("[a7.7 gate 2] issued=" << issued.size() << " kept(live)=" << kept << " back_in_reusable=" << reissuable
                                 << " dropped_past_high_water=" << past_mark << " block_count " << blocks_before_round
                                 << " -> " << high_water);
    CHECK(kept > 0);                                // the compact really did allocate live state
    CHECK(reissuable + past_mark > 0);              // ...and the round's own metadata came back
    CHECK(bm.total_blocks() <= blocks_before_round); // a failed round never leaves the mark higher

    // The rollback did not take anything the in-memory tree depends on: read the DATA.
    REQUIRE(scan_and_count(*steady.table, env) == ROLLBACK_ROWS);
    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// A7.7 GATE 2b — THE PREMISE, MEASURED. "Allocated for the root under construction" and
// "already released" DO intersect, and the rollback's pending_free_.erase is load-bearing
// TODAY rather than future-proofing.
//
// The rollback's own note used to claim the opposite ("an id this round allocated cannot be in
// pending_free_ today"), reasoning from free_block_id drawing only from reusable_. That
// reasoning is about ONE round. issued_since_root_ spans every round since the last COMMITTED
// header, and a compaction inside a later one releases the collection an EARLIER failed round
// built — ids that are still in issued_since_root_ because no header ever promoted them out.
//
// Two failed compacting rounds are the smallest shape that produces it, and it is the ordinary
// one: agent_disk retries, and a retry that is allowed to compact rebuilds again.
// ---------------------------------------------------------------------------------------
TEST_CASE("failed_round: a later round's compaction releases ids the round journal still holds", "[a7.7]") {
    const auto path = rollback_db_path("premise");
    remove_file(path);
    rollback_env_t env;

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    auto steady = reach_steady_state(env, bm, path, &plan);

    // The journal now mirrors issued_since_root_: the last round COMMITTED, so
    // promote_durable_root emptied that set and this reset empties its shadow.
    bm.dev_reset_tracking();

    // Round 1: compact, then fail the header. Its rebuilt collection is registry-alive, so the
    // rollback KEEPS those ids — they stay in issued_since_root_.
    REQUIRE(steady.table->compact(WATERMARK));
    REQUIRE_FALSE(checkpoint_round(bm, *steady.table, &plan, true).committed);
    REQUIRE_FALSE(bm.degraded());

    // Round 2's compaction swaps that collection out and mark_as_free's its blocks. They are
    // not in reusable_, so they go to pending_free_ — while still being ids this un-promoted
    // window issued.
    REQUIRE(steady.table->compact(WATERMARK));
    const std::set<uint64_t> issued_since_commit(bm.dev_issued_ids().begin(), bm.dev_issued_ids().end());
    const auto pending_after_compact = bm.dev_pending_free_snapshot();
    std::set<uint64_t> overlap;
    for (auto id : issued_since_commit) {
        if (pending_after_compact.count(id) != 0) {
            overlap.insert(id);
        }
    }
    WARN("[a7.7 gate 2b] issued_since_commit=" << issued_since_commit.size()
                                               << " pending_free=" << pending_after_compact.size()
                                               << " intersection=" << overlap.size());
    CHECK_FALSE(overlap.empty());

    // ...and the rollback of round 2 puts every one of them on exactly one side, with the
    // erase doing the work the old note said was unnecessary: released ids move OUT of
    // pending_free_ into reusable_ (root N cannot name them, so quarantining them would strand
    // the space until a commit that is not coming), and kept ids stay quarantined.
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

// ---------------------------------------------------------------------------------------
// A7.7 GATE 3 — the reachability walker reports ZERO unexplained after a failed round.
//
// The walker classifies every id below the file's high-water mark as durable-chain /
// durable-data / registry-live / durably-free, and calls anything else unexplained. Before the
// rollback a failed round's allocations push the mark past the durable root's block_count and
// land in none of the four bins; after it the mark does not move and every id is accounted for.
// ---------------------------------------------------------------------------------------
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
                             << " durable_data=" << report.durable_data.size()
                             << " registry=" << report.registry_live.size()
                             << " freelist=" << report.free_list_content.size()
                             << " unexplained=" << report.unexplained.size());
        CHECK(report.unexplained.empty());
        // A block that is BOTH durably reachable and published as free would be reissued over
        // live data. The rollback adds ids to the pool, so this is the invariant it could break.
        CHECK(report.reachable_free_overlap.empty());
        // The high-water descent must never uncover a block something still needs: nothing the
        // durable root references, and nothing the live tree holds, may sit past the mark.
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

// ---------------------------------------------------------------------------------------
// A7.7 GATE 4 — a TRANSIENT failure still recovers. The rollback must not have taken anything
// the next round needs, and the round after the failure must commit and reload correctly from
// a FRESH manager (i.e. from the file, not from this process's memory).
// ---------------------------------------------------------------------------------------
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

        // One failed round...
        REQUIRE_FALSE(checkpoint_round(bm, *steady.table, &plan, true).committed);
        REQUIRE_FALSE(bm.degraded());
        tstorage::database_header_t during{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, during));
        CHECK(during.iteration == before.iteration); // the previous root stands, unchanged

        // ...then the device recovers and the next round commits.
        REQUIRE(steady.table->compact(WATERMARK));
        REQUIRE(checkpoint_round(bm, *steady.table, &plan, false).committed);
        CHECK(file_size_of(path) == steady.size);

        tstorage::database_header_t after{};
        REQUIRE(otterbrix_test::read_active_durable_header(path, after));
        CHECK(after.iteration == before.iteration + 1);
        iteration_after_failure = after.iteration;
    }

    // Reload from the file with a fresh manager: the recovered root must carry the whole table.
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

// ---------------------------------------------------------------------------------------
// A7.7 GATE 5 — THE SAFETY HALF. When the header write leaves the durable root INDETERMINATE,
// the rollback must refuse outright.
//
// A torn header write can leave a CRC-VALID header of the NEW generation on the device while
// write() reports failure (every byte that differs between two generations lives in the first
// 48, i.e. inside the first hardware sector, and the padding is zeros in both). If the fsync
// also failed, the read-back cannot say whether that slot reached the device — so the new root
// MAY be the one a crash recovers, and it names every block this round allocated. Giving them
// back to the allocator would be exactly the corruption A7.2/A7.3 exist to prevent.
//
// The gate proves both halves: nothing is released, and the file really does reopen on that
// new root with its data intact — which is what makes "do not release" the only correct answer.
// ---------------------------------------------------------------------------------------
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

        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        // TEAR the header write and fail the fsync that follows it: the slot reassembles into a
        // byte-exact copy of the NEW generation (it passes the CRC), but no fsync ever confirmed
        // it, so nothing can say whether the device has it. reconcile case 3.
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
        // ...and the high-water mark must not have been walked down either: a truncate against
        // a lowered mark would cut into the very root that may be durable.
        CHECK(bm.total_blocks() >= blocks_before_round);

        // A later checkpoint attempt (the orchestrator retries) must still release nothing.
        const uint64_t rolled_back = bm.roll_back_uncommitted_round();
        CHECK(rolled_back == 0);
    }

    // The torn slot really is the durable root now: reopen and read the DATA out of it. This is
    // what makes the refusal load-bearing rather than paranoid.
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        REQUIRE(scan_and_count(*table, env) == ROLLBACK_ROWS);
    }
    remove_file(path);
}
