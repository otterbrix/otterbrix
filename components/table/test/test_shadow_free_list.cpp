// A7.2 — shadow paging, step 2: a block the DURABLE root still points at must not be handed
// out again.
//
// A7.1 made the previous root real: a checkpoint writes only the slot its iteration owns, the
// header sector is self-validating, and the open path picks the greatest VALID iteration. So a
// crash mid-checkpoint recovers the PREVIOUS root. That is worth nothing while the blocks that
// root reads can be reallocated underneath it, and on HEAD they are, by the shortest path in
// the engine:
//
//   agent_disk_t::checkpoint_inner  ->  data_table_t::compact(watermark)
//        compact swaps the collection and mark_as_free's every block of the OUTGOING one —
//        exactly the blocks the CURRENT durable root still references. mark_as_free has no
//        other production caller, so the free list is empty at every other moment;
//   ...then, immediately, table_storage_t::checkpoint
//        whose first act is metadata_manager_t::allocate_handle -> free_block_id, which draws
//        from that free list.
//
// Crash before the header write and the file recovers the OLD root, whose data pointers now
// address a block holding the NEW checkpoint's metadata — rewritten with a freshly valid CRC,
// so read() succeeds and the rows are silently wrong.
//
// A7.2 splits the free list: reusable_ (free under the current DURABLE root) and pending_free_
// (released by the in-flight checkpoint). free_block_id draws only from reusable_;
// pending_free_ merges in only after write_header's slot write AND its fsync have both
// succeeded. The gates below are the four halves of that: not reissued before the header, the
// OLD root still readable after a crash, genuinely reusable after a success, and NOT promoted
// after a failure.
//
// Crash states come only from the T3 fault-injection seam (fault_injection_file.hpp); no test
// here lays out file bytes by hand.

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
#include "table_segment_scan.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t TOTAL_ROWS = 120000;
    constexpr uint64_t DELETED_ROWS = 60000;

    std::string free_list_db_path(const char* tag) {
        return "/tmp/test_otterbrix_shadow_free_list_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct free_list_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        free_list_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // Deliberately INCOMPRESSIBLE values (splitmix64 of the row index). A sequential column
    // checkpoints into a compressed segment, and the point of the crash case below is to read a
    // block back that HEAD has overwritten with metadata: decoding foreign bytes through a
    // compressed reader is a guess about how far it will run, while a full-width uncompressed
    // int64 column just yields wrong numbers. The gate should report a wrong ROW, not depend on
    // how a codec reacts to garbage.
    int64_t row_value(uint64_t row) {
        uint64_t x = row + 0x9E3779B97F4A7C15ull;
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ull;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBull;
        x ^= x >> 31;
        return static_cast<int64_t>(x & 0x7FFFFFFFFFFFFFFFull);
    }

    std::unique_ptr<data_table_t> make_table(free_list_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "shadow_free_list_table");
    }

    void append_rows(data_table_t& table, free_list_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, row_value(start + offset + i));
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // Everything table_storage_t::checkpoint (services/disk/manager_disk.cpp) does UP TO but not
    // including the header write: table metadata -> set_meta_block -> free list -> barrier
    // fsync. Split out because every A7.2 gate is a statement about the window between the
    // release and the header, so the tests have to stand inside it.
    tstorage::database_header_t prepare_checkpoint(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
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
        return header;
    }

    void checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        auto header = prepare_checkpoint(bm, table);
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // Delete rows [0, count), commit, publish and land the deletes, so compact() has something
    // to reclaim. The hazard opens ONLY when compact actually frees blocks.
    void delete_leading_rows(data_table_t& table,
                             free_list_env_t& env,
                             transaction_manager_t& mgr,
                             uint64_t count) {
        auto session = components::session::session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        const auto txn_id = txn.data().transaction_id;
        uint64_t deleted = 0;
        while (deleted < count) {
            uint64_t batch = std::min(count - deleted, uint64_t(DEFAULT_VECTOR_CAPACITY));
            auto row_ids_chunk = data_chunk_t(&env.resource, id_type, batch);
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

    std::set<uint64_t> released_by_compact(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        bm.dev_reset_tracking();
        REQUIRE(table.compact(std::numeric_limits<uint64_t>::max()));
        std::set<uint64_t> released(bm.dev_freed_ids().begin(), bm.dev_freed_ids().end());
        // Guard rail, not decoration: with nothing reclaimed the free list stays empty and every
        // gate below would pass vacuously.
        REQUIRE_FALSE(released.empty());
        return released;
    }

    // Load the table the way the engine does on open: through the durable root the manager
    // selected. A LOADED table is the realistic subject of compact() — its segments reference
    // only what the durable root references.
    std::unique_ptr<data_table_t> load_table(free_list_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    // Attribution, as in test_block_reachability: a block the walker cannot place is only a
    // hole if no EARLIER round's durable state already owned it. Blocks a previous checkpoint
    // superseded are known garbage (they leak until A7.3 reclaims old roots) and must not be
    // read as an A7.2 failure.
    void absorb_known(std::set<uint64_t>& known, const otterbrix_test::walk_report_t& r) {
        known.insert(r.chain_blocks.begin(), r.chain_blocks.end());
        known.insert(r.durable_data.begin(), r.durable_data.end());
        known.insert(r.registry_live.begin(), r.registry_live.end());
        known.insert(r.free_list_content.begin(), r.free_list_content.end());
        known.insert(r.scratch_issued.begin(), r.scratch_issued.end());
    }

    std::set<uint64_t> unattributable(const otterbrix_test::walk_report_t& current,
                                      const std::set<uint64_t>& known_prior) {
        std::set<uint64_t> result;
        for (auto id : current.unexplained) {
            if (known_prior.count(id) == 0) {
                result.insert(id);
            }
        }
        return result;
    }

    // Session 1 of every gate: build the table, give it a durable root, close the file. Returns
    // that root's meta_block. Split out because all four gates need the SAME starting point — a
    // file whose durable root is real and whose next open loads from it.
    uint64_t seed_durable_root(free_list_env_t& env, const std::string& path) {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, TOTAL_ROWS);
        checkpoint_production(bm, *table);
        const uint64_t root = bm.meta_block();
        REQUIRE(root != tstorage::INVALID_INDEX);
        return root;
    }

    std::string id_set(const std::set<uint64_t>& ids) {
        std::string s = "{";
        for (auto id : ids) {
            s += std::to_string(id) + ",";
        }
        s += "}";
        return s;
    }

} // namespace

// --- Gate 1: the release window ------------------------------------------------------
//
// The DIRECT statement, asserted rather than inferred: between compact()'s mark_as_free and
// the header write, not one of the released ids comes back out of free_block_id. The probe is
// the checkpoint's own allocations, which is the very caller that consumed them on HEAD.
TEST_CASE("shadow_free_list: a block released by the in-flight checkpoint is not reissued before the header") {
    const std::string path = free_list_db_path("window");
    remove_file(path);
    free_list_env_t env;

    seed_durable_root(env, path);

    // REOPEN: a freshly loaded table's segments reference only the durable root's blocks, so
    // every id compact releases below is one a crash would recover through.
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.load_existing_database().has_error());
    auto table = load_table(env, bm);

    transaction_manager_t mgr(&env.resource);
    delete_leading_rows(*table, env, mgr, DELETED_ROWS);
    const auto released = released_by_compact(bm, *table);

    // Immediately after the release: quarantined, not allocatable.
    {
        const auto reusable = bm.dev_reusable_snapshot();
        const auto pending = bm.dev_pending_free_snapshot();
        std::set<uint64_t> wrongly_reusable;
        std::set<uint64_t> not_quarantined;
        for (auto id : released) {
            if (reusable.count(id) != 0) {
                wrongly_reusable.insert(id);
            }
            if (pending.count(id) == 0) {
                not_quarantined.insert(id);
            }
        }
        INFO("released=" << id_set(released));
        INFO("released ids offered to the allocator: " << id_set(wrongly_reusable));
        CHECK(wrongly_reusable.empty());
        INFO("released ids not quarantined: " << id_set(not_quarantined));
        CHECK(not_quarantined.empty());
    }

    // Now run the checkpoint up to (not including) the header write and watch every id it
    // draws. On HEAD the first of them IS a released block.
    const size_t issued_before = bm.dev_issued_ids().size();
    auto header = prepare_checkpoint(bm, *table);

    std::set<uint64_t> reissued;
    const auto& journal = bm.dev_issued_ids();
    for (size_t i = issued_before; i < journal.size(); ++i) {
        if (released.count(journal[i]) != 0) {
            reissued.insert(journal[i]);
        }
    }
    INFO("checkpoint allocations that reused a released block: " << id_set(reissued));
    CHECK(reissued.empty());

    // And the header write is still the thing that ends the window.
    REQUIRE_FALSE(bm.write_header(header).has_error());

    remove_file(path);
}

// --- Gate 2: the plan's own gate -----------------------------------------------------
//
// kill -9 after the release and after the barrier fsync, before the header. The recovered root
// is the OLD one; its ROWS must still be there, and the walker — which reads the durable header
// straight from the file, i.e. judges what a crash actually recovers — must find no block it
// cannot account for.
TEST_CASE("shadow_free_list: a crash between the release and the header write leaves the OLD root's rows intact") {
    const std::string path = free_list_db_path("crash");
    const std::string copy_path = path + ".crashcopy";
    remove_file(path);
    remove_file(copy_path);

    free_list_env_t env;
    uint64_t root_a = tstorage::INVALID_INDEX;
    std::set<uint64_t> known_prior;

    // Session 1: a durable root, and the walker's picture of it. Absorbing that picture is how
    // the later walk tells THIS round's accounting holes from garbage the previous round already
    // owned (the same attribution test_block_reachability uses).
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, TOTAL_ROWS);
        checkpoint_production(bm, *table);
        root_a = bm.meta_block();
        REQUIRE(root_a != tstorage::INVALID_INDEX);

        auto r0 = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(r0.ok);
        absorb_known(known_prior, r0);
    }

    // Session 2: REOPEN before touching anything. This is not decoration — it is what makes the
    // reclaim the real one. A table that never left memory still holds the write-through blocks
    // its checkpoint superseded, so compact() releases those too and the allocator eats the
    // harmless ones first. A freshly LOADED table's segments reference only what the durable root
    // references, so every id compact releases here is an id a crash would recover through.
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        REQUIRE(bm.meta_block() == root_a);
        auto table = load_table(env, bm);

        transaction_manager_t mgr(&env.resource);
        delete_leading_rows(*table, env, mgr, DELETED_ROWS);
        const auto released = released_by_compact(bm, *table);
        WARN("[A7.2] compact released " << released.size() << " blocks of the durable root: " << id_set(released));

        // Metadata, free list and the pre-header barrier all land. The barrier is a real fsync,
        // so everything written in this window is on the device and the crash below takes
        // NOTHING away — which is precisely why reissuing a released block here is durable
        // damage rather than a lost write.
        auto header = prepare_checkpoint(bm, *table);
        // Same defect seen from the header side: if the checkpoint ate the released blocks for
        // its own metadata there is nothing left to publish, so the new root would claim an
        // EMPTY free list while the space it reclaimed is gone. A CHECK, not a REQUIRE — the
        // data gate below is the one this case exists for and must still run.
        CHECK(header.free_list != tstorage::INVALID_INDEX);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert(); // power cut before write_header

        std::filesystem::copy_file(path, copy_path, std::filesystem::copy_options::overwrite_existing);
    }

    // Session 3: what the crash actually left.
    {
        free_list_env_t recovery_env;
        tstorage::single_file_block_manager_t bm(recovery_env.buffer_manager, recovery_env.fs, copy_path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());

        // The recovered root is A — the checkpoint never committed.
        CHECK(bm.meta_block() == root_a);

        auto recovered = load_table(recovery_env, bm);

        // READ THE DATA, not just the open: every pre-delete row, by value. A block that was
        // handed out and rewritten during the aborted checkpoint reads back with a valid CRC,
        // so only the VALUES can tell the difference.
        uint64_t scanned = 0;
        uint64_t wrong = 0;
        uint64_t null_seen = 0;
        otterbrix_test::scan_table_segment(*recovered, 0, TOTAL_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto cell = chunk.data[0].value(i);
                if (cell.is_null()) {
                    null_seen++;
                    continue;
                }
                if (cell.value<int64_t>() != row_value(scanned + i)) {
                    wrong++;
                }
            }
            scanned += chunk.size();
        });
        INFO("scanned=" << scanned << " wrong=" << wrong << " null=" << null_seen);
        CHECK(scanned == TOTAL_ROWS);
        CHECK(null_seen == 0);
        CHECK(wrong == 0);

        // The walker's verdict on the same file. It reads the durable header straight off the
        // disk, so it judges what a crash recovers rather than what this process believes.
        auto report = otterbrix_test::walk_blocks(bm, copy_path, &recovery_env.resource);
        REQUIRE(report.ok);
        WARN("[A7.2] walker: block_count=" << report.block_count << " chain=" << id_set(report.chain_blocks)
                                           << " durable_data=" << id_set(report.durable_data)
                                           << " registry=" << id_set(report.registry_live)
                                           << " freelist=" << id_set(report.free_list_content)
                                           << " unexplained=" << id_set(report.unexplained));
        // Zero UNATTRIBUTABLE blocks: every id the walker cannot place must already have been
        // accounted for by the previous round's durable state. An id that appears out of nowhere
        // is an accounting hole opened by this round.
        const auto holes = unattributable(report, known_prior);
        INFO("unattributable=" << id_set(holes));
        CHECK(holes.empty());
        INFO("needed AND free-listed=" << id_set(report.reachable_free_overlap));
        CHECK(report.reachable_free_overlap.empty());
    }

    remove_file(path);
    remove_file(copy_path);
}

// --- Gate 3: the opposite failure ----------------------------------------------------
//
// Quarantining forever is not a fix, it is a leak. Once the header IS durable the released
// blocks must be back in the allocator — that is the promotion point doing its job.
TEST_CASE("shadow_free_list: a durable header makes the released blocks reusable") {
    const std::string path = free_list_db_path("promote");
    remove_file(path);
    free_list_env_t env;

    seed_durable_root(env, path);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.load_existing_database().has_error());
    auto table = load_table(env, bm);

    transaction_manager_t mgr(&env.resource);
    delete_leading_rows(*table, env, mgr, DELETED_ROWS);
    const auto released = released_by_compact(bm, *table);

    auto header = prepare_checkpoint(bm, *table);
    REQUIRE_FALSE(bm.write_header(header).has_error());

    const auto reusable = bm.dev_reusable_snapshot();
    std::set<uint64_t> still_withheld;
    for (auto id : released) {
        if (reusable.count(id) == 0) {
            still_withheld.insert(id);
        }
    }
    INFO("released but not reusable after a durable header: " << id_set(still_withheld));
    CHECK(still_withheld.empty());
    CHECK(bm.dev_pending_free_snapshot().empty());

    // Not just bookkeeping — the allocator really hands them back.
    //
    // RETARGETED by A7.3 (semantics preserved, question asked more directly). This used to be
    // `released.count(bm.free_block_id()) == 1`: the very next id had to be one of compact's.
    // After A7.3 the same durable header ALSO promotes the blocks of the root it superseded —
    // that root's metadata chain, its free-list chain and its packed data copy — and
    // free_block_id hands out the SMALLEST id in the pool, which can now legitimately be one
    // of those instead. That is the reclaim working, not the promotion failing. The property
    // under test is untouched: the released blocks must be genuinely back in the allocator,
    // not merely recorded as free. Drain the pool and require every one of them to come out of
    // it — a strictly stronger statement than "the first one did" — and require the file not
    // to grow while doing it, which is what proves they came from the pool rather than from
    // the end of the file.
    const uint64_t before_blocks = bm.total_blocks();
    std::set<uint64_t> drawn;
    for (size_t i = 0, n = reusable.size(); i < n; ++i) {
        drawn.insert(bm.free_block_id());
    }
    INFO("drawn from the pool: " << id_set(drawn));
    CHECK(bm.total_blocks() == before_blocks);
    std::set<uint64_t> never_returned;
    for (auto id : released) {
        if (drawn.count(id) == 0) {
            never_returned.insert(id);
        }
    }
    INFO("released but never handed back: " << id_set(never_returned));
    CHECK(never_returned.empty());

    // And the durable free list published the same set, so a reopen agrees.
    tstorage::database_header_t durable;
    REQUIRE(otterbrix_test::read_active_durable_header(path, durable));
    CHECK(durable.free_list != tstorage::INVALID_INDEX);

    remove_file(path);
}

// --- Gate 4: the failure decision ----------------------------------------------------
//
// A checkpoint whose header does not land leaves the OLD root current, so its blocks are still
// live and promotion would be exactly wrong. The decision recorded at promote_pending_free():
// keep them quarantined — neither promoted nor discarded — until some later header commits.
TEST_CASE("shadow_free_list: a FAILED header write does not promote the released blocks") {
    const std::string path = free_list_db_path("failed");
    remove_file(path);

    free_list_env_t env;
    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    seed_durable_root(env, path);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.load_existing_database().has_error());
    auto table = load_table(env, bm);

    transaction_manager_t mgr(&env.resource);
    delete_leading_rows(*table, env, mgr, DELETED_ROWS);
    const auto released = released_by_compact(bm, *table);

    auto header = prepare_checkpoint(bm, *table);

    // Everything from here on fails. The very next write IS the header slot write, so the
    // checkpoint dies at its single point of durability with the old root untouched.
    plan.fail_after_writes = plan.writes_seen;
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());
    CHECK(committed.error().type == core::error_code_t::io_error);

    const auto reusable = bm.dev_reusable_snapshot();
    const auto pending = bm.dev_pending_free_snapshot();
    std::set<uint64_t> promoted_anyway;
    std::set<uint64_t> lost;
    for (auto id : released) {
        if (reusable.count(id) != 0) {
            promoted_anyway.insert(id);
        }
        if (pending.count(id) == 0 && reusable.count(id) == 0) {
            lost.insert(id);
        }
    }
    INFO("promoted despite a failed header: " << id_set(promoted_anyway));
    CHECK(promoted_anyway.empty());
    // ...and not silently dropped either: the decision is "keep quarantined", not "discard".
    INFO("released blocks in neither pool (leaked): " << id_set(lost));
    CHECK(lost.empty());

    remove_file(path);
}

// --- Gate 5 (A7.3 / ITEM 3): a large free list must not publish a block of its OWN chain ---
//
// serialize_free_list snapshots the pool AFTER metadata_writer_t's constructor has taken the
// chain's FIRST block, which is what keeps that one out of the published list. Every FURTHER
// chain block is allocated mid-write, by metadata_writer_t::ensure_space -> allocate_handle ->
// free_block_id, i.e. drawn from reusable_ -- which is already inside the published snapshot.
// With a 256 KiB block one chain block holds ~32,608 ids, so a free list past that size
// publishes a list naming a block of its own chain.
//
// The consequence is not cosmetic: a restart runs deserialize_free_list, inserts that id into
// reusable_, and the next allocation hands out a block the durable root's own free-list chain
// occupies -- and the round after that reads the chain back through a block someone else has
// since overwritten with a valid CRC.
//
// No test went anywhere near that list size, which is why it survived A7.2. The ids below are
// never written to; only the chain's own (small) block ids reach the file.
TEST_CASE("shadow_free_list: a chain-spanning free list never lists its own chain blocks") {
    const std::string path = free_list_db_path("selfchain");
    remove_file(path);
    free_list_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());

    // Comfortably past one chain block's worth of ids (~32.6k at the default 256 KiB block),
    // so the chain needs a second and a third -- the ones allocated MID-WRITE.
    constexpr uint64_t FREE_IDS = 70000;
    for (uint64_t id = 1; id <= FREE_IDS; ++id) {
        bm.mark_as_free(id);
    }
    {
        // A durable header is what moves them from pending_free_ into the pool free_block_id
        // actually draws from; without it the hazard cannot even arise.
        tstorage::database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }
    REQUIRE(bm.dev_reusable_snapshot().size() == FREE_IDS);

    auto free_ptr = bm.serialize_free_list();
    REQUIRE_FALSE(free_ptr.has_error());
    REQUIRE(free_ptr.value().is_valid());

    // The blocks the chain physically occupies, and the ids the chain CONTAINS. Both are read
    // back through the production readers, not recomputed by this test.
    tstorage::metadata_manager_t chain_mgr(bm);
    std::pmr::vector<uint64_t> chain(&env.resource);
    REQUIRE_FALSE(chain_mgr.chain_blocks(free_ptr.value(), chain).has_error());
    INFO("free-list chain spans " << chain.size() << " block(s)");
    CHECK(chain.size() > 1); // otherwise this case is not exercising the mid-write allocation

    std::set<uint64_t> content;
    {
        tstorage::metadata_reader_t reader(chain_mgr, free_ptr.value());
        auto count = reader.read<uint64_t>();
        for (uint64_t i = 0; i < count && !reader.finished(); ++i) {
            content.insert(reader.read<uint64_t>());
        }
        REQUIRE_FALSE(reader.has_error());
    }

    std::set<uint64_t> self_listed;
    for (uint64_t block_id : chain) {
        if (content.count(block_id) != 0) {
            self_listed.insert(block_id);
        }
    }
    INFO("chain blocks the published list calls free: " << id_set(self_listed));
    CHECK(self_listed.empty());

    // And the restart consequence, through the real deserializer: no chain block may come back
    // as reusable.
    tstorage::database_header_t header;
    header.initialize();
    header.free_list = free_ptr.value().block_pointer;
    REQUIRE_FALSE(bm.write_header(header).has_error());

    tstorage::single_file_block_manager_t reopened(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(reopened.load_existing_database().has_error());
    const auto reusable_after_restart = reopened.dev_reusable_snapshot();
    std::set<uint64_t> resurrected;
    for (uint64_t block_id : chain) {
        if (reusable_after_restart.count(block_id) != 0) {
            resurrected.insert(block_id);
        }
    }
    INFO("chain blocks handed back to the allocator after a restart: " << id_set(resurrected));
    CHECK(resurrected.empty());

    remove_file(path);
}
