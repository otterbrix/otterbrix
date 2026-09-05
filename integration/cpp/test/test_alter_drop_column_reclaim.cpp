#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/block_reachability_walker.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <core/pmr.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdlib>
#include <filesystem>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// ALTER TABLE DROP COLUMN must reach the storage primitive.
//
// table_storage_t::drop_column is whole (rebuild, blocks NAMED into pending_released_blocks_,
// released by the checkpoint round that can commit the release), gated by
// services::disk::table_storage::drop_column_disk_frees_blocks — a test that calls the primitive
// DIRECTLY. Nothing called it from the ALTER path: the operator wrote the pg_attribute tombstone
// and stopped, so on a regular disk table the physical column survived every DROP forever. This
// test judges the SQL statement instead of the primitive.
//
// SHAPE, and every part of it is load-bearing:
//   * column "b" is bigint[40] — 40 * 8 B * 2048 rows = 640 KiB of child payload per row group,
//     past partial_block_manager_t's FULL_THRESHOLD, so b's segments take DEDICATED blocks that
//     "a" cannot share. Without that, block packing puts several columns' segments into ONE
//     256 KiB block and a dropped column sharing blocks with a survivor looks reclaimed when
//     nothing happened;
//   * rows are added in TWO rounds around a checkpoint, so some of b's blocks are named by no
//     durable root — reclaim_superseded_root walks only the root's own data blocks and is
//     structurally blind to those;
//   * every measurement is taken with the ENGINE DOWN, against a freshly loaded .otbx: both the
//     walker's "right after load" contract and the rule that a leak or a bad free usually shows
//     only on a reopened file;
//   * the file is walked once more after two further empty checkpoint rounds, because an
//     un-released block keeps costing round over round.
//
// The gate is deliberately NOT "SELECT no longer shows b" — the tombstone alone passes that and
// is exactly the state this task exists to leave behind.

using components::catalog::FIRST_USER_OID;

namespace {

    constexpr std::size_t FIRST_ROWS = 3072;  // 1.5 row groups, checkpointed
    constexpr std::size_t SECOND_ROWS = 1024; // appended AFTER that root
    constexpr std::size_t TOTAL_ROWS = FIRST_ROWS + SECOND_ROWS;
    constexpr std::size_t ARRAY_LENGTH = 40;
    constexpr std::size_t INSERT_BATCH = 512;

    // The only user table in this test: `<main_path>/.../<oid>/table.otbx` with oid past
    // FIRST_USER_OID. Every system catalog sits under an oid below it, so the filter picks the
    // user table without the test having to learn its oid.
    std::filesystem::path find_user_table_otbx(const std::filesystem::path& root) {
        std::filesystem::path found;
        if (!std::filesystem::exists(root)) {
            return found;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
            if (!entry.is_regular_file() || entry.path().filename() != "table.otbx") {
                continue;
            }
            const std::string oid_dir = entry.path().parent_path().filename().string();
            char* end = nullptr;
            const unsigned long oid = std::strtoul(oid_dir.c_str(), &end, 10);
            if (end == nullptr || *end != '\0' || oid < FIRST_USER_OID) {
                continue;
            }
            found = entry.path();
        }
        return found;
    }

    struct offline_walk_t {
        otterbrix_test::walk_report_t report;
        std::vector<std::string> columns;
        std::uintmax_t file_size{0};
    };

    // Judge the DURABLE file with the engine shut down. A fresh table_storage_t load is the
    // walker's supported entry point (the registry only reflects the live data blocks once the
    // table is open) and is itself the reopen the whole gate turns on.
    offline_walk_t walk_offline(const std::filesystem::path& otbx, std::pmr::memory_resource* resource) {
        offline_walk_t out;
        std::error_code ec;
        out.file_size = std::filesystem::file_size(otbx, ec);
        if (ec) {
            out.file_size = 0;
        }
        services::disk::table_storage_t ts(resource, otbx, std::vector<components::table::column_definition_t>{});
        REQUIRE_FALSE(ts.construction_failed());
        for (const auto& c : ts.table().columns()) {
            out.columns.emplace_back(c.name());
        }
        components::table::storage::single_file_block_manager_t* bm = nullptr;
        {
            // Counted collection copy scoped to reading the manager reference out of it: a
            // holder kept alive across a reclaim keeps block handles alive too.
            auto collection = ts.table().row_group();
            bm = static_cast<components::table::storage::single_file_block_manager_t*>(&collection->block_manager());
        }
        out.report = otterbrix_test::walk_blocks(*bm, otbx.string(), resource);
        return out;
    }

    std::string dump_ids(const std::set<uint64_t>& ids) {
        std::stringstream s;
        s << "{";
        for (auto id : ids) {
            s << id << ",";
        }
        s << "}";
        return s.str();
    }

    // One INSERT statement per batch: `(i, ARRAY[i*100, i*100+1, ...])`, content-addressed so a
    // block freed while something still read it shows up as wrong data rather than a row count
    // that happens to match.
    void insert_rows(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t first, std::size_t count) {
        std::size_t done = 0;
        while (done < count) {
            const std::size_t batch = std::min(INSERT_BATCH, count - done);
            std::stringstream q;
            q << "INSERT INTO TestDatabase.wide (a, b) VALUES ";
            for (std::size_t i = 0; i < batch; ++i) {
                const std::size_t row = first + done + i;
                q << "(" << row << ", ARRAY[";
                for (std::size_t j = 0; j < ARRAY_LENGTH; ++j) {
                    q << (row * 100 + j) << (j + 1 == ARRAY_LENGTH ? "" : ",");
                }
                q << "])" << (i + 1 == batch ? ";" : ", ");
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            done += batch;
        }
    }

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("SQL: " << sql);
        REQUIRE(cur->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::test_alter_drop_column_reclaim::disk_drop_column_returns_blocks") {
    auto config = test_create_config(integration_fixture_path("test_alter_drop_column_reclaim/disk_drop"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;

    INFO("phase 1: disk-backed table with a dedicated-block column, filled and checkpointed");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.wide (a bigint, b bigint[40]);");
        insert_rows(dispatcher, 0, FIRST_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a FROM TestDatabase.wide;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == FIRST_ROWS);
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    // The premise, asserted rather than assumed: the durable root really does name both
    // columns' blocks before the drop, so the "left the root" set below cannot be vacuous.
    auto before = walk_offline(otbx, &resource);
    REQUIRE(before.report.ok);
    REQUIRE(before.columns.size() == 2);
    REQUIRE(before.columns[0] == "a");
    REQUIRE(before.columns[1] == "b");
    CHECK(before.report.reachable_free_overlap.empty());
    REQUIRE_FALSE(before.report.root_data.empty());

    INFO("phase 2: more rows past that root, then ALTER TABLE DROP COLUMN + CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_rows(dispatcher, FIRST_ROWS, SECOND_ROWS);
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide DROP COLUMN b;");
        run_sql(dispatcher, "CHECKPOINT;");

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.wide;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == TOTAL_ROWS);
        // CHECK, not REQUIRE: the storage-schema and block assertions below are the real
        // gate, and aborting here would hide them behind a visibility symptom.
        CHECK(cur->column_count() == 1);
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);

    // The wiring itself: the reopened file's own schema no longer carries the column. An ALTER
    // that writes only the tombstone goes red here.
    INFO("columns after the drop: " << after.columns.size());
    CHECK(after.columns.size() == 1);
    CHECK(after.columns.front() == "a");

    // Blocks the durable root named before the drop and does not name after it must all be
    // accounted for: back in the free list, or still held by a surviving column that shares
    // the block (packing).
    std::set<uint64_t> gone;
    for (auto id : before.report.root_data) {
        if (after.report.root_data.count(id) == 0) {
            gone.insert(id);
        }
    }
    INFO("root before=" << before.report.root_data.size() << " after=" << after.report.root_data.size()
                        << " left the root=" << dump_ids(gone)
                        << " unexplained=" << dump_ids(after.report.unexplained));
    REQUIRE_FALSE(gone.empty());
    for (auto id : gone) {
        // "Came back" has exactly three honest shapes: published in the free list, still held
        // by a surviving column that shared the block (packing), or already re-issued as
        // this round's metadata chain. Anything else is a block named by no owner — the shape
        // a tombstone-only DROP produces (block 52, below).
        INFO("block " << id << " left the durable root when column b was dropped");
        CHECK((after.report.free_list_content.count(id) != 0 || after.report.registry_live.count(id) != 0 ||
               after.report.chain_blocks.count(id) != 0));
    }
    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());

    INFO("phase 3: surviving column is complete after the reopen, and the file settles");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT a FROM TestDatabase.wide ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
            for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
            }
        }
        // Round over round the file must not grow: anything left un-released keeps costing.
        run_sql(dispatcher, "CHECKPOINT;");
        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto settled = walk_offline(otbx, &resource);
    REQUIRE(settled.report.ok);
    REQUIRE(settled.columns.size() == 1);
    INFO("settled unexplained=" << dump_ids(settled.report.unexplained));
    CHECK(settled.report.reachable_free_overlap.empty());
    CHECK(settled.report.unexplained.empty());
    CHECK(settled.report.block_count <= after.report.block_count);
    CHECK(settled.file_size <= after.file_size);
}

// A crash between the ALTER's commit and the table's next checkpoint must not leak the
// dropped column's space FOREVER.
//
// The live path: the commit names the outgoing column's blocks into
// table_storage_t::pending_released_blocks_ and the next checkpoint releases them. That set is
// IN MEMORY. Kill the process in between and it is gone, while the disk keeps two durable facts
// that disagree: the pg_attribute tombstone (attisdropped = true, durable through the WAL commit
// marker) and the column itself, still physically present because the durable root was never
// rewritten. The table reloads with the column BACK in its collection, the catalog hides it,
// every query looks right — and nothing can ever re-derive the drop, compact() least of all:
// after the reload the column is genuinely part of the collection, so its blocks are live.
//
// THE CRASH. `test_spaces`' destructor issues a CHECKPOINT, so a clean scope exit would perform
// exactly the release this test needs to be missing. Arming fail_writes_from AFTER the ALTER
// makes every later .otbx write fail, so no header commits for any table and the durable files
// stay byte-identical (conservative crash semantics). The WAL is a different file and does NOT
// go through the block manager's interposer, so the ALTER's commit marker survives — the whole
// point: the tombstone must be durable while the physical drop is not.
//
// The shape is the sibling test's, for the same reasons: bigint[40] so b's segments take
// DEDICATED blocks past FULL_THRESHOLD (packing would otherwise hand b's ids to a's walk and
// hide the question), rows added in two rounds around the checkpoint, and every measurement
// taken with the engine DOWN against a freshly loaded .otbx.
TEST_CASE("integration::cpp::test_alter_drop_column_reclaim::crash_before_checkpoint_rearms_the_release") {
    auto config = test_create_config(integration_fixture_path("test_alter_drop_column_reclaim/crash_rearm"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;

    INFO("phase 1: filled and checkpointed, so the durable root names both columns' blocks");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.wide (a bigint, b bigint[40]);");
        insert_rows(dispatcher, 0, FIRST_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    auto before = walk_offline(otbx, &resource);
    REQUIRE(before.report.ok);
    REQUIRE(before.columns.size() == 2);
    REQUIRE(before.columns[1] == "b");
    REQUIRE_FALSE(before.report.root_data.empty());
    CHECK(before.report.reachable_free_overlap.empty());

    INFO("phase 2: more rows, ALTER TABLE DROP COLUMN, then KILL before any checkpoint commits");
    {
        // Declared before the engine so the interposer is installed when the block managers
        // open their files (wrap() runs once per open) and is still installed while the engine
        // tears down. Every knob is off until the kill is armed, so phase 2 runs normally.
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_rows(dispatcher, FIRST_ROWS, SECOND_ROWS);
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide DROP COLUMN b;");

        // KILL. fail_writes_from is compared with >=, so 1 fails every write from here on
        // without the test having to count them. The engine is idle between statements, so
        // this write to the shared plan cannot race an in-flight one.
        plan.fail_writes_from = 1;
    } // ← the destructor's CHECKPOINT runs here and can commit nothing.

    // What the crash left: the durable root is untouched, so the DROPPED COLUMN IS BACK. This
    // is the state the fix has to recognise, and it is asserted rather than assumed — if the
    // kill silently failed to land, the phase-3 claims below would pass for the wrong reason.
    auto crashed = walk_offline(otbx, &resource);
    REQUIRE(crashed.report.ok);
    REQUIRE(crashed.columns.size() == 2);
    REQUIRE(crashed.columns[1] == "b");
    CHECK(crashed.report.root_data == before.report.root_data);
    CHECK(crashed.report.reachable_free_overlap.empty());

    INFO("phase 3: restart — bootstrap must re-derive the drop — then CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // The catalog kept the tombstone across the crash (WAL commit marker + replay), so the
        // column is invisible to SQL even on the unfixed build. That is exactly why this cannot
        // be the gate.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.wide;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == TOTAL_ROWS);
        CHECK(cur->column_count() == 1);

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);

    // GATE 1 — the reopened file's OWN schema. On the unfixed build the restart puts b back in
    // the collection and the checkpoint writes it out again, so this is 2.
    INFO("columns after the restart+checkpoint: " << after.columns.size());
    CHECK(after.columns.size() == 1);
    CHECK(after.columns.front() == "a");

    // GATE 2 — the space actually came back. The table now holds MORE rows than `before` did,
    // yet its durable root must name FEWER data blocks, because b (40 * 8 B per row against
    // a's 8 B) is no longer part of it. On the unfixed build the root grows instead.
    INFO("root data blocks before=" << before.report.root_data.size() << " after="
                                    << after.report.root_data.size());
    CHECK(after.report.root_data.size() < before.report.root_data.size());

    // GATE 3 — nothing was orphaned on the way. Every block the crashed root named that the new
    // root does not must be back in the free list, still held by a surviving column sharing the
    // block (packing), or already re-issued as this round's metadata chain.
    std::set<uint64_t> gone;
    for (auto id : crashed.report.root_data) {
        if (after.report.root_data.count(id) == 0) {
            gone.insert(id);
        }
    }
    INFO("left the root=" << dump_ids(gone) << " unexplained=" << dump_ids(after.report.unexplained));
    REQUIRE_FALSE(gone.empty());
    for (auto id : gone) {
        INFO("block " << id << " left the durable root across the restart");
        CHECK((after.report.free_list_content.count(id) != 0 || after.report.registry_live.count(id) != 0 ||
               after.report.chain_blocks.count(id) != 0));
    }
    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());

    INFO("phase 4: the surviving column is complete across the reopen and the file settles");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT a FROM TestDatabase.wide ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
            for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
            }
        }
        run_sql(dispatcher, "CHECKPOINT;");
        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto settled = walk_offline(otbx, &resource);
    REQUIRE(settled.report.ok);
    REQUIRE(settled.columns.size() == 1);
    INFO("settled unexplained=" << dump_ids(settled.report.unexplained));
    CHECK(settled.report.reachable_free_overlap.empty());
    CHECK(settled.report.unexplained.empty());
    CHECK(settled.report.block_count <= after.report.block_count);
    CHECK(settled.file_size <= after.file_size);
}
