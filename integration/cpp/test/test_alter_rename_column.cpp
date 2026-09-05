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

// ALTER TABLE ... RENAME COLUMN — the statement, and the storage invariant it is bound to.
//
// THE NO-OP. node_alter_column_t::set_attoid has no callers anywhere in the pipeline, so
// operator_alter_column_rename_t saw attoid_ == INVALID_OID on every execution, took its early
// return and reported SUCCESS having written nothing. This is the exact sibling of the DROP
// COLUMN defect B3c1 fixed, and it was pinned by a note at
// integration/cpp/test/test_multi_database_isolation.cpp (the RENAME case there asserts
// cross-database isolation only, deliberately not the rename itself).
//
// THE MINE UNDER THE FIX, which is why the second case below is the real gate.
// manager_disk_t::rearm_dropped_column_blocks_sync (B3c2) reconciles the loaded storage's
// columns against the live pg_attribute rows at every bootstrap, BY NAME, and treats a storage
// column the catalog does not name as a DROP — it removes it from the collection and arms its
// blocks for release. That is sound only while nothing can write a new name into pg_attribute
// without renaming the column in the storage too. Making RENAME write the catalog and stop
// there does not leave the old harmless no-op behind: it leaves the next start reading the
// storage's OLD name as a dropped column and physically removing a SURVIVING one, with its
// data. So the gate is not "the catalog says the new name" — it is "the column and every one of
// its rows are still there, under the new name, after a restart".

using components::catalog::FIRST_USER_OID;

namespace {

    constexpr std::size_t FIRST_ROWS = 3072;  // 1.5 row groups, checkpointed
    constexpr std::size_t SECOND_ROWS = 1024; // appended AFTER that root
    constexpr std::size_t TOTAL_ROWS = FIRST_ROWS + SECOND_ROWS;
    constexpr std::size_t ARRAY_LENGTH = 40;
    constexpr std::size_t INSERT_BATCH = 512;

    // The only user table in these cases: `<main_path>/.../<oid>/table.otbx` with oid past
    // FIRST_USER_OID. Every system catalog sits under an oid below it.
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
        // RN-oid: the durable schema's IDENTITIES. This is the field the reconciliation keys
        // on, so it is read straight off the file rather than inferred from behaviour.
        std::vector<std::uint32_t> attoids;
    };

    // Judge the DURABLE file with the engine shut down: a fresh table_storage_t load reads the
    // schema back out of the root the last committed header names, which is the only place the
    // storage's own idea of a column NAME lives.
    offline_walk_t walk_offline(const std::filesystem::path& otbx, std::pmr::memory_resource* resource) {
        offline_walk_t out;
        services::disk::table_storage_t ts(resource, otbx, std::vector<components::table::column_definition_t>{});
        REQUIRE_FALSE(ts.construction_failed());
        for (const auto& c : ts.table().columns()) {
            out.columns.emplace_back(c.name());
            out.attoids.emplace_back(c.attoid());
        }
        components::table::storage::single_file_block_manager_t* bm = nullptr;
        {
            // Counted collection copy scoped to reading the manager reference out of it
            // (ITEM C): a holder kept alive across a reclaim keeps block handles alive too.
            auto collection = ts.table().row_group();
            bm = static_cast<components::table::storage::single_file_block_manager_t*>(&collection->block_manager());
        }
        out.report = otterbrix_test::walk_blocks(*bm, otbx.string(), resource);
        return out;
    }

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("SQL: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    // One INSERT statement per batch: `(i, ARRAY[i*100, i*100+1, ...])`, content-addressed so a
    // block freed while something still read it shows up as wrong data rather than a row count
    // that happens to match.
    void insert_rows(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t first, std::size_t count) {
        std::size_t done = 0;
        while (done < count) {
            const std::size_t batch = std::min(INSERT_BATCH, count - done);
            std::stringstream q;
            q << "INSERT INTO TestDatabase.wide (a, payload) VALUES ";
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

} // namespace

// CASE 1 — the statement itself. Today it reports success and changes nothing, so the OLD name
// still resolves and the NEW one does not: both halves below are inverted on the unfixed build.
TEST_CASE("integration::cpp::test_alter_rename_column::rename_column_rebinds_the_name") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/rebind"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
    run_sql(dispatcher, "CREATE TABLE TestDatabase.t (a bigint, payload bigint);");
    run_sql(dispatcher, "INSERT INTO TestDatabase.t (a, payload) VALUES (1, 10), (2, 20), (3, 30);");

    run_sql(dispatcher, "ALTER TABLE TestDatabase.t RENAME COLUMN payload TO renamed;");

    INFO("the new name resolves and carries the rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT renamed FROM TestDatabase.t ORDER BY renamed;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 20);
        REQUIRE(cur->value(0, 2).value<int64_t>() == 30);
    }

    INFO("the old name is gone — a rename that leaves it resolvable has not renamed anything");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT payload FROM TestDatabase.t;");
        REQUIRE(cur->is_error());
    }

    INFO("the untouched column is unaffected");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a FROM TestDatabase.t ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

// CASE 2 — THE GATE. RENAME -> CHECKPOINT -> RESTART: the column, and every row of it, must
// come back under the new name, and its blocks must NOT have been released.
//
// SHAPE, and every part is load-bearing:
//   * `payload` is bigint[40] — 40 * 8 B * 2048 rows per row group, past
//     partial_block_manager_t's FULL_THRESHOLD, so its segments take DEDICATED blocks that "a"
//     cannot share. If B3c2's walk mistakes the rename for a drop, the durable root loses those
//     blocks and the loss is measurable rather than hidden behind B2's packing;
//   * rows are added in TWO rounds around a checkpoint, so some of the column's blocks are
//     named by no durable root;
//   * the content is checked per row, not just the count: a walk that dropped the column and a
//     scan that reads it as all-NULL both keep the row count intact.
TEST_CASE("integration::cpp::test_alter_rename_column::renamed_column_survives_restart_with_its_data") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/restart"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;

    INFO("phase 1: filled and checkpointed, so the durable root names both columns' blocks");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.wide (a bigint, payload bigint[40]);");
        insert_rows(dispatcher, 0, FIRST_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    auto before = walk_offline(otbx, &resource);
    REQUIRE(before.report.ok);
    REQUIRE(before.columns.size() == 2);
    REQUIRE(before.columns[1] == "payload");
    REQUIRE_FALSE(before.report.root_data.empty());

    INFO("phase 2: more rows, RENAME COLUMN, CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_rows(dispatcher, FIRST_ROWS, SECOND_ROWS);
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide RENAME COLUMN payload TO payload2;");

        // Visible to the live session straight away — the catalog half of the rename.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT payload2 FROM TestDatabase.wide;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    // GATE 1 — the DURABLE storage schema carries the new name. This is the half that keeps the
    // bootstrap reconciliation from reading the rename as a drop; a catalog-only rename leaves
    // "payload" here and arms the mine.
    auto renamed = walk_offline(otbx, &resource);
    REQUIRE(renamed.report.ok);
    INFO("durable columns after RENAME+CHECKPOINT: " << renamed.columns.size() << " ["
                                                     << (renamed.columns.empty() ? std::string{} : renamed.columns[0])
                                                     << ", "
                                                     << (renamed.columns.size() < 2 ? std::string{} : renamed.columns[1])
                                                     << "]");
    REQUIRE(renamed.columns.size() == 2);
    CHECK(renamed.columns[0] == "a");
    CHECK(renamed.columns[1] == "payload2");
    CHECK(renamed.report.reachable_free_overlap.empty());

    INFO("phase 3: restart — the bootstrap reconciliation runs here");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // GATE 2 — the column and ALL of its rows are there, under the new name, with the
        // content they were written with. A dropped column would fail this whether it comes
        // back as an error, a missing column or a NULL one.
        {
            // Read the payload through the renamed column with 1-based array subscripts: both
            // ends of every row's array, so a column that came back empty, NULL-filled or
            // shifted fails here rather than passing on the row count alone.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT a, payload2[1], payload2[40] FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
            REQUIRE(cur->column_count() == 3);
            for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
                REQUIRE(cur->value(1, i).value<int64_t>() == static_cast<int64_t>(i * 100));
                REQUIRE(cur->value(2, i).value<int64_t>() ==
                        static_cast<int64_t>(i * 100 + ARRAY_LENGTH - 1));
            }
        }

        // The old name must not resolve after the restart either.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT payload FROM TestDatabase.wide;");
            REQUIRE(cur->is_error());
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    // GATE 3 — nothing was released. A rename moves no bytes, so the restart+checkpoint must
    // leave the column's data blocks exactly where they were; a root that SHRANK is the
    // signature of the reconciliation having dropped a surviving column.
    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);
    REQUIRE(after.columns.size() == 2);
    CHECK(after.columns[1] == "payload2");
    INFO("root data blocks before=" << before.report.root_data.size()
                                    << " renamed=" << renamed.report.root_data.size()
                                    << " after=" << after.report.root_data.size());
    CHECK(after.report.root_data.size() >= renamed.report.root_data.size());
    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());
}

// CASE 3 — RN-oid. THE CRASH WINDOW, and the reason the reconciliation cannot key on names.
//
// CASE 2 above proves the rename reaches the storage and survives a CLEAN restart, because the
// CHECKPOINT in between made both halves durable together. This case removes that checkpoint,
// which is the only thing that was holding the invariant up.
//
// What the two halves are durable at is not symmetric and cannot be made so by ordering:
//   * the CATALOG half — the pg_attribute row carrying the new attname — is durable at the
//     ALTER's WAL commit marker;
//   * the STORAGE half — the renamed column definition inside the .otbx — is durable only at
//     that table's NEXT CHECKPOINT, which is an unbounded time later.
// Kill the process in between and the next start loads a storage naming `payload` against a
// catalog naming `payload2`. A reconciliation that reads "in the storage, not in the live
// catalog" as a DROP then releases a SURVIVING column's blocks — the column and every one of
// its rows, gone, on a database that was never asked to drop anything.
//
// THE CRASH is executed only through the T3 fault-injection seam. A clean scope exit runs
// test_spaces' destructor, which issues a CHECKPOINT — precisely the event this case must be
// missing. Arming fail_writes_from AFTER the RENAME makes every later .otbx write fail, so no
// header commits for any table and every durable file stays byte-identical (the conservative
// crash semantics). The WAL is a different file and does NOT go through the block manager's
// interposer, so the ALTER's commit marker survives — which is the whole point: the catalog
// half must be durable while the storage half is not.
//
// EVERY ROW IS CHECKPOINTED BEFORE THE RENAME, deliberately, and it costs the case nothing: the
// rows are added in two rounds (so the column's blocks are named by two different durable roots)
// and both rounds are committed before the kill, so the ONLY thing the crash takes away is the
// storage half of the rename. Leaving round 2 WAL-only instead would drag in a defect that has
// nothing to do with this one — an ARRAY column's element data does not survive WAL replay (see
// the report note), so the check would fail for a reason no reconciliation can influence.
//
// THE GATE is the CONTENT, on many rows, not a column count and not a row count: a column that
// was dropped and re-derived as all-NULL keeps both intact. `payload` is bigint[40] so its
// segments take DEDICATED blocks past partial_block_manager_t's FULL_THRESHOLD (B2 packing
// would otherwise hide the released space behind the surviving column's), and the durable root
// is walked offline before and after so a release shows up as a root that SHRANK.
TEST_CASE("integration::cpp::test_alter_rename_column::renamed_column_survives_a_crash_before_the_checkpoint") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/crash"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;

    INFO("phase 1: filled and checkpointed, so the durable root names both columns' blocks");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.wide (a bigint, payload bigint[40]);");
        insert_rows(dispatcher, 0, FIRST_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    auto before = walk_offline(otbx, &resource);
    REQUIRE(before.report.ok);
    REQUIRE(before.columns.size() == 2);
    REQUIRE(before.columns[1] == "payload");
    REQUIRE_FALSE(before.report.root_data.empty());
    // The identity the whole mechanism turns on is DURABLE, and it is the catalog's own
    // pg_attribute.attoid, so it is well past the well-known system OIDs.
    INFO("durable attoids: " << before.attoids[0] << ", " << before.attoids[1]);
    REQUIRE(before.attoids.size() == 2);
    CHECK(before.attoids[0] != 0);
    CHECK(before.attoids[1] != 0);
    CHECK(before.attoids[0] != before.attoids[1]);

    INFO("phase 2: a second committed round, then RENAME COLUMN, then KILL before any checkpoint");
    {
        // Declared before the engine so the interposer is installed when the block managers
        // open their files (wrap() runs once per open) and is still installed while the engine
        // tears down. Every knob is off until the kill is armed, so phase 2 runs normally.
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_rows(dispatcher, FIRST_ROWS, SECOND_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide RENAME COLUMN payload TO payload2;");

        // KILL. fail_writes_from is compared with >=, so 1 fails every write from here on
        // without the test having to count them. The engine is idle between statements, so
        // this write to the shared plan cannot race an in-flight one.
        plan.fail_writes_from = 1;
    } // ← the destructor's CHECKPOINT runs here and can commit nothing.

    // What the crash left, asserted rather than assumed: the durable storage still names the
    // OLD column, and carries the SAME attoid it always did. If the kill silently failed to
    // land, the phase-3 claims would pass for the wrong reason.
    auto crashed = walk_offline(otbx, &resource);
    REQUIRE(crashed.report.ok);
    REQUIRE(crashed.columns.size() == 2);
    REQUIRE(crashed.columns[1] == "payload");
    REQUIRE(crashed.attoids == before.attoids);

    INFO("phase 3: restart — the bootstrap reconciliation runs against the DIVERGED names");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // GATE 1 — the column and ALL of its rows are there, under the NEW name, with the
        // content they were written with. On a name-keyed reconciliation the column is gone by
        // now: this either errors, or comes back NULL-filled.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT a, payload2[1], payload2[40] FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
            REQUIRE(cur->column_count() == 3);
            for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
                REQUIRE(cur->value(1, i).value<int64_t>() == static_cast<int64_t>(i * 100));
                REQUIRE(cur->value(2, i).value<int64_t>() ==
                        static_cast<int64_t>(i * 100 + ARRAY_LENGTH - 1));
            }
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);
    INFO("durable columns after the restart+checkpoint: " << after.columns.size());
    REQUIRE(after.columns.size() == 2);

    // GATE 2 — IDENTITY, and it is the point of the whole change: the durable schema still
    // carries the same two attoids, in the same order. A rename moves the NAME and nothing
    // else, so the reconciliation had a stable key to match on and no divergence to misread.
    CHECK(after.attoids == before.attoids);
    // ...and the stale storage name was repaired FROM the catalog, so the write path (whose
    // column expansion and drop_column are name-addressed) is no longer looking at a name the
    // catalog retired.
    CHECK(after.columns[1] == "payload2");

    // GATE 3 — nothing was released. A rename moves no bytes and the reconciliation must move
    // none either, so the root that the post-restart checkpoint commits still names every data
    // block the crashed one did. A root that SHRANK is the signature of a surviving column
    // having been dropped.
    INFO("root data blocks crashed=" << crashed.report.root_data.size()
                                     << " after=" << after.report.root_data.size());
    CHECK(after.report.root_data.size() >= crashed.report.root_data.size());
    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());
}

// CASE 4 — RN-oid's ACQUIRED PROPERTY: with identity carried by the attoid, a RENAME and an
// ADD COLUMN that storage has not materialized yet are TELLABLE APART, and they sit on opposite
// sides of the comparison:
//   * the renamed column's attoid IS in the live catalog — under a different name — so it is
//     not a drop and nothing may be released;
//   * the added column's attoid is in the live catalog and NOT in the storage — an extra
//     CATALOG-only attoid, which is legal and common. It is not merely ignored: its identity is
//     PUBLISHED FORWARD, so the INSERT that eventually materializes the column stamps the
//     catalog's attoid onto it instead of leaving a 0 the next reconciliation must refuse.
// By NAME those two are the same observation ("the two lists disagree"), which is exactly why
// the name-keyed walk had no guard to add on its own side. This case puts BOTH in flight in the
// same crash window, and then closes the loop by materializing the added column after the
// restart and checking the identity it was born with, offline, in the durable file.
TEST_CASE("integration::cpp::test_alter_rename_column::rename_and_unmaterialized_add_column_are_distinguishable") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/add_vs_rename"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;

    INFO("phase 1: two columns, filled and checkpointed");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.wide (a bigint, payload bigint[40]);");
        insert_rows(dispatcher, 0, FIRST_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    REQUIRE_FALSE(otbx.empty());
    auto before = walk_offline(otbx, &resource);
    REQUIRE(before.report.ok);
    REQUIRE(before.columns.size() == 2);
    REQUIRE(before.attoids.size() == 2);

    INFO("phase 2: ADD COLUMN (never inserted into, so never materialized) + RENAME, then KILL");
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // No INSERT follows, so the storage never grows this column: it exists ONLY as a
        // pg_attribute row. That is the "extra catalog attoid" half.
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide ADD COLUMN extra bigint;");
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide RENAME COLUMN payload TO payload2;");

        plan.fail_writes_from = 1;
    }

    auto crashed = walk_offline(otbx, &resource);
    REQUIRE(crashed.report.ok);
    REQUIRE(crashed.columns.size() == 2);
    REQUIRE(crashed.columns[1] == "payload");
    REQUIRE(crashed.attoids == before.attoids);

    INFO("phase 3: restart — neither half may be read as a drop, and the added column is "
         "materialized by an INSERT that must give it the CATALOG's attoid");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // The renamed column kept its data. Every row here was checkpointed in phase 1, so
        // this is the durable content, not a WAL-replayed approximation of it.
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT a, payload2[1] FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == FIRST_ROWS);
            for (std::size_t i = 0; i < FIRST_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
                REQUIRE(cur->value(1, i).value<int64_t>() == static_cast<int64_t>(i * 100));
            }
        }

        // Materialize `extra`. The identity the ALTER minted for it was parked on the owning
        // agent by the ALTER's own commit and LOST with the crash; the bootstrap re-published
        // it from pg_attribute (an oid-set difference against the storage), so this INSERT's
        // schema-growth stage has it to stamp.
        {
            std::stringstream q;
            q << "INSERT INTO TestDatabase.wide (a, payload2, extra) VALUES (" << FIRST_ROWS << ", ARRAY[";
            for (std::size_t j = 0; j < ARRAY_LENGTH; ++j) {
                q << (FIRST_ROWS * 100 + j) << (j + 1 == ARRAY_LENGTH ? "" : ",");
            }
            q << "], 777);";
            run_sql(dispatcher, q.str());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT a, extra FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == FIRST_ROWS + 1);
            REQUIRE(cur->value(1, FIRST_ROWS).value<int64_t>() == 777);
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);

    // GATE — the durable schema now carries THREE columns, the first two with the identities
    // they were created with, and the third with a NON-ZERO one it could only have got from the
    // catalog row the ALTER wrote before the crash. A 0 there is the mine this task exists to
    // remove: the next reconciliation would have to refuse the whole table.
    INFO("durable columns after the restart+checkpoint: " << after.columns.size());
    REQUIRE(after.columns.size() == 3);
    REQUIRE(after.attoids.size() == 3);
    CHECK(after.columns[1] == "payload2");
    CHECK(after.columns[2] == "extra");
    CHECK(after.attoids[0] == before.attoids[0]);
    CHECK(after.attoids[1] == before.attoids[1]);
    INFO("attoid of the materialized ADD COLUMN: " << after.attoids[2]);
    CHECK(after.attoids[2] != 0);
    CHECK(after.attoids[2] != after.attoids[0]);
    CHECK(after.attoids[2] != after.attoids[1]);

    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());
}
