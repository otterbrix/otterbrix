#include "integration_fixture_path.hpp"
#include "test_config.hpp"

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

// node_alter_column_t::set_attoid had no callers, so the RENAME operator no-oped while reporting
// success. Bootstrap reconciliation matched storage columns against pg_attribute BY NAME, so a
// catalog-only rename made the old name look dropped, releasing a SURVIVING column's blocks.

using components::catalog::FIRST_USER_OID;

namespace {

    constexpr std::size_t FIRST_ROWS = 3072;
    constexpr std::size_t SECOND_ROWS = 1024;
    constexpr std::size_t TOTAL_ROWS = FIRST_ROWS + SECOND_ROWS;
    constexpr std::size_t ARRAY_LENGTH = 40;
    constexpr std::size_t INSERT_BATCH = 512;

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
        std::vector<std::uint32_t> attoids;
    };

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

TEST_CASE("integration::cpp::test_alter_rename_column::rename_column_rebinds_the_name") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/rebind"));
    test_clear_directory(config);
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

TEST_CASE("integration::cpp::test_alter_rename_column::renamed_column_survives_restart_with_its_data") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/restart"));
    test_clear_directory(config);
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT payload2 FROM TestDatabase.wide;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto renamed = walk_offline(otbx, &resource);
    REQUIRE(renamed.report.ok);
    INFO("durable columns after RENAME+CHECKPOINT: "
         << renamed.columns.size() << " [" << (renamed.columns.empty() ? std::string{} : renamed.columns[0]) << ", "
         << (renamed.columns.size() < 2 ? std::string{} : renamed.columns[1]) << "]");
    REQUIRE(renamed.columns.size() == 2);
    CHECK(renamed.columns[0] == "a");
    CHECK(renamed.columns[1] == "payload2");
    CHECK(renamed.report.reachable_free_overlap.empty());

    INFO("phase 3: restart — the bootstrap reconciliation runs here");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT a, payload2[1], payload2[40] FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
            REQUIRE(cur->column_count() == 3);
            for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
                REQUIRE(cur->value(1, i).value<int64_t>() == static_cast<int64_t>(i * 100));
                REQUIRE(cur->value(2, i).value<int64_t>() == static_cast<int64_t>(i * 100 + ARRAY_LENGTH - 1));
            }
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT payload FROM TestDatabase.wide;");
            REQUIRE(cur->is_error());
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);
    REQUIRE(after.columns.size() == 2);
    CHECK(after.columns[1] == "payload2");
    INFO("root data blocks before=" << before.report.root_data.size() << " renamed=" << renamed.report.root_data.size()
                                    << " after=" << after.report.root_data.size());
    CHECK(after.report.root_data.size() >= renamed.report.root_data.size());
    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());
}

// The two halves of a rename are not durable at the same time: the CATALOG half is durable at the
// ALTER's WAL commit marker, while the STORAGE half (the renamed column definition in the .otbx)
// is durable only at that table's next CHECKPOINT — kill the process in between and a restart
// loads a storage naming `payload` against a catalog naming `payload2`. Arming fail_writes_from
// after the RENAME fails every later .otbx write while the WAL, a different file, is untouched.
TEST_CASE("integration::cpp::test_alter_rename_column::renamed_column_survives_a_crash_before_the_checkpoint") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/crash"));
    test_clear_directory(config);
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
    INFO("durable attoids: " << before.attoids[0] << ", " << before.attoids[1]);
    REQUIRE(before.attoids.size() == 2);
    CHECK(before.attoids[0] != 0);
    CHECK(before.attoids[1] != 0);
    CHECK(before.attoids[0] != before.attoids[1]);

    INFO("phase 2: a second committed round, then RENAME COLUMN, then KILL before any checkpoint");
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_rows(dispatcher, FIRST_ROWS, SECOND_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide RENAME COLUMN payload TO payload2;");

        plan.fail_writes_from = 1;
    } // ← the destructor's CHECKPOINT runs here and can commit nothing.

    auto crashed = walk_offline(otbx, &resource);
    REQUIRE(crashed.report.ok);
    REQUIRE(crashed.columns.size() == 2);
    REQUIRE(crashed.columns[1] == "payload");
    REQUIRE(crashed.attoids == before.attoids);

    INFO("phase 3: restart — the bootstrap reconciliation runs against the DIVERGED names");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT a, payload2[1], payload2[40] FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == TOTAL_ROWS);
            REQUIRE(cur->column_count() == 3);
            for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
                REQUIRE(cur->value(1, i).value<int64_t>() == static_cast<int64_t>(i * 100));
                REQUIRE(cur->value(2, i).value<int64_t>() == static_cast<int64_t>(i * 100 + ARRAY_LENGTH - 1));
            }
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);
    INFO("durable columns after the restart+checkpoint: " << after.columns.size());
    REQUIRE(after.columns.size() == 2);

    // The durable schema keeps the same attoids across the rename — the reconciliation's stable
    // key — but drop_column and column expansion still address storage BY NAME, so the stale
    // storage name has to be repaired from the catalog: the rename cannot be left un-applied there.
    CHECK(after.attoids == before.attoids);
    CHECK(after.columns[1] == "payload2");

    // A rename moves no bytes; a root that SHRANK is the signature of a surviving column dropped.
    INFO("root data blocks crashed=" << crashed.report.root_data.size() << " after=" << after.report.root_data.size());
    CHECK(after.report.root_data.size() >= crashed.report.root_data.size());
    CHECK(after.report.reachable_free_overlap.empty());
    CHECK(after.report.unexplained.empty());
}

// With identity carried by the attoid, a RENAME and an unmaterialized ADD COLUMN are tellable
// apart: the renamed column's attoid is in the live catalog under a different name (not a drop),
// while the added column's attoid is in the catalog but not yet in storage — published forward so
// the eventual INSERT stamps it instead of leaving a 0 the next reconciliation must refuse.
TEST_CASE("integration::cpp::test_alter_rename_column::rename_and_unmaterialized_add_column_are_distinguishable") {
    auto config = test_create_config(integration_fixture_path("test_alter_rename_column/add_vs_rename"));
    test_clear_directory(config);
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT a, payload2[1] FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == FIRST_ROWS);
            for (std::size_t i = 0; i < FIRST_ROWS; ++i) {
                INFO("row " << i);
                REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
                REQUIRE(cur->value(1, i).value<int64_t>() == static_cast<int64_t>(i * 100));
            }
        }

        // The ALTER's minted identity was lost with the crash; bootstrap re-published it from
        // pg_attribute for this INSERT to stamp.
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
            auto cur = dispatcher->execute_sql(session, "SELECT a, extra FROM TestDatabase.wide ORDER BY a;");
            INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == FIRST_ROWS + 1);
            REQUIRE(cur->value(1, FIRST_ROWS).value<int64_t>() == 777);
        }

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);

    // The third column's attoid is non-zero only because it came from the catalog row before the crash.
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
