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

// ALTER TABLE DROP COLUMN wrote only the pg_attribute tombstone; nothing on the ALTER path called
// table_storage_t::drop_column, so the physical column survived DROP forever. Column b is bigint[40]
// to force DEDICATED blocks, and every measurement runs offline against a reloaded .otbx.

using components::catalog::FIRST_USER_OID;

namespace {

    constexpr std::size_t FIRST_ROWS = 3072;  // 1.5 row groups, checkpointed
    constexpr std::size_t SECOND_ROWS = 1024; // appended AFTER that root
    constexpr std::size_t TOTAL_ROWS = FIRST_ROWS + SECOND_ROWS;
    constexpr std::size_t ARRAY_LENGTH = 40;
    constexpr std::size_t INSERT_BATCH = 512;

    // Every system catalog sits under an oid below FIRST_USER_OID, so filtering on that picks the user table.
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
            // Scoped to reading the manager reference out: a holder kept alive across a reclaim keeps block handles alive too.
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

    // Content-addressed rows: a block freed while something still reads it shows up as wrong data, not a matching count.
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

}

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
        CHECK(cur->column_count() == 1);
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);

    INFO("columns after the drop: " << after.columns.size());
    CHECK(after.columns.size() == 1);
    CHECK(after.columns.front() == "a");

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
        // "Came back" has three honest shapes: free list, a surviving column sharing the block, or reissued -- else orphaned.
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

// A crash between the ALTER's commit and the next checkpoint must not leak the dropped column's
// space: pending_released_blocks_ is in-memory and lost on kill, so the durable root still names
// the blocks after reload even though the tombstone (WAL commit marker) is durable.
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
        // Declared before the engine: wrap() runs once per open, so the interposer must already
        // be installed when the block managers open their files.
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_rows(dispatcher, FIRST_ROWS, SECOND_ROWS);
        run_sql(dispatcher, "ALTER TABLE TestDatabase.wide DROP COLUMN b;");

        plan.fail_writes_from = 1;
    }

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

        // The catalog kept the tombstone across the crash, so the column is invisible to SQL even unfixed -- not the real gate.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.wide;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == TOTAL_ROWS);
        CHECK(cur->column_count() == 1);

        run_sql(dispatcher, "CHECKPOINT;");
    }

    auto after = walk_offline(otbx, &resource);
    REQUIRE(after.report.ok);

    // On the unfixed build the restart puts b back and the checkpoint rewrites it, so this schema would show 2 columns.
    INFO("columns after the restart+checkpoint: " << after.columns.size());
    CHECK(after.columns.size() == 1);
    CHECK(after.columns.front() == "a");

    // The table now holds more rows than `before` did, yet its durable root must name fewer data
    // blocks (b is 40 * 8 B per row against a's 8 B); on the unfixed build the root grows instead.
    INFO("root data blocks before=" << before.report.root_data.size() << " after="
                                    << after.report.root_data.size());
    CHECK(after.report.root_data.size() < before.report.root_data.size());

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
