#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/block_reachability_walker.hpp>
#include <core/pmr.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdlib>
#include <filesystem>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// B3c1 — ALTER TABLE DROP COLUMN must reach the storage primitive.
//
// B3c made table_storage_t::drop_column correct in BOTH modes (rebuild now, blocks NAMED into
// pending_released_blocks_, released by the checkpoint round that can commit the release), and
// gated that behaviour with services::disk::table_storage::drop_column_disk_frees_blocks — a
// test that calls the primitive DIRECTLY. Nothing called it from the ALTER path: the operator
// wrote the pg_attribute tombstone and stopped, so on a regular disk table the physical column
// survived every DROP forever. This test judges the SQL statement instead of the primitive.
//
// SHAPE, and every part of it is load-bearing:
//   * column "b" is bigint[40] — 40 * 8 B * 2048 rows = 640 KiB of child payload per row group,
//     past partial_block_manager_t's FULL_THRESHOLD, so b's segments take DEDICATED blocks that
//     "a" cannot share. Without that (F6's technique, reused here) B2 packs several columns'
//     segments into ONE 256 KiB block and a dropped column that shares blocks with a survivor
//     looks reclaimed when nothing happened at all;
//   * rows are added in TWO rounds around a checkpoint, so some of b's blocks are named by no
//     durable root — A7.3's reclaim_superseded_root walks only the root's own data blocks and
//     is structurally blind to those;
//   * every measurement is taken with the ENGINE DOWN, against a freshly loaded .otbx. That is
//     both the walker's "right after load" contract and the A7.4 lesson: a leak or a bad free
//     usually shows only on a reopened file;
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
            // Counted collection copy scoped to reading the manager reference out of it
            // (ITEM C): a holder kept alive across a reclaim keeps block handles alive too.
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
    auto config = test_create_config("/tmp/otterbrix/integration/test_alter_drop_column_reclaim/disk_drop");
    test_clear_directory(config);
    config.disk.on = true;
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

    // The wiring itself: the reopened file's own schema no longer carries the column. Before
    // B3c1 the ALTER wrote only the tombstone, so this is where the test goes red.
    INFO("columns after the drop: " << after.columns.size());
    CHECK(after.columns.size() == 1);
    CHECK(after.columns.front() == "a");

    // Blocks the durable root named before the drop and does not name after it must all be
    // accounted for: back in the free list, or still held by a surviving column that shares
    // the block (B2 packing).
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
        // by a surviving column that shared the block (B2 packing), or already re-issued as
        // this round's metadata chain. Anything else is a block named by no owner — which is
        // what the pre-B3c1 run produced (block 52, below).
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
