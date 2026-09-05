#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// T4 (plan): what a lookup THROUGH THE INDEX must answer after a crash that left a
// tombstone in the middle of the table.
//
// The shift these cases were written against was a numbering one: the startup rebuild
// scanned with VISIBILITY FILTERING, which compacts POSITIONS while physical row ids keep
// their gaps, and then keyed each rebuilt entry by its position in the scan, while
// collection_t::fetch resolves row ids PHYSICALLY. Every row after the tombstone was
// therefore indexed one row low. The numbering was fixed (entries are keyed by the
// scan's own chunk.row_ids) and the startup rebuild pass itself has since been removed:
// once the last in-memory index went away it cleared and refilled a buffer it then
// erased, i.e. did nothing, at the cost of a full scan of every table on every start.
//
// The cases stay because their SUBJECT is the answer, not the mechanism: a lookup through
// the index after this crash must not name the deleted row, must not shift, and must not
// resurrect a table that was emptied.
//
// kill -9 is simulated with the T3 crash mechanism: COPY the live data directory while the
// engine is up (the destructor checkpoint then mutates only the ORIGINAL, hiding nothing on
// the copy) and reopen the COPY under a fresh engine. No test lays out files by hand.

namespace {

    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

} // namespace

TEST_CASE("integration::cpp::index_rebuild_crash::mid_table_delete_shifts_rebuilt_row_ids") {
    auto config = test_create_config(integration_fixture_path("test_index_rebuild_crash/orig"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    const std::filesystem::path crash_dir = integration_fixture_path("test_index_rebuild_crash/crashed");
    constexpr int64_t kRows = 2000;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX k_idx ON b.t (k);")->is_success());

        // 2000 rows, k = 10*id: enough that a mid-table tombstone shifts a long tail.
        for (int64_t start = 1; start <= kRows; start += 500) {
            std::string sql = "INSERT INTO b.t (id, k) VALUES ";
            for (int64_t i = start; i < start + 500 && i <= kRows; i++) {
                if (i != start) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
            }
            sql += ";";
            REQUIRE(exec(sql)->is_success());
        }
        REQUIRE(exec("CHECKPOINT;")->is_success());

        // The mid-table tombstone: rows after id=1000 shift by one in a
        // visibility-filtered, position-compacted rebuild scan.
        REQUIRE(exec("DELETE FROM b.t WHERE id = 1000;")->is_success());

        // kill -9 happens here.
        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor checkpoint runs against the ORIGINAL dir only

    auto crash_config = test_create_config(crash_dir);
    crash_config.wal.on = true;
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        // The deleted key must be gone.
        auto deleted = exec("SELECT id FROM b.t WHERE k = 10000;");
        REQUIRE(deleted->is_success());
        CHECK(deleted->size() == 0);

        // The LAST row must be findable through the rebuilt index — and must be the right
        // row. With the off-by-one rebuild the entry points one row early (or nowhere).
        auto last = exec("SELECT id FROM b.t WHERE k = " + std::to_string(10 * kRows) + ";");
        REQUIRE(last->is_success());
        REQUIRE(last->size() == 1);
        CHECK(last->value(0, 0).value<int64_t>() == kRows);

        // A row right after the tombstone: the first shifted victim.
        auto shifted = exec("SELECT id FROM b.t WHERE k = 10010;");
        REQUIRE(shifted->is_success());
        REQUIRE(shifted->size() == 1);
        CHECK(shifted->value(0, 0).value<int64_t>() == 1001);
    }
    std::filesystem::remove_all(crash_dir);
}

TEST_CASE("integration::cpp::index_rebuild_crash::delete_all_then_crash_returns_nothing") {
    auto config = test_create_config(integration_fixture_path("test_index_rebuild_crash/orig_all"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    const std::filesystem::path crash_dir =
        integration_fixture_path("test_index_rebuild_crash/crashed_all");

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX k_idx ON b.t (k);")->is_success());
        REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());
        REQUIRE(exec("CHECKPOINT;")->is_success());
        REQUIRE(exec("DELETE FROM b.t;")->is_success());

        copy_dir_as_crash(config.main_path, crash_dir);
    }

    auto crash_config = test_create_config(crash_dir);
    crash_config.wal.on = true;
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        // Every row was deleted before the crash; the rebuilt index (row_count == 0 skips
        // the rebuild — the second hole) must NOT resurrect them.
        auto by_key = exec("SELECT id FROM b.t WHERE k = 20;");
        REQUIRE(by_key->is_success());
        CHECK(by_key->size() == 0);

        auto all = exec("SELECT id FROM b.t;");
        REQUIRE(all->is_success());
        CHECK(all->size() == 0);
    }
    std::filesystem::remove_all(crash_dir);
}
