#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// A crash-recovery lookup through the index must not name a deleted row, must not shift, and
// must not resurrect an emptied table. (These cases originally caught a rebuild that keyed
// entries by scan POSITION instead of physical row id; fixed, and the startup rebuild pass that
// carried it later removed — the cases stay because the SUBJECT is the answer, not the mechanism.)
//
// kill -9 is simulated by COPYing the live data directory while the engine is up (the destructor
// checkpoint then mutates only the ORIGINAL) and reopening the COPY under a fresh engine.

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

        REQUIRE(exec("DELETE FROM b.t WHERE id = 1000;")->is_success());

        // kill -9 happens here.
        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor checkpoint runs against the ORIGINAL dir only

    auto crash_config = test_create_config(crash_dir);
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        auto deleted = exec("SELECT id FROM b.t WHERE k = 10000;");
        REQUIRE(deleted->is_success());
        CHECK(deleted->size() == 0);

        // The LAST row must resolve to itself, not a neighbor off by one.
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
    config.log.level = log_t::level::off;

    const std::filesystem::path crash_dir = integration_fixture_path("test_index_rebuild_crash/crashed_all");

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
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        auto by_key = exec("SELECT id FROM b.t WHERE k = 20;");
        REQUIRE(by_key->is_success());
        CHECK(by_key->size() == 0);

        auto all = exec("SELECT id FROM b.t;");
        REQUIRE(all->is_success());
        CHECK(all->size() == 0);
    }
    std::filesystem::remove_all(crash_dir);
}
