// A rolled-back bulk insert leaves the journal with records that span pages; the committed inserts after it must
// all come back from a crash image, byte for byte, before and after a checkpoint and a second reopen.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

namespace {
    struct sql_t {
        otterbrix::wrapper_dispatcher_t* d;
        auto operator()(const std::string& sql) const {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        }
    };

    std::string payload_of(int id) { return "p" + std::to_string(id) + std::string(static_cast<size_t>(id % 13), 'q'); }

    void insert_committed(const sql_t& exec, int from, int to) {
        for (int base = from; base < to; base += 50) {
            std::string sql = "INSERT INTO w.t (id, payload) VALUES ";
            for (int i = base; i < std::min(to, base + 50); ++i) {
                sql += (i != base ? ", (" : "(") + std::to_string(i) + ", '" + payload_of(i) + "')";
            }
            REQUIRE(exec(sql + ";")->is_success());
        }
    }

    void verify_all(const sql_t& exec, int rows) {
        auto cur = exec("SELECT id, payload FROM w.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(rows));
        for (size_t r = 0; r < cur->size(); ++r) {
            const auto id_cell = cur->value(0, r);
            const auto payload_cell = cur->value(1, r);
            INFO("row " << r);
            REQUIRE(id_cell.value<int64_t>() == static_cast<int64_t>(r));
            REQUIRE(payload_cell.value<std::string_view>() == payload_of(static_cast<int>(r)));
        }
    }
} // namespace

TEST_CASE("integration::cpp::wal_span_crash_recovery::committed_inserts_after_a_rollback_survive_a_crash") {
    auto config = test_create_config(integration_fixture_path("wal_span_crash_recovery/src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash_dir = integration_fixture_path("wal_span_crash_recovery/crash");
    constexpr int kRows = 1000;
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        auto txn = otterbrix::session_id_t();
        auto* d = space.dispatcher();
        REQUIRE(d->execute_sql(txn, "BEGIN;")->is_success());
        const std::string stale(200, 'z');
        for (int base = 0; base < kRows; base += 50) {
            std::string sql = "INSERT INTO w.t (id, payload) VALUES ";
            for (int i = base; i < base + 50; ++i) {
                sql += (i != base ? ", (" : "(") + std::to_string(-1 - i) + ", '" + stale + "')";
            }
            REQUIRE(d->execute_sql(txn, sql + ";")->is_success());
        }
        REQUIRE(d->execute_sql(txn, "ROLLBACK;")->is_success());
        insert_committed(exec, 0, kRows);
        verify_all(exec, kRows);
        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }
    auto crash_config = test_create_config(crash_dir);
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        verify_all(exec, kRows);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        verify_all(exec, kRows);
        insert_committed(exec, kRows, kRows + 200);
    }
    {
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        verify_all(exec, kRows + 200);
    }
}
