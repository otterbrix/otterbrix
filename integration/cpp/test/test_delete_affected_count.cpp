// affected_rows_ counts SCAN-matched rows, not what storage marks: MVCC hides a transaction's
// own prior deletes, so a re-DELETE always scans (and reports) 0 -- this is intended, not a gap.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <unistd.h>

TEST_CASE("integration::cpp::delete_affected_count::txn_re_delete_reports_zero", "[deletecount]") {
    auto config = test_create_config(integration_fixture_path("test_delete_affected_count/txn"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](otterbrix::session_id_t& s, const std::string& sql) { return d->execute_sql(s, sql); };

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(s, "CREATE DATABASE dc;")->is_success());
        REQUIRE(exec(s, "CREATE TABLE dc.t (id bigint);")->is_success());
        REQUIRE(exec(s, "INSERT INTO dc.t (id) VALUES (1), (2), (3);")->is_success());
    }

    auto txn = otterbrix::session_id_t();
    REQUIRE(exec(txn, "BEGIN;")->is_success());
    {
        auto cur = exec(txn, "DELETE FROM dc.t WHERE id <= 2;");
        REQUIRE(cur->is_success());
        INFO("two rows matched, two marks placed, two reported");
        CHECK(cur->size() == 2);
    }
    {
        auto cur = exec(txn, "DELETE FROM dc.t WHERE id <= 2;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0);
    }
    REQUIRE(exec(txn, "COMMIT;")->is_success());

    {
        auto s = otterbrix::session_id_t();
        auto cur = exec(s, "SELECT id FROM dc.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 3);
    }
}
