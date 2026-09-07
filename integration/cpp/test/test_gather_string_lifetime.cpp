#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/row_group.hpp>
#include <string>

// A borrowed std::string_view from fetch_row's gather branch is only safe while its block stays
// pinned at the same address; the buffer pool can now spill a transient block to disk and reload
// it elsewhere, so an unpinned borrow can dangle.

// Counts borrowed strings rather than trying to catch the dangling read directly: a crash-based
// check would be flaky in the dangerous direction, passing while the defect is present.
TEST_CASE("integration::cpp::test_gather_string_lifetime::gather_leaves_no_borrowed_strings", "[.][gatherstr]") {
    auto config = test_create_config(integration_fixture_path("test_gather_string/lifetime"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    constexpr int kRows = 50000;
    constexpr int kBatch = 1000;

    REQUIRE(exec("CREATE DATABASE g;")->is_success());
    REQUIRE(exec("CREATE TABLE g.t (id bigint, tag text, v bigint);")->is_success());
    for (int base = 0; base < kRows; base += kBatch) {
        std::string sql = "INSERT INTO g.t (id, tag, v) VALUES ";
        for (int i = 0; i < kBatch; ++i) {
            const int n = base + i;
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(n) + ", 'tag_value_" + std::to_string(n) + "', " + std::to_string(n) + ")";
        }
        sql += ";";
        auto session = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(session, sql)->is_success());
    }

    components::table::reset_gathered_borrowed_strings();
    {
        auto cur = exec("SELECT tag FROM g.t WHERE id > 49900;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() > 0);
        CHECK(cur->size() == 99);
    }
    const auto borrowed = components::table::gathered_borrowed_strings();

    INFO("string cells the gather filled with a view borrowed from a pin it then dropped: " << borrowed);
    CHECK(borrowed == 0);
}
