#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <string>

// An equi-JOIN whose key columns differ only in integer width must still match equal values.

namespace {
    using test_helpers::exec;
    using test_helpers::ok;

    template<typename D>
    uint64_t rows(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        INFO((c->is_success() ? std::string{} : std::string{c->get_error().what}));
        REQUIRE(c->is_success());
        return c->size();
    }
} // namespace

TEST_CASE("integration::cpp::join_key_width::int32_int64") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/int32_int64"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE m;"));
    REQUIRE(ok(d, "CREATE TABLE m.narrow (k INT, a INT);"));
    REQUIRE(ok(d, "CREATE TABLE m.wide (k BIGINT, b INT);"));
    REQUIRE(ok(d, "INSERT INTO m.narrow (k, a) VALUES (1,10),(2,20),(3,30);"));
    REQUIRE(ok(d, "INSERT INTO m.wide (k, b) VALUES (2,200),(3,300),(4,400);"));

    CHECK(rows(d, "SELECT * FROM m.narrow JOIN m.wide ON m.narrow.k = m.wide.k;") == 2);
    CHECK(rows(d, "SELECT * FROM m.wide JOIN m.narrow ON m.wide.k = m.narrow.k;") == 2);
    CHECK(rows(d, "SELECT * FROM m.narrow, m.wide WHERE m.narrow.k = m.wide.k;") == 2);
    CHECK(rows(d, "SELECT * FROM m.narrow LEFT JOIN m.wide ON m.narrow.k = m.wide.k;") == 3);
}

TEST_CASE("integration::cpp::join_key_width::smallint_int32") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/smallint_int32"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE m;"));
    REQUIRE(ok(d, "CREATE TABLE m.s (k SMALLINT);"));
    REQUIRE(ok(d, "CREATE TABLE m.i (k INT);"));
    REQUIRE(ok(d, "INSERT INTO m.s (k) VALUES (1),(2),(3);"));
    REQUIRE(ok(d, "INSERT INTO m.i (k) VALUES (3),(1),(7);"));

    CHECK(rows(d, "SELECT * FROM m.s JOIN m.i ON m.s.k = m.i.k;") == 2);
}
