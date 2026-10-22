#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <core/date/date_parse.hpp>
#include <optional>
#include <string>
#include <vector>

using namespace test_helpers;

namespace {

    using opt = std::optional<int64_t>;

    void refused(otterbrix::wrapper_dispatcher_t* d, const std::string& sql, const std::string& what) {
        auto cur = exec(d, sql);
        INFO(sql);
        REQUIRE(cur);
        REQUIRE(cur->is_error());
        CHECK(std::string(cur->get_error().what) == what);
    }

    components::cursor::cursor_t_ptr answered(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO(sql);
        REQUIRE(cur);
        REQUIRE(cur->is_success());
        return cur;
    }

    std::vector<opt> column(otterbrix::wrapper_dispatcher_t* d, const std::string& sql, uint64_t col = 0) {
        auto cur = answered(d, sql);
        INFO(sql);
        std::vector<opt> out;
        for (uint64_t r = 0; r < cur->size(); ++r) {
            auto null_cell = cur->is_null(col, r);
            REQUIRE_FALSE(null_cell.has_error());
            if (null_cell.value()) {
                out.emplace_back();
                continue;
            }
            auto cell = cur->get<int64_t>(col, r);
            REQUIRE_FALSE(cell.has_error());
            out.emplace_back(cell.value());
        }
        return out;
    }

    void seed_t(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(ok(d, "CREATE DATABASE k;"));
        REQUIRE(ok(d, "CREATE TABLE k.t (a INT);"));
        REQUIRE(ok(d, "INSERT INTO k.t (a) VALUES (1), (2), (3);"));
    }

    std::vector<core::date::timetz_t> timetz_column(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = answered(d, sql);
        INFO(sql);
        std::vector<core::date::timetz_t> out;
        for (uint64_t r = 0; r < cur->size(); ++r) {
            auto cell = cur->get<core::date::timetz_t>(0, r);
            REQUIRE_FALSE(cell.has_error());
            out.push_back(cell.value());
        }
        return out;
    }

    core::date::timetz_t timetz(std::string_view text) {
        auto parsed = core::date::parse_timetz(text);
        REQUIRE(parsed);
        return *parsed;
    }

    const std::string decimal_width = "DECIMAL width must be between 1 and 38 and scale must not exceed width";
    const std::string unknown_cast = "no cast from text to unknown";

} // namespace

TEST_CASE("integration::cpp::kernel_type_reachability::decimal_wider_than_38_is_refused_at_bind") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/decimal"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_t(d);

    refused(d, "CREATE TABLE k.d (x NUMERIC(39,0));", decimal_width);
    refused(d, "SELECT CAST(1 AS NUMERIC(39,0));", decimal_width);
    refused(d, "SELECT CAST(a AS NUMERIC(39,0)) FROM k.t;", decimal_width);
    refused(d, "SELECT DISTINCT CAST(a AS NUMERIC(39,0)) FROM k.t;", decimal_width);
    refused(d, "SELECT CAST(a AS NUMERIC(39,0)) FROM k.t GROUP BY 1;", decimal_width);
    refused(d, "SELECT COUNT(DISTINCT CAST(a AS NUMERIC(39,0))) FROM k.t;", decimal_width);
    refused(d, "SELECT CAST(a AS NUMERIC(39,0)) FROM k.t ORDER BY 1;", decimal_width);
    refused(d, "SELECT CAST('1' AS NUMERIC(39,0)) AS v UNION ALL SELECT CAST('2' AS NUMERIC(39,0));", decimal_width);

    CHECK(column(d, "SELECT a FROM k.t ORDER BY a;") == std::vector<opt>{1, 2, 3});
}

TEST_CASE("integration::cpp::kernel_type_reachability::cast_to_unknown_is_refused_at_bind") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/unknown"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_t(d);

    refused(d, "SELECT 'abc'::unknown;", unknown_cast);
    refused(d, "SELECT DISTINCT 'abc'::unknown FROM k.t;", unknown_cast);
    refused(d, "SELECT 'abc'::unknown AS u FROM k.t GROUP BY 1;", unknown_cast);
}

TEST_CASE("integration::cpp::kernel_type_reachability::bit_and_blob_are_refused_at_bind") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/bit"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_t(d);

    refused(d,
            "CREATE TABLE k.b (x BIT);",
            "column 'x' of type  cannot be stored: a table column cannot be built from a type without storage");
    refused(d, "SELECT CAST(1 AS BIT);", "a literal cast to BIT is not supported yet");
    refused(d, "SELECT CAST('a' AS BLOB);", "a literal cast to BLOB is not supported yet");
    refused(d, "SELECT B'101';", "unsupported constant kind: NodeTag(654)");

    CHECK(column(d, "SELECT a FROM k.t ORDER BY a;") == std::vector<opt>{1, 2, 3});
}

TEST_CASE("integration::cpp::kernel_type_reachability::timetz_answers_through_copy_flatten_and_hash") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/timetz"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE k;"));
    REQUIRE(ok(d, "CREATE TABLE k.z (t TIMETZ);"));
    REQUIRE(ok(d, "INSERT INTO k.z (t) VALUES ('12:34:56+03'), ('01:02:03-05'), ('10:00:00+05'), ('09:00:00-02');"));
    REQUIRE(ok(d, "CREATE TABLE k.s (t TIMETZ);"));
    REQUIRE(ok(d, "INSERT INTO k.s (t) VALUES ('11:00:00+01'), ('12:00:00+02');"));

    CHECK(timetz_column(d, "SELECT t FROM k.z ORDER BY t;") ==
          std::vector<core::date::timetz_t>{timetz("10:00:00+05"),
                                            timetz("01:02:03-05"),
                                            timetz("12:34:56+03"),
                                            timetz("09:00:00-02")});
    CHECK(answered(d, "SELECT DISTINCT t FROM k.z;")->size() == 4);
    CHECK(timetz_column(d, "SELECT t FROM k.s ORDER BY t;") ==
          std::vector<core::date::timetz_t>{timetz("12:00:00+02"), timetz("11:00:00+01")});
    CHECK(answered(d, "SELECT DISTINCT t FROM k.s;")->size() == 2);
    CHECK(timetz_column(d, "SELECT CAST('12:00:00+01' AS TIMETZ);") ==
          std::vector<core::date::timetz_t>{timetz("12:00:00+01")});
}

TEST_CASE("integration::cpp::kernel_type_reachability::index_on_struct_or_list_is_refused_before_hash32") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/index"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE k;"));
    REQUIRE(ok(d, "CREATE TYPE pair_t AS (x INT, y INT);"));
    REQUIRE(ok(d, "CREATE TABLE k.s (id INT, p pair_t);"));
    REQUIRE(ok(d, "INSERT INTO k.s (id, p) VALUES (1, ROW(1,2)), (2, ROW(3,4));"));
    REQUIRE(ok(d, "CREATE TABLE k.l (id INT, v INT[]);"));
    REQUIRE(ok(d, "INSERT INTO k.l (id, v) VALUES (1, ARRAY[1,2]), (2, ARRAY[3]);"));

    refused(d,
            "CREATE INDEX i_s ON k.s (p);",
            "CREATE INDEX: key 'p' has type pair_t, which the index key encoders cannot represent");
    refused(d,
            "CREATE INDEX i_l ON k.l (v);",
            "CREATE INDEX: key 'v' has type int4[], which the index key encoders cannot represent");
    refused(d,
            "CREATE INDEX i_s_h ON k.s USING hash (p);",
            "CREATE INDEX: key 'p' has type pair_t, which the index key encoders cannot represent");

    REQUIRE(ok(d, "INSERT INTO k.s (id, p) VALUES (3, ROW(5,6));"));
    CHECK(column(d, "SELECT id FROM k.s ORDER BY id;") == std::vector<opt>{1, 2, 3});
    CHECK(column(d, "SELECT id FROM k.l ORDER BY id;") == std::vector<opt>{1, 2});
}

TEST_CASE("integration::cpp::kernel_type_reachability::null_columns_answer_through_copy_flatten_and_hash") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/null"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_t(d);

    CHECK(column(d, "SELECT NULL FROM k.t;") == std::vector<opt>{opt{}, opt{}, opt{}});
    CHECK(column(d, "SELECT NULL UNION ALL SELECT NULL;") == std::vector<opt>{opt{}, opt{}});
    CHECK(column(d, "SELECT NULL FROM k.t WHERE a > 1 LIMIT 1;") == std::vector<opt>{opt{}});
    CHECK(column(d, "SELECT n FROM (SELECT NULL AS n FROM k.t) sub WHERE n IS NULL;") ==
          std::vector<opt>{opt{}, opt{}, opt{}});
    CHECK(column(d, "SELECT a FROM k.t ORDER BY NULL, a;") == std::vector<opt>{1, 2, 3});
    CHECK(column(d, "SELECT a FROM k.t WHERE a IN (NULL, NULL);").empty());
    CHECK(column(d, "SELECT t1.a FROM k.t t1 JOIN (SELECT NULL AS j) q ON t1.a = q.j;").empty());
    CHECK(column(d, "SELECT n FROM k.t t1 JOIN k.t t2 USING (a) CROSS JOIN (SELECT NULL AS n) q;") ==
          std::vector<opt>{opt{}, opt{}, opt{}});

    REQUIRE(ok(d, "CREATE TABLE k.t2 (a INT);"));
    REQUIRE(ok(d, "INSERT INTO k.t2 SELECT NULL FROM k.t;"));
    CHECK(column(d, "SELECT a FROM k.t2;") == std::vector<opt>{opt{}, opt{}, opt{}});
}

TEST_CASE("integration::cpp::kernel_type_reachability::struct_and_list_keys_reach_hash") {
    auto config = make_test_config(integration_fixture_path("test_kernel_type_reachability/nested"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE k;"));
    REQUIRE(ok(d, "CREATE TYPE pair_t AS (x INT, y INT);"));
    REQUIRE(ok(d, "CREATE TYPE inner_t AS (q INT[]);"));
    REQUIRE(ok(d, "CREATE TABLE k.s (id INT, p pair_t);"));
    REQUIRE(ok(d, "INSERT INTO k.s (id, p) VALUES (1, ROW(1,2)), (2, ROW(1,2));"));
    REQUIRE(ok(d, "CREATE TABLE k.l (id INT, v INT[]);"));
    REQUIRE(ok(d, "INSERT INTO k.l (id, v) VALUES (1, ARRAY[1,2]), (2, ARRAY[1,2]), (3, ARRAY[3]);"));
    REQUIRE(ok(d, "CREATE TABLE k.n (id INT, p inner_t);"));
    REQUIRE(ok(d, "INSERT INTO k.n (id, p) VALUES (1, ROW(ARRAY[1])), (2, ROW(ARRAY[1]));"));

    CHECK(column(d, "SELECT COUNT(DISTINCT p) FROM k.s;") == std::vector<opt>{1});
    CHECK(answered(d, "SELECT DISTINCT p FROM k.s;")->size() == 1);
    CHECK(column(d, "SELECT COUNT(*) FROM k.s GROUP BY p;") == std::vector<opt>{2});

    CHECK(column(d, "SELECT COUNT(DISTINCT v) FROM k.l;") == std::vector<opt>{2});
    CHECK(answered(d, "SELECT DISTINCT v FROM k.l;")->size() == 2);
    CHECK(column(d, "SELECT COUNT(*) FROM k.l GROUP BY v ORDER BY 1;") == std::vector<opt>{1, 2});
    CHECK(column(d, "SELECT t1.id FROM k.l t1 JOIN k.l t2 ON t1.v = t2.v ORDER BY 1;") ==
          std::vector<opt>{1, 1, 2, 2, 3});
    CHECK(answered(d, "SELECT v FROM k.l ORDER BY v;")->size() == 3);
    CHECK(answered(d, "SELECT v FROM k.l UNION SELECT v FROM k.l;")->size() == 2);

    CHECK(column(d, "SELECT COUNT(DISTINCT p) FROM k.n;") == std::vector<opt>{1});
}
