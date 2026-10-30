#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

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
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
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
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.s (k SMALLINT);"));
    REQUIRE(ok(d, "CREATE TABLE m.i (k INT);"));
    REQUIRE(ok(d, "INSERT INTO m.s (k) VALUES (1),(2),(3);"));
    REQUIRE(ok(d, "INSERT INTO m.i (k) VALUES (3),(1),(7);"));

    CHECK(rows(d, "SELECT * FROM m.s JOIN m.i ON m.s.k = m.i.k;") == 2);
}

// A join key under a cast (written, or added by the validator to widen the narrow side) is hashed like a bare one.
namespace {
    // Every cell as text, NULL as "N"; a row ends with ';'.
    std::string table_text(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        INFO((c->is_success() ? std::string{} : std::string{c->get_error().what}));
        REQUIRE(c->is_success());
        std::string out;
        for (size_t r = 0; r < c->size(); ++r) {
            for (size_t col = 0; col < c->column_count(); ++col) {
                auto null = c->is_null(col, r);
                REQUIRE_FALSE(null.has_error());
                if (null.value()) {
                    out += "N";
                } else {
                    auto cell = c->get<int64_t>(col, r);
                    REQUIRE_FALSE(cell.has_error());
                    out += std::to_string(cell.value());
                }
                out += col + 1 == c->column_count() ? ";" : ",";
            }
        }
        return out;
    }

    std::string plan_text(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto c = exec(d, "EXPLAIN " + sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        std::string out;
        for (size_t r = 0; r < c->size(); ++r) {
            auto cell = c->get<std::string_view>(0, r);
            REQUIRE_FALSE(cell.has_error());
            out += std::string(cell.value());
            out += '\n';
        }
        return out;
    }

    bool contains(const std::string& hay, const std::string& needle) { return hay.find(needle) != std::string::npos; }

    size_t occurrences(const std::string& hay, const std::string& needle) {
        size_t count = 0;
        for (size_t at = hay.find(needle); at != std::string::npos; at = hay.find(needle, at + needle.size())) {
            ++count;
        }
        return count;
    }

    // 4294967297 and -4294967295 share their low 32 bits with 1: an INT64 key read as int32 would match x.k = 1.
    void seed_cast_key_tables(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(ok(d, "CREATE SCHEMA m;"));
        REQUIRE(ok(d, "CREATE TABLE m.a (id INT, k INT);"));
        REQUIRE(ok(d, "CREATE TABLE m.b (id INT, k BIGINT);"));
        REQUIRE(ok(d, "CREATE TABLE m.w (id INT, p1 INT, k INT, p3 INT);"));
        REQUIRE(ok(d, "INSERT INTO m.a (id, k) VALUES (1,1),(2,2),(3,3),(4,NULL);"));
        REQUIRE(ok(d,
                   "INSERT INTO m.b (id, k) VALUES (10,1),(11,4294967297),(12,3),(13,3),(14,NULL),"
                   "(15,-4294967295);"));
        REQUIRE(ok(d, "INSERT INTO m.w (id, p1, k, p3) VALUES (1,0,1,100),(2,0,3,300),(3,0,3,301);"));
    }
} // namespace

TEST_CASE("integration::cpp::join_key_width::cast_key_hash_join") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/cast_key_hash_join"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_cast_key_tables(d);

    const std::string inner = "1,10;3,12;3,13;";
    const std::string q1 = "SELECT x.id, y.id FROM m.a x JOIN m.b y ON x.k = y.k ORDER BY x.id, y.id";
    const std::string q2 = "SELECT x.id, y.id FROM m.a x JOIN m.b y ON CAST(x.k AS BIGINT) = y.k ORDER BY x.id, y.id";
    const std::string q3 = "SELECT x.id, y.id FROM m.a x JOIN m.b y ON y.k = x.k ORDER BY x.id, y.id";
    const std::string q4 = "SELECT x.id, y.id FROM m.a x, m.b y WHERE x.k = y.k ORDER BY x.id, y.id";
    const std::string q5 = "SELECT x.id, y.id FROM m.a x LEFT JOIN m.b y ON x.k = y.k ORDER BY x.id, y.id";
    const std::string q6 = "SELECT x.id, y.id FROM m.a x FULL JOIN m.b y ON x.k = y.k";

    CHECK(table_text(d, q1) == inner);
    CHECK(table_text(d, q2) == inner);
    CHECK(table_text(d, q3) == inner);
    CHECK(table_text(d, q4) == inner);
    CHECK(table_text(d, q5) == "1,10;2,N;3,12;3,13;4,N;");
    {
        std::string full = table_text(d, q6);
        std::vector<std::string> rows;
        for (size_t at = 0; at < full.size();) {
            const size_t end = full.find(';', at);
            rows.push_back(full.substr(at, end - at));
            at = end + 1;
        }
        std::sort(rows.begin(), rows.end());
        CHECK(rows == std::vector<std::string>{"1,10", "2,N", "3,12", "3,13", "4,N", "N,11", "N,14", "N,15"});
    }

    for (const auto* sql : {&q1, &q2, &q3, &q4, &q5, &q6}) {
        const auto plan = plan_text(d, *sql);
        INFO(*sql << "\n" << plan);
        CHECK(contains(plan, "Hash Join"));
        CHECK_FALSE(contains(plan, "Nested Loop"));
    }

    const std::string range =
        "SELECT x.id, y.id FROM m.a x JOIN m.b y ON x.k >= y.k AND x.k <= y.k ORDER BY x.id, y.id";
    CHECK(table_text(d, range) == inner);
    const auto range_plan = plan_text(d, range);
    INFO(range_plan);
    CHECK(contains(range_plan, "Nested Loop"));
}

// A WHERE on the key reaches the other scan through the join, as with bare keys.
TEST_CASE("integration::cpp::join_key_width::cast_key_transitive_filter") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/cast_key_transitive"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_cast_key_tables(d);

    const std::string on_wide =
        "SELECT x.id, y.id FROM m.a x JOIN m.b y ON x.k = y.k WHERE y.k = 3 ORDER BY x.id, y.id";
    CHECK(table_text(d, on_wide) == "3,12;3,13;");
    const auto plan = plan_text(d, on_wide);
    INFO(plan);
    CHECK(contains(plan, "Hash Join"));
    CHECK(occurrences(plan, "Filter") == 2);

    const std::string on_narrow =
        "SELECT x.id, y.id FROM m.a x JOIN m.b y ON x.k = y.k WHERE x.k = 3 ORDER BY x.id, y.id";
    CHECK(table_text(d, on_narrow) == "3,12;3,13;");
    const auto narrow_plan = plan_text(d, on_narrow);
    INFO(narrow_plan);
    CHECK(occurrences(narrow_plan, "Filter") == 2);

    // An INT constant leaves x.k bare, so y.k gets CAST(3 AS BIGINT) instead of the same cast of x.k.
    const std::string on_bare_narrow = "SELECT x.id, y.id FROM m.a x JOIN m.b y ON x.k = y.k "
                                       "WHERE x.k = CAST(3 AS INT) ORDER BY x.id, y.id";
    CHECK(table_text(d, on_bare_narrow) == "3,12;3,13;");
    const auto bare_plan = plan_text(d, on_bare_narrow);
    INFO(bare_plan);
    CHECK(occurrences(bare_plan, "Filter") == 2);
}

// Column pruning keeps the key column under the cast while dropping the columns nobody reads.
TEST_CASE("integration::cpp::join_key_width::cast_key_pruned_scan") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/cast_key_pruned"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_cast_key_tables(d);

    const std::string sql = "SELECT x.p3, y.id FROM m.w x JOIN m.b y ON x.k = y.k ORDER BY y.id, x.p3";
    CHECK(table_text(d, sql) == "100,10;300,12;301,12;300,13;301,13;");
    const auto plan = plan_text(d, sql);
    INFO(plan);
    CHECK(contains(plan, "Hash Join"));
}

// MIN pushed below a join whose key is cast: the partial groups by the column under the cast.
TEST_CASE("integration::cpp::join_key_width::cast_key_eager_aggregation") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/cast_key_eager"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_cast_key_tables(d);

    const std::string sql =
        "SELECT x.id, MIN(x.p3) FROM m.w x JOIN m.b y ON x.k = y.k GROUP BY x.id ORDER BY x.id";
    CHECK(table_text(d, sql) == "1,100;2,300;3,301;");
    const auto plan = plan_text(d, sql);
    INFO(plan);
    CHECK(contains(plan, "Hash Join"));
    CHECK(contains(plan, "Pushed Aggregate"));
}

// A comma-join star whose dimension keys are narrower than the fact's: every boundary is hashed.
TEST_CASE("integration::cpp::join_key_width::cast_key_star") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/cast_key_star"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.d1 (id INT, n BIGINT);"));
    REQUIRE(ok(d, "CREATE TABLE m.d2 (id INT, n BIGINT);"));
    REQUIRE(ok(d, "CREATE TABLE m.f (a BIGINT, b BIGINT, v BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.d1 (id, n) VALUES (1,100),(2,200);"));
    REQUIRE(ok(d, "INSERT INTO m.d2 (id, n) VALUES (1,1000),(2,2000);"));
    REQUIRE(ok(d, "INSERT INTO m.f (a, b, v) VALUES (1,1,10),(2,2,20),(1,2,30),(2,1,40);"));

    const std::string sql =
        "SELECT d1.n, d2.n, f.v FROM m.d1, m.d2, m.f WHERE f.a = d1.id AND f.b = d2.id ORDER BY f.v";
    CHECK(table_text(d, sql) == "100,1000,10;200,2000,20;100,2000,30;200,1000,40;");
    const auto plan = plan_text(d, sql);
    INFO(plan);
    CHECK(occurrences(plan, "Hash Join") == 2);
    CHECK_FALSE(contains(plan, "Nested Loop"));
}

// A comma join on a function of one side becomes an INNER join with that equality as its condition.
TEST_CASE("integration::cpp::join_key_width::comma_join_on_function_key") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/comma_function_key"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.f (a BIGINT, v BIGINT);"));
    REQUIRE(ok(d, "CREATE TABLE m.d (id BIGINT, n BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.f (a, v) VALUES (1,10),(-2,20),(3,30),(-1,40);"));
    REQUIRE(ok(d, "INSERT INTO m.d (id, n) VALUES (1,100),(2,200);"));

    const std::string comma = "SELECT f.v, d.n FROM m.f, m.d WHERE abs(f.a) = d.id ORDER BY f.v";
    const std::string joined = "SELECT f.v, d.n FROM m.f JOIN m.d ON abs(f.a) = d.id ORDER BY f.v";
    CHECK(table_text(d, comma) == "10,100;20,200;40,100;");
    CHECK(table_text(d, joined) == "10,100;20,200;40,100;");
    const auto comma_plan = plan_text(d, comma);
    const auto joined_plan = plan_text(d, joined);
    INFO(comma_plan << "\n" << joined_plan);
    CHECK(comma_plan == joined_plan);
    CHECK(contains(comma_plan, "Hash Join"));
}

// As in PostgreSQL 18 (nodeHashjoin.c): an inner or left join whose probe side is empty never builds the hash
// table, and an inner join whose hash table has no non-NULL key (a right join: no build row) never evaluates
// the probe keys. A key that would fail to convert on the skipped side therefore raises nothing.
TEST_CASE("integration::cpp::join_key_width::empty_side_skips_the_other_sides_keys") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/empty_side"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.e (id INT, k INT);"));
    REQUIRE(ok(d, "CREATE TABLE m.n (id INT, k INT);"));
    REQUIRE(ok(d, "CREATE TABLE m.s (id INT, s TEXT);"));
    REQUIRE(ok(d, "INSERT INTO m.n (id, k) VALUES (1, NULL);"));
    REQUIRE(ok(d, "INSERT INTO m.s (id, s) VALUES (10, 'x'), (11, 'y'), (12, 'z');"));

    const std::string inner_empty_first = "SELECT x.id, y.id FROM m.e x JOIN m.s y ON x.k = CAST(y.s AS INT)";
    const std::string inner_empty_last = "SELECT x.id, y.id FROM m.s y JOIN m.e x ON CAST(y.s AS INT) = x.k";
    const std::string inner_null_first = "SELECT x.id, y.id FROM m.n x JOIN m.s y ON x.k = CAST(y.s AS INT)";
    const std::string inner_null_last = "SELECT x.id, y.id FROM m.s y JOIN m.n x ON CAST(y.s AS INT) = x.k";
    const std::string left_empty_probe =
        "SELECT x.id, y.id FROM m.e x LEFT JOIN m.s y ON x.k = CAST(y.s AS INT)";
    const std::string right_null_build =
        "SELECT x.id, y.id FROM m.s y RIGHT JOIN m.n x ON CAST(y.s AS INT) = x.k";

    // The probe input is listed first under Hash Join: the smaller inner side is the build side either way.
    const auto probe_first = [&](const std::string& sql, const std::string& probe, const std::string& build) {
        const auto plan = plan_text(d, sql);
        INFO(sql << "\n" << plan);
        REQUIRE(contains(plan, "Hash Join"));
        CHECK(plan.find("Seq Scan on " + probe) < plan.find("Seq Scan on " + build));
    };
    probe_first(inner_empty_first, "s", "e");
    probe_first(inner_empty_last, "s", "e");
    probe_first(inner_null_first, "s", "n");
    probe_first(inner_null_last, "s", "n");
    probe_first(left_empty_probe, "e", "s");
    probe_first(right_null_build, "s", "n");

    for (const auto* sql :
         {&inner_empty_first, &inner_empty_last, &inner_null_first, &inner_null_last, &left_empty_probe}) {
        CHECK(table_text(d, *sql).empty());
    }
    auto right_join = exec(d, right_null_build);
    REQUIRE(right_join);
    CHECK_FALSE(right_join->is_success());
}

// The build side is LEFT JOIN's right input, which is never swapped: 3000 rows span several build chunks.
TEST_CASE("integration::cpp::join_key_width::computed_build_key_across_chunks") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/computed_build_key"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.p (k BIGINT, s TEXT);"));
    REQUIRE(ok(d, "CREATE TABLE m.q (k BIGINT, v BIGINT, s TEXT);"));
    constexpr unsigned rows = 3000;
    REQUIRE(test_helpers::seed_rows(d, "m.p", "k, s", rows, [](unsigned i) {
                return "(" + std::to_string(i) + ", 'k" + std::to_string(i) + "')";
            })->is_success());
    REQUIRE(test_helpers::seed_rows(d, "m.q", "k, v, s", rows, [](unsigned i) {
                return "(" + std::to_string(i) + ", " + std::to_string(i * 10) + ", 'K" + std::to_string(i) + "')";
            })->is_success());

    const std::string arithmetic =
        "SELECT COUNT(y.v), SUM(y.v) FROM m.p x LEFT JOIN m.q y ON x.k = y.k + 0";
    const std::string lowered =
        "SELECT COUNT(y.v), SUM(y.v) FROM m.p x LEFT JOIN m.q y ON x.s = lower(y.s)";
    const std::string expected = std::to_string(rows) + "," + std::to_string(10ull * rows * (rows - 1) / 2) + ";";
    for (const auto* sql : {&arithmetic, &lowered}) {
        INFO(*sql << "\n" << plan_text(d, *sql));
        CHECK(contains(plan_text(d, *sql), "Hash Join"));
        CHECK(table_text(d, *sql) == expected);
    }
}

// RIGHT JOIN keeps its right input as the build side, here the narrow one under the widening cast.
TEST_CASE("integration::cpp::join_key_width::right_join_with_cast_build_key") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/right_cast_build"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_cast_key_tables(d);

    const std::string sql = "SELECT x.id, y.id FROM m.b y RIGHT JOIN m.a x ON y.k = x.k ORDER BY x.id, y.id";
    INFO(plan_text(d, sql));
    CHECK(contains(plan_text(d, sql), "Hash Join"));
    CHECK(table_text(d, sql) == "1,10;2,N;3,12;3,13;4,N;");
}

// The mirror of cast_key_transitive_filter: the wide table first, the WHERE on its bare key.
TEST_CASE("integration::cpp::join_key_width::cast_key_transitive_filter_wide_side_first") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/cast_key_transitive_mirror"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_cast_key_tables(d);

    const std::string sql =
        "SELECT x.id, y.id FROM m.b y JOIN m.a x ON y.k = x.k WHERE y.k = 3 ORDER BY x.id, y.id";
    CHECK(table_text(d, sql) == "3,12;3,13;");
    const auto plan = plan_text(d, sql);
    INFO(plan);
    CHECK(occurrences(plan, "Filter") == 2);
}

// A LEFT join keeps every probe row when the build side yields no rows at all, whether the table is empty or a
// filter below the join removes every row.
TEST_CASE("integration::cpp::join_key_width::left_join_onto_empty_build_side") {
    auto config = test_helpers::make_test_config(integration_fixture_path("join_key_width/left_join_empty_build"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.a (k INT);"));
    REQUIRE(ok(d, "CREATE TABLE m.empty (k INT);"));
    REQUIRE(ok(d, "CREATE TABLE m.b (k INT);"));
    REQUIRE(ok(d, "INSERT INTO m.a (k) VALUES (1), (2), (3);"));
    REQUIRE(ok(d, "INSERT INTO m.b (k) VALUES (1), (2);"));

    const std::string empty_table = "SELECT a.k, e.k FROM m.a a LEFT JOIN m.empty e ON a.k = e.k ORDER BY a.k";
    const std::string filtered_away =
        "SELECT a.k, e.k FROM m.a a LEFT JOIN (SELECT * FROM m.b WHERE k < 0) e ON a.k = e.k ORDER BY a.k";
    for (const auto* sql : {&empty_table, &filtered_away}) {
        const auto plan = plan_text(d, *sql);
        INFO(*sql << "\n" << plan);
        CHECK(contains(plan, "Hash Join"));
        CHECK(table_text(d, *sql) == "1,N;2,N;3,N;");
    }
}
