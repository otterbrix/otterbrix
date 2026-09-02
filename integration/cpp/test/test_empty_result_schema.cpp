#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

namespace {
    using components::types::logical_type;
    using test_helpers::exec;

    // One column of a result descriptor, in output order.
    using schema_t = std::vector<std::pair<logical_type, std::string>>;

    schema_t schema_of(const components::cursor::cursor_t_ptr& cursor) {
        schema_t columns;
        for (const auto& type : cursor->type_data()) {
            columns.emplace_back(type.type(), type.alias());
        }
        return columns;
    }

    std::string describe(const schema_t& schema) {
        std::string text;
        for (const auto& [type, name] : schema) {
            text += " " + std::to_string(static_cast<int>(type)) + "/" + (name.empty() ? "<unnamed>" : name);
        }
        return text.empty() ? std::string{" <no columns>"} : text;
    }

    template<typename D>
    void expect_columns(D* dispatcher, const std::string& sql, const schema_t& expected) {
        auto cursor = exec(dispatcher, sql);
        REQUIRE(cursor);
        INFO(sql);
        REQUIRE(cursor->is_success());
        const auto actual = schema_of(cursor);
        INFO("expected:" << describe(expected));
        INFO("actual:  " << describe(actual));
        CHECK(cursor->size() == 0);
        CHECK(actual == expected);
    }

    template<typename D>
    void seed(D* dispatcher) {
        auto ok = [&](const std::string& sql) {
            auto cursor = exec(dispatcher, sql);
            return cursor && cursor->is_success();
        };
        REQUIRE(ok("CREATE DATABASE m;"));
        REQUIRE(ok("CREATE TABLE m.t (a INT, b INT, z INT);"));
        REQUIRE(ok("INSERT INTO m.t (a, b, z) VALUES (1,10,100),(1,20,200),(2,30,300);"));
        REQUIRE(ok("CREATE TABLE m.empty (a INT, b INT);"));
        REQUIRE(ok("CREATE TABLE m.empty2 (a INT, b INT);"));
    }

    const schema_t ab{{logical_type::INTEGER, "a"}, {logical_type::INTEGER, "b"}};
    const schema_t abz{{logical_type::INTEGER, "a"}, {logical_type::INTEGER, "b"}, {logical_type::INTEGER, "z"}};
    const schema_t a_only{{logical_type::INTEGER, "a"}};
    const schema_t a_count{{logical_type::INTEGER, "a"}, {logical_type::UBIGINT, "count"}};
    const schema_t count_only{{logical_type::UBIGINT, "count"}};
} // namespace

// A filter that matches no row.
TEST_CASE("integration::cpp::empty_result_schema::scan_and_projection") {
    auto config = test_helpers::make_test_config("/tmp/empty_result_schema/scan");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    expect_columns(dispatcher, "SELECT a, b FROM m.t WHERE a = 999;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.t WHERE a = 999 ORDER BY a;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.t WHERE a = 999 LIMIT 10;", ab);
    expect_columns(dispatcher, "SELECT DISTINCT a, b FROM m.t WHERE a = 999;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.t LIMIT 0;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.t OFFSET 100;", ab);
    // An AS alias names the column, whatever the expression under it
    expect_columns(dispatcher, "SELECT a AS key FROM m.t WHERE a = 999;", {{logical_type::INTEGER, "key"}});
    // BIGINT because '1' is int64
    expect_columns(dispatcher,
                   "SELECT a + 1 AS inc, b * 2 AS dbl FROM m.t WHERE a = 999;",
                   {{logical_type::BIGINT, "inc"}, {logical_type::BIGINT, "dbl"}});
}

// An empty TABLE, with no filter at all — the scan drains on its first fetch instead of filtering
// rows away, so every operator above it sees a zero-row batch it never had a populated one of.
TEST_CASE("integration::cpp::empty_result_schema::empty_table") {
    auto config = test_helpers::make_test_config("/tmp/empty_result_schema/empty_table");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    expect_columns(dispatcher, "SELECT a, b FROM m.empty;", ab);
    // A star expands from the input chunk rather than a named target list.
    expect_columns(dispatcher, "SELECT * FROM m.empty;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.empty ORDER BY a;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.empty LIMIT 10;", ab);
    expect_columns(dispatcher, "SELECT DISTINCT a, b FROM m.empty;", ab);
}

TEST_CASE("integration::cpp::empty_result_schema::grouping") {
    auto config = test_helpers::make_test_config("/tmp/empty_result_schema/grouping");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    expect_columns(dispatcher, "SELECT a, count(b) FROM m.t WHERE a = 999 GROUP BY a;", a_count);
    expect_columns(dispatcher, "SELECT a FROM m.t WHERE a = 999 GROUP BY a;", a_only);
    expect_columns(dispatcher, "SELECT count(b) FROM m.t WHERE a = 999 GROUP BY a;", count_only);
    expect_columns(dispatcher, "SELECT a, count(b) FROM m.empty GROUP BY a;", a_count);
    expect_columns(dispatcher, "SELECT a FROM m.empty GROUP BY a;", a_only);
    // An aggregate is named after its function; an AS alias overrides that.
    expect_columns(dispatcher, "SELECT max(z) FROM m.t WHERE a = 999 GROUP BY a;", {{logical_type::INTEGER, "max"}});
    expect_columns(dispatcher,
                   "SELECT count(b) AS total FROM m.t WHERE a = 999 GROUP BY a;",
                   {{logical_type::UBIGINT, "total"}});
    // HAVING that removes every group.
    expect_columns(dispatcher, "SELECT a, count(b) FROM m.t GROUP BY a HAVING count(b) > 100;", a_count);
    // HAVING over an aggregate NOT in the target list: the group carries a hidden __having_* column
    // that must not reach the result.
    expect_columns(dispatcher, "SELECT a FROM m.t GROUP BY a HAVING count(z) > 100;", a_only);
}

TEST_CASE("integration::cpp::empty_result_schema::joins_and_set_ops") {
    auto config = test_helpers::make_test_config("/tmp/empty_result_schema/joins");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    expect_columns(dispatcher, "SELECT t.a, s.b FROM m.t AS t JOIN m.t AS s ON t.a = s.a WHERE t.a = 999;", ab);
    expect_columns(dispatcher, "SELECT t.a, e.b FROM m.empty AS t LEFT JOIN m.empty2 AS e ON t.a = e.a;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.t WHERE a IN (SELECT a FROM m.empty);", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.empty UNION ALL SELECT a, b FROM m.empty2;", ab);
    expect_columns(dispatcher, "SELECT a, b FROM m.empty UNION SELECT a, b FROM m.empty2;", ab);
    expect_columns(dispatcher,
                   "SELECT x.a, x.c FROM (SELECT a, count(b) AS c FROM m.t GROUP BY a) AS x WHERE x.a = 999;",
                   {{logical_type::INTEGER, "a"}, {logical_type::UBIGINT, "c"}});
}

// RETURNING is a result set like any other. Every WHERE matches nothing, so the table is left
// untouched and these are order-independent.
TEST_CASE("integration::cpp::empty_result_schema::dml_returning") {
    auto config = test_helpers::make_test_config("/tmp/empty_result_schema/returning");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    expect_columns(dispatcher, "UPDATE m.t SET z = 0 WHERE a = 999 RETURNING a, b;", ab);
    expect_columns(dispatcher, "UPDATE m.t SET z = 0 WHERE a = 999 RETURNING *;", abz);
    expect_columns(dispatcher, "DELETE FROM m.t WHERE a = 999 RETURNING a, b;", ab);
    expect_columns(dispatcher, "DELETE FROM m.t WHERE a = 999 RETURNING *;", abz);
}