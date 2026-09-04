#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

// GROUP BY over a general expression. A grouping key is an expression like any other: the group
// evaluates every computed key before grouping and appends it to the chunk, so from there down a
// computed key is an ordinary input column. What that buys, and what these cases pin, is that a
// SELECT / HAVING / ORDER BY naming the same expression READS the group's key column instead of
// recomputing it over rows the group has already folded away.

namespace {
    using test_helpers::exec;
    using integers = std::vector<int64_t>;
    using doubles = std::vector<double>;

    template<typename Dispatcher>
    bool succeeds(Dispatcher* dispatcher, const std::string& sql) {
        auto cursor = exec(dispatcher, sql);
        return cursor && cursor->is_success();
    }

    template<typename Dispatcher>
    integers integer_column(Dispatcher* dispatcher, const std::string& sql, uint64_t column = 0) {
        auto cursor = exec(dispatcher, sql);
        REQUIRE(cursor);
        INFO(sql);
        REQUIRE(cursor->is_success());
        integers values;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            values.push_back(cursor->value(column, row).template value<int64_t>());
        }
        return values;
    }

    template<typename Dispatcher>
    doubles double_column(Dispatcher* dispatcher, const std::string& sql, uint64_t column = 0) {
        auto cursor = exec(dispatcher, sql);
        REQUIRE(cursor);
        INFO(sql);
        REQUIRE(cursor->is_success());
        doubles values;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            values.push_back(cursor->value(column, row).template value<double>());
        }
        return values;
    }

    template<typename Dispatcher>
    bool rejected(Dispatcher* dispatcher, const std::string& sql) {
        auto cursor = exec(dispatcher, sql);
        INFO(sql);
        return cursor && cursor->is_error();
    }

    template<typename Dispatcher>
    void seed_table(Dispatcher* dispatcher) {
        REQUIRE(succeeds(dispatcher, "CREATE DATABASE g;"));
        REQUIRE(succeeds(dispatcher, "CREATE TABLE g.t (id BIGINT, a BIGINT, b BIGINT);"));
        // a+b takes two values: 2 (rows 1,2) and 5 (rows 3,4). a alone takes three.
        REQUIRE(succeeds(dispatcher, "INSERT INTO g.t (id, a, b) VALUES (1,1,1),(2,1,1),(3,2,3),(4,4,1);"));
    }
} // namespace

TEST_CASE("integration::cpp::group_by_expressions::groups_by_the_computed_value") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/groups");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // Two distinct values of a+b, two rows each.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY a + b ORDER BY a + b;") == integers{2, 2});
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY abs(a) ORDER BY abs(a);") == integers{2, 1, 1});
    // a*2-b is 1 for the first three rows and 7 for the last.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY a * 2 - b ORDER BY a * 2 - b;") ==
          integers{3, 1});

    // A key that reads no column is one group over the whole input.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY 'abc';") == integers{4});

    // Two keys, one plain and one computed.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY a, b + 1 ORDER BY a, b + 1;") ==
          integers{2, 1, 1});
    // The same key twice is still one grouping.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY a + b, a + b ORDER BY a + b;") ==
          integers{2, 2});
}

TEST_CASE("integration::cpp::group_by_expressions::projects_the_key") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/project");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // The VALUES are the point, not the row count: a target list naming the key must read the
    // group's column. Recomputing it over rows the group has folded away is a wrong answer, which
    // no assertion on shape alone would catch.
    CHECK(integer_column(dispatcher, "SELECT a + b FROM g.t GROUP BY a + b ORDER BY a + b;") == integers{2, 5});
    CHECK(integer_column(dispatcher, "SELECT a + b, count(*) FROM g.t GROUP BY a + b ORDER BY a + b;", 1) ==
          integers{2, 2});

    // An expression BUILT ON the key is one value per group too.
    CHECK(integer_column(dispatcher, "SELECT (a + b) * 10 FROM g.t GROUP BY a + b ORDER BY a + b;") ==
          integers{20, 50});

    // The key inside a reduction still folds the group's ROWS: sum(a+b) over two rows of 2 is 4.
    CHECK(integer_column(dispatcher, "SELECT sum(a + b) FROM g.t GROUP BY a + b ORDER BY a + b;") == integers{4, 10});
    // ... and outside and inside one at a time.
    CHECK(integer_column(dispatcher, "SELECT a + b, sum(a), count(*) FROM g.t GROUP BY a + b ORDER BY a + b;", 1) ==
          integers{2, 6});
}

TEST_CASE("integration::cpp::group_by_expressions::case_expression_key") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/case_expr");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // a is 1,1,2,4 — so the arms fold rows 1 and 2 into key 0 and leave 2 and 4 alone.
    const std::string key = "CASE WHEN a = 1 THEN 0 ELSE a END";
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY " + key + " ORDER BY " + key + ";") ==
          integers{2, 1, 1});
    // Projecting the key reads the group's column; recomputing it over folded rows would be a
    // wrong answer, and a CASE also has to keep the type the planner promoted it to.
    CHECK(integer_column(dispatcher, "SELECT " + key + " FROM g.t GROUP BY " + key + " ORDER BY " + key + ";") ==
          integers{0, 2, 4});
    CHECK(integer_column(dispatcher,
                         "SELECT " + key + ", count(*) FROM g.t GROUP BY " + key + " ORDER BY " + key + ";",
                         1) == integers{2, 1, 1});
}

TEST_CASE("integration::cpp::group_by_expressions::cast_key") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/cast");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // The cast is part of the key's identity: grouping is over the cast VALUE.
    CHECK(
        integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY CAST(a AS DOUBLE) ORDER BY CAST(a AS DOUBLE);") ==
        integers{2, 1, 1});
    CHECK(double_column(dispatcher,
                        "SELECT CAST(a AS DOUBLE) FROM g.t GROUP BY CAST(a AS DOUBLE) ORDER BY CAST(a AS DOUBLE);") ==
          doubles{1.0, 2.0, 4.0});
    // Same value, narrower target — still one group per distinct cast result.
    CHECK(integer_column(dispatcher,
                         "SELECT count(*) FROM g.t GROUP BY CAST(a AS SMALLINT) ORDER BY CAST(a AS SMALLINT);") ==
          integers{2, 1, 1});
}

TEST_CASE("integration::cpp::group_by_expressions::function_key") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/function");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // A literal argument: the two spellings of `2` bind parameters of their own, so matching the
    // target list against the key has to compare the VALUES behind the ids.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY pow(a, 2) ORDER BY pow(a, 2);") ==
          integers{2, 1, 1});
    CHECK(double_column(dispatcher, "SELECT pow(a, 2) FROM g.t GROUP BY pow(a, 2) ORDER BY pow(a, 2);") ==
          doubles{1.0, 4.0, 16.0});

    // A NESTED expression argument: the key and the target list each build their own `a - 2`, so
    // matching them has to compare the subtrees structurally. |a-2| is 1,1,0,2.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY abs(a - 2) ORDER BY abs(a - 2);") ==
          integers{1, 2, 1});
    CHECK(integer_column(dispatcher, "SELECT abs(a - 2) FROM g.t GROUP BY abs(a - 2) ORDER BY abs(a - 2);") ==
          integers{0, 1, 2});
    CHECK(integer_column(dispatcher,
                         "SELECT abs(a - 2), count(*) FROM g.t GROUP BY abs(a - 2) ORDER BY abs(a - 2);",
                         1) == integers{1, 2, 1});
}

TEST_CASE("integration::cpp::group_by_expressions::having_and_order_by_name_the_key") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/above_group");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // HAVING and ORDER BY sit ABOVE the group and read its output, so the key has to reach them
    // as a column of that output.
    CHECK(integer_column(dispatcher, "SELECT a + b FROM g.t GROUP BY a + b HAVING a + b > 2;") == integers{5});
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY a + b HAVING a + b > 2;") == integers{2});
    CHECK(integer_column(dispatcher, "SELECT a + b FROM g.t GROUP BY a + b ORDER BY a + b DESC;") == integers{5, 2});
    CHECK(integer_column(dispatcher, "SELECT a + b FROM g.t GROUP BY a + b HAVING a + b < 5 ORDER BY a + b;") ==
          integers{2});
    // A key named only above the group, never projected.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.t GROUP BY a + b ORDER BY a + b DESC;") == integers{2, 2});
}

TEST_CASE("integration::cpp::group_by_expressions::ordinal_addresses_the_select_list") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/ordinal");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    CHECK(integer_column(dispatcher, "SELECT a, count(*) FROM g.t GROUP BY 1 ORDER BY a;") == integers{1, 2, 4});
    // The ordinal names the n-th select EXPRESSION, computed or not.
    CHECK(integer_column(dispatcher, "SELECT a + b, count(*) FROM g.t GROUP BY 1 ORDER BY a + b;", 1) ==
          integers{2, 2});
    // A position past the select list addresses nothing.
    CHECK(rejected(dispatcher, "SELECT count(*) FROM g.t GROUP BY 42;"));
    CHECK(rejected(dispatcher, "SELECT count(*) FROM g.t GROUP BY 0;"));
}

TEST_CASE("integration::cpp::group_by_expressions::nested_and_null_keys") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/nested");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(succeeds(dispatcher, "CREATE DATABASE g;"));
    REQUIRE(succeeds(dispatcher, "CREATE TABLE g.s (id BIGINT, v BIGINT);"));
    REQUIRE(succeeds(dispatcher, "INSERT INTO g.s (id, v) VALUES (1, 1), (2, NULL), (3, 1), (4, NULL);"));

    // Rows whose key evaluates to NULL are one group, as they are for a plain column key.
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.s GROUP BY v + 1;") == integers{2, 2});
    CHECK(integer_column(dispatcher, "SELECT count(*) FROM g.s GROUP BY v IS NULL;") == integers{2, 2});
}

TEST_CASE("integration::cpp::group_by_expressions::more_groups_than_one_chunk") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/many");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(succeeds(dispatcher, "CREATE DATABASE g;"));
    REQUIRE(succeeds(dispatcher, "CREATE TABLE g.big (id BIGINT, v BIGINT);"));
    // 3000 distinct keys: past DEFAULT_VECTOR_CAPACITY, so key blocks grow, the index rehashes,
    // and finalize emits several chunks.
    std::string rows;
    for (int key = 0; key < 3000; ++key) {
        rows += (key ? "," : "") + std::string("(") + std::to_string(key) + "," + std::to_string(key) + ")";
    }
    REQUIRE(succeeds(dispatcher, "INSERT INTO g.big (id, v) VALUES " + rows + ";"));

    auto cursor = exec(dispatcher, "SELECT v * 2, count(*) FROM g.big GROUP BY v * 2;");
    REQUIRE(cursor);
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 3000);
    int64_t counted_rows = 0;
    for (uint64_t row = 0; row < cursor->size(); ++row) {
        counted_rows += cursor->value(1, row).value<int64_t>();
    }
    CHECK(counted_rows == 3000);
}

TEST_CASE("integration::cpp::group_by_expressions::rejects") {
    auto config = test_helpers::make_test_config("/tmp/group_by_expressions/rejects");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // A reduction cannot be what a group is grouped BY.
    CHECK(rejected(dispatcher, "SELECT count(*) FROM g.t GROUP BY sum(a);"));
    // Grouping by a+b does not make a or b one value per group.
    CHECK(rejected(dispatcher, "SELECT a FROM g.t GROUP BY a + b;"));
    CHECK(rejected(dispatcher, "SELECT a + 1 FROM g.t GROUP BY a + b;"));
    // A star projects the base columns, none of which the computed key makes groupable.
    CHECK(rejected(dispatcher, "SELECT * FROM g.t GROUP BY a + b;"));
    // A key that names no column is refused HERE. That is what lets the group operator assert on
    // a resolved column path instead of checking for one: nothing downstream can be handed a key
    // validation did not resolve.
    CHECK(rejected(dispatcher, "SELECT count(*) FROM g.t GROUP BY nope;"));
    CHECK(rejected(dispatcher, "SELECT count(*) FROM g.t GROUP BY nope + 1;"));
}
