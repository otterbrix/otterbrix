#include "test_config.hpp"
#include <algorithm>
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cmath>
#include <string>
#include <vector>

using namespace test_helpers;
using components::cursor::cursor_t_ptr;

namespace {

    cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    // One table per column type, each holding the same two magnitudes (-2 and 3) so a single
    // set of expectations covers every type.
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
        REQUIRE(run(d, "CREATE TABLE sel.i16 (x SMALLINT);")->is_success());
        REQUIRE(run(d, "CREATE TABLE sel.i32 (x INTEGER);")->is_success());
        REQUIRE(run(d, "CREATE TABLE sel.i64 (x BIGINT);")->is_success());
        REQUIRE(run(d, "CREATE TABLE sel.f32 (x REAL);")->is_success());
        REQUIRE(run(d, "CREATE TABLE sel.f64 (x DOUBLE PRECISION);")->is_success());
        REQUIRE(run(d, "CREATE TABLE sel.dec (x DECIMAL(10,2));")->is_success());
        for (const char* table : {"i16", "i32", "i64", "f32", "f64", "dec"}) {
            REQUIRE(run(d, "INSERT INTO sel." + std::string(table) + " (x) VALUES (-2), (3);")->is_success());
        }
    }

    size_t column_count(const cursor_t_ptr& cursor) {
        if (!cursor->is_success() || cursor->chunks().empty()) {
            return 0;
        }
        return cursor->chunks().front().column_count();
    }

    bool is_null_at(const cursor_t_ptr& cursor, uint64_t row) { return cursor->chunks().front().data[0].is_null(row); }

    bool bool_at(const cursor_t_ptr& cursor, uint64_t row) {
        const auto& column = cursor->chunks().front().data[0];
        REQUIRE(column.type().type() == components::types::logical_type::BOOLEAN);
        return column.get_value<bool>(row);
    }

    double numeric_at(const cursor_t_ptr& cursor, uint64_t row) {
        using components::types::logical_type;
        const auto& column = cursor->chunks().front().data[0];
        const auto& type = column.type();
        switch (type.type()) {
            case logical_type::SMALLINT:
                return static_cast<double>(column.get_value<int16_t>(row));
            case logical_type::INTEGER:
                return static_cast<double>(column.get_value<int32_t>(row));
            case logical_type::BIGINT:
                return static_cast<double>(column.get_value<int64_t>(row));
            case logical_type::UBIGINT:
                return static_cast<double>(column.get_value<uint64_t>(row));
            case logical_type::FLOAT:
                return static_cast<double>(column.get_value<float>(row));
            case logical_type::DOUBLE:
                return column.get_value<double>(row);
            case logical_type::DECIMAL: {
                const auto* extension =
                    reinterpret_cast<const components::types::decimal_logical_type_extension*>(type.extension());
                return static_cast<double>(column.get_value<int64_t>(row)) /
                       std::pow(10.0, static_cast<double>(extension->scale()));
            }
            default:
                FAIL("projected column is not a numeric type");
                return 0.0;
        }
    }

} // namespace

// ---------------------------------------------------------------- SELECT x

TEST_CASE("integration::cpp::select_rework::plain column passes through untouched") {
    auto config = make_test_config("/tmp/test_select_rework/plain");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    for (const char* table : {"i16", "i32", "i64", "f32", "f64", "dec"}) {
        auto cursor = run(d, "SELECT x FROM sel." + std::string(table) + " ORDER BY x;");
        INFO("table " << table);
        REQUIRE(cursor->is_success());
        CHECK(cursor->size() == 2);
        CHECK(column_count(cursor) == 1);
    }
}

TEST_CASE("integration::cpp::select_rework::explicit cast of a column projects one column") {
    auto config = make_test_config("/tmp/test_select_rework/plain_cast");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT CAST(x AS BIGINT) FROM sel.i32 ORDER BY x;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 2);
    CHECK(column_count(cursor) == 1);
}

// ------------------------------------------------------------ SELECT x + 1

TEST_CASE("integration::cpp::select_rework::arithmetic over a column and a literal", "[select_rework]") {
    auto config = make_test_config("/tmp/test_select_rework/arithmetic");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    for (const char* table : {"i16", "i32", "i64", "f32", "f64", "dec"}) {
        INFO("table " << table);
        auto cursor = run(d, "SELECT x + 1 AS v FROM sel." + std::string(table) + " ORDER BY x;");
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 2);
        CHECK(column_count(cursor) == 1);
        // -2 + 1 = -1 and 3 + 1 = 4, whatever the operand types were.
        CHECK(numeric_at(cursor, 0) == Catch::Approx(-1.0));
        CHECK(numeric_at(cursor, 1) == Catch::Approx(4.0));
    }
}

TEST_CASE("integration::cpp::select_rework::a column used twice still projects one result") {
    auto config = make_test_config("/tmp/test_select_rework/twice");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT x + x AS v FROM sel.i32 ORDER BY x;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 2);
    CHECK(column_count(cursor) == 1);
}

TEST_CASE("integration::cpp::select_rework::cast applied to an arithmetic result") {
    auto config = make_test_config("/tmp/test_select_rework/result_cast");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT CAST(x + 1 AS BIGINT) AS v FROM sel.f64 ORDER BY x;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 2);
    CHECK(column_count(cursor) == 1);
    // The VALUES, not just the shape. This projection used to be folded into a single
    // constant parameter by get_value, which reads a cast's operand as an A_Const -- over
    // the `x + 1` A_Expr that landed on the operator node's lexpr POINTER, so both rows
    // came back carrying the same run-dependent garbage and the shape checks above never
    // noticed. x is -2 and 3.
    CHECK(numeric_at(cursor, 0) == -1.0);
    CHECK(numeric_at(cursor, 1) == 4.0);
}

TEST_CASE("integration::cpp::select_rework::two columns of different types unify") {
    auto config = make_test_config("/tmp/test_select_rework/mixed");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.pair (a INTEGER, b DOUBLE PRECISION);")->is_success());
    REQUIRE(run(d, "INSERT INTO sel.pair (a, b) VALUES (1, 2.5);")->is_success());

    auto cursor = run(d, "SELECT a + b AS v FROM sel.pair;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 1);
    CHECK(column_count(cursor) == 1);
}

// ------------------------------------------------------------ SELECT x > 1

TEST_CASE("integration::cpp::select_rework::comparison projects a boolean") {
    auto config = make_test_config("/tmp/test_select_rework/comparison");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    for (const char* table : {"i16", "i32", "i64", "f32", "f64", "dec"}) {
        INFO("table " << table);
        auto cursor = run(d, "SELECT x > 1 AS v FROM sel." + std::string(table) + " ORDER BY x;");
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 2);
        REQUIRE(column_count(cursor) == 1);
        CHECK(bool_at(cursor, 0) == false);
        CHECK(bool_at(cursor, 1) == true);
    }
}

TEST_CASE("integration::cpp::select_rework::comparison unifies its operands first") {
    auto config = make_test_config("/tmp/test_select_rework/comparison_mixed");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.pair (a INTEGER, b DOUBLE PRECISION);")->is_success());
    REQUIRE(run(d, "INSERT INTO sel.pair (a, b) VALUES (1, 2.5), (9, 2.5);")->is_success());

    auto cursor = run(d, "SELECT a > b AS v FROM sel.pair ORDER BY a;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
    REQUIRE(column_count(cursor) == 1);
    CHECK(bool_at(cursor, 0) == false);
    CHECK(bool_at(cursor, 1) == true);
}

// ----------------------------------------------------------- SELECT abs(x)

TEST_CASE("integration::cpp::select_rework::row function alone in the target list") {
    auto config = make_test_config("/tmp/test_select_rework/row_function_bare");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT abs(x) FROM sel.i32;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
    // REQUIRE, not CHECK: reading a value below indexes the projected column, so a projection
    // that produced none has to stop the case rather than segfault in the helper.
    REQUIRE(column_count(cursor) == 1);
    std::vector<double> got;
    for (uint64_t row = 0; row < cursor->size(); row++) {
        got.push_back(numeric_at(cursor, row));
    }
    std::sort(got.begin(), got.end());
    CHECK(got == std::vector<double>{2.0, 3.0});
}

TEST_CASE("integration::cpp::select_rework::function over a column in its domain") {
    auto config = make_test_config("/tmp/test_select_rework/function");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    for (const char* table : {"i16", "i32", "i64", "f32", "f64", "dec"}) {
        INFO("table " << table);
        auto cursor = run(d, "SELECT abs(x) AS v FROM sel." + std::string(table) + " ORDER BY x;");
        REQUIRE(cursor->is_success());
        CHECK(cursor->size() == 2);
        CHECK(column_count(cursor) == 1);
    }
}

TEST_CASE("integration::cpp::select_rework::function argument is cast into the domain") {
    auto config = make_test_config("/tmp/test_select_rework/function_cast");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.u (x UBIGINT);")->is_success());

    // NOTE: no row can be put into a UBIGINT column today -- neither VALUES (7) nor
    // VALUES (CAST(7 AS UBIGINT)) has an assignment cast -- so this case stops here until the
    // INSERT work lands. The resolution half of it, that abs over an unsigned argument keeps
    // UBIGINT instead of widening, is covered by dispatcher::resolve_function::abs_unsigned.
    REQUIRE(run(d, "INSERT INTO sel.u (x) VALUES (7);")->is_success());

    auto cursor = run(d, "SELECT abs(x) AS v FROM sel.u;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    REQUIRE(column_count(cursor) == 1);
    // abs over an unsigned argument keeps the argument's own type: it is the identity, not a
    // widening into a signed type wide enough to hold a negation that can never occur.
    CHECK(cursor->chunks().front().data[0].type().type() == components::types::logical_type::UBIGINT);
    CHECK(numeric_at(cursor, 0) == Catch::Approx(7.0));
}

TEST_CASE("integration::cpp::select_rework::function over an operator result") {
    auto config = make_test_config("/tmp/test_select_rework/function_of_operator");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT abs(x + 1) AS v FROM sel.i32 ORDER BY x;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 2);
    CHECK(column_count(cursor) == 1);
}

TEST_CASE("integration::cpp::select_rework::cast on column, on literal and on result together") {
    auto config = make_test_config("/tmp/test_select_rework/all_three");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT CAST(abs(x + 1) AS DOUBLE PRECISION) AS v FROM sel.i16 ORDER BY x;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 2);
    CHECK(column_count(cursor) == 1);
    // Same constant-folding trap as the arithmetic case, one nesting level deeper:
    // abs(-2 + 1) = 1, abs(3 + 1) = 4.
    CHECK(numeric_at(cursor, 0) == 1.0);
    CHECK(numeric_at(cursor, 1) == 4.0);
}

TEST_CASE("integration::cpp::select_rework::several computed columns keep their own slots") {
    auto config = make_test_config("/tmp/test_select_rework/several");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT x, x + 1 AS plus, abs(x) AS magnitude FROM sel.i32 ORDER BY x;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 2);
    CHECK(column_count(cursor) == 3);
}

TEST_CASE("integration::cpp::select_rework::star expands in place among computed columns") {
    auto config = make_test_config("/tmp/test_select_rework/star_mixed");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.pair (a INTEGER, b INTEGER);")->is_success());
    // Every column of the result must hold a DIFFERENT value, or the order cannot be pinned:
    // with b = 2 the computed a + 1 is also 2, and a wrong order reading (a, a+1, b, a, b)
    // produces the very same sequence. b = 20 makes a=1, b=20 and a+1=2 mutually distinct.
    REQUIRE(run(d, "INSERT INTO sel.pair (a, b) VALUES (1, 20);")->is_success());

    auto cursor = run(d, "SELECT *, a + 1 AS plus, * FROM sel.pair;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    REQUIRE(column_count(cursor) == 5);
    const auto& chunk = cursor->chunks().front();
    // Position matters: the computed column sits BETWEEN the two expansions.
    const std::vector<int32_t> expected{1, 20, 2, 1, 20};
    for (size_t column = 0; column < expected.size(); column++) {
        INFO("column " << column);
        CHECK(chunk.data[column].get_value<int32_t>(0) == expected[column]);
    }
    // The names have to follow the same order, not just the values.
    const std::vector<std::string> expected_names{"a", "b", "plus", "a", "b"};
    for (size_t column = 0; column < expected_names.size(); column++) {
        INFO("column " << column);
        REQUIRE(chunk.data[column].type().has_alias());
        CHECK(std::string{chunk.data[column].type().alias()} == expected_names[column]);
    }

    // Stars with NOTHING computed beside them. Validation expands each into its columns, so the
    // projection still computes and is not the star-only passthrough that RETURNING * takes.
    auto two_stars = run(d, "SELECT *, * FROM sel.pair;");
    REQUIRE(two_stars->is_success());
    REQUIRE(two_stars->size() == 1);
    REQUIRE(column_count(two_stars) == 4);
    const auto& doubled = two_stars->chunks().front();
    const std::vector<int32_t> expected_pair{1, 20, 1, 20};
    for (size_t column = 0; column < expected_pair.size(); column++) {
        INFO("column " << column);
        CHECK(doubled.data[column].get_value<int32_t>(0) == expected_pair[column]);
    }
}

TEST_CASE("integration::cpp::select_rework::star inside a call is not expanded") {
    auto config = make_test_config("/tmp/test_select_rework/star_call");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    // ONE column, and one row of it is NULL: count(*) is 3, count(x) is 2. A star expanded into
    // the column list cannot tell those apart.
    REQUIRE(run(d, "CREATE TABLE sel.one (x INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO sel.one (x) VALUES (1), (2), (NULL);")->is_success());

    // count(*) = 3 and count(x) = 2 cannot be asserted yet: a target-list call is now a
    // function_expression_t and the GROUP path still reads only aggregate_expression_t, so any
    // executing aggregate aborts. That is the grouped-path work, not the star's -- what IS pinned
    // here is that validation leaves the star alone inside a call, which the two rejections below
    // can only report because the call still has zero arguments rather than an expanded column.

    // A parameterless aggregate has to be spelled with the star: count() names no input.
    CHECK(run(d, "SELECT count() AS n FROM sel.one;")->is_error());
    // sum has no zero-argument signature at all, so the star form does not resolve either.
    CHECK(run(d, "SELECT sum(*) AS n FROM sel.one;")->is_error());
    // An ordinary zero-argument function is NOT caught by that rule -- only aggregates are.
    CHECK(run(d, "SELECT abs() AS n FROM sel.one;")->is_error());
}

TEST_CASE("integration::cpp::select_rework::bare star is a passthrough") {
    auto config = make_test_config("/tmp/test_select_rework/star_bare");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.pair (a INTEGER, b INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO sel.pair (a, b) VALUES (1, 2);")->is_success());

    auto cursor = run(d, "SELECT * FROM sel.pair;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    REQUIRE(column_count(cursor) == 2);
    CHECK(cursor->chunks().front().data[0].get_value<int32_t>(0) == 1);
    CHECK(cursor->chunks().front().data[1].get_value<int32_t>(0) == 2);
}

// --------------------------------------------------------------------- NULLs

namespace {

    // Same shape as seed(), plus a NULL row per table: -2, 3, NULL.
    void seed_with_nulls(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(run(d, "CREATE DATABASE nul;")->is_success());
        REQUIRE(run(d, "CREATE TABLE nul.i32 (x INTEGER);")->is_success());
        REQUIRE(run(d, "CREATE TABLE nul.f64 (x DOUBLE PRECISION);")->is_success());
        REQUIRE(run(d, "CREATE TABLE nul.dec (x DECIMAL(10,2));")->is_success());
        for (const char* table : {"i32", "f64", "dec"}) {
            REQUIRE(run(d, "INSERT INTO nul." + std::string(table) + " (x) VALUES (-2), (3), (NULL);")->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::select_rework::null propagates through an operator", "[select_rework]") {
    auto config = make_test_config("/tmp/test_select_rework/null_operator");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    for (const char* table : {"i32", "f64", "dec"}) {
        INFO("table " << table);
        auto cursor = run(d, "SELECT x + 1 AS v FROM nul." + std::string(table) + ";");
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 3);
        CHECK(column_count(cursor) == 1);
        // the null row stays null; the other two are computed. No ORDER BY, so check the set.
        size_t nulls = 0;
        std::vector<double> computed;
        for (uint64_t row = 0; row < 3; row++) {
            if (is_null_at(cursor, row)) {
                nulls++;
            } else {
                computed.push_back(numeric_at(cursor, row));
            }
        }
        CHECK(nulls == 1);
        REQUIRE(computed.size() == 2);
        std::sort(computed.begin(), computed.end());
        CHECK(computed[0] == Catch::Approx(-1.0));
        CHECK(computed[1] == Catch::Approx(4.0));
    }
}

TEST_CASE("integration::cpp::select_rework::comparison against null is unknown") {
    auto config = make_test_config("/tmp/test_select_rework/null_comparison");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    auto cursor = run(d, "SELECT x > 1 AS v FROM nul.i32;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 3);
    REQUIRE(column_count(cursor) == 1);
    // -2, 3 and NULL, in no guaranteed order without an ORDER BY: exactly one row is UNKNOWN,
    // and the other two are the honest comparisons. A null read as false gives two false rows.
    size_t nulls = 0;
    size_t trues = 0;
    size_t falses = 0;
    for (uint64_t row = 0; row < cursor->size(); row++) {
        if (is_null_at(cursor, row)) {
            nulls++;
        } else if (bool_at(cursor, row)) {
            trues++;
        } else {
            falses++;
        }
    }
    CHECK(nulls == 1);
    CHECK(trues == 1);
    CHECK(falses == 1);
}

TEST_CASE("integration::cpp::select_rework::null propagates through a function") {
    auto config = make_test_config("/tmp/test_select_rework/null_function");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    for (const char* table : {"i32", "f64", "dec"}) {
        INFO("table " << table);
        auto cursor = run(d, "SELECT abs(x) AS v FROM nul." + std::string(table) + ";");
        REQUIRE(cursor->is_success());
        CHECK(cursor->size() == 3);
        CHECK(column_count(cursor) == 1);
    }
}

TEST_CASE("integration::cpp::select_rework::cast over a null keeps it null") {
    auto config = make_test_config("/tmp/test_select_rework/null_cast");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    auto cursor = run(d, "SELECT CAST(x AS BIGINT) AS v FROM nul.i32;");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 3);
    CHECK(column_count(cursor) == 1);
}

TEST_CASE("integration::cpp::select_rework::untyped null literal adopts a type") {
    auto config = make_test_config("/tmp/test_select_rework/null_literal");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    auto bare = run(d, "SELECT NULL AS v FROM nul.i32;");
    REQUIRE(bare->is_success());
    REQUIRE(bare->size() == 3);
    for (uint64_t row = 0; row < bare->size(); row++) {
        CHECK(is_null_at(bare, row));
    }

    // Meeting a typed operand, the literal takes that operand's type and the result is null --
    // EVERY row, including the ones where x itself is not null. A literal that adopted nothing
    // and defaulted to text would not have resolved the operator at all.
    auto in_operator = run(d, "SELECT x + NULL AS v FROM nul.i32;");
    REQUIRE(in_operator->is_success());
    REQUIRE(in_operator->size() == 3);
    REQUIRE(column_count(in_operator) == 1);
    CHECK(in_operator->chunks().front().data[0].type().type() == components::types::logical_type::INTEGER);
    for (uint64_t row = 0; row < in_operator->size(); row++) {
        CHECK(is_null_at(in_operator, row));
    }

    // Spelled with a target type it is simply a typed null.
    auto typed = run(d, "SELECT CAST(NULL AS INTEGER) AS v FROM nul.i32;");
    REQUIRE(typed->is_success());
    REQUIRE(typed->size() == 3);
    for (uint64_t row = 0; row < typed->size(); row++) {
        CHECK(is_null_at(typed, row));
    }
}

// ------------------------------------------------------- cast versus try_cast

TEST_CASE("integration::cpp::select_rework::cast fails hard on a value that does not fit") {
    auto config = make_test_config("/tmp/test_select_rework/cast_overflow");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE cst;")->is_success());
    REQUIRE(run(d, "CREATE TABLE cst.t (x INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO cst.t (x) VALUES (100000), (1);")->is_success());

    auto cursor = run(d, "SELECT CAST(x AS SMALLINT) AS v FROM cst.t;");
    CHECK(cursor->is_error());
}

TEST_CASE("integration::cpp::select_rework::cast of an unparsable string fails") {
    auto config = make_test_config("/tmp/test_select_rework/cast_parse");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE cst;")->is_success());
    REQUIRE(run(d, "CREATE TABLE cst.s (x TEXT);")->is_success());
    REQUIRE(run(d, "INSERT INTO cst.s (x) VALUES ('42'), ('abc');")->is_success());

    // string -> number is explicit-only, so it has to be spelled; '42' converts, 'abc' does not.
    auto good = run(d, "SELECT CAST(x AS BIGINT) AS v FROM cst.s WHERE x = '42';");
    REQUIRE(good->is_success());
    REQUIRE(good->size() == 1);
    REQUIRE(column_count(good) == 1);
    // The conversion has to have actually HAPPENED: a cast that only annotated the reference
    // hands back the TEXT column unchanged, which succeeds and reads as the wrong type.
    CHECK(good->chunks().front().data[0].type().type() == components::types::logical_type::BIGINT);
    CHECK(numeric_at(good, 0) == Catch::Approx(42.0));

    auto bad = run(d, "SELECT CAST(x AS BIGINT) AS v FROM cst.s;");
    CHECK(bad->is_error());
}

TEST_CASE("integration::cpp::select_rework::try_cast nulls the failing rows") {
    auto config = make_test_config("/tmp/test_select_rework/try_cast");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE cst;")->is_success());
    REQUIRE(run(d, "CREATE TABLE cst.t (x INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO cst.t (x) VALUES (100000), (1);")->is_success());

    auto cursor = run(d, "SELECT TRY_CAST(x AS SMALLINT) AS v FROM cst.t ORDER BY x;");
    REQUIRE(cursor->is_success());
    // Both rows survive: 1 converts, 100000 becomes null.
    REQUIRE(cursor->size() == 2);
    REQUIRE(column_count(cursor) == 1);
    // The column really is the NARROWER type -- a try_cast that gave up and passed the INTEGER
    // through would satisfy every count above.
    CHECK(cursor->chunks().front().data[0].type().type() == components::types::logical_type::SMALLINT);
    // ORDER BY x, so row 0 is 1 (converts) and row 1 is 100000 (does not fit).
    REQUIRE(!is_null_at(cursor, 0));
    CHECK(numeric_at(cursor, 0) == Catch::Approx(1.0));
    CHECK(is_null_at(cursor, 1));
}

TEST_CASE("integration::cpp::select_rework::try_cast over unparsable strings") {
    auto config = make_test_config("/tmp/test_select_rework/try_cast_parse");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE cst;")->is_success());
    REQUIRE(run(d, "CREATE TABLE cst.s (x TEXT);")->is_success());
    REQUIRE(run(d, "INSERT INTO cst.s (x) VALUES ('42'), ('abc');")->is_success());

    auto cursor = run(d, "SELECT TRY_CAST(x AS BIGINT) AS v FROM cst.s;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
    REQUIRE(column_count(cursor) == 1);
    CHECK(cursor->chunks().front().data[0].type().type() == components::types::logical_type::BIGINT);
    // '42' parses, 'abc' does not; row order is not fixed here, so count the two outcomes.
    size_t nulls = 0;
    size_t converted = 0;
    for (uint64_t row = 0; row < cursor->size(); row++) {
        if (is_null_at(cursor, row)) {
            nulls++;
        } else {
            converted++;
            CHECK(numeric_at(cursor, row) == Catch::Approx(42.0));
        }
    }
    CHECK(nulls == 1);
    CHECK(converted == 1);
}

// ------------------------------------------------------------------- WHERE

TEST_CASE("integration::cpp::select_rework::where comparison filters rows", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_cmp");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    for (const char* table : {"i16", "i32", "i64", "f32", "f64", "dec"}) {
        INFO("table " << table);
        auto cursor = run(d, "SELECT x FROM sel." + std::string(table) + " WHERE x > 0;");
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        CHECK(numeric_at(cursor, 0) == Catch::Approx(3.0));
    }
}

TEST_CASE("integration::cpp::select_rework::where and or not compose", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_logic");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto both = run(d, "SELECT x FROM sel.i32 WHERE x > -5 AND x < 0;");
    REQUIRE(both->is_success());
    REQUIRE(both->size() == 1);
    CHECK(numeric_at(both, 0) == Catch::Approx(-2.0));

    auto either = run(d, "SELECT x FROM sel.i32 WHERE x < -1 OR x > 1 ORDER BY x;");
    REQUIRE(either->is_success());
    REQUIRE(either->size() == 2);
    CHECK(numeric_at(either, 0) == Catch::Approx(-2.0));
    CHECK(numeric_at(either, 1) == Catch::Approx(3.0));

    auto negated = run(d, "SELECT x FROM sel.i32 WHERE NOT (x > 0);");
    REQUIRE(negated->is_success());
    REQUIRE(negated->size() == 1);
    CHECK(numeric_at(negated, 0) == Catch::Approx(-2.0));
}

TEST_CASE("integration::cpp::select_rework::where null row does not survive", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_null");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    auto positive = run(d, "SELECT x FROM nul.i32 WHERE x > 0;");
    REQUIRE(positive->is_success());
    REQUIRE(positive->size() == 1);
    CHECK(numeric_at(positive, 0) == Catch::Approx(3.0));

    auto negated = run(d, "SELECT x FROM nul.i32 WHERE NOT (x > 0);");
    REQUIRE(negated->is_success());
    REQUIRE(negated->size() == 1);
    CHECK(numeric_at(negated, 0) == Catch::Approx(-2.0));
}

TEST_CASE("integration::cpp::select_rework::where is null and is not null", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_isnull");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    auto nulls = run(d, "SELECT x FROM nul.i32 WHERE x IS NULL;");
    REQUIRE(nulls->is_success());
    REQUIRE(nulls->size() == 1);
    CHECK(is_null_at(nulls, 0));

    auto present = run(d, "SELECT x FROM nul.i32 WHERE x IS NOT NULL ORDER BY x;");
    REQUIRE(present->is_success());
    REQUIRE(present->size() == 2);
    CHECK(numeric_at(present, 0) == Catch::Approx(-2.0));
    CHECK(numeric_at(present, 1) == Catch::Approx(3.0));
}

TEST_CASE("integration::cpp::select_rework::where unifies its operands", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_mixed");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.pair (a INTEGER, b DOUBLE PRECISION);")->is_success());
    REQUIRE(run(d, "INSERT INTO sel.pair (a, b) VALUES (1, 2.5), (9, 2.5);")->is_success());

    auto cursor = run(d, "SELECT a FROM sel.pair WHERE a > b;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    CHECK(numeric_at(cursor, 0) == Catch::Approx(9.0));
}

TEST_CASE("integration::cpp::select_rework::where over a computed operand", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_computed");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT x FROM sel.i32 WHERE x + 1 > 0;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    CHECK(numeric_at(cursor, 0) == Catch::Approx(3.0));
}

TEST_CASE("integration::cpp::select_rework::where like filters", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_like");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE sel;")->is_success());
    REQUIRE(run(d, "CREATE TABLE sel.s (x TEXT);")->is_success());
    REQUIRE(run(d, "INSERT INTO sel.s (x) VALUES ('alpha'), ('beta');")->is_success());

    auto cursor = run(d, "SELECT x FROM sel.s WHERE x LIKE 'al%';");
    REQUIRE(cursor->is_success());
    CHECK(cursor->size() == 1);
}

TEST_CASE("integration::cpp::select_rework::where in filters", "[.pushdown_filter]") {
    auto config = make_test_config("/tmp/test_select_rework/where_in");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cursor = run(d, "SELECT x FROM sel.i32 WHERE x IN (3, 7);");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    CHECK(numeric_at(cursor, 0) == Catch::Approx(3.0));
}

// ------------------------------------------------------- GROUP BY / HAVING

TEST_CASE("integration::cpp::select_rework::scalar aggregate over all rows") {
    auto config = make_test_config("/tmp/test_select_rework/agg_scalar");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_with_nulls(d);

    // -2, 3, NULL: count(*) counts ROWS, count(x) counts non-null values.
    auto rows = run(d, "SELECT count(*) AS n FROM nul.i32;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 1);
    CHECK(numeric_at(rows, 0) == Catch::Approx(3.0));

    auto values = run(d, "SELECT count(x) AS n FROM nul.i32;");
    REQUIRE(values->is_success());
    REQUIRE(values->size() == 1);
    CHECK(numeric_at(values, 0) == Catch::Approx(2.0));

    auto total = run(d, "SELECT sum(x) AS s FROM nul.i32;");
    REQUIRE(total->is_success());
    REQUIRE(total->size() == 1);
    CHECK(numeric_at(total, 0) == Catch::Approx(1.0));
}

TEST_CASE("integration::cpp::select_rework::group by a key") {
    auto config = make_test_config("/tmp/test_select_rework/agg_group");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE grp;")->is_success());
    REQUIRE(run(d, "CREATE TABLE grp.t (k INTEGER, v INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO grp.t (k, v) VALUES (1, 10), (1, 20), (2, 5);")->is_success());

    auto cursor = run(d, "SELECT k, sum(v) AS s FROM grp.t GROUP BY k ORDER BY k;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
    REQUIRE(column_count(cursor) == 2);
    const auto& chunk = cursor->chunks().front();
    CHECK(chunk.data[0].get_value<int32_t>(0) == 1);
    CHECK(chunk.data[0].get_value<int32_t>(1) == 2);
    // 10 + 20 = 30 for k=1, 5 for k=2. sum() is registered with same_type_resolver(0), so it
    // returns the ARGUMENT's type -- INTEGER here, not PostgreSQL's widened BIGINT.
    CHECK(chunk.data[1].type().type() == components::types::logical_type::INTEGER);
    CHECK(chunk.data[1].get_value<int32_t>(0) == 30);
    CHECK(chunk.data[1].get_value<int32_t>(1) == 5);
}

TEST_CASE("integration::cpp::select_rework::having filters groups") {
    auto config = make_test_config("/tmp/test_select_rework/agg_having");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE grp;")->is_success());
    REQUIRE(run(d, "CREATE TABLE grp.t (k INTEGER, v INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO grp.t (k, v) VALUES (1, 10), (1, 20), (2, 5);")->is_success());

    auto cursor = run(d, "SELECT k, sum(v) AS s FROM grp.t GROUP BY k HAVING sum(v) > 10;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    REQUIRE(column_count(cursor) == 2);
    CHECK(cursor->chunks().front().data[0].get_value<int32_t>(0) == 1);
    // sum() returns the argument's type (same_type_resolver), so INTEGER in and INTEGER out.
    CHECK(cursor->chunks().front().data[1].get_value<int32_t>(0) == 30);
}

TEST_CASE("integration::cpp::select_rework::grouped operator over a call and a cast") {
    auto config = make_test_config("/tmp/test_select_rework/agg_operand_kinds");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE grp;")->is_success());
    REQUIRE(run(d, "CREATE TABLE grp.t (k INTEGER, v INTEGER);")->is_success());
    REQUIRE(run(d, "INSERT INTO grp.t (k, v) VALUES (-1, 10), (-1, 20), (2, 5);")->is_success());

    // A call under the operator, over a grouping key.
    auto call_operand = run(d, "SELECT abs(k) + 1 AS a FROM grp.t GROUP BY k ORDER BY k;");
    REQUIRE(call_operand->is_success());
    REQUIRE(call_operand->size() == 2);
    CHECK(numeric_at(call_operand, 0) == Catch::Approx(2.0));
    CHECK(numeric_at(call_operand, 1) == Catch::Approx(3.0));

    // A cast spelled over the key, under the operator.
    auto cast_operand = run(d, "SELECT k::BIGINT + 1 AS a FROM grp.t GROUP BY k ORDER BY k;");
    REQUIRE(cast_operand->is_success());
    REQUIRE(cast_operand->size() == 2);
    CHECK(numeric_at(cast_operand, 0) == Catch::Approx(0.0));
    CHECK(numeric_at(cast_operand, 1) == Catch::Approx(3.0));

    // A call over the AGGREGATE's result: the marker is the operand, and the call around it is
    // still a row-shaped operation on one value per group.
    auto call_over_aggregate = run(d, "SELECT abs(sum(v)) + 1 AS a FROM grp.t GROUP BY k ORDER BY k;");
    REQUIRE(call_over_aggregate->is_success());
    REQUIRE(call_over_aggregate->size() == 2);
    CHECK(numeric_at(call_over_aggregate, 0) == Catch::Approx(31.0));
    CHECK(numeric_at(call_over_aggregate, 1) == Catch::Approx(6.0));
}

TEST_CASE("integration::cpp::select_rework::large groups span several chunks") {
    auto config = make_test_config("/tmp/test_select_rework/agg_large_groups");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE big;")->is_success());
    REQUIRE(run(d, "CREATE TABLE big.t (k INTEGER, v INTEGER);")->is_success());

    // 5000 rows over two groups, several times DEFAULT_VECTOR_CAPACITY (1024) so both groups
    // span multiple chunks. k = 0 takes the even i, k = 1 the odd.
    constexpr int row_count = 5000;
    std::string values;
    for (int i = 0; i < row_count; ++i) {
        values += (i ? ", (" : "(") + std::to_string(i % 2) + ", " + std::to_string(i) + ")";
    }
    REQUIRE(run(d, "INSERT INTO big.t (k, v) VALUES " + values + ";")->is_success());

    // Every group's row count, not just its first chunk's.
    auto counts = run(d, "SELECT count(*) AS n FROM big.t GROUP BY k ORDER BY k;");
    REQUIRE(counts->is_success());
    REQUIRE(counts->size() == 2);
    CHECK(numeric_at(counts, 0) == Catch::Approx(2500.0));
    CHECK(numeric_at(counts, 1) == Catch::Approx(2500.0));

    // sum over every row of the group: evens 0+2+...+4998, odds 1+3+...+4999.
    auto sums = run(d, "SELECT sum(v) AS s FROM big.t GROUP BY k ORDER BY k;");
    REQUIRE(sums->is_success());
    REQUIRE(sums->size() == 2);
    CHECK(numeric_at(sums, 0) == Catch::Approx(6247500.0));
    CHECK(numeric_at(sums, 1) == Catch::Approx(6250000.0));

    // min/max must come from the whole group, so both live outside the first chunk.
    auto extremes = run(d, "SELECT min(v) AS lo, max(v) AS hi FROM big.t GROUP BY k ORDER BY k;");
    REQUIRE(extremes->is_success());
    REQUIRE(extremes->size() == 2);
    const auto& extreme_rows = extremes->chunks().front();
    CHECK(extreme_rows.data[0].get_value<int32_t>(0) == 0);
    CHECK(extreme_rows.data[1].get_value<int32_t>(0) == 4998);
    CHECK(extreme_rows.data[0].get_value<int32_t>(1) == 1);
    CHECK(extreme_rows.data[1].get_value<int32_t>(1) == 4999);

    // The scalar case: one group holding every row.
    auto scalar = run(d, "SELECT count(*) AS n FROM big.t;");
    REQUIRE(scalar->is_success());
    REQUIRE(scalar->size() == 1);
    CHECK(numeric_at(scalar, 0) == Catch::Approx(static_cast<double>(row_count)));

    // An operation AROUND the reduction must apply to the merged value, not to a per-chunk
    // partial -- the whole reason nodes above the reduction run once, after it.
    auto around = run(d, "SELECT sum(v) * 2 AS s FROM big.t GROUP BY k ORDER BY k;");
    REQUIRE(around->is_success());
    REQUIRE(around->size() == 2);
    CHECK(numeric_at(around, 0) == Catch::Approx(12495000.0));
    CHECK(numeric_at(around, 1) == Catch::Approx(12500000.0));
}
