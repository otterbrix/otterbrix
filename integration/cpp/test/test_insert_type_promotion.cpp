#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/sql/transformer/utils.hpp>
#include <cstdint>
#include <limits>
#include <string>

namespace {
    // column_index answers a result_wrapper_t, not a SIZE_MAX sentinel behind an assert: an
    // embedder asking for a missing column gets a refusal. Unwrap loudly, as example/cpp/main.cpp
    // does at column_of.
    inline uint64_t column_of(const components::cursor::cursor_t_ptr& c, std::string_view name) {
        auto idx = c->column_index(name);
        REQUIRE_FALSE(idx.has_error());
        return idx.value();
    }
} // namespace


// promote_column rebuilds a whole column, cell by cell through logical_value_t, every time a
// later row widens that column's type — the shape usually called quadratic promotion. A workload
// with one type per column never enters this path at all.
//
// This is a counter test, not a timing test: it says what the growth IS rather than how long it
// took on a busy machine. If the cost were quadratic in the row count, doubling the rows would
// roughly quadruple the rewrites. If it is bounded by the type lattice (int -> bigint -> double
// is only two steps), doubling the rows merely doubles them.
namespace {
    // WORST CASE on purpose: the widening literal comes LAST, so every row already parsed has
    // to be rewritten. Putting it early (the obvious way to write this test) measures nothing —
    // the column reaches its widest type on row 3 and never widens again.
    std::string late_widening_values(int rows) {
        std::string sql = "INSERT INTO p.t (id, v) VALUES ";
        for (int i = 0; i < rows; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", ";
            sql += (i == rows - 1) ? (std::to_string(i) + ".5") : std::to_string(i);
            sql += ")";
        }
        return sql + ";";
    }
} // namespace

TEST_CASE("integration::cpp::test_insert_type_promotion::growth_is_not_quadratic") {
    auto config = test_create_config(integration_fixture_path("test_insert_type_promotion/growth"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE p;")->is_success());
    REQUIRE(exec("CREATE TABLE p.t (id bigint, v double);")->is_success());

    auto rewrites_for = [&](int rows) {
        components::sql::transform::reset_insert_promote_rows();
        REQUIRE(exec(late_widening_values(rows))->is_success());
        return components::sql::transform::insert_promote_rows();
    };

    const auto small = rewrites_for(300);
    const auto large = rewrites_for(600);

    INFO("rows rewritten by promote_column (widening literal last): 300 values -> " << small << ", 600 values -> "
                                                                                    << large);
    // Doubling the rows must not more than triple the rewrites. Quadratic growth would be ~4x;
    // the bound leaves room for the lattice-climb constant without admitting N^2.
    CHECK(large <= small * 3 + 16);
}

namespace {
    // Store one literal in a table of its own and read it back. A computing table declares
    // no column, so nothing but the literal decides what is stored; a single row means no
    // sibling value can widen it afterwards.
    components::cursor::cursor_t_ptr
    store_literal(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table, const std::string& literal) {
        auto run = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            auto cursor = dispatcher->execute_sql(session, sql);
            INFO("statement: " << sql);
            INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "none"));
            REQUIRE(cursor->is_success());
            return cursor;
        };
        run("CREATE TABLE " + table + " ();");
        run("INSERT INTO " + table + " (v) VALUES (" + literal + ");");
        return run("SELECT v FROM " + table + ";");
    }

    // A literal within 64 bits is a BIGINT worth exactly what was written. The literal text
    // is derived from the expected value, so the two cannot drift apart.
    void check_literal(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table, int64_t expected) {
        auto cursor = store_literal(dispatcher, table, std::to_string(expected));
        INFO("literal " << expected);
        REQUIRE(cursor->size() == 1);
        const auto value = cursor->value(column_of(cursor, "v"), 0);
        CHECK(value.type().type() == components::types::logical_type::BIGINT);
        CHECK(value.value<int64_t>() == expected);
    }

    // Past 64 bits there is no BIGINT to hold the value, so it takes the next integer type
    // that can. `neighbour` is the literal one less: matching it would mean the digits were
    // not all kept, and it pins the value without needing a 128-bit read.
    void check_wide_literal(otterbrix::wrapper_dispatcher_t* dispatcher,
                            const std::string& table,
                            const std::string& literal,
                            const std::string& neighbour,
                            components::types::logical_type expected_type) {
        auto cursor = store_literal(dispatcher, table, literal);
        INFO("literal " << literal);
        REQUIRE(cursor->size() == 1);
        CHECK(cursor->value(column_of(cursor, "v"), 0).type().type() == expected_type);

        auto session = otterbrix::session_id_t();
        auto exact = dispatcher->execute_sql(session, "SELECT v FROM " + table + " WHERE v = " + literal + ";");
        REQUIRE(exact->is_success());
        CHECK(exact->size() == 1);
        auto off_by_one = dispatcher->execute_sql(session, "SELECT v FROM " + table + " WHERE v = " + neighbour + ";");
        REQUIRE(off_by_one->is_success());
        CHECK(off_by_one->size() == 0);
    }
} // namespace

// An integer literal is worth exactly what is written, at every width. The grammar carries
// values up to INT32_MAX in a token of its own and hands anything wider on as text, so the
// boundaries either side of that seam are where a literal is most likely to be re-read at
// the wrong width — as are the ones either side of the widest integer type.
TEST_CASE("integration::cpp::test_insert_type_promotion::integer_literals_keep_their_value") {
    auto config = test_create_config(integration_fixture_path("test_insert_type_promotion/literal_values"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE p;")->is_success());
    }

    SECTION("values a BIGINT can hold") {
        check_literal(dispatcher, "p.zero", 0);
        check_literal(dispatcher, "p.minus_one", -1);

        // Either side of each narrower integer type's range. A literal is a BIGINT
        // throughout — these are simply the widths it could wrongly be re-read at.
        check_literal(dispatcher, "p.int8_max", std::numeric_limits<std::int8_t>::max());
        check_literal(dispatcher, "p.past_int8", std::int64_t{std::numeric_limits<std::int8_t>::max()} + 1);
        check_literal(dispatcher, "p.uint8_max", std::numeric_limits<std::uint8_t>::max());
        check_literal(dispatcher, "p.past_uint8", std::int64_t{std::numeric_limits<std::uint8_t>::max()} + 1);
        check_literal(dispatcher, "p.int16_max", std::numeric_limits<std::int16_t>::max());
        check_literal(dispatcher, "p.past_int16", std::int64_t{std::numeric_limits<std::int16_t>::max()} + 1);

        // The seam: the last value the grammar carries as a token, and the first it cannot.
        check_literal(dispatcher, "p.int32_max", std::numeric_limits<std::int32_t>::max());
        check_literal(dispatcher, "p.past_int32", std::int64_t{std::numeric_limits<std::int32_t>::max()} + 1);
        check_literal(dispatcher, "p.negative_past_int32", -std::int64_t{std::numeric_limits<std::int32_t>::max()} - 1);

        check_literal(dispatcher, "p.int64_max", std::numeric_limits<std::int64_t>::max());
        check_literal(dispatcher, "p.int64_min", std::numeric_limits<std::int64_t>::min());
    }

    SECTION("values past what a BIGINT can hold") {
        // Each rung of the ladder with the literal one below it: past INT64_MAX the value
        // is a HUGEINT, and past INT128_MAX only an unsigned 128-bit type is left.
        check_wide_literal(dispatcher,
                           "p.past_int64",
                           "9223372036854775808",
                           "9223372036854775807",
                           components::types::logical_type::HUGEINT);
        check_wide_literal(dispatcher,
                           "p.int128_max",
                           "170141183460469231731687303715884105727",
                           "170141183460469231731687303715884105726",
                           components::types::logical_type::HUGEINT);
        // The neighbour here is the value ABOVE, keeping both sides of the probe unsigned.
        // One below would cross into HUGEINT, where the two now meet at the signed type and
        // a value this large has nowhere to go — a comparison question, not a parsing one.
        check_wide_literal(dispatcher,
                           "p.past_int128",
                           "170141183460469231731687303715884105728",
                           "170141183460469231731687303715884105729",
                           components::types::logical_type::UHUGEINT);
        check_wide_literal(dispatcher,
                           "p.uint128_max",
                           "340282366920938463463374607431768211455",
                           "340282366920938463463374607431768211454",
                           components::types::logical_type::UHUGEINT);
    }
}

// A column has a range, and a value outside it is refused — never folded into range and
// stored as some other number.
TEST_CASE("integration::cpp::test_insert_type_promotion::literal_outside_the_column_range_is_rejected") {
    auto config = test_create_config(integration_fixture_path("test_insert_type_promotion/column_range"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE p;")->is_success());

    // One column of the declared type, one INSERT; the cursor is the answer under test.
    auto insert_into = [&](const std::string& table, const std::string& declared, const std::string& literal) {
        REQUIRE(exec("CREATE TABLE " + table + " (v " + declared + ");")->is_success());
        return exec("INSERT INTO " + table + " (v) VALUES (" + literal + ");");
    };
    auto rows_in = [&](const std::string& table) {
        auto cursor = exec("SELECT v FROM " + table + ";");
        REQUIRE(cursor->is_success());
        return cursor->size();
    };

    // Each integer type with the four literals that define its edges: the two it must
    // hold, and the two just outside it. Written out rather than computed, so a row can be
    // read against the type's documented range.
    struct integer_range_t {
        std::string declared;
        std::string min;
        std::string max;
        std::string under_min;
        std::string over_max;
    };
    const std::vector<integer_range_t> ranges = {
        {"tinyint", "-128", "127", "-129", "128"},
        {"utinyint", "0", "255", "-1", "256"},
        {"smallint", "-32768", "32767", "-32769", "32768"},
        {"usmallint", "0", "65535", "-1", "65536"},
        {"integer", "-2147483648", "2147483647", "-2147483649", "2147483648"},
        {"uinteger", "0", "4294967295", "-1", "4294967296"},
        {"bigint", "-9223372036854775808", "9223372036854775807", "-9223372036854775809", "9223372036854775808"},
        {"ubigint", "0", "18446744073709551615", "-1", "18446744073709551616"},
        {"hugeint",
         "-170141183460469231731687303715884105728",
         "170141183460469231731687303715884105727",
         "-170141183460469231731687303715884105729",
         "170141183460469231731687303715884105728"},
        {"uhugeint", "0", "340282366920938463463374607431768211455", "-1", "340282366920938463463374607431768211456"},
    };

    for (const auto& range : ranges) {
        INFO("column type " << range.declared);

        CHECK(insert_into("p." + range.declared + "_at_min", range.declared, range.min)->is_success());
        CHECK(insert_into("p." + range.declared + "_at_max", range.declared, range.max)->is_success());

        // Outside the range the value has nowhere to go, so the statement must fail and
        // the table stay empty — a stored row would mean it was folded into range.
        CHECK(insert_into("p." + range.declared + "_under", range.declared, range.under_min)->is_error());
        CHECK(rows_in("p." + range.declared + "_under") == 0);
        CHECK(insert_into("p." + range.declared + "_over", range.declared, range.over_max)->is_error());
        CHECK(rows_in("p." + range.declared + "_over") == 0);
    }
}
