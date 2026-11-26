#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/vector_builders.hpp>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace {
    // column_index answers a result_wrapper_t, not a SIZE_MAX behind an assert — unwrap loudly,
    // as example/cpp/main.cpp's column_of does.
    inline uint64_t column_of(const components::cursor::cursor_t_ptr& c, std::string_view name) {
        auto idx = c->column_index(name);
        REQUIRE_FALSE(idx.has_error());
        return idx.value();
    }
} // namespace

// promote_column rebuilds a whole column, cell by cell, whenever a later
// row widens that column's type (the shape usually called quadratic promotion); a workload with
// one type per column never enters this path.
//
// This is a counter test, not a timing test: it measures the growth shape rather than how long it
// took on a busy machine. Quadratic growth would roughly quadruple the rewrites when the rows
// double; growth bounded by the type lattice (int -> bigint -> double is two steps) would merely
// double them.
namespace {
    // Worst case on purpose: the widening literal comes last, so every row already parsed has
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
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE SCHEMA p;")->is_success());
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
    // The threshold leaves slack for the lattice-climb constant (+16) without admitting true
    // quadratic growth, which would show roughly 4x here.
    CHECK(large <= small * 3 + 16);
}

namespace {
    // A computing table declares no column, so nothing but the literal decides what is stored;
    // a single row means no sibling value can widen it afterwards.
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
        CHECK(cursor->type_data()[column_of(cursor, "v")].type() == components::types::logical_type::BIGINT);
        auto cell5 = cursor->get<int64_t>(column_of(cursor, "v"), 0);
        REQUIRE_FALSE(cell5.has_error());
        CHECK(cell5.value() == expected);
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
        CHECK(cursor->type_data()[column_of(cursor, "v")].type() == expected_type);
        auto cell6 = cursor->is_null(column_of(cursor, "v"), 0);
        REQUIRE_FALSE(cell6.has_error());
        CHECK_FALSE(cell6.value());

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
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA p;")->is_success());
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
        // The neighbour here is the value above, keeping both sides of the probe unsigned.
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
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE SCHEMA p;")->is_success());

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

        // The statement must fail and the table stay empty; a stored row would mean the value was
        // folded into range instead of refused.
        CHECK(insert_into("p." + range.declared + "_under", range.declared, range.under_min)->is_error());
        CHECK(rows_in("p." + range.declared + "_under") == 0);
        CHECK(insert_into("p." + range.declared + "_over", range.declared, range.over_max)->is_error());
        CHECK(rows_in("p." + range.declared + "_over") == 0);
    }
}

TEST_CASE("integration::cpp::test_insert_type_promotion::string_literals_assign_to_typed_columns") {
    auto config = test_create_config(integration_fixture_path("test_insert_type_promotion/string_literals"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE SCHEMA sl;")->is_success());
    REQUIRE(exec("CREATE TABLE sl.t (d date, n bigint, amount decimal(10,2), flag boolean);")->is_success());
    REQUIRE(exec("INSERT INTO sl.t (d, n, amount, flag) VALUES ('2020-01-15', '42', '1.50', 'true');")->is_success());

    auto stored = exec("SELECT n, flag FROM sl.t;");
    REQUIRE(stored->is_success());
    REQUIRE(stored->size() == 1);
    auto cell0 = stored->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    REQUIRE(cell0.value() == 42);
    auto cell1 = stored->get<bool>(1, 0);
    REQUIRE_FALSE(cell1.has_error());
    REQUIRE(cell1.value());

    // The date and the decimal kept their values, not merely a non-null cell.
    auto matched = exec("SELECT n FROM sl.t WHERE d = '2020-01-15'::date AND amount = 1.50;");
    REQUIRE(matched->is_success());
    REQUIRE(matched->size() == 1);

    REQUIRE(exec("INSERT INTO sl.t (d) VALUES ('not-a-date');")->is_error());
    REQUIRE(exec("INSERT INTO sl.t (n) VALUES ('not-a-number');")->is_error());
}

TEST_CASE("integration::cpp::test_insert_type_promotion::boolean_column_takes_literal_spellings") {
    auto config = test_create_config(integration_fixture_path("test_insert_type_promotion/boolean"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE SCHEMA bl;")->is_success());
    REQUIRE(exec("CREATE TABLE bl.t (id bigint, flag boolean);")->is_success());
    REQUIRE(exec("INSERT INTO bl.t (id, flag) VALUES (1, 'true'), (2, 'false');")->is_success());
    REQUIRE(exec("INSERT INTO bl.t (id, flag) VALUES (3, true);")->is_success());
    // An integer is cast on assignment, as a MySQL client writes a BOOLEAN as 1 / 0.
    REQUIRE(exec("INSERT INTO bl.t (id, flag) VALUES (4, 0);")->is_success());

    auto stored = exec("SELECT flag FROM bl.t ORDER BY id;");
    REQUIRE(stored->is_success());
    REQUIRE(stored->size() == 4);
    auto cell2 = stored->get<bool>(0, 0);
    REQUIRE_FALSE(cell2.has_error());
    REQUIRE(cell2.value());
    auto cell3 = stored->get<bool>(0, 1);
    REQUIRE_FALSE(cell3.has_error());
    REQUIRE_FALSE(cell3.value());
    auto cell4 = stored->get<bool>(0, 2);
    REQUIRE_FALSE(cell4.has_error());
    REQUIRE(cell4.value());
    auto cell5 = stored->get<bool>(0, 3);
    REQUIRE_FALSE(cell5.has_error());
    REQUIRE_FALSE(cell5.value());
}

namespace {
    // `lead` rows of `item` (an %d in it is the row number), then the `tail` rows.
    std::string values_list(const std::string& item, int lead, const std::vector<std::string>& tail) {
        std::string sql;
        for (int row = 0; row < lead; ++row) {
            std::string cell = item;
            if (auto at = cell.find("%d"); at != std::string::npos) {
                cell.replace(at, 2, std::to_string(row));
            }
            sql += (row == 0 ? "(" : ", (") + cell + ")";
        }
        for (const auto& cell : tail) {
            sql += ", (" + cell + ")";
        }
        return sql;
    }
} // namespace

// INSERT ... VALUES is split into chunks of 1024 rows; a column has one type over the whole list, so the
// rows past the first chunk are stored as the same rows are inside one chunk.
TEST_CASE("integration::cpp::test_insert_type_promotion::values_across_chunks") {
    auto config = test_create_config(integration_fixture_path("test_insert_type_promotion/values_across_chunks"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session, sql);
        INFO("statement: " << sql.substr(0, 120));
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "none"));
        REQUIRE(cursor->is_success());
        return cursor;
    };
    auto number = [&](const std::string& sql, uint64_t column = 0) {
        auto cursor = exec(sql);
        REQUIRE(cursor->size() == 1);
        auto cell = cursor->get<int64_t>(column, 0);
        REQUIRE_FALSE(cell.has_error());
        return cell.value();
    };
    REQUIRE(exec("CREATE SCHEMA c;")->is_success());

    for (const int lead : {1023, 1024, 2048}) {
        INFO("rows before the tail: " << lead);
        const std::string t = "c.t" + std::to_string(lead);

        DYNAMIC_SECTION(lead << " rows, " <<"integers, then a fraction, into double precision") {
            exec("CREATE TABLE " + t + " (x double precision);");
            exec("INSERT INTO " + t + " VALUES " + values_list("%d", lead, {"2.5"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = 2.5;") == 1);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"NULLs, then values") {
            for (const std::string type : {"integer", "text", "decimal(10,2)", "date"}) {
                INFO("column type: " << type);
                const std::string first = type == "date" ? "DATE '2024-01-02'" : type == "text" ? "'v'" : "1";
                exec("CREATE TABLE " + t + " (x " + type + ");");
                exec("INSERT INTO " + t + " VALUES " + values_list("NULL", lead, {first, first}) + ";");
                CHECK(number("SELECT count(*) FROM " + t + ";") == lead + 2);
                CHECK(number("SELECT count(x) FROM " + t + ";") == 2);
                exec("DROP TABLE " + t + ";");
            }
        }

        DYNAMIC_SECTION(lead << " rows, " <<"two columns, one of which widens") {
            exec("CREATE TABLE " + t + " (a bigint, b double precision);");
            exec("INSERT INTO " + t + " VALUES " + values_list("%d, 0", lead, {std::to_string(lead) + ", 0.1"}) +
                 ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE a = " + std::to_string(lead) + " AND b = 0.1;") == 1);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE a = 7 AND b = 0;") == 1);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"fractions, then an integer, into integer") {
            exec("CREATE TABLE " + t + " (x integer);");
            exec("INSERT INTO " + t + " VALUES " + values_list("2.0", lead, {"7"}) + ";");
            CHECK(number("SELECT min(x) FROM " + t + ";") == 2);
            CHECK(number("SELECT max(x) FROM " + t + ";") == 7);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"fractions, then an integer, into decimal") {
            exec("CREATE TABLE " + t + " (x decimal(10,2));");
            exec("INSERT INTO " + t + " VALUES " + values_list("1.25", lead, {"7"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = 7;") == 1);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = 1.25;") == lead);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"integers, then a typed decimal") {
            exec("CREATE TABLE " + t + " (x decimal(10,2));");
            exec("INSERT INTO " + t + " VALUES " + values_list("1", lead, {"CAST('2.50' AS DECIMAL(10,2))"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = 2.5;") == 1);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"integers, then a string, into text") {
            exec("CREATE TABLE " + t + " (x text);");
            exec("INSERT INTO " + t + " VALUES " + values_list("%d", lead, {"'abc'"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = 'abc';") == 1);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = '7';") == 1);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"values of the column's type, then another") {
            exec("CREATE TABLE " + t + " (d double precision, s text, day date);");
            exec("INSERT INTO " + t + " VALUES " +
                 values_list("0.5, 'a', DATE '2024-01-01'", lead, {"7, 5, '2024-01-02'"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE d = 7 AND s = '5' AND day = DATE '2024-01-02';") == 1);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"quoted dates, then a DATE literal") {
            exec("CREATE TABLE " + t + " (x date);");
            exec("INSERT INTO " + t + " VALUES " + values_list("'2024-01-01'", lead, {"DATE '2024-01-02'"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = DATE '2024-01-02';") == 1);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE x = DATE '2024-01-01';") == lead);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"strings, then a number") {
            exec("CREATE TABLE " + t + " (s text, n integer);");
            exec("INSERT INTO " + t + " VALUES " + values_list("'b', '2'", lead, {"7, 7", "'z', '9'"}) + ";");
            CHECK(number("SELECT count(*) FROM " + t + ";") == lead + 2);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE s = '7' AND n = 7;") == 1);
            CHECK(number("SELECT sum(n) FROM " + t + ";") == 2 * lead + 16);
        }

        DYNAMIC_SECTION(lead << " rows, " <<"a parameter after the first chunk") {
            exec("CREATE TABLE " + t + " (id bigint, s text);");
            auto* resource = dispatcher->resource();
            std::vector<std::pair<size_t, components::vector::vector_t>> params;
            params.emplace_back(1, components::vector::make_scalar(resource, int64_t{99}));
            auto insert =
                dispatcher->execute_sql_with_params(otterbrix::session_id_t(),
                                                    "INSERT INTO " + t + " (s) VALUES " +
                                                        values_list("'i'", lead, {"$1", "13"}) + ";",
                                                    params);
            INFO("error: " << (insert->is_error() ? insert->get_error().what.c_str() : "none"));
            REQUIRE(insert->is_success());
            CHECK(number("SELECT count(*) FROM " + t + " WHERE s = '99';") == 1);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE s = '13';") == 1);
            CHECK(number("SELECT count(*) FROM " + t + " WHERE s = 'i';") == lead);
        }
    }
}

// The binding's cast reads the type it was resolved from: a column of another type is refused before any row is
// read, never reinterpreted. push() reaches no pipeline context for a local table, so none is given.
TEST_CASE("integration::cpp::test_insert_type_promotion::insert_binding") {
    using namespace components;
    auto* resource = std::pmr::new_delete_resource();
    operators::operator_insert insert(resource,
                                      log_t{},
                                      catalog::oid_t{16384},
                                      table::storage_address_t{},
                                      std::pmr::string{"t", resource},
                                      std::pmr::vector<catalog::oid_t>{resource},
                                      std::pmr::vector<operators::projected_column_t>{resource});
    logical_plan::insert_column_bindings_t bindings(resource);
    bindings.push_back(logical_plan::insert_column_binding_t{
        .target_index = 0,
        .target_name = std::pmr::string{"x", resource},
        .target_type = types::complex_logical_type{types::logical_type::BIGINT},
        .source_type = types::complex_logical_type{types::logical_type::BIGINT},
        .cast = {}});
    insert.set_column_bindings(std::move(bindings));

    SECTION("a column of another type than the binding was resolved for") {
        std::pmr::vector<types::complex_logical_type> schema(resource);
        schema.emplace_back(types::logical_type::DOUBLE);
        vector::data_chunk_t chunk(resource, schema, 1);
        chunk.set_cardinality(1);
        chunk.data[0].set_value(0, 2.5);
        operators::chunks_vector_t out(resource);
        CHECK(insert.push(nullptr, std::move(chunk), out).contains_error());
    }
}
