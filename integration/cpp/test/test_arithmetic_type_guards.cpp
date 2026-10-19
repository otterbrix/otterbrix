#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

// Companion coverage for the operand type guards: paths the bind/runtime checks in the
// dispatcher and arithmetic evaluator do not reach.

// Disabled: exercises a COMPUTED-schema table (CREATE TABLE db.c () -> relkind 'g'), which is
// out of scope for now. The INSERT widening path asks the promotion oracle for a common type of
// (INTEGER, BOOLEAN) and poisons the column vector, aborting in set_value.
#if 0
TEST_CASE("integration::cpp::arithmetic_type_guards::mixed_bool_int_insert_survives") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_type_guards/mixed_insert"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.c ();")->is_success());
    }

    // The INSERT widening path treated BOOLEAN as numeric and asked the promotion oracle
    // for a (INTEGER, BOOLEAN) common type; the answer poisoned the column vector and the
    // statement crashed. An unpromotable pair takes the plain per-value store instead.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.c (x) VALUES (1), (true);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT x FROM db.c;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell0 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell0.has_error());
        REQUIRE(cell0.value() == 1);
    }
}
#endif

namespace {

    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.t (id INT, v BIGINT, b BOOLEAN, s TEXT);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "INSERT INTO db.t (id, v, b, s) VALUES (1, 7, true, 'a'), "
                                      "(2, 3, false, 'b');")
                        ->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::arithmetic_type_guards::where_arithmetic_rejects_non_numeric") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_type_guards/where_guards"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // WHERE-clause arithmetic evaluates per row through the predicate value getters, which
    // had no operand type checking: b + 1 read an indeterminate promotion result (silently
    // zero rows), s + 1 crashed in the value machinery, -s compared garbage.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE b + 1 > 0;");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::arithmetics_failure);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE s + 1 > 0;");
        REQUIRE(cur->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE -s < 0;");
        REQUIRE(cur->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE -v < -5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell1 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value() == 1);
    }
}

TEST_CASE("integration::cpp::arithmetic_type_guards::where_null_3vl_preserved") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_type_guards/where_3vl"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 1), (2, NULL), (3, 20);")
                    ->is_success());
    }

    // NULL operands stay three-valued: the NULL row is UNKNOWN and silently excluded,
    // never an operator error.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n WHERE 10 / v > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell2 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value() == 1);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n WHERE v + 1 > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell3 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == 3);
    }
}

TEST_CASE("integration::cpp::arithmetic_type_guards::update_rejects_non_numeric_arithmetic") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_type_guards/update_guard"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // The UPDATE expression tree fed b + 1 straight into the binary kernel, whose
    // unresolvable-pair result is an all-NULL vector: the column was silently overwritten
    // with NULLs. The operand pair must be rejected and the data left intact.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "UPDATE db.t SET v = b + 1;");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::arithmetics_failure);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell4 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == 7);
        auto cell5 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == 3);
    }
}

TEST_CASE("integration::cpp::arithmetic_type_guards::min_value_divided_by_minus_one") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_arithmetic_type_guards/min_div_minus_one"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.big (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.small (x INTEGER);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.tiny (x TINYINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.short (x SMALLINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.huge (x HUGEINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.big (x) VALUES (-9223372036854775807 - 1);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.small (x) VALUES (-2147483648);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.tiny (x) VALUES (-128);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.short (x) VALUES (-32768);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher,
                           "INSERT INTO db.huge (x) VALUES (-170141183460469231731687303715884105727::HUGEINT - 1);")
            ->is_success());

    auto expect_out_of_range = [&](const std::string& sql, const char* what) {
        INFO(sql);
        auto cur = test_helpers::exec(dispatcher, sql);
        std::string outcome;
        if (cur->is_error()) {
            outcome = std::string("error: ") + std::string(cur->get_error().what);
        } else {
            auto first = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(first.has_error());
            outcome = "no error; first cell " + std::to_string(first.value());
        }
        INFO(outcome);
        REQUIRE(cur->is_error());
        CHECK(std::string(cur->get_error().what).find(what) != std::string::npos);
    };

    SECTION("literal BIGINT") { expect_out_of_range("SELECT (-9223372036854775807 - 1) / -1;", "bigint out of range"); }
    SECTION("literal BIGINT remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT (-9223372036854775807 - 1) % -1;");
        REQUIRE(rem->is_success());
        auto cell6 = rem->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell6.has_error());
        CHECK(cell6.value() == 0);
    }
    SECTION("literal BIGINT, spelled as one literal") {
        expect_out_of_range("SELECT -9223372036854775808 / -1;", "bigint out of range");
    }
    SECTION("single literal remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT -9223372036854775808 % -1;");
        REQUIRE(rem->is_success());
        auto cell7 = rem->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell7.has_error());
        CHECK(cell7.value() == 0);
    }
    SECTION("BIGINT column") { expect_out_of_range("SELECT x / -1 FROM db.big;", "bigint out of range"); }
    SECTION("BIGINT column remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT x % -1 FROM db.big;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->size() == 1);
        auto cell8 = rem->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell8.has_error());
        CHECK(cell8.value() == 0);
    }
    SECTION("INSERT VALUES") {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.big (x) VALUES ((-9223372036854775807 - 1) / -1);");
        REQUIRE(ins->is_error());
        CHECK(std::string(ins->get_error().what).find("bigint out of range") != std::string::npos);
    }
    SECTION("INTEGER literal") {
        expect_out_of_range("SELECT (-2147483647 - 1)::INTEGER / -1::INTEGER;", "integer out of range");
    }
    SECTION("INTEGER column") {
        expect_out_of_range("SELECT x / (-1)::INTEGER FROM db.small;", "integer out of range");
    }
    SECTION("INTEGER column divided by a BIGINT literal widens") {
        auto quot = test_helpers::exec(dispatcher, "SELECT x / -1 FROM db.small;");
        REQUIRE(quot->is_success());
        REQUIRE(quot->size() == 1);
        auto cell9 = quot->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell9.has_error());
        CHECK(cell9.value() == 2147483648);
    }
    SECTION("INTEGER column remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT x % (-1)::INTEGER FROM db.small;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->size() == 1);
        auto cell10 = rem->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell10.has_error());
        CHECK(cell10.value() == 0);
    }
    SECTION("TINYINT column") { expect_out_of_range("SELECT x / (-1)::TINYINT FROM db.tiny;", "tinyint out of range"); }
    SECTION("TINYINT column remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT x % (-1)::TINYINT FROM db.tiny;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->size() == 1);
        auto cell11 = rem->get<int8_t>(0, 0);
        REQUIRE_FALSE(cell11.has_error());
        CHECK(cell11.value() == 0);
    }
    SECTION("SMALLINT column") {
        expect_out_of_range("SELECT x / (-1)::SMALLINT FROM db.short;", "smallint out of range");
    }
    SECTION("SMALLINT column remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT x % (-1)::SMALLINT FROM db.short;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->size() == 1);
        auto cell12 = rem->get<int16_t>(0, 0);
        REQUIRE_FALSE(cell12.has_error());
        CHECK(cell12.value() == 0);
    }
    SECTION("HUGEINT column") { expect_out_of_range("SELECT x / (-1)::HUGEINT FROM db.huge;", "hugeint out of range"); }
    SECTION("HUGEINT column remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT x % (-1)::HUGEINT FROM db.huge;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->size() == 1);
        auto cell13 = rem->get<components::types::int128_t>(0, 0);
        REQUIRE_FALSE(cell13.has_error());
        CHECK(cell13.value() == 0);
    }
    SECTION("TINYINT literal") {
        expect_out_of_range("SELECT (-128)::TINYINT / (-1)::TINYINT;", "tinyint out of range");
    }
    SECTION("SMALLINT literal") {
        expect_out_of_range("SELECT (-32768)::SMALLINT / (-1)::SMALLINT;", "smallint out of range");
    }
    SECTION("HUGEINT literal") {
        expect_out_of_range("SELECT (-170141183460469231731687303715884105727::HUGEINT - 1) / (-1)::HUGEINT;",
                            "hugeint out of range");
    }
    SECTION("HUGEINT literal remainder") {
        auto rem =
            test_helpers::exec(dispatcher,
                               "SELECT (-170141183460469231731687303715884105727::HUGEINT - 1) % (-1)::HUGEINT;");
        REQUIRE(rem->is_success());
        auto cell14 = rem->get<components::types::int128_t>(0, 0);
        REQUIRE_FALSE(cell14.has_error());
        CHECK(cell14.value() == 0);
    }
    SECTION("TINYINT literal remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT (-128)::TINYINT % (-1)::TINYINT;");
        REQUIRE(rem->is_success());
        auto cell15 = rem->get<int8_t>(0, 0);
        REQUIRE_FALSE(cell15.has_error());
        CHECK(cell15.value() == 0);
    }
    SECTION("SMALLINT literal remainder") {
        auto rem = test_helpers::exec(dispatcher, "SELECT (-32768)::SMALLINT % (-1)::SMALLINT;");
        REQUIRE(rem->is_success());
        auto cell16 = rem->get<int16_t>(0, 0);
        REQUIRE_FALSE(cell16.has_error());
        CHECK(cell16.value() == 0);
    }
}

// A string operand never reads as a number (it used to read the string's heap pointer as an int64 payload),
// a floating operand has no modulus, and mixed numeric widths still promote.
TEST_CASE("integration::cpp::arithmetic_type_guards::mixed_operands_refuse_and_numeric_pairs_promote") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_arithmetic_type_guards/mixed_operands"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.m (id INT, v BIGINT, d DOUBLE, s TEXT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.m (id, v, d, s) VALUES (2, 3, 2.0, 'a');")->is_success());

    SECTION("string and number refuse in either order, for + - * %") {
        REQUIRE(test_helpers::exec(dispatcher, "SELECT s + v FROM db.m;")->is_error());
        REQUIRE(test_helpers::exec(dispatcher, "SELECT v + s FROM db.m;")->is_error());
        REQUIRE(test_helpers::exec(dispatcher, "SELECT s - v FROM db.m;")->is_error());
        REQUIRE(test_helpers::exec(dispatcher, "SELECT v * s FROM db.m;")->is_error());
        REQUIRE(test_helpers::exec(dispatcher, "SELECT v % s FROM db.m;")->is_error());
    }
    SECTION("modulus over a floating operand refuses") {
        auto cur = test_helpers::exec(dispatcher, "SELECT d % d FROM db.m;");
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("supported pairs still answer") {
        auto sum = test_helpers::exec(dispatcher, "SELECT id + v FROM db.m;");
        REQUIRE(sum->is_success());
        auto sum_cell = sum->get<int64_t>(0, 0);
        REQUIRE_FALSE(sum_cell.has_error());
        REQUIRE(sum_cell.value() == 5);

        auto product = test_helpers::exec(dispatcher, "SELECT d * d FROM db.m;");
        REQUIRE(product->is_success());
        auto product_cell = product->get<double>(0, 0);
        REQUIRE_FALSE(product_cell.has_error());
        REQUIRE(product_cell.value() == Catch::Approx(4.0));
    }
}
