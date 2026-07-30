// ============================================================================
// CHECK CONSTRAINTS OVER COLUMN TYPES THAT ARE NOT bigint.
//
// operator_check_constraint parses its CHECK text by hand and compares the column
// value against a constant produced by parse_const, which can only ever answer one
// of THREE types: STRING_LITERAL (quoted), DOUBLE (contains a '.'), or BIGINT
// (everything else). logical_value_t comparison does NO type promotion -- operator==
// opens with
//     assert(type_ == rhs.type_ && "logical_value_t has to be casted to the same
//                                   type before comparison");
// so the operator must cast the literal to the column's DECLARED type before it
// compares.
//
// Every pre-existing CHECK test in the suite declares its columns `bigint`, which is
// exactly the type parse_const yields for an integer literal -- so the mismatch was
// never exercised. Unreconciled, it is not just a debug abort: with NDEBUG the
// comparison switches on the LEFT operand's type and reads the RIGHT through it, so
// a BIGINT 5 compared against a DOUBLE column is reinterpreted as the denormal
// 2.47e-323 and `CHECK (x > 5)` accepts every positive value -- silently, with no
// crash, no error, no log line. These cases pin the reconciliation across the column
// types the DDL accepts.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {

    // A CHECK over `column_type`, then one satisfying and one violating INSERT.
    // `low` violates `> 5`, `high` satisfies it.
    void check_over_type(otterbrix::wrapper_dispatcher_t* dispatcher,
                         const std::string& table,
                         const std::string& column_type,
                         const std::string& low,
                         const std::string& high) {
        INFO("column type: " << column_type);
        REQUIRE(exec(dispatcher, "CREATE TABLE ChkDb." + table + " (id bigint, x " + column_type + ");")
                    ->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE ChkDb." + table + " ADD CONSTRAINT chk_x CHECK (x > 5);")
                    ->is_success());

        // Satisfying row: accepted.
        auto ok = exec(dispatcher, "INSERT INTO ChkDb." + table + " (id, x) VALUES (1, " + high + ");");
        INFO("satisfying insert: " << (ok->is_error() ? std::string(ok->get_error().what) : std::string("ok")));
        REQUIRE(ok->is_success());

        // Violating row: rejected.
        auto bad = exec(dispatcher, "INSERT INTO ChkDb." + table + " (id, x) VALUES (2, " + low + ");");
        INFO("violating insert: " << (bad->is_error() ? std::string(bad->get_error().what) : std::string("accepted")));
        REQUIRE(bad->is_error());

        // Exactly the satisfying row survives.
        auto count = exec(dispatcher, "SELECT COUNT(id) AS c FROM ChkDb." + table + ";");
        REQUIRE(count->is_success());
        REQUIRE(count->value(0, 0).value<uint64_t>() == 1u);
    }

} // namespace

// bigint is the ONLY type the existing suite covers, and it is the one type that
// happens to match parse_const's output exactly. It is here as the control: if this
// case ever goes red, the failure is in the CHECK machinery itself and not in the
// type pairing the cases below are about.
TEST_CASE("integration::cpp::check_types::bigint_column_is_the_covered_control") {
    auto config = make_test_config(test_temp_path("check_types/bigint"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE ChkDb;")->is_success());
    check_over_type(dispatcher, "t_bigint", "bigint", "1", "10");
}

// An INTEGER column against the BIGINT constant parse_const produces from `5`: with
// the literal left unreconciled this is a debug abort. Left as its own case rather
// than folded into a loop so a regression names the type it broke on.
TEST_CASE("integration::cpp::check_types::integer_column") {
    auto config = make_test_config(test_temp_path("check_types/integer"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE ChkDb;")->is_success());
    check_over_type(dispatcher, "t_int", "integer", "1", "10");
}

// A DOUBLE column against a BIGINT constant: the CHECK text says `> 5`, with no '.', so
// parse_const answers BIGINT however the column is declared. THE case that silently
// accepted violating rows in a release build -- `x > 5` evaluated as `x > 2.47e-323`.
TEST_CASE("integration::cpp::check_types::double_column_against_an_integer_literal") {
    auto config = make_test_config(test_temp_path("check_types/double"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE ChkDb;")->is_success());
    check_over_type(dispatcher, "t_dbl", "double precision", "1.5", "10.5");
}

// The remaining numeric widths. Each is a distinct payload width or representation, and
// each therefore reinterprets the BIGINT constant differently when the types are not
// reconciled first.
TEST_CASE("integration::cpp::check_types::other_numeric_widths") {
    auto config = make_test_config(test_temp_path("check_types/numeric_widths"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE ChkDb;")->is_success());

    check_over_type(dispatcher, "t_small", "smallint", "1", "10");
    check_over_type(dispatcher, "t_real", "real", "1.5", "10.5");
    // `numeric` is deliberately absent: the DDL does not accept it as a column type at all
    // ("CREATE TABLE ... (x numeric)" fails), so there is no CHECK path over it to exercise.
    // It is listed here rather than silently dropped so the omission is a recorded fact and
    // not an oversight.
}

// A DATE column against a QUOTED literal was the case most likely to still be wrong: a
// STRING_LITERAL's payload is a std::string POINTER while a DATE's is an int32 day count,
// so an unreconciled comparison reads a pointer as a date -- the DOUBLE reinterpretation
// on a much wider gap.
//
// It turns out to be UNREACHABLE through SQL, and this pins why: the INSERT is refused
// before any constraint runs, because the engine cannot convert a string literal to a
// DATE column ("insert_node: can not convert data column[1] type to table type"). So the
// CHECK comparison over a DATE column cannot be driven from SQL today, and the pointer
// reinterpretation is latent rather than live.
//
// This case is deliberately written to go RED the day DATE inserts start working: at that
// moment the CHECK path over a DATE becomes reachable and has to be re-examined, and a
// silently passing test would let it through.
TEST_CASE("integration::cpp::check_types::date_column_is_unreachable_from_sql") {
    auto config = make_test_config(test_temp_path("check_types/date"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE ChkDb;")->is_success());

    REQUIRE(exec(dispatcher, "CREATE TABLE ChkDb.t_date (id bigint, d date);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE ChkDb.t_date ADD CONSTRAINT chk_d CHECK (d > '2024-01-01');")
                ->is_success());

    // Not "the constraint rejected it" -- the INSERT itself cannot build the row.
    auto attempted = exec(dispatcher, "INSERT INTO ChkDb.t_date (id, d) VALUES (1, '2024-06-01');");
    INFO("insert result: " << (attempted->is_error() ? std::string(attempted->get_error().what)
                                                     : std::string("SUCCEEDED -- the CHECK path over a "
                                                                   "DATE column is now reachable, re-examine it")));
    REQUIRE(attempted->is_error());
}
