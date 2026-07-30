// ============================================================================
// CHECK CONSTRAINTS OVER COLUMN TYPES THAT ARE NOT bigint.
//
// operator_check_constraint parses its CHECK text by hand and compares the column
// value against a constant produced by parse_const, which can only ever answer one
// of THREE types: STRING_LITERAL (quoted), DOUBLE (contains a '.'), or BIGINT
// (everything else). The comparison itself goes through logical_value_t::compare_sql,
// which does NO type promotion -- it forwards to compare(), and operator== opens with
//     assert(type_ == rhs.type_ && "logical_value_t has to be casted to the same
//                                   type before comparison");
//
// So a CHECK over any column whose type is not one of those three compares two
// differently-typed values. Every pre-existing CHECK test in the suite declares its
// columns `bigint`, which is exactly the type parse_const yields for an integer
// literal -- so the mismatch has never been exercised.
//
// These cases exercise it. They are characterization pins written BEFORE migrating
// the operator onto the bound expression layer: whatever the engine answers today is
// what it must still answer afterwards, and where today's answer is a crash rather
// than an answer, that is a finding about the operator and not about the migration.
//
// ---------------------------------------------------------------------------
// WHAT A RELEASE BUILD DOES, MEASURED
//
// With NDEBUG the assert is compiled out and the comparison proceeds on the two
// differently-typed values. Measured by recompiling components/types/logical_value.cpp
// with -DNDEBUG and driving compare_sql through this operator's own compare_tri:
//
//   column value        `x > 5` answers   row        correct?
//   BIGINT 10 / 1       TRUE  / FALSE     ok         yes  (control)
//   INTEGER 10 / 1      TRUE  / FALSE     ok         yes
//   SMALLINT 10 / 1     TRUE  / FALSE     ok         yes
//   DOUBLE 10.5         TRUE              accepted   yes
//   DOUBLE 1.5          TRUE              accepted   *** NO -- VIOLATING ROW ACCEPTED ***
//   REAL 1.5            TRUE              accepted   *** NO -- VIOLATING ROW ACCEPTED ***
//   BOOLEAN true        TRUE              accepted   *** NO ***
//
// The integer widths survive by luck: logical_value_t keeps its payload in one 64-bit
// data_, and the integer arms compare that payload, so INTEGER-vs-BIGINT reads the
// right bits. The floating arms do not: operator< switches on the LEFT operand's type
// and reads the RIGHT through it, so a BIGINT 5 is reinterpreted as a double --
//
//     BIGINT 5 payload reinterpreted as double = 2.47033e-323   (a denormal)
//
// -- and `CHECK (x > 5)` over a DOUBLE column is therefore evaluated as `x > ~0`.
// Every positive value passes it. The constraint is not enforced, and nothing reports
// that it was not: no crash, no error, no log line. That is strictly worse than the
// Debug abort, and it is the reason this is a live engine defect rather than a
// migration hazard.
// ---------------------------------------------------------------------------
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

// An INTEGER column against the BIGINT constant parse_const produces from `5`.
//
// UN-HIDDEN: it aborted until the comparison was taught to promote its operands, and it
// is now the pin that proves it does. Left as its own case rather than folded into a
// loop so a regression names the type it broke on.
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
