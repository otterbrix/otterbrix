// ============================================================================
// A CHECK THE ENGINE CANNOT EVALUATE MUST NOT BE ACCEPTED AS IF IT COULD.
//
// The CHECK text stored in pg_constraint is compiled at DML time by a small
// recogniser in operator_check_constraint. It understands exactly four shapes:
//
//     column OP constant        (OP one of  =  <>  <  >  <=  >= )
//     column IS [NOT] NULL
//     (A) AND (B) / (A) OR (B)
//     NOT (A)
//
// Everything else used to compile to the constant TRUE — the constraint was in
// the catalog, the user was told the declaration succeeded, and no row was ever
// judged by it. The forms below all survive deparsing (so the declaration is
// accepted today) and all fail to reach that recogniser:
//
//   * an arithmetic operand      CHECK (a + b > 0)   — "a + b" is not a column
//                                                      name, so the whole
//                                                      predicate is TRUE;
//   * an arithmetic constant     CHECK (a > 1 + 1)   — the constant parser reads
//                                                      "1 + 1" as 1, so a > 1 is
//                                                      enforced instead;
//   * column against column      CHECK (lo <= hi)    — "hi" is not a number, so
//                                                      the constant parser reads
//                                                      0 and lo <= 0 is enforced
//                                                      instead: rows that satisfy
//                                                      the declared constraint are
//                                                      rejected.
//
// Each case is written as the pair that must never happen, so the test states
// the invariant rather than where the refusal is made: it is satisfied by a
// refusal at declaration (which is what this branch does — a rejected CREATE
// TABLE / ALTER TABLE is recoverable, a constraint sitting unenforced in the
// catalog is not) and equally by a refusal at the first write.
//
// The last case is the opposite shape: `name = 'a > b'` IS one of the four
// recognised forms, and must keep being accepted AND enforced. It is here
// because the operator used to scan for its comparison operator without regard
// for quoting, so the " > " INSIDE the string literal was taken for the
// predicate's operator and the whole CHECK collapsed to TRUE.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config("/tmp/otterbrix/integration/test_check_expr_unenforceable/" + dir)) {
            test_clear_directory(config);
            config.wal.on = false;
            config.log.level = log_t::level::off;
        }
    };
} // namespace

#define MAKE_ENV(dirname)                                                                                              \
    env_t env(dirname);                                                                                                \
    test_spaces space(env.config);                                                                                     \
    auto* d = space.dispatcher();                                                                                      \
    auto exec = [&](const std::string& sql) {                                                                          \
        auto session = otterbrix::session_id_t();                                                                      \
        return d->execute_sql(session, sql);                                                                           \
    };                                                                                                                 \
    [[maybe_unused]] auto count_of = [&](const std::string& sql) -> std::uint64_t {                                    \
        auto cur = exec(sql);                                                                                          \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == 1);                                                                                     \
        return cur->value(0, 0).value<std::uint64_t>();                                                                \
    };                                                                                                                 \
    REQUIRE(exec("CREATE DATABASE c;")->is_success())

TEST_CASE("integration::cpp::check_expr_unenforceable::arithmetic_operand", "[checkexpr]") {
    MAKE_ENV("arith_operand");
    REQUIRE(exec("CREATE TABLE c.t (a bigint, b bigint);")->is_success());

    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_sum CHECK (a + b > 0);");
    INFO("declaring a CHECK the engine cannot evaluate must be refused AT THE DECLARATION");
    CHECK(declared->is_error());

    // The invariant, independent of where the refusal is made: a declaration that
    // reported success must not admit the row it forbids.
    const bool admitted = exec("INSERT INTO c.t (a, b) VALUES (-5, -5);")->is_success();
    INFO("declared=" << declared->is_success() << " admitted=" << admitted);
    CHECK_FALSE((declared->is_success() && admitted));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::arithmetic_constant", "[checkexpr]") {
    MAKE_ENV("arith_constant");
    REQUIRE(exec("CREATE TABLE c.t (a bigint);")->is_success());

    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_gt CHECK (a > 1 + 1);");
    INFO("a constant the engine folds wrongly is a constraint it does not enforce");
    CHECK(declared->is_error());

    // a = 2 violates `a > 1 + 1`; the recogniser used to read the bound as 1 and let it in.
    const bool admitted = exec("INSERT INTO c.t (a) VALUES (2);")->is_success();
    INFO("declared=" << declared->is_success() << " admitted=" << admitted);
    CHECK_FALSE((declared->is_success() && admitted));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::column_against_column", "[checkexpr]") {
    MAKE_ENV("col_vs_col");
    REQUIRE(exec("CREATE TABLE c.t (lo bigint, hi bigint);")->is_success());

    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_range CHECK (lo <= hi);");
    INFO("comparing two columns is not one of the shapes the engine can evaluate");
    CHECK(declared->is_error());

    // (1, 10) SATISFIES `lo <= hi`. The mis-read constraint (lo <= 0) rejects it, so
    // here the damage runs the other way: a declaration that reported success must not
    // reject a row it permits.
    const bool rejected = exec("INSERT INTO c.t (lo, hi) VALUES (1, 10);")->is_error();
    INFO("declared=" << declared->is_success() << " rejected=" << rejected);
    CHECK_FALSE((declared->is_success() && rejected));
}

// The inline forms of CREATE TABLE reach the same deparser, so they must refuse the
// same expressions. Column-level and table-level are separate extractors.
TEST_CASE("integration::cpp::check_expr_unenforceable::inline_table_level", "[checkexpr]") {
    MAKE_ENV("inline_table");
    auto declared = exec("CREATE TABLE c.t (a bigint, b bigint, CHECK (a + b > 0));");
    INFO("a table-level inline CHECK goes through the same guard");
    CHECK(declared->is_error());

    if (declared->is_success()) {
        const bool admitted = exec("INSERT INTO c.t (a, b) VALUES (-5, -5);")->is_success();
        CHECK_FALSE(admitted);
    }
}

TEST_CASE("integration::cpp::check_expr_unenforceable::inline_column_level", "[checkexpr]") {
    MAKE_ENV("inline_column");
    auto declared = exec("CREATE TABLE c.t (a bigint, b bigint CHECK (b + 1 > 0));");
    INFO("a column-level inline CHECK goes through the same guard");
    CHECK(declared->is_error());

    if (declared->is_success()) {
        const bool admitted = exec("INSERT INTO c.t (a, b) VALUES (1, -5);")->is_success();
        CHECK_FALSE(admitted);
    }
}

// A recognised form that the operator used to misread: the string literal contains
// " > ", and the operator search took that for the predicate's operator.
TEST_CASE("integration::cpp::check_expr_unenforceable::operator_inside_a_string_literal", "[checkexpr]") {
    MAKE_ENV("op_in_literal");
    REQUIRE(exec("CREATE TABLE c.t (id bigint, name text);")->is_success());
    REQUIRE(exec("ALTER TABLE c.t ADD CONSTRAINT chk_name CHECK (name = 'a > b');")->is_success());

    INFO("the value the CHECK demands goes in");
    CHECK(exec("INSERT INTO c.t (id, name) VALUES (1, 'a > b');")->is_success());
    INFO("any other value must be rejected");
    CHECK(exec("INSERT INTO c.t (id, name) VALUES (2, 'zzz');")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM c.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM c.t WHERE id = 2;") == 0);
}
