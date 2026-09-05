// A CHECK must be evaluated, or refused -- never accepted and ignored.
//
// The CHECK text stored in pg_constraint used to be compiled at DML time by a recogniser
// that understood exactly four shapes (`column OP constant`, `column IS [NOT] NULL`,
// `(A) AND/OR (B)`, `NOT (A)`); everything else compiled to constant TRUE, so an
// unrecognised CHECK sat in the catalog and judged no row, ever, in silence. One inverted
// case did worse: a column-against-column CHECK read the second column as the number 0 and
// REJECTED rows that satisfied the declared constraint. Upstream #629 now EVALUATES those
// forms instead, so the cases below assert the declaration is accepted AND the constraint
// is enforced on the write, in place of the earlier refusal-at-declaration.
//
// Two cases keep their original meaning: `name = 'a > b'` is a recognised form and must be
// accepted AND enforced (the operator used to scan for " > " without regard for quoting,
// misreading the literal's own text as the predicate's operator); and a CHECK naming a
// nonexistent column is refused, since upstream resolves CHECK column names against the
// schema at DDL time.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config(integration_fixture_path("test_check_expr_unenforceable/" + dir))) {
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
    INFO("an arithmetic operand is evaluated, not collapsed to TRUE");
    CHECK(declared->is_success());

    // -5 + -5 is not > 0. The invariant is unchanged -- a declaration that reported
    // success must not admit the row it forbids -- but the refusal now comes from the
    // write, which is where a constraint the engine CAN evaluate belongs.
    const bool admitted = exec("INSERT INTO c.t (a, b) VALUES (-5, -5);")->is_success();
    INFO("declared=" << declared->is_success() << " admitted=" << admitted);
    CHECK_FALSE(admitted);
    CHECK_FALSE((declared->is_success() && admitted));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::arithmetic_constant", "[checkexpr]") {
    MAKE_ENV("arith_constant");
    REQUIRE(exec("CREATE TABLE c.t (a bigint);")->is_success());

    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_gt CHECK (a > 1 + 1);");
    INFO("the bound is folded to 2, not read as 1");
    CHECK(declared->is_success());

    // a = 2 violates `a > 1 + 1`. A recogniser that read the bound as 1 let it in; that
    // is the exact value this case exists to keep out.
    const bool admitted = exec("INSERT INTO c.t (a) VALUES (2);")->is_success();
    INFO("declared=" << declared->is_success() << " admitted=" << admitted);
    CHECK_FALSE(admitted);
    CHECK_FALSE((declared->is_success() && admitted));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::column_against_column", "[checkexpr]") {
    MAKE_ENV("col_vs_col");
    REQUIRE(exec("CREATE TABLE c.t (lo bigint, hi bigint);")->is_success());

    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_range CHECK (lo <= hi);");
    INFO("two columns compare against each other, not against a mis-read 0");
    CHECK(declared->is_success());

    // (1, 10) SATISFIES `lo <= hi`. The mis-read constraint (lo <= 0) rejected it, so
    // here the damage ran the other way: a declaration that reported success must not
    // reject a row it permits.
    const bool rejected = exec("INSERT INTO c.t (lo, hi) VALUES (1, 10);")->is_error();
    INFO("declared=" << declared->is_success() << " rejected=" << rejected);
    CHECK_FALSE(rejected);
    CHECK_FALSE((declared->is_success() && rejected));

    // And the violating pair is still kept out, so the case cannot pass by the
    // constraint having quietly become TRUE again.
    CHECK(exec("INSERT INTO c.t (lo, hi) VALUES (10, 1);")->is_error());
}

// The inline forms of CREATE TABLE reach the same deparser, so they must refuse the
// same expressions. Column-level and table-level are separate extractors.
TEST_CASE("integration::cpp::check_expr_unenforceable::inline_table_level", "[checkexpr]") {
    MAKE_ENV("inline_table");
    auto declared = exec("CREATE TABLE c.t (a bigint, b bigint, CHECK (a + b > 0));");
    INFO("a table-level inline CHECK reaches the same evaluator");
    CHECK(declared->is_success());

    const bool admitted = exec("INSERT INTO c.t (a, b) VALUES (-5, -5);")->is_success();
    CHECK_FALSE(admitted);
    CHECK_FALSE((declared->is_success() && admitted));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::inline_column_level", "[checkexpr]") {
    MAKE_ENV("inline_column");
    auto declared = exec("CREATE TABLE c.t (a bigint, b bigint CHECK (b + 1 > 0));");
    INFO("a column-level inline CHECK reaches the same evaluator; it is a separate extractor");
    CHECK(declared->is_success());

    const bool admitted = exec("INSERT INTO c.t (a, b) VALUES (1, -5);")->is_success();
    CHECK_FALSE(admitted);
    CHECK_FALSE((declared->is_success() && admitted));
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

// A CHECK naming a column the table does not have is a CHECK over nothing.
//
// `CHECK (nosuchcol > 0)` used to deparse cleanly (a recognised shape: column OP constant)
// and reach DML time, where the operator recogniser's find_col_index missed the column and
// compiled the predicate to constant TRUE -- the declared constraint judged no row, ever, in
// silence. Upstream #629 now resolves a CHECK's column names against the schema at DDL time,
// so the ALTER case below refuses at declaration. operator_check_constraint's write-time
// find_col_index guard stays as a floor for whatever inline-form path does not go through
// that DDL resolution (the two inline cases below still gate on it conditionally).

TEST_CASE("integration::cpp::check_expr_unenforceable::unknown_column_alter_refuses_at_write", "[checkexpr]") {
    MAKE_ENV("unknown_col_alter");
    REQUIRE(exec("CREATE TABLE c.t (a bigint);")->is_success());
    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_typo CHECK (nosuchcol > 0);");
    INFO("a CHECK naming a column the table does not have must not be accepted");
    REQUIRE(declared->is_error());
    const std::string ddl_what{declared->get_error().what};
    INFO("ddl error: " << ddl_what);
    CHECK(ddl_what.find("nosuchcol") != std::string::npos);

    // The invariant this case has always carried: whatever the engine decides about the
    // declaration, the table must not end up holding a row that a live constraint forbids.
    // With the constraint refused there is no constraint, so the write is judged only by
    // the column list -- and the row lands. Assert the state, not a guessed refusal.
    const bool admitted = exec("INSERT INTO c.t (a) VALUES (-1);")->is_success();
    INFO("declared=" << declared->is_success() << " admitted=" << admitted);
    CHECK_FALSE((declared->is_success() && admitted));
    CHECK(count_of("SELECT COUNT(*) FROM c.t;") == (admitted ? 1u : 0u));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::unknown_column_inline_table_level_refuses_at_write",
          "[checkexpr]") {
    MAKE_ENV("unknown_col_inline");
    // The inline table-level CHECK is stored the same way and compiled by the same
    // operator, so it hits the same floor.
    if (exec("CREATE TABLE c.t (a bigint, CHECK (nosuch > 0));")->is_success()) {
        auto ins = exec("INSERT INTO c.t (a) VALUES (-1);");
        INFO("inline table-level CHECK over a missing column must refuse the write");
        REQUIRE(ins->is_error());
        CHECK(std::string{ins->get_error().what}.find("nosuch") != std::string::npos);
    }
}

TEST_CASE("integration::cpp::check_expr_unenforceable::unknown_column_inline_column_level_refuses_at_write",
          "[checkexpr]") {
    MAKE_ENV("unknown_col_inline_col");
    if (exec("CREATE TABLE c.t (a bigint, b bigint CHECK (height > 0));")->is_success()) {
        auto ins = exec("INSERT INTO c.t (a, b) VALUES (1, 2);");
        INFO("inline column-level CHECK over a missing column must refuse the write");
        REQUIRE(ins->is_error());
        CHECK(std::string{ins->get_error().what}.find("height") != std::string::npos);
    }
}

// The floor triggers ONLY on a genuinely missing column: a valid reference to a
// sibling column stays legal and enforced.
TEST_CASE("integration::cpp::check_expr_unenforceable::sibling_column_stays_legal", "[checkexpr]") {
    MAKE_ENV("sibling_col");
    REQUIRE(exec("CREATE TABLE c.t (a bigint, b bigint CHECK (a > 0));")->is_success());
    CHECK(exec("INSERT INTO c.t (a, b) VALUES (1, -5);")->is_success());
    CHECK(exec("INSERT INTO c.t (a, b) VALUES (-1, 5);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM c.t;") == 1);
}
