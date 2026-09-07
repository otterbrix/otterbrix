// A CHECK must be evaluated, or refused -- never accepted and ignored.

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
}

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

// These forms fell outside a 4-shape recogniser and silently compiled to constant TRUE;
// column-vs-column also misread its second column as literal 0. Upstream #629 evaluates them instead.
TEST_CASE("integration::cpp::check_expr_unenforceable::arithmetic_operand", "[checkexpr]") {
    MAKE_ENV("arith_operand");
    REQUIRE(exec("CREATE TABLE c.t (a bigint, b bigint);")->is_success());

    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_sum CHECK (a + b > 0);");
    INFO("an arithmetic operand is evaluated, not collapsed to TRUE");
    CHECK(declared->is_success());

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

    // The mis-read (lo <= 0) would reject (1, 10) -- the opposite failure from the other cases.
    const bool rejected = exec("INSERT INTO c.t (lo, hi) VALUES (1, 10);")->is_error();
    INFO("declared=" << declared->is_success() << " rejected=" << rejected);
    CHECK_FALSE(rejected);
    CHECK_FALSE((declared->is_success() && rejected));

    CHECK(exec("INSERT INTO c.t (lo, hi) VALUES (10, 1);")->is_error());
}

// Column-level and table-level inline CHECKs go through separate extractors into one evaluator.
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

// The operator search used to scan literal text for " > " without regard to quoting.
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

// A CHECK naming a nonexistent column used to compile to TRUE (find_col_index missed it). Upstream
// #629 resolves columns at DDL time, so ALTER refuses upfront; inline forms rely on the write-time floor.

TEST_CASE("integration::cpp::check_expr_unenforceable::unknown_column_alter_refuses_at_write", "[checkexpr]") {
    MAKE_ENV("unknown_col_alter");
    REQUIRE(exec("CREATE TABLE c.t (a bigint);")->is_success());
    auto declared = exec("ALTER TABLE c.t ADD CONSTRAINT chk_typo CHECK (nosuchcol > 0);");
    INFO("a CHECK naming a column the table does not have must not be accepted");
    REQUIRE(declared->is_error());
    const std::string ddl_what{declared->get_error().what};
    INFO("ddl error: " << ddl_what);
    CHECK(ddl_what.find("nosuchcol") != std::string::npos);

    // A refused declaration leaves no constraint, so only declared&&admitted must never both hold.
    const bool admitted = exec("INSERT INTO c.t (a) VALUES (-1);")->is_success();
    INFO("declared=" << declared->is_success() << " admitted=" << admitted);
    CHECK_FALSE((declared->is_success() && admitted));
    CHECK(count_of("SELECT COUNT(*) FROM c.t;") == (admitted ? 1u : 0u));
}

TEST_CASE("integration::cpp::check_expr_unenforceable::unknown_column_inline_table_level_refuses_at_write",
          "[checkexpr]") {
    MAKE_ENV("unknown_col_inline");
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

TEST_CASE("integration::cpp::check_expr_unenforceable::sibling_column_stays_legal", "[checkexpr]") {
    MAKE_ENV("sibling_col");
    REQUIRE(exec("CREATE TABLE c.t (a bigint, b bigint CHECK (a > 0));")->is_success());
    CHECK(exec("INSERT INTO c.t (a, b) VALUES (1, -5);")->is_success());
    CHECK(exec("INSERT INTO c.t (a, b) VALUES (-1, 5);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM c.t;") == 1);
}
