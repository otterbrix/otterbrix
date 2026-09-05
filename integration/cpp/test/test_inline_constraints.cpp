#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// Constraints written inside CREATE TABLE.
//
// They can be lost in two independent places, and both are covered here:
//
//  * COLUMN-level: the transformer's column-constraint switch. Handling only DEFAULT and NOT
//    NULL degrades CONSTR_PRIMARY to not_null = true and drops the key itself, while
//    CONSTR_UNIQUE, CONSTR_CHECK and CONSTR_FOREIGN fall through `default: break;` and never
//    reach the node.
//  * TABLE-level: those do reach node_create_collection_t and read back through constraints(),
//    but rewrite_create_table has to hand them to build_create_table_writes — passing only
//    column_definitions() writes nothing to the catalog.
//
// Each case declares a constraint inline and then violates it. Every violation is checked
// against the table CONTENTS, not only against the statement's status: a constraint that
// reports an error and writes the row anyway is not enforcement.
//
// COST, stated plainly because it is the reason this stayed broken: a table declared with an
// inline PRIMARY KEY or UNIQUE now loads at the speed of a table that HAS one. UNIQUE and
// PRIMARY KEY are enforced by a full table pass per 1024-row batch, measured at N^1.96
// (bulk-loading 400k rows into a table with a primary key cost 5643 ms against 1761 ms
// without). Dropping the constraint was masking that quadratic, not avoiding it — the declared
// key simply was not there. Index-backed enforcement is the fix for the cost; a declaration the
// engine accepts and does not enforce is not.

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config(integration_fixture_path("test_inline_constraints/" + dir))) {
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
    auto count_of = [&](const std::string& sql) -> std::uint64_t {                                                     \
        auto cur = exec(sql);                                                                                          \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == 1);                                                                                     \
        return cur->value(0, 0).value<std::uint64_t>();                                                                \
    };                                                                                                                 \
    REQUIRE(exec("CREATE DATABASE i;")->is_success())

TEST_CASE("integration::cpp::test_inline_constraints::column_primary_key", "[inlinecons]") {
    MAKE_ENV("col_pk");
    REQUIRE(exec("CREATE TABLE i.t (id bigint PRIMARY KEY, v bigint);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 10);")->is_success());
    INFO("a duplicate of an inline PRIMARY KEY must be rejected");
    CHECK(exec("INSERT INTO i.t (id, v) VALUES (1, 20);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE v = 20;") == 0);
}

TEST_CASE("integration::cpp::test_inline_constraints::column_unique", "[inlinecons]") {
    MAKE_ENV("col_unique");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, code bigint UNIQUE);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, code) VALUES (1, 100);")->is_success());
    INFO("a duplicate of an inline UNIQUE must be rejected");
    CHECK(exec("INSERT INTO i.t (id, code) VALUES (2, 100);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE id = 2;") == 0);
    INFO("a distinct value still goes in");
    CHECK(exec("INSERT INTO i.t (id, code) VALUES (3, 101);")->is_success());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 2);
}

TEST_CASE("integration::cpp::test_inline_constraints::column_check", "[inlinecons]") {
    MAKE_ENV("col_check");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, age bigint CHECK (age > 0));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, age) VALUES (1, 5);")->is_success());
    INFO("a row violating an inline CHECK must be rejected");
    CHECK(exec("INSERT INTO i.t (id, age) VALUES (2, -1);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE id = 2;") == 0);
}

TEST_CASE("integration::cpp::test_inline_constraints::table_primary_key", "[inlinecons]") {
    MAKE_ENV("tbl_pk");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, v bigint, PRIMARY KEY (id));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 10);")->is_success());
    INFO("a duplicate of a table-level PRIMARY KEY must be rejected");
    CHECK(exec("INSERT INTO i.t (id, v) VALUES (1, 20);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE v = 20;") == 0);
}

TEST_CASE("integration::cpp::test_inline_constraints::table_unique", "[inlinecons]") {
    MAKE_ENV("tbl_unique");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, code bigint, UNIQUE (code));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, code) VALUES (1, 100);")->is_success());
    INFO("a duplicate of a table-level UNIQUE must be rejected");
    CHECK(exec("INSERT INTO i.t (id, code) VALUES (2, 100);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE id = 2;") == 0);
}

TEST_CASE("integration::cpp::test_inline_constraints::table_check", "[inlinecons]") {
    MAKE_ENV("tbl_check");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, age bigint, CHECK (age > 0));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, age) VALUES (1, 5);")->is_success());
    INFO("a row violating a table-level CHECK must be rejected");
    CHECK(exec("INSERT INTO i.t (id, age) VALUES (2, -1);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE id = 2;") == 0);
}

TEST_CASE("integration::cpp::test_inline_constraints::table_foreign_key", "[inlinecons]") {
    MAKE_ENV("tbl_fk");
    REQUIRE(exec("CREATE TABLE i.parent (id bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE i.parent ADD CONSTRAINT ppk PRIMARY KEY (id);")->is_success());
    REQUIRE(exec("INSERT INTO i.parent (id) VALUES (1);")->is_success());
    REQUIRE(exec("CREATE TABLE i.child (id bigint, pid bigint, FOREIGN KEY (pid) REFERENCES i.parent (id));")
                ->is_success());
    REQUIRE(exec("INSERT INTO i.child (id, pid) VALUES (10, 1);")->is_success());
    INFO("a child row pointing at a missing parent must be rejected");
    CHECK(exec("INSERT INTO i.child (id, pid) VALUES (11, 999);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.child;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.child WHERE pid = 999;") == 0);
}

// The referenced column list omitted: `REFERENCES parent` binds to the parent's PRIMARY KEY.
// This is the same resolution the ALTER path grew; the inline form must reach it too.
TEST_CASE("integration::cpp::test_inline_constraints::column_foreign_key_implicit_pk", "[inlinecons]") {
    MAKE_ENV("col_fk_implicit");
    REQUIRE(exec("CREATE TABLE i.parent (id bigint, PRIMARY KEY (id));")->is_success());
    REQUIRE(exec("INSERT INTO i.parent (id) VALUES (1);")->is_success());
    REQUIRE(exec("CREATE TABLE i.child (id bigint, pid bigint REFERENCES i.parent);")->is_success());
    REQUIRE(exec("INSERT INTO i.child (id, pid) VALUES (10, 1);")->is_success());
    INFO("a child row pointing at a missing parent must be rejected");
    CHECK(exec("INSERT INTO i.child (id, pid) VALUES (11, 999);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.child;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.child WHERE pid = 999;") == 0);
}

// A foreign key pointing back at the very table being created. The referenced table does not
// exist at the moment the constraint is read, and it never will as a separate object — it is
// the statement's own product.
TEST_CASE("integration::cpp::test_inline_constraints::self_referencing_foreign_key", "[inlinecons]") {
    MAKE_ENV("self_fk");
    REQUIRE(exec("CREATE TABLE i.t (id bigint PRIMARY KEY, parent bigint REFERENCES i.t (id));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, parent) VALUES (1, 1);")->is_success());
    INFO("a row pointing at a missing row of its own table must be rejected");
    CHECK(exec("INSERT INTO i.t (id, parent) VALUES (2, 999);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM i.t WHERE id = 2;") == 0);
    INFO("a row pointing at an existing row of its own table is accepted");
    CHECK(exec("INSERT INTO i.t (id, parent) VALUES (3, 1);")->is_success());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 2);
}

// Rule 6: a constraint that cannot be created is a loud failure on CREATE TABLE, never a
// silent drop. These are the same refusals the ALTER path already makes; the inline form
// must go through them rather than around them.
TEST_CASE("integration::cpp::test_inline_constraints::unresolvable_declarations_are_refused", "[inlinecons]") {
    MAKE_ENV("refusals");

    INFO("a foreign key naming a table that does not exist");
    CHECK(exec("CREATE TABLE i.a (id bigint, pid bigint, FOREIGN KEY (pid) REFERENCES i.nosuch (id));")->is_error());
    CHECK(exec("SELECT COUNT(*) FROM i.a;")->is_error());

    INFO("a unique key naming a column that was not declared");
    CHECK(exec("CREATE TABLE i.b (id bigint, UNIQUE (nosuchcol));")->is_error());
    CHECK(exec("SELECT COUNT(*) FROM i.b;")->is_error());

    INFO("a foreign key whose referenced table has no primary key to bind to");
    REQUIRE(exec("CREATE TABLE i.nokey (id bigint);")->is_success());
    CHECK(exec("CREATE TABLE i.c (id bigint, pid bigint REFERENCES i.nokey);")->is_error());
    CHECK(exec("SELECT COUNT(*) FROM i.c;")->is_error());
    // No declared columns means a dynamic-schema table (relkind='g'): it has no
    // pg_attribute rows, so there is no attoid a conkey could name.
    INFO("a constraint on a table declared without any columns");
    CHECK(exec("CREATE TABLE i.dyn (CHECK (x > 0));")->is_error());
    CHECK(exec("SELECT COUNT(*) FROM i.dyn;")->is_error());

    INFO("the refusals left the tables that DID get created alone");
    CHECK(count_of("SELECT COUNT(*) FROM i.nokey;") == 0);
}

// The control: the same constraint added the long way round IS enforced, which is what made the
// cases above defects rather than an unimplemented feature.
TEST_CASE("integration::cpp::test_inline_constraints::alter_table_control", "[inlinecons]") {
    MAKE_ENV("alter_control");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, v bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE i.t ADD CONSTRAINT t_pk PRIMARY KEY (id);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 10);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 20);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM i.t;") == 1);
}
