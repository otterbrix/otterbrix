#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// Constraints written inside CREATE TABLE are parsed and thrown away.
//
// They are lost twice over, independently:
//
//  * COLUMN-level: the transformer's column-constraint switch handles only DEFAULT and NOT NULL.
//    CONSTR_PRIMARY degrades to not_null = true and the key itself is dropped; CONSTR_UNIQUE,
//    CONSTR_CHECK and CONSTR_FOREIGN fall through `default: break;` and never reach the node.
//  * TABLE-level: those do reach node_create_collection_t and can be read back through
//    constraints(), but rewrite_create_collection passes only column_definitions() to
//    build_create_table_writes, so nothing is ever written to the catalog. The only consumers of
//    constraints() in the whole tree are parser tests.
//
// Each case below declares a constraint inline and then violates it; a passing case means the
// constraint was enforced. Seven of the eight fail today; only the ALTER TABLE control passes.
//
// HIDDEN ([.]) ON PURPOSE: fixing this would make writes SLOWER. UNIQUE and PRIMARY KEY are
// enforced today by a full table pass per 1024-row batch, measured at N^1.96 (bulk-loading 400k
// rows into a table with a primary key costs 5643 ms against 1761 ms without). The dropped
// constraints are currently MASKING that quadratic — a table declared with an inline PRIMARY KEY
// loads fast precisely because it has no key. So the order is the reverse of the obvious one:
// index-backed constraint enforcement first, then inline forms.

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config("/tmp/otterbrix/integration/test_inline_constraints/" + dir)) {
            test_clear_directory(config);
            config.disk.on = true;
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
    REQUIRE(exec("CREATE DATABASE i;")->is_success())

TEST_CASE("integration::cpp::test_inline_constraints::column_primary_key", "[.][inlinecons]") {
    MAKE_ENV("col_pk");
    REQUIRE(exec("CREATE TABLE i.t (id bigint PRIMARY KEY, v bigint);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 10);")->is_success());
    INFO("a duplicate of an inline PRIMARY KEY must be rejected");
    CHECK(exec("INSERT INTO i.t (id, v) VALUES (1, 20);")->is_error());
}

TEST_CASE("integration::cpp::test_inline_constraints::column_unique", "[.][inlinecons]") {
    MAKE_ENV("col_unique");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, code bigint UNIQUE);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, code) VALUES (1, 100);")->is_success());
    INFO("a duplicate of an inline UNIQUE must be rejected");
    CHECK(exec("INSERT INTO i.t (id, code) VALUES (2, 100);")->is_error());
}

TEST_CASE("integration::cpp::test_inline_constraints::column_check", "[.][inlinecons]") {
    MAKE_ENV("col_check");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, age bigint CHECK (age > 0));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, age) VALUES (1, 5);")->is_success());
    INFO("a row violating an inline CHECK must be rejected");
    CHECK(exec("INSERT INTO i.t (id, age) VALUES (2, -1);")->is_error());
}

TEST_CASE("integration::cpp::test_inline_constraints::table_primary_key", "[.][inlinecons]") {
    MAKE_ENV("tbl_pk");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, v bigint, PRIMARY KEY (id));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 10);")->is_success());
    INFO("a duplicate of a table-level PRIMARY KEY must be rejected");
    CHECK(exec("INSERT INTO i.t (id, v) VALUES (1, 20);")->is_error());
}

TEST_CASE("integration::cpp::test_inline_constraints::table_unique", "[.][inlinecons]") {
    MAKE_ENV("tbl_unique");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, code bigint, UNIQUE (code));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, code) VALUES (1, 100);")->is_success());
    INFO("a duplicate of a table-level UNIQUE must be rejected");
    CHECK(exec("INSERT INTO i.t (id, code) VALUES (2, 100);")->is_error());
}

TEST_CASE("integration::cpp::test_inline_constraints::table_check", "[.][inlinecons]") {
    MAKE_ENV("tbl_check");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, age bigint, CHECK (age > 0));")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, age) VALUES (1, 5);")->is_success());
    INFO("a row violating a table-level CHECK must be rejected");
    CHECK(exec("INSERT INTO i.t (id, age) VALUES (2, -1);")->is_error());
}

TEST_CASE("integration::cpp::test_inline_constraints::table_foreign_key", "[.][inlinecons]") {
    MAKE_ENV("tbl_fk");
    REQUIRE(exec("CREATE TABLE i.parent (id bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE i.parent ADD CONSTRAINT ppk PRIMARY KEY (id);")->is_success());
    REQUIRE(exec("INSERT INTO i.parent (id) VALUES (1);")->is_success());
    REQUIRE(exec("CREATE TABLE i.child (id bigint, pid bigint, FOREIGN KEY (pid) REFERENCES i.parent (id));")
                ->is_success());
    REQUIRE(exec("INSERT INTO i.child (id, pid) VALUES (10, 1);")->is_success());
    INFO("a child row pointing at a missing parent must be rejected");
    CHECK(exec("INSERT INTO i.child (id, pid) VALUES (11, 999);")->is_error());
}

// The control: the same constraint added the long way round IS enforced, which is what makes the
// cases above defects rather than an unimplemented feature.
TEST_CASE("integration::cpp::test_inline_constraints::alter_table_control", "[.][inlinecons]") {
    MAKE_ENV("alter_control");
    REQUIRE(exec("CREATE TABLE i.t (id bigint, v bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE i.t ADD CONSTRAINT t_pk PRIMARY KEY (id);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 10);")->is_success());
    REQUIRE(exec("INSERT INTO i.t (id, v) VALUES (1, 20);")->is_error());
}
