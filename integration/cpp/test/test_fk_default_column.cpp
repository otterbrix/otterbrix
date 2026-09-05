// ============================================================================
// AN INSERT THAT DOES NOT NAME THE FOREIGN KEY COLUMN IS STILL BOUND BY IT.
//
// operator_fk_check addresses the referencing columns BY POSITION in the chunk
// the DML just wrote (fk_info_t::child_col_indices). Those positions are
// resolved in enrich, and the INSERT branch resolved them against the
// statement's own column list only. A column the statement did not name is not
// in that list — it is DEFAULT-expanded by operator_insert::push() and appended
// to the chunk AFTER the named ones — so its position came back "absent".
//
// An absent position then took the operator's quietest path: the row qualified
// for no parent lookup, every row was skipped, the qualifying count stayed 0 —
// and 0 is the operator's SUCCESS path. So `pid bigint DEFAULT 42` with no
// parent row 42 was inserted without a word, leaving a child row referencing a
// parent that does not exist. The check has to see the row that was actually
// stored, whichever half of the statement produced the value.
//
// The same statement declared inline in CREATE TABLE takes the same path, so
// both declaration forms are covered.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config(integration_fixture_path("test_fk_default_column/" + dir))) {
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
    REQUIRE(exec("CREATE DATABASE f;")->is_success());                                                                 \
    REQUIRE(exec("CREATE TABLE f.parent (id bigint, val text);")->is_success());                                       \
    REQUIRE(exec("INSERT INTO f.parent (id, val) VALUES (1, 'p1');")->is_success())

TEST_CASE("integration::cpp::fk_default_column::alter_declared_fk_sees_the_defaulted_key", "[fkdefault]") {
    MAKE_ENV("alter");
    REQUIRE(exec("CREATE TABLE f.child (id bigint, pid bigint DEFAULT 42);")->is_success());
    REQUIRE(exec("ALTER TABLE f.child ADD CONSTRAINT fk_pid "
                 "FOREIGN KEY (pid) REFERENCES f.parent (id);")
                ->is_success());

    INFO("naming the key column: the orphan is caught today");
    CHECK(exec("INSERT INTO f.child (id, pid) VALUES (10, 999);")->is_error());

    INFO("omitting it: DEFAULT 42 has no parent row either, and it is the SAME constraint");
    CHECK(exec("INSERT INTO f.child (id) VALUES (11);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 0);

    INFO("a key that does resolve still goes in, named or defaulted");
    CHECK(exec("INSERT INTO f.child (id, pid) VALUES (12, 1);")->is_success());
    CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 1);
}

TEST_CASE("integration::cpp::fk_default_column::inline_declared_fk_sees_the_defaulted_key", "[fkdefault]") {
    MAKE_ENV("inline");
    REQUIRE(exec("CREATE TABLE f.child (id bigint, pid bigint DEFAULT 42, "
                 "FOREIGN KEY (pid) REFERENCES f.parent (id));")
                ->is_success());

    INFO("the inline declaration is the same constraint and must catch the same orphan");
    CHECK(exec("INSERT INTO f.child (id) VALUES (11);")->is_error());
    CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 0);
}

// The green half: a DEFAULT that names a parent row that EXISTS must still be
// accepted, and must be stored. Without this the fix above could be "reject every
// insert that omits the key column", which is not enforcement either.
TEST_CASE("integration::cpp::fk_default_column::a_resolvable_default_is_accepted", "[fkdefault]") {
    MAKE_ENV("resolvable");
    REQUIRE(exec("CREATE TABLE f.child (id bigint, pid bigint DEFAULT 1);")->is_success());
    REQUIRE(exec("ALTER TABLE f.child ADD CONSTRAINT fk_pid "
                 "FOREIGN KEY (pid) REFERENCES f.parent (id);")
                ->is_success());

    CHECK(exec("INSERT INTO f.child (id) VALUES (20);")->is_success());
    CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM f.child WHERE pid = 1;") == 1);
}

// A NULL foreign key is not a violation (MATCH SIMPLE): a column the statement
// omitted that has NO default is filled with NULL, and the row must go in.
TEST_CASE("integration::cpp::fk_default_column::an_omitted_key_without_a_default_is_null", "[fkdefault]") {
    MAKE_ENV("null_key");
    REQUIRE(exec("CREATE TABLE f.child (id bigint, pid bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE f.child ADD CONSTRAINT fk_pid "
                 "FOREIGN KEY (pid) REFERENCES f.parent (id);")
                ->is_success());

    CHECK(exec("INSERT INTO f.child (id) VALUES (30);")->is_success());
    CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 1);
    CHECK(count_of("SELECT COUNT(*) FROM f.child WHERE pid IS NULL;") == 1);
}
