// operator_fk_check addresses referencing columns BY POSITION in the DML chunk (fk_info_t::child_col_indices),
// resolved in enrich against the statement's own column list; a DEFAULT-expanded column is appended AFTER
// the named ones by operator_insert::push(), so its position came back absent and every such row took the
// operator's quiet zero-qualifying-rows success path instead of being checked.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config(integration_fixture_path("test_fk_default_column/" + dir))) {
            test_clear_directory(config);
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

// Guards against an overcorrection that rejects every omitted key instead of checking it.
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

// MATCH SIMPLE: an omitted key with no default is NULL, not a violation.
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
