#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {
    struct env_t {
        configuration::config config;
        explicit env_t(const std::string& dir)
            : config(test_create_config(integration_fixture_path("test_fk_positions_after_drop/" + dir))) {
            test_clear_directory(config);
            config.log.level = log_t::level::off;
        }
    };
} // namespace

#define MAKE_ENV(dirname)                                                                                              \
    env_t env(dirname);                                                                                                \
    test_spaces space(env.config);                                                                                     \
    auto* d = space.dispatcher();                                                                                      \
    auto session = otterbrix::session_id_t();                                                                          \
    auto exec = [&](const std::string& sql) { return d->execute_sql(session, sql); };                                  \
    auto count_of = [&](const std::string& sql) -> std::uint64_t {                                                     \
        auto cur = exec(sql);                                                                                          \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == 1);                                                                                     \
        return cur->value(0, 0).value<std::uint64_t>();                                                                \
    };                                                                                                                 \
    REQUIRE(exec("CREATE DATABASE f;")->is_success());                                                                 \
    REQUIRE(exec("CREATE TABLE f.parent (gone bigint, id bigint, alt bigint);")->is_success());                        \
    REQUIRE(exec("INSERT INTO f.parent (gone, id, alt) VALUES (0, 1, 100), (0, 2, 1);")->is_success());                \
    REQUIRE(exec("CREATE TABLE f.child (gone bigint, pid bigint, other bigint);")->is_success());                      \
    REQUIRE(                                                                                                           \
        exec("ALTER TABLE f.child ADD CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES f.parent (id);")->is_success());  \
    REQUIRE(exec("ALTER TABLE f.parent DROP COLUMN gone;")->is_success());                                             \
    REQUIRE(exec("ALTER TABLE f.child DROP COLUMN gone;")->is_success())

TEST_CASE("integration::cpp::fk_positions_after_drop::statements") {
    MAKE_ENV("statements");

    SECTION("INSERT") {
        CHECK(exec("INSERT INTO f.child (pid, other) VALUES (999, 1);")->is_error());
        CHECK(exec("INSERT INTO f.child (pid, other) VALUES (2, 999);")->is_success());
        CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 1);
    }
    SECTION("UPDATE") {
        REQUIRE(exec("INSERT INTO f.child (pid, other) VALUES (1, 1);")->is_success());
        CHECK(exec("UPDATE f.child SET pid = 999;")->is_error());
        CHECK(exec("UPDATE f.child SET pid = 2, other = 999;")->is_success());
        CHECK(count_of("SELECT COUNT(*) FROM f.child WHERE pid = 2;") == 1);
    }
    SECTION("DELETE of a referenced parent") {
        // The parent row id = 2 carries alt = 1, the key the child points at through id = 1
        REQUIRE(exec("INSERT INTO f.child (pid, other) VALUES (1, 0);")->is_success());
        CHECK(exec("DELETE FROM f.parent WHERE id = 1;")->is_error());
        CHECK(exec("DELETE FROM f.parent WHERE id = 2;")->is_success());
        CHECK(count_of("SELECT COUNT(*) FROM f.parent;") == 1);
    }
}

TEST_CASE("integration::cpp::fk_positions_after_drop::commit") {
    MAKE_ENV("commit");

    SECTION("child side reruns over the appended rows") {
        REQUIRE(exec("BEGIN;")->is_success());
        REQUIRE(exec("INSERT INTO f.child (pid, other) VALUES (2, 999);")->is_success());
        CHECK(exec("COMMIT;")->is_success());
        CHECK(count_of("SELECT COUNT(*) FROM f.child;") == 1);
    }
    SECTION("parent side reruns over the deleted rows") {
        REQUIRE(exec("INSERT INTO f.child (pid, other) VALUES (1, 0);")->is_success());
        REQUIRE(exec("BEGIN;")->is_success());
        REQUIRE(exec("DELETE FROM f.parent WHERE id = 2;")->is_success());
        CHECK(exec("COMMIT;")->is_success());
        CHECK(count_of("SELECT COUNT(*) FROM f.parent;") == 1);
    }
}
