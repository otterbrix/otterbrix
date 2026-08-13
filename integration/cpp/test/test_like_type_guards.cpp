#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>

// A non-string LIKE pattern constant is rejected at transform time ("LIKE: right side
// must be a string"). These cases pin the rejection across every position that lowers
// LIKE there — WHERE, CASE WHEN, JOIN ON, for LIKE / NOT LIKE / ILIKE, with integer and
// float patterns, over typed and computing tables — plus string-pattern controls.

namespace {

    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.t (id INT, s TEXT);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.t (id, s) VALUES (1, '1a'), (2, '2b');")
                        ->is_success());
        }
    }

    void check_error(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO(sql);
        REQUIRE(cur->is_error());
    }

} // namespace

TEST_CASE("integration::cpp::like_type_guards::non_string_pattern_is_error") {
    auto config = test_create_config("/tmp/test_like_type_guards/pattern");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    check_error(dispatcher, "SELECT * FROM db.t WHERE s LIKE 1;");
    check_error(dispatcher, "SELECT * FROM db.t WHERE s NOT LIKE 1;");
    check_error(dispatcher, "SELECT * FROM db.t WHERE s ILIKE 1;");
    check_error(dispatcher, "SELECT * FROM db.t WHERE s LIKE 1.5;");
    check_error(dispatcher, "SELECT * FROM db.t WHERE id LIKE 1;");
    check_error(dispatcher, "SELECT CASE WHEN s LIKE 1 THEN 1 ELSE 0 END FROM db.t;");
    check_error(dispatcher, "SELECT t1.id FROM db.t AS t1 JOIN db.t AS t2 ON t1.s LIKE 1;");
}

TEST_CASE("integration::cpp::like_type_guards::non_string_pattern_computing_table") {
    auto config = test_create_config("/tmp/test_like_type_guards/computing");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.c ();")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.c (s) VALUES ('x');")->is_success());
    }

    check_error(dispatcher, "SELECT * FROM db.c WHERE s LIKE 1;");
}

TEST_CASE("integration::cpp::like_type_guards::string_pattern_still_works") {
    auto config = test_create_config("/tmp/test_like_type_guards/control");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE s LIKE '1%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE s NOT LIKE '1%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 2);
    }
}
