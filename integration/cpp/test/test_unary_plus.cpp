#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>

// Unary plus is the identity (SQLite semantics: "+X is equivalent to X"). The transformer
// used to lower every no-left-operand arithmetic operator to unary minus, so `SELECT +v`
// silently returned -v and `SELECT +s` over TEXT crashed in the negation kernel.

namespace {

    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.t (id INT, v BIGINT, s TEXT);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "INSERT INTO db.t (id, v, s) VALUES (1, 7, 'a'), (2, 3, 'b'), (3, 10, 'c');")
                        ->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::unary_plus::select_column_identity") {
    auto config = test_create_config("/tmp/test_unary_plus/select_identity");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT +v FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    REQUIRE(cur->value(0, 0).value<int64_t>() == 7);
    REQUIRE(cur->value(0, 1).value<int64_t>() == 3);
    REQUIRE(cur->value(0, 2).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::unary_plus::select_text_identity") {
    auto config = test_create_config("/tmp/test_unary_plus/select_text");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // +s used to reach the numeric negation kernel and kill the process.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT +s FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    REQUIRE(cur->value(0, 0).value<std::string_view>() == "a");
    REQUIRE(cur->value(0, 1).value<std::string_view>() == "b");
    REQUIRE(cur->value(0, 2).value<std::string_view>() == "c");
}

TEST_CASE("integration::cpp::unary_plus::nested_layers") {
    auto config = test_create_config("/tmp/test_unary_plus/nested");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT ++v FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 7);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT -(+v) FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<int64_t>() == -7);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v + (+v) FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 14);
    }
}

TEST_CASE("integration::cpp::unary_plus::where_operand_identity") {
    auto config = test_create_config("/tmp/test_unary_plus/where_operand");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // With + lowered to negation, WHERE +v > 5 filtered on -v > 5 and matched nothing.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE +v > 5;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
    REQUIRE(cur->value(0, 1).value<int32_t>() == 3);
}

TEST_CASE("integration::cpp::unary_plus::aggregate_argument_identity") {
    auto config = test_create_config("/tmp/test_unary_plus/aggregate_arg");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT SUM(+v) FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    REQUIRE(cur->value(0, 0).value<int64_t>() == 20);
}

TEST_CASE("integration::cpp::unary_plus::computing_table_identity") {
    auto config = test_create_config("/tmp/test_unary_plus/computing");
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
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.c (v) VALUES (7), (3);")->is_success());
    }

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT +v FROM db.c;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    REQUIRE(cur->value(0, 0).value<int64_t>() == 7);
    REQUIRE(cur->value(0, 1).value<int64_t>() == 3);
}
