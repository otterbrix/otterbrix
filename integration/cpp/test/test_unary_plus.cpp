#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

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
            REQUIRE(
                dispatcher
                    ->execute_sql(session, "INSERT INTO db.t (id, v, s) VALUES (1, 7, 'a'), (2, 3, 'b'), (3, 10, 'c');")
                    ->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::unary_plus::select_column_identity") {
    auto config = test_create_config(integration_fixture_path("test_unary_plus/select_identity"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT +v FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell0 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    REQUIRE(cell0.value() == 7);
    auto cell1 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell1.has_error());
    REQUIRE(cell1.value() == 3);
    auto cell2 = cur->get<int64_t>(0, 2);
    REQUIRE_FALSE(cell2.has_error());
    REQUIRE(cell2.value() == 10);
}

TEST_CASE("integration::cpp::unary_plus::select_text_identity") {
    auto config = test_create_config(integration_fixture_path("test_unary_plus/select_text"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // +s used to reach the numeric negation kernel and kill the process.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT +s FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell3 = cur->get<std::string_view>(0, 0);
    REQUIRE_FALSE(cell3.has_error());
    REQUIRE(cell3.value() == "a");
    auto cell4 = cur->get<std::string_view>(0, 1);
    REQUIRE_FALSE(cell4.has_error());
    REQUIRE(cell4.value() == "b");
    auto cell5 = cur->get<std::string_view>(0, 2);
    REQUIRE_FALSE(cell5.has_error());
    REQUIRE(cell5.value() == "c");
}

TEST_CASE("integration::cpp::unary_plus::nested_layers") {
    auto config = test_create_config(integration_fixture_path("test_unary_plus/nested"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT ++v FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell6 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == 7);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT -(+v) FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell7 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell7.has_error());
        REQUIRE(cell7.value() == -7);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v + (+v) FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell8 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell8.has_error());
        REQUIRE(cell8.value() == 14);
    }
}

TEST_CASE("integration::cpp::unary_plus::where_operand_identity") {
    auto config = test_create_config(integration_fixture_path("test_unary_plus/where_operand"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // With + lowered to negation, WHERE +v > 5 filtered on -v > 5 and matched nothing.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE +v > 5;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell9 = cur->get<int32_t>(0, 0);
    REQUIRE_FALSE(cell9.has_error());
    REQUIRE(cell9.value() == 1);
    auto cell10 = cur->get<int32_t>(0, 1);
    REQUIRE_FALSE(cell10.has_error());
    REQUIRE(cell10.value() == 3);
}

TEST_CASE("integration::cpp::unary_plus::aggregate_argument_identity") {
    auto config = test_create_config(integration_fixture_path("test_unary_plus/aggregate_arg"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT SUM(+v) FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell11 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell11.has_error());
    REQUIRE(cell11.value() == 20);
}

TEST_CASE("integration::cpp::unary_plus::computing_table_identity") {
    auto config = test_create_config(integration_fixture_path("test_unary_plus/computing"));
    test_clear_directory(config);
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
    auto cell12 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell12.has_error());
    REQUIRE(cell12.value() == 7);
    auto cell13 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell13.has_error());
    REQUIRE(cell13.value() == 3);
}
