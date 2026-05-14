#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

TEST_CASE("integration::cpp::test_collection") {
    auto resource = std::pmr::synchronized_pool_resource();

    auto config = test_create_config("/tmp/test_collection");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                fmt::format("CREATE TABLE {}.{}(count bigint, count_str string, "
                            "count_double double, count_bool bool, count_array ubigint[5], "
                            "count_decimal decimal(15,7));",
                            database_name, collection_name));
        }
    }

    INFO("insert") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session, gen_data_chunk_insert_sql(database_name, collection_name, 50));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }
    }

    INFO("insert_more") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session, gen_data_chunk_insert_sql(database_name, collection_name, 50, 50));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{};", database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90;", database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count_str LIKE '%9';", database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90 OR count_str LIKE '%9';",
                            database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE (count > 90 OR count_str LIKE '%9') AND count <= 30;",
                            database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }
    INFO("cursor") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("SELECT * FROM {}.{};", database_name, collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100);
    }
    INFO("find_one") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count_str = '1' LIMIT 1;",
                            database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count = 10 LIMIT 1;",
                            database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90 AND count_str LIKE '%9' "
                            "ORDER BY count DESC LIMIT 1;",
                            database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 99);
        }
    }
    INFO("drop_collection") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session, fmt::format("DROP TABLE {}.{};", database_name, collection_name));
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session, fmt::format("DROP TABLE {}.{};", database_name, collection_name));
            REQUIRE(cur->is_error());
        }
    }
}
