#include "test_config.hpp"
#include <components/types/operations_helper.hpp>
#include <core/operations_helper.hpp>

#include <catch2/catch.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";
static const collection_name_t copy_collection_name = "copytestcollection";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

TEST_CASE("integration::cpp::test_collection::sql::base") {
    auto config = test_create_config("/tmp/test_collection_sql/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
    }

    INFO("insert") {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("schema") {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "CREATE TABLE TestDatabase.TestCollection1(field1 string, field2 int[10]);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair("testdatabase", "testcollection"), std::make_pair("testdatabase", "testcollection1")});

            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            auto computed = cur->type_data()[0];
            auto stated = cur->type_data()[1];

            REQUIRE(types::complex_logical_type::contains(computed, [](const types::complex_logical_type& type) {
                return type.alias() == "name" && type.type() == types::logical_type::STRING_LITERAL;
            }));
            REQUIRE(types::complex_logical_type::contains(computed, [](const types::complex_logical_type& type) {
                return type.alias() == "count" && type.type() == types::logical_type::BIGINT;
            }));

            REQUIRE(types::complex_logical_type::contains(stated, [](const types::complex_logical_type& type) {
                return type.alias() == "field1" && type.type() == types::logical_type::STRING_LITERAL;
            }));
            REQUIRE(types::complex_logical_type::contains(stated, [](const types::complex_logical_type& type) {
                if (type.type() != types::logical_type::ARRAY) {
                    return false;
                }
                auto array = static_cast<types::array_logical_type_extension*>(type.extension());
                return type.alias() == "field2" && array->internal_type() == types::logical_type::INTEGER &&
                       array->size() == 10;
            }));
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
    }

    INFO("find order by") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 0);
            REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 1);
            REQUIRE(cur->chunk_data().value(1, 2).value<int64_t>() == 2);
            REQUIRE(cur->chunk_data().value(1, 3).value<int64_t>() == 3);
            REQUIRE(cur->chunk_data().value(1, 4).value<int64_t>() == 4);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 99);
            REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 98);
            REQUIRE(cur->chunk_data().value(1, 2).value<int64_t>() == 97);
            REQUIRE(cur->chunk_data().value(1, 3).value<int64_t>() == 96);
            REQUIRE(cur->chunk_data().value(1, 4).value<int64_t>() == 95);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY name;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 0);
            REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 1);
            REQUIRE(cur->chunk_data().value(1, 2).value<int64_t>() == 10);
            REQUIRE(cur->chunk_data().value(1, 3).value<int64_t>() == 11);
            REQUIRE(cur->chunk_data().value(1, 4).value<int64_t>() == 12);
        }
    }

    INFO("comparison operators") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count != 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 99);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count >= 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count <= 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 11);
        }
    }

    INFO("standalone aggregates") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(count) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT MIN(count) AS min_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT MAX(count) AS max_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 99);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT SUM(count) AS sum_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 4950);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT AVG(count) AS avg_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 49);
        }
    }

    INFO("filtered aggregates") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT COUNT(count) AS cnt FROM TestDatabase.TestCollection "
                                               "WHERE count <= 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 11);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT MIN(count) AS min_val FROM TestDatabase.TestCollection "
                                               "WHERE count > 80;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 81);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT MAX(count) AS max_val FROM TestDatabase.TestCollection "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT SUM(count) AS sum_val FROM TestDatabase.TestCollection "
                                               "WHERE count < 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 45);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT AVG(count) AS avg_val FROM TestDatabase.TestCollection "
                                               "WHERE count < 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 4);
        }
    }

    INFO("select with limit") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "LIMIT 5;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "LIMIT 1;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection "
                                               "SET count = 1000 "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count == 1000;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
    }
}

TEST_CASE("integration::cpp::test_collection::sql::group_by") {
    auto config = test_create_config("/tmp/test_collection_sql/group_by");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
            }
            dispatcher->execute_sql(session, query.str());
        }
    }

    INFO("group by") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT name, COUNT(count) AS count_, )_"
                                           R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                           R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY name;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        for (size_t number = 0; number < 10; ++number) {
            REQUIRE(cur->chunk_data().value(0, number).value<std::string_view>() == "Name " + std::to_string(number));
            REQUIRE(cur->chunk_data().value(1, number).value<uint64_t>() == 10);
            REQUIRE(cur->chunk_data().value(2, number).value<int64_t>() ==
                    5 * (static_cast<int64_t>(number) % 20) + 5 * ((static_cast<int64_t>(number) + 10) % 20));
            REQUIRE(cur->chunk_data().value(3, number).value<int64_t>() ==
                    static_cast<int64_t>((number % 20 + (number + 10) % 20)) / 2);
            REQUIRE(cur->chunk_data().value(4, number).value<int64_t>() == static_cast<int64_t>(number) % 20);
            REQUIRE(cur->chunk_data().value(5, number).value<int64_t>() == (static_cast<int64_t>(number) + 10) % 20);
        }
    }

    INFO("group by with order by") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT name, COUNT(count) AS count_, )_"
                                           R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                           R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY name )_"
                                           R"_(ORDER BY name DESC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        for (size_t row = 0; row < 10; ++row) {
            int number = 9 - static_cast<int>(row);
            REQUIRE(cur->chunk_data().value(0, row).value<std::string_view>() == "Name " + std::to_string(number));
            REQUIRE(cur->chunk_data().value(1, row).value<uint64_t>() == 10);
            REQUIRE(cur->chunk_data().value(2, row).value<int64_t>() == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(cur->chunk_data().value(3, row).value<int64_t>() ==
                    static_cast<int64_t>((number % 20 + (number + 10) % 20)) / 2);
            REQUIRE(cur->chunk_data().value(4, row).value<int64_t>() == number % 20);
            REQUIRE(cur->chunk_data().value(5, row).value<int64_t>() == (number + 10) % 20);
        }
    }

    INFO("group by unique field") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, COUNT(name) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    // TODO: fix
    /*
    INFO("unknown function") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT name, UNREGISTERED_FUNCTION(count) AS result )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY name )_"
                                           R"_(ORDER BY name DESC;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == cursor::error_code_t::unrecognized_function);
    }
    */
}

TEST_CASE("integration::cpp::test_collection::sql::invalid_queries") {
    auto config = test_create_config("/tmp/test_collection_sql/invalid_queries");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("not exists database") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == cursor::error_code_t::database_not_exists);
    }

    INFO("create database") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, R"_(CREATE DATABASE TestDatabase;)_");
    }

    INFO("not exists database") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == cursor::error_code_t::collection_not_exists);
    }
}

TEST_CASE("integration::cpp::test_collection::sql::index") {
    auto config = test_create_config("/tmp/test_collection_sql/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
    }

    INFO("create index before insert") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX base_name ON TestDatabase.TestCollection (name);");
        REQUIRE(cur->is_success());
    }

    INFO("insert") {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("create_index") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX base_count ON TestDatabase.TestCollection (count);");
        REQUIRE(cur->is_success());
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("find with limit") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90 LIMIT 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90 LIMIT 1;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("drop") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DROP INDEX TestDatabase.TestCollection.base_name;");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DROP INDEX TestDatabase.TestCollection.base_count;");
            REQUIRE(cur->is_success());
        }
    }
}

TEST_CASE("integration::cpp::test_collection::sql::udt") {
    auto config = test_create_config("/tmp/test_collection_sql/udt");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("register types") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, R"_(CREATE TYPE custom_type_field AS (f1 float, f2 int);)_");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                R"_(CREATE TYPE custom_type_name AS (f1 int, f2 string, f3 custom_type_field);)_");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, R"_(CREATE TYPE custom_enum AS ENUM ('odd', 'even');)_");
            REQUIRE(cur->is_success());
        }
    }

    INFO("create table") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, R"_(CREATE DATABASE TestDatabase;)_");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                R"_(CREATE TABLE TestDatabase.TestCollection (custom_type custom_type_name, oddness custom_enum );)_");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                R"_(CREATE TABLE TestDatabase.CopyTestCollection (custom_type custom_type_name, oddness custom_enum);)_");
            REQUIRE(cur->is_success());
        }
    }

    INFO("insert") {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (custom_type, oddness) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "(ROW(" << num << ", '"
                      << "text_" << num + 1 << "', ROW(" << static_cast<float>(num) + 0.5f << ", " << num * 2 << ")), "
                      << (num % 2 == 0 ? R"_('even')_" : R"_('odd')_") << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                R"_(INSERT INTO TestDatabase.CopyTestCollection SELECT * FROM TestDatabase.TestCollection ORDER BY (custom_type).f1 DESC;)_");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, copy_collection_name) == 100);
        }
    }

    INFO("order by nested udt field") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDatabase.TestCollection ORDER BY (custom_type).f1 DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            for (size_t i = 0; i < 100; ++i) {
                REQUIRE(cur->chunk_data().value(0, i).children()[0] ==
                        types::logical_value_t{dispatcher->resource(), static_cast<int32_t>(99 - i)});
            }
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDatabase.TestCollection ORDER BY (custom_type).f1 ASC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            for (size_t i = 0; i < 100; ++i) {
                REQUIRE(cur->chunk_data().value(0, i).children()[0] ==
                        types::logical_value_t{dispatcher->resource(), static_cast<int32_t>(i)});
            }
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE (custom_type).f1 > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
            REQUIRE(cur->chunk_data().column_count() == 2);
            REQUIRE(cur->chunk_data().value(0, 0).children()[0] == types::logical_value_t{dispatcher->resource(), 91});
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT (custom_type).* FROM TestDatabase.TestCollection "
                                               "WHERE (custom_type).f1 > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
            REQUIRE(cur->chunk_data().column_count() == 3);
            REQUIRE(cur->chunk_data().data[0].type().alias() == "f1");
            REQUIRE(cur->chunk_data().data[1].type().alias() == "f2");
            REQUIRE(cur->chunk_data().data[2].type().alias() == "f3");
            REQUIRE(cur->chunk_data().value(0, 0) == types::logical_value_t{dispatcher->resource(), 91});
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT (custom_type).* FROM TestDatabase.TestCollection "
                                               "WHERE ((custom_type).f3).f2 > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 54);
            REQUIRE(cur->chunk_data().column_count() == 3);
            REQUIRE(cur->chunk_data().data[0].type().alias() == "f1");
            REQUIRE(cur->chunk_data().data[1].type().alias() == "f2");
            REQUIRE(cur->chunk_data().data[2].type().alias() == "f3");
            REQUIRE(cur->chunk_data().value(2, 0).children()[1] == types::logical_value_t{dispatcher->resource(), 92});
        }
    }
    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "UPDATE TestDatabase.TestCollection SET custom_type.f3.f1 = custom_type.f3.f1 * 3.0;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT ((custom_type).f3).f1 FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->chunk_data().column_count() == 1);
            for (size_t num = 0; num < 100; ++num) {
                REQUIRE(core::is_equals(cur->chunk_data().value(0, num).value<float>(),
                                        (static_cast<float>(num) + 0.5f) * 3.0f));
            }
        }
    }
    INFO("join") {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT * FROM TestDatabase.TestCollection"
                                        " JOIN TestDatabase.CopyTestCollection ON"
                                        " TestCollection.custom_type.f3.f1 = CopyTestCollection.custom_type.f3.f1");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 33);
        }
    }
    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "DELETE FROM TestDatabase.TestCollection WHERE ((custom_type).f3).f2 < 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 45);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 55);
        }
    }
    INFO("group by non-existent field") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT no_such_field, COUNT(count) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY no_such_field;");
        REQUIRE(cur->is_error());
    }
}