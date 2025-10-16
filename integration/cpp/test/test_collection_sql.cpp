#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <variant>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

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
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1, dispatcher->resource()) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
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
                return type.alias() == "_id" && type.type() == logical_type::STRING_LITERAL;
            }));
            REQUIRE(types::complex_logical_type::contains(computed, [](const types::complex_logical_type& type) {
                return type.alias() == "name" && type.type() == logical_type::STRING_LITERAL;
            }));
            REQUIRE(types::complex_logical_type::contains(computed, [](const types::complex_logical_type& type) {
                return type.alias() == "count" && type.type() == logical_type::BIGINT;
            }));

            REQUIRE(types::complex_logical_type::contains(stated, [](const types::complex_logical_type& type) {
                return type.alias() == "field1" && type.type() == logical_type::STRING_LITERAL;
            }));
            REQUIRE(types::complex_logical_type::contains(stated, [](const types::complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY) {
                    return false;
                }
                auto array = static_cast<types::array_logical_type_extension*>(type.extension());
                return type.alias() == "field2" && array->internal_type() == logical_type::INTEGER &&
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
            REQUIRE(cur->next_document()->get_long("count") == 0);
            REQUIRE(cur->next_document()->get_long("count") == 1);
            REQUIRE(cur->next_document()->get_long("count") == 2);
            REQUIRE(cur->next_document()->get_long("count") == 3);
            REQUIRE(cur->next_document()->get_long("count") == 4);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->next_document()->get_long("count") == 99);
            REQUIRE(cur->next_document()->get_long("count") == 98);
            REQUIRE(cur->next_document()->get_long("count") == 97);
            REQUIRE(cur->next_document()->get_long("count") == 96);
            REQUIRE(cur->next_document()->get_long("count") == 95);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY name;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->next_document()->get_long("count") == 0);
            REQUIRE(cur->next_document()->get_long("count") == 1);
            REQUIRE(cur->next_document()->get_long("count") == 10);
            REQUIRE(cur->next_document()->get_long("count") == 11);
            REQUIRE(cur->next_document()->get_long("count") == 12);
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
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1, dispatcher->resource()) << "', "
                      << "'Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
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
        int number = 0;
        while (auto doc = cur->next_document()) {
            REQUIRE(doc->get_string("name") == std::pmr::string("Name " + std::to_string(number)));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_double("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            ++number;
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
        int number = 9;
        while (auto doc = cur->next_document()) {
            REQUIRE(doc->get_string("name") == std::pmr::string("Name " + std::to_string(number)));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_double("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            --number;
        }
    }
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
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1, dispatcher->resource()) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
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
