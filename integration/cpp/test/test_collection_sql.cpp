#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <components/types/operations_helper.hpp>
#include <core/operations_helper.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

static const core::dbname_t database_name{"testdatabase"};
static const core::relname_t collection_name{"testcollection"};

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

TEST_CASE("integration::cpp::test_collection::sql::base") {
    auto config = test_create_config(integration_fixture_path("test_collection_sql/base"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name.t + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name);
        }
    }

    INFO("insert");
    {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 100);
        }
    }

    INFO("schema");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "CREATE TABLE TestDatabase.TestCollection1(field1 string, field2 int[10]);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("find");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->column_count() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT *, * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->column_count() == 4);
            REQUIRE(cur->chunks().front().data[0].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[1].type().alias() == "count");
            REQUIRE(cur->chunks().front().data[2].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[3].type().alias() == "count");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT *, TestCollection.name, count, TestCollection.* FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->column_count() == 6);
            REQUIRE(cur->chunks().front().data[0].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[1].type().alias() == "count");
            REQUIRE(cur->chunks().front().data[2].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[3].type().alias() == "count");
            REQUIRE(cur->chunks().front().data[4].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[5].type().alias() == "count");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT *, table_alias.name, count, table_alias.* FROM TestDatabase.TestCollection AS table_alias;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->column_count() == 6);
            REQUIRE(cur->chunks().front().data[0].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[1].type().alias() == "count");
            REQUIRE(cur->chunks().front().data[2].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[3].type().alias() == "count");
            REQUIRE(cur->chunks().front().data[4].type().alias() == "name");
            REQUIRE(cur->chunks().front().data[5].type().alias() == "count");
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

    INFO("find order by");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            auto cell0 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell0.has_error());
            REQUIRE(cell0.value() == 0);
            auto cell1 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell1.has_error());
            REQUIRE(cell1.value() == 1);
            auto cell2 = cur->get<int64_t>(1, 2);
            REQUIRE_FALSE(cell2.has_error());
            REQUIRE(cell2.value() == 2);
            auto cell3 = cur->get<int64_t>(1, 3);
            REQUIRE_FALSE(cell3.has_error());
            REQUIRE(cell3.value() == 3);
            auto cell4 = cur->get<int64_t>(1, 4);
            REQUIRE_FALSE(cell4.has_error());
            REQUIRE(cell4.value() == 4);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            auto cell5 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell5.has_error());
            REQUIRE(cell5.value() == 99);
            auto cell6 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell6.has_error());
            REQUIRE(cell6.value() == 98);
            auto cell7 = cur->get<int64_t>(1, 2);
            REQUIRE_FALSE(cell7.has_error());
            REQUIRE(cell7.value() == 97);
            auto cell8 = cur->get<int64_t>(1, 3);
            REQUIRE_FALSE(cell8.has_error());
            REQUIRE(cell8.value() == 96);
            auto cell9 = cur->get<int64_t>(1, 4);
            REQUIRE_FALSE(cell9.has_error());
            REQUIRE(cell9.value() == 95);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY name;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            auto cell10 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell10.has_error());
            REQUIRE(cell10.value() == 0);
            auto cell11 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell11.has_error());
            REQUIRE(cell11.value() == 1);
            auto cell12 = cur->get<int64_t>(1, 2);
            REQUIRE_FALSE(cell12.has_error());
            REQUIRE(cell12.value() == 10);
            auto cell13 = cur->get<int64_t>(1, 3);
            REQUIRE_FALSE(cell13.has_error());
            REQUIRE(cell13.value() == 11);
            auto cell14 = cur->get<int64_t>(1, 4);
            REQUIRE_FALSE(cell14.has_error());
            REQUIRE(cell14.value() == 12);
        }
    }

    INFO("comparison operators");
    {
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

    INFO("standalone aggregates");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(count) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell15 = cur->get<uint64_t>(0, 0);
            REQUIRE_FALSE(cell15.has_error());
            REQUIRE(cell15.value() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT MIN(count) AS min_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell16 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell16.has_error());
            REQUIRE(cell16.value() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT MAX(count) AS max_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell17 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell17.has_error());
            REQUIRE(cell17.value() == 99);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT SUM(count) AS sum_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell18 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell18.has_error());
            REQUIRE(cell18.value() == 4950);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT AVG(count) AS avg_val FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            // AVG over an integer column answers DOUBLE: 4950 / 100
            auto cell19 = cur->get<double>(0, 0);
            REQUIRE_FALSE(cell19.has_error());
            REQUIRE(cell19.value() == Catch::Approx(49.5));
        }
    }

    INFO("filtered aggregates");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT COUNT(count) AS cnt FROM TestDatabase.TestCollection "
                                               "WHERE count <= 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell20 = cur->get<uint64_t>(0, 0);
            REQUIRE_FALSE(cell20.has_error());
            REQUIRE(cell20.value() == 11);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT MIN(count) AS min_val FROM TestDatabase.TestCollection "
                                               "WHERE count > 80;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell21 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell21.has_error());
            REQUIRE(cell21.value() == 81);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT MAX(count) AS max_val FROM TestDatabase.TestCollection "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell22 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell22.has_error());
            REQUIRE(cell22.value() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT SUM(count) AS sum_val FROM TestDatabase.TestCollection "
                                               "WHERE count < 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell23 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell23.has_error());
            REQUIRE(cell23.value() == 45);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT AVG(count) AS avg_val FROM TestDatabase.TestCollection "
                                               "WHERE count < 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell24 = cur->get<double>(0, 0);
            REQUIRE_FALSE(cell24.has_error());
            REQUIRE(cell24.value() == Catch::Approx(4.5));
        }
    }

    INFO("select with limit");
    {
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

    INFO("select with offset");
    {
        {
            // OFFSET 0 is same as no offset: first 5 rows by count order
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count LIMIT 5 OFFSET 0;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
            auto cell25 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell25.has_error());
            REQUIRE(cell25.value() == 0);
            auto cell26 = cur->get<int64_t>(1, 4);
            REQUIRE_FALSE(cell26.has_error());
            REQUIRE(cell26.value() == 4);
        }
        {
            // Skip first 5 rows, take next 5: count values 5..9
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count LIMIT 5 OFFSET 5;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
            auto cell27 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell27.has_error());
            REQUIRE(cell27.value() == 5);
            auto cell28 = cur->get<int64_t>(1, 4);
            REQUIRE_FALSE(cell28.has_error());
            REQUIRE(cell28.value() == 9);
        }
        {
            // Last 5 rows: count values 95..99
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count LIMIT 5 OFFSET 95;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
            auto cell29 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell29.has_error());
            REQUIRE(cell29.value() == 95);
            auto cell30 = cur->get<int64_t>(1, 4);
            REQUIRE_FALSE(cell30.has_error());
            REQUIRE(cell30.value() == 99);
        }
        {
            // Offset past end of result set
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count LIMIT 5 OFFSET 100;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("delete");
    {
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
            REQUIRE(cur->affected_rows() == 9);
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

    INFO("update");
    {
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
            REQUIRE(cur->affected_rows() == 20);
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
    auto config = test_create_config(integration_fixture_path("test_collection_sql/group_by"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name.t + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name);
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

    INFO("group by");
    {
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
            auto cell31 = cur->get<std::string_view>(0, number);
            REQUIRE_FALSE(cell31.has_error());
            REQUIRE(cell31.value() == "Name " + std::to_string(number));
            auto cell32 = cur->get<uint64_t>(1, number);
            REQUIRE_FALSE(cell32.has_error());
            REQUIRE(cell32.value() == 10);
            auto cell33 = cur->get<int64_t>(2, number);
            REQUIRE_FALSE(cell33.has_error());
            REQUIRE(cell33.value() ==
                    5 * (static_cast<int64_t>(number) % 20) + 5 * ((static_cast<int64_t>(number) + 10) % 20));
            auto cell34 = cur->get<double>(3, number);
            REQUIRE_FALSE(cell34.has_error());
            REQUIRE(cell34.value() ==
                    Catch::Approx(static_cast<double>(number % 20 + (number + 10) % 20) / 2));
            auto cell35 = cur->get<int64_t>(4, number);
            REQUIRE_FALSE(cell35.has_error());
            REQUIRE(cell35.value() == static_cast<int64_t>(number) % 20);
            auto cell36 = cur->get<int64_t>(5, number);
            REQUIRE_FALSE(cell36.has_error());
            REQUIRE(cell36.value() == (static_cast<int64_t>(number) + 10) % 20);
        }
    }

    INFO("group by with order by");
    {
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
            auto cell37 = cur->get<std::string_view>(0, row);
            REQUIRE_FALSE(cell37.has_error());
            REQUIRE(cell37.value() == "Name " + std::to_string(number));
            auto cell38 = cur->get<uint64_t>(1, row);
            REQUIRE_FALSE(cell38.has_error());
            REQUIRE(cell38.value() == 10);
            auto cell39 = cur->get<int64_t>(2, row);
            REQUIRE_FALSE(cell39.has_error());
            REQUIRE(cell39.value() == 5 * (number % 20) + 5 * ((number + 10) % 20));
            auto cell40 = cur->get<double>(3, row);
            REQUIRE_FALSE(cell40.has_error());
            REQUIRE(cell40.value() ==
                    Catch::Approx(static_cast<double>(number % 20 + (number + 10) % 20) / 2));
            auto cell41 = cur->get<int64_t>(4, row);
            REQUIRE_FALSE(cell41.has_error());
            REQUIRE(cell41.value() == number % 20);
            auto cell42 = cur->get<int64_t>(5, row);
            REQUIRE_FALSE(cell42.has_error());
            REQUIRE(cell42.value() == (number + 10) % 20);
        }
    }

    INFO("group by unique field");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, COUNT(name) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("unknown function");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT name, UNREGISTERED_FUNCTION(count) AS result )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY name )_"
                                           R"_(ORDER BY name DESC;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::unrecognized_function);
    }
}

TEST_CASE("integration::cpp::test_collection::sql::invalid_queries") {
    auto config = test_create_config(integration_fixture_path("test_collection_sql/invalid_queries"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("not exists database");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::database_not_exists);
    }

    INFO("create database");
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, R"_(CREATE DATABASE TestDatabase;)_");
    }

    INFO("not exists database");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::table_not_exists);
    }
}

TEST_CASE("integration::cpp::test_collection::sql::index") {
    auto config = test_create_config(integration_fixture_path("test_collection_sql/base"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name.t + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name);
        }
    }

    INFO("create index before insert");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX base_name ON TestDatabase.TestCollection (name);");
        REQUIRE(cur->is_error());
    }

    INFO("insert");
    {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 100);
        }
    }

    INFO("create_index");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE INDEX base_name ON TestDatabase.TestCollection (name);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE INDEX base_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("find");
    {
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

    INFO("find with limit");
    {
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

    INFO("find with limit and offset");
    {
        {
            // count > 90 → 9 rows (91..99); skip 2 (91,92), take 3 → count values 93,94,95
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90 ORDER BY count LIMIT 3 OFFSET 2;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            auto cell43 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell43.has_error());
            REQUIRE(cell43.value() == 93);
            auto cell44 = cur->get<int64_t>(1, 2);
            REQUIRE_FALSE(cell44.has_error());
            REQUIRE(cell44.value() == 95);
        }
        {
            // count > 90 → 9 rows (91..99); skip 8 (91..98), take 5 → only 1 remaining (99)
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90 ORDER BY count LIMIT 5 OFFSET 8;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell45 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell45.has_error());
            REQUIRE(cell45.value() == 99);
        }
    }

    INFO("drop");
    {
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
    auto config = test_create_config(integration_fixture_path("test_collection_sql/udt"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("register types");
    {
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

    INFO("create table");
    {
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

    INFO("insert");
    {
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
            REQUIRE(cur->affected_rows() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                R"_(INSERT INTO TestDatabase.CopyTestCollection SELECT * FROM TestDatabase.TestCollection ORDER BY (custom_type).f1 DESC;)_");
            REQUIRE(cur->is_success());
        }
    }

    INFO("compare on enum column");
    {
        // TODO: push-down filter for ENUM is broken — the planner pushes the WHERE into full_scan as a
        // constant_filter_t, and storage segments hand the comparator a raw int32_t (fixed_size_check_row<int32_t>)
        // wrapped as a typeless INTEGER value. The ENUM extension is gone by then. Fix needs the column's logical_type
        // threaded into constant_filter_t at construction so the literal is pre-resolved into the ordinal once.
        // Until that lands, plain `WHERE enum_col = 'label'` against a single-table scan returns 0 rows;
        // the JOIN wrapper here is the workaround.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDatabase.TestCollection WHERE oddness = 'even'::custom_enum;");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 50);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT t.* FROM TestDatabase.TestCollection t "
                                               "INNER JOIN TestDatabase.CopyTestCollection c "
                                               "        ON (t.custom_type).f1 = (c.custom_type).f1 "
                                               "WHERE t.oddness = 'even'::custom_enum;");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 50);
        }
    }

    INFO("order by nested udt field");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT * FROM TestDatabase.TestCollection ORDER BY (custom_type).f1 DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            for (size_t i = 0; i < 100; ++i) {
                REQUIRE(cur->type_data()[0].child_types()[0].type() == types::logical_type::INTEGER);
                auto cell47 = cur->get<int32_t>(0, i, {0});
                REQUIRE_FALSE(cell47.has_error());
                REQUIRE(cell47.value() == static_cast<int32_t>(99 - i));
            }
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT * FROM TestDatabase.TestCollection ORDER BY (custom_type).f1 ASC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            for (size_t i = 0; i < 100; ++i) {
                REQUIRE(cur->type_data()[0].child_types()[0].type() == types::logical_type::INTEGER);
                auto cell48 = cur->get<int32_t>(0, i, {0});
                REQUIRE_FALSE(cell48.has_error());
                REQUIRE(cell48.value() == static_cast<int32_t>(i));
            }
        }
    }

    INFO("find");
    {
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
            REQUIRE(cur->column_count() == 2);
            REQUIRE(cur->type_data()[0].child_types()[0].type() == types::logical_type::INTEGER);
            auto cell49 = cur->get<int32_t>(0, 0, {0});
            REQUIRE_FALSE(cell49.has_error());
            REQUIRE(cell49.value() == 91);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT (custom_type).* FROM TestDatabase.TestCollection "
                                               "WHERE (custom_type).f1 > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
            REQUIRE(cur->column_count() == 3);
            REQUIRE(cur->chunks().front().data[0].type().alias() == "f1");
            REQUIRE(cur->chunks().front().data[1].type().alias() == "f2");
            REQUIRE(cur->chunks().front().data[2].type().alias() == "f3");
            REQUIRE(cur->type_data()[0].type() == types::logical_type::INTEGER);
            auto cell50 = cur->get<int32_t>(0, 0);
            REQUIRE_FALSE(cell50.has_error());
            REQUIRE(cell50.value() == 91);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT (custom_type).* FROM TestDatabase.TestCollection "
                                               "WHERE ((custom_type).f3).f2 > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 54);
            REQUIRE(cur->column_count() == 3);
            REQUIRE(cur->chunks().front().data[0].type().alias() == "f1");
            REQUIRE(cur->chunks().front().data[1].type().alias() == "f2");
            REQUIRE(cur->chunks().front().data[2].type().alias() == "f3");
            REQUIRE(cur->type_data()[2].child_types()[1].type() == types::logical_type::INTEGER);
            auto cell51 = cur->get<int32_t>(2, 0, {1});
            REQUIRE_FALSE(cell51.has_error());
            REQUIRE(cell51.value() == 92);
        }
    }
    INFO("update");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "UPDATE TestDatabase.TestCollection SET custom_type.f3.f1 = (custom_type).f3.f1 * 3.0;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT ((custom_type).f3).f1 FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->column_count() == 1);
            for (size_t num = 0; num < 100; ++num) {
                auto cell46 = cur->get<float>(0, num);
                REQUIRE_FALSE(cell46.has_error());
                REQUIRE(core::is_equals(cell46.value(), (static_cast<float>(num) + 0.5f) * 3.0f));
            }
        }
    }
    INFO("join");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection"
                                               " JOIN TestDatabase.CopyTestCollection ON"
                                               " (TestCollection.custom_type).f3.f1 ="
                                               " (CopyTestCollection.custom_type).f3.f1");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 33);
        }
    }
    INFO("delete");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "DELETE FROM TestDatabase.TestCollection WHERE ((custom_type).f3).f2 < 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 45);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 55);
        }
    }
    INFO("group by non-existent field");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT no_such_field, COUNT(count) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY no_such_field;");
        REQUIRE(cur->is_error());
    }
}

// UNION runtime retype must not depend on row contents: the validator reconciles the
// union schema data-INDEPENDENTLY (PostgreSQL rules) and stamps it on the union node,
// and operator_union must trust that stamp — a genuinely-typed all-NULL INTEGER column
// must not flip to the NULL-literal branch's text type based on what the table holds.
TEST_CASE("integration::cpp::test_collection::sql::union_all_null_column_keeps_stamped_type") {
    auto config = test_create_config(integration_fixture_path("test_collection_sql/union_stamped_type"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.t (a integer);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.t (a) VALUES (NULL);")->is_success());

    INFO("typed all-NULL column UNION ALL a NULL literal keeps the stamped INTEGER type");
    {
        auto cur = exec("SELECT a FROM TestDatabase.t UNION ALL SELECT NULL FROM TestDatabase.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE_FALSE(cur->chunks().empty());
        for (const auto& chunk : cur->chunks()) {
            REQUIRE(chunk.column_count() == 1);
            // The result column type is the stamped union schema type (the left branch's
            // INTEGER; the right branch is the reconciled NULL literal). It must not flip
            // to the right branch's text type just because the left DATA is all-NULL.
            REQUIRE(chunk.data[0].type().type() == types::logical_type::INTEGER);
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                REQUIRE(chunk.is_null(0, row));
            }
        }
    }
}

// The UNION NULL-literal reconciliation must take only the RIGHT branch's TYPE, never
// its alias: PostgreSQL keeps the FIRST SELECT's output names, and renaming the output
// column breaks outer references to the left branch's alias through a derived table.
TEST_CASE("integration::cpp::test_collection::sql::union_null_branch_keeps_left_column_name") {
    auto config = test_create_config(integration_fixture_path("test_collection_sql/union_null_alias"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.prices (price integer);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.prices (price) VALUES (10), (20);")->is_success());

    INFO("outer reference to the NULL branch's alias resolves through the derived table");
    {
        auto cur = exec("SELECT x FROM (SELECT NULL AS x FROM TestDatabase.prices "
                        "UNION ALL SELECT price FROM TestDatabase.prices) AS sub;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE_FALSE(cur->chunks().empty());
        // Position 0 keeps the LEFT branch's name 'x' (with the right branch's TYPE).
        REQUIRE(cur->chunks().front().data[0].type().alias() == "x");
        REQUIRE(cur->chunks().front().data[0].type().type() == types::logical_type::INTEGER);
    }
    INFO("filtering on the preserved name returns exactly the NULL-branch rows");
    {
        auto cur = exec("SELECT x FROM (SELECT NULL AS x FROM TestDatabase.prices "
                        "UNION ALL SELECT price FROM TestDatabase.prices) AS sub "
                        "WHERE x IS NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}