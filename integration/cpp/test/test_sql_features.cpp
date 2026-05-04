#include "test_config.hpp"
#include "types/operations_helper.hpp"

#include <catch2/catch.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

TEST_CASE("integration::cpp::test_sql_features::is_null") {
    auto config = test_create_config("/tmp/test_sql_features/is_null");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (name string, value bigint);");
        }
    }

    INFO("insert data with nulls") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, value) VALUES "
                                               "('Alice', 10), ('Bob', 20), ('Charlie', 30);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            // Insert rows with missing value (NULL)
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('Dave'), ('Eve');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }

    INFO("IS NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection WHERE value IS NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("IS NOT NULL") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection WHERE value IS NOT NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("IS NULL combined with AND") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE value IS NULL AND name = 'Dave';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("IS NOT NULL combined with filter") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE value IS NOT NULL AND value > 15;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("COUNT with IS NOT NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT COUNT(name) AS cnt FROM TestDatabase.TestCollection "
                                           "WHERE value IS NOT NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 3);
    }

    INFO("DELETE with IS NULL") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE value IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::in_list") {
    auto config = test_create_config("/tmp/test_sql_features/in_list");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
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
                query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
    }

    INFO("IN with integers") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE count IN (1, 5, 10, 50, 99);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("IN with strings") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name IN ('Name 0', 'Name 50', 'Name 99');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("NOT IN") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE count NOT IN (0, 1, 2, 3, 4);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 95);
    }

    INFO("IN combined with AND") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE count IN (10, 20, 30, 40, 50) AND count > 25;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("IN with single value") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE count IN (42);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::test_sql_features::like") {
    auto config = test_create_config("/tmp/test_sql_features/like");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES "
                                               "('Alice', 1), ('Bob', 2), ('Charlie', 3), "
                                               "('Alex', 4), ('Alfred', 5), ('Brian', 6), "
                                               "('test_value', 7), ('test123', 8), ('abc', 9), ('xyz', 10);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("LIKE with prefix wildcard") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE 'Al%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // Alice, Alex, Alfred
    }

    INFO("LIKE with suffix wildcard") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE '%e';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // Alice, Charlie, test_value
    }

    INFO("LIKE with middle wildcard") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE '%li%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2); // Alice, Charlie
    }

    INFO("LIKE with underscore") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE 'A___';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1); // Alex
    }

    INFO("LIKE exact match") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE 'Bob';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("NOT LIKE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name NOT LIKE 'Al%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 7); // All except Alice, Alex, Alfred
    }
}

TEST_CASE("integration::cpp::test_sql_features::distinct") {
    auto config = test_create_config("/tmp/test_sql_features/distinct");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, category, value) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << (num % 10) << "', 'Cat " << (num % 5) << "', " << num << ")"
                      << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
    }

    INFO("SELECT DISTINCT single column") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT DISTINCT name FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("SELECT DISTINCT two columns") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT DISTINCT name, category FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("SELECT DISTINCT with WHERE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT DISTINCT name FROM TestDatabase.TestCollection "
                                           "WHERE value > 50;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("SELECT DISTINCT category") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT DISTINCT category FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
}

TEST_CASE("integration::cpp::test_sql_features::count_distinct") {
    auto config = test_create_config("/tmp/test_sql_features/count_distinct");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, category) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('Name " << (num % 10) << "', 'Cat " << (num % 5) << "')" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
    }

    INFO("COUNT(DISTINCT col)") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT COUNT(DISTINCT name) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 10);
    }

    INFO("COUNT(DISTINCT category)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT COUNT(DISTINCT category) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 5);
    }

    INFO("COUNT(DISTINCT) vs COUNT") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(name) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT COUNT(DISTINCT name) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 10);
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::having") {
    auto config = test_create_config("/tmp/test_sql_features/having");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
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
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
    }

    INFO("HAVING with COUNT") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, COUNT(count) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY name "
                                           "HAVING COUNT(count) > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10); // all groups have 10 rows each
    }

    INFO("HAVING filter some groups") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, SUM(count) AS total "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY name "
                                           "HAVING SUM(count) > 90;");
        REQUIRE(cur->is_success());
        // Each group has 10 entries with values (n%20, (n+10)%20, ...)
        // SUM for group i: 5*(i%20) + 5*((i+10)%20)
        // For i=5..9: SUM = 5*i + 5*(i+10)%20 = 5*i + 5*(i-10) = 10i-50
        // i=5: SUM = 5*5 + 5*15 = 25+75 = 100 > 90 ✓
        // i=6: SUM = 5*6 + 5*16 = 30+80 = 110 > 90 ✓
        // i=7: SUM = 5*7 + 5*17 = 35+85 = 120 > 90 ✓
        // i=8: SUM = 5*8 + 5*18 = 40+90 = 130 > 90 ✓
        // i=9: SUM = 5*9 + 5*19 = 45+95 = 140 > 90 ✓
        // i=0: SUM = 5*0 + 5*10 = 0+50 = 50 < 90
        // i=1: SUM = 5*1 + 5*11 = 5+55 = 60 < 90
        // i=2: SUM = 5*2 + 5*12 = 10+60 = 70 < 90
        // i=3: SUM = 5*3 + 5*13 = 15+65 = 80 < 90
        // i=4: SUM = 5*4 + 5*14 = 20+70 = 90 = 90 (not > 90)
        REQUIRE(cur->size() == 5);
    }
}

TEST_CASE("integration::cpp::test_sql_features::edge_cases") {
    auto config = test_create_config("/tmp/test_sql_features/edge_cases");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
    }

    INFO("empty table SELECT") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("empty table COUNT") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(name) AS cnt FROM TestDatabase.TestCollection;");
        // column with name 'name' does not exists
        REQUIRE(cur->is_error());
    }

    INFO("single row operations") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES "
                                               "('OnlyRow', 42);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET count = 100 "
                                               "WHERE name = 'OnlyRow';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection WHERE count = 100;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE name = 'OnlyRow';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("pagination with ORDER BY and LIMIT") {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 50; ++num) {
                query << "('Item " << num << "', " << num << ")" << (num == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count >= 10 ORDER BY count LIMIT 5;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 10);
            REQUIRE(cur->chunk_data().value(1, 4).value<int64_t>() == 14);
        }
    }

    INFO("large batch insert") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count >= 0;");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 5000; ++num) {
                query << "('Row " << num << "', " << num << ")" << (num == 4999 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5000);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5000);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(name) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 5000);
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::coalesce") {
    auto config = test_create_config("/tmp/test_sql_features/coalesce");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (name string, nickname string, value bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, nickname, value) VALUES "
                                               "('Alice', 'Ali', 10), ('Bob', 'Bobby', 20);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            // Insert rows with missing nickname (NULL)
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, value) VALUES "
                                               "('Charlie', 30);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            // Insert row with missing both nickname and value
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('Dave');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("COALESCE with column and constant") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, COALESCE(nickname, 'no_nickname') AS display_name "
                                           "FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("COALESCE with two columns") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, COALESCE(nickname, name) AS display "
                                           "FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }
}

TEST_CASE("integration::cpp::test_sql_features::case_when") {
    auto config = test_create_config("/tmp/test_sql_features/case_when");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, score) VALUES "
                                               "('Alice', 95), ('Bob', 72), ('Charlie', 45), "
                                               "('Dave', 88), ('Eve', 30);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
        }
    }

    INFO("searched CASE WHEN with ranges") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, CASE WHEN score >= 90 THEN 'A' "
                                           "WHEN score >= 70 THEN 'B' "
                                           "WHEN score >= 50 THEN 'C' "
                                           "ELSE 'F' END AS grade "
                                           "FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("simple CASE with equality") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, CASE name WHEN 'Alice' THEN 'first' "
                                           "WHEN 'Bob' THEN 'second' "
                                           "ELSE 'other' END AS position "
                                           "FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("CASE WHEN without ELSE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name, CASE WHEN score > 80 THEN 'pass' END AS result "
                                           "FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
}

TEST_CASE("integration::cpp::test_sql_features::update_with_is_null") {
    auto config = test_create_config("/tmp/test_sql_features/update_is_null");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (name string, value bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, value) VALUES "
                                               "('Alice', 10), ('Bob', 20);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('NoValue1'), ('NoValue2');");
            REQUIRE(cur->is_success());
        }
    }

    INFO("UPDATE WHERE IS NULL") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET value = 0 "
                                               "WHERE value IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection WHERE value IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection WHERE value = 0;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::decimal_type") {
    auto config = test_create_config("/tmp/test_sql_features/decimal_type");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (num_value numeric(10,2), value bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (num_value, value) VALUES "
                                               "(500.195, 10), (500.204, 20), (500.2, 30), (500, 40);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("scan") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 4);

            auto dec_type = cur->chunk_data().data[0].type();
            const auto* ext =
                reinterpret_cast<const components::types::decimal_logical_type_extension*>(dec_type.extension());
            // decimal(10,2) should fall into int64_t range
            REQUIRE(dec_type.to_physical_type() == components::types::physical_type::INT64);
            for (size_t i = 0; i < 4; i++) {
                REQUIRE(components::types::decimal_to_string(*(cur->chunk_data().data[0].data<int64_t>()),
                                                             ext->width(),
                                                             ext->scale()) == "500.20");
            }
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::check_constraint") {
    auto config = test_create_config("/tmp/test_sql_features/check_constraint");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.items (id bigint, age bigint, name text);");
        }
    }

    INFO("simple check: age > 0") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "ALTER TABLE TestDatabase.items ADD CONSTRAINT chk_age CHECK (age > 0);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.items (id, age, name) VALUES (1, -1, 'bad');");
            REQUIRE(cur->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.items (id, age, name) VALUES (1, 25, 'alice');");
            INFO("second insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
        }
    }

    INFO("compound check: x > 0 AND x < 100") {
        auto config2 = test_create_config("/tmp/test_sql_features/check_constraint_compound");
        test_clear_directory(config2);
        config2.disk.on = true;
        config2.wal.on = false;
        test_spaces space2(config2);
        auto* d2 = space2.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            d2->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            d2->execute_sql(session, "CREATE TABLE TestDatabase.scores (id bigint, val bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = d2->execute_sql(
                session,
                "ALTER TABLE TestDatabase.scores ADD CONSTRAINT chk_val CHECK (val > 0 AND val < 100);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = d2->execute_sql(
                session,
                "INSERT INTO TestDatabase.scores (id, val) VALUES (1, 0);");
            REQUIRE(cur->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = d2->execute_sql(
                session,
                "INSERT INTO TestDatabase.scores (id, val) VALUES (2, 100);");
            REQUIRE(cur->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = d2->execute_sql(
                session,
                "INSERT INTO TestDatabase.scores (id, val) VALUES (3, 50);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("IS NOT NULL check") {
        auto config3 = test_create_config("/tmp/test_sql_features/check_constraint_notnull");
        test_clear_directory(config3);
        config3.disk.on = true;
        config3.wal.on = false;
        test_spaces space3(config3);
        auto* d3 = space3.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            d3->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            d3->execute_sql(session, "CREATE TABLE TestDatabase.data (id bigint, val bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = d3->execute_sql(
                session,
                "ALTER TABLE TestDatabase.data ADD CONSTRAINT chk_notnull CHECK (val IS NOT NULL);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = d3->execute_sql(
                session,
                "INSERT INTO TestDatabase.data (id, val) VALUES (1, 42);");
            REQUIRE(cur->is_success());
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::ddl_error_propagation") {
    // Verifies that ddl_result_t errors are surfaced to the caller rather than
    // silently discarded via (void)co_await. Exercises:
    //   - ddl_create_table (via CREATE TABLE)
    //   - ddl_add_column / ddl_drop_column (via ALTER TABLE)
    //   - ddl_create_constraint CHECK (via ALTER TABLE ADD CONSTRAINT)
    //   - drop path (via DROP TABLE)
    auto config = test_create_config("/tmp/test_sql_features/ddl_error_propagation");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.items (id bigint, val bigint);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("alter table: add column propagates success") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "ALTER TABLE TestDatabase.items ADD COLUMN extra bigint;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("alter table: drop column propagates success") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "ALTER TABLE TestDatabase.items DROP COLUMN extra;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("alter table: add check constraint propagates success") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "ALTER TABLE TestDatabase.items ADD CONSTRAINT chk_val CHECK (val > 0);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("check constraint violation surfaces as error cursor (not silent pass-through)") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.items (id, val) VALUES (1, -5);");
            REQUIRE(cur->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.items (id, val) VALUES (2, 10);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("drop table propagates success") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DROP TABLE TestDatabase.items;");
            REQUIRE(cur->is_success());
        }
    }
}

TEST_CASE("integration::cpp::test_sql_features::check_pred_cache") {
    // Verifies that the compiled CHECK predicate cache works correctly:
    //   - repeated inserts hit the cache (cache hit path)
    //   - violation still detected after many cache-hit inserts
    //   - after DROP COLUMN (column_count changes), cache is invalidated and
    //     the constraint is re-evaluated correctly against the new schema
    auto config = test_create_config("/tmp/test_sql_features/check_pred_cache");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session,
                "CREATE TABLE TestDatabase.items (id bigint, x bigint, extra bigint);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session,
                "ALTER TABLE TestDatabase.items ADD CONSTRAINT chk_x CHECK (x > 0);")->is_success());
        }
    }

    INFO("50 valid inserts hit cache on 2nd+") {
        for (int i = 1; i <= 50; ++i) {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.items (id, x, extra) VALUES ("
                + std::to_string(i) + ", " + std::to_string(i) + ", 0);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("violation still detected after cached inserts") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.items (id, x, extra) VALUES (99, -1, 0);");
            REQUIRE(cur->is_error());
        }
    }

    INFO("valid insert after violation") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.items (id, x, extra) VALUES (100, 100, 0);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("drop extra column invalidates cache (column_count changes), constraint still enforced") {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session,
                "ALTER TABLE TestDatabase.items DROP COLUMN extra;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.items (id, x) VALUES (101, -5);");
            REQUIRE(cur->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.items (id, x) VALUES (102, 5);");
            REQUIRE(cur->is_success());
        }
    }
}
