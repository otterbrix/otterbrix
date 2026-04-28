#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <variant>

using namespace components;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const std::string database_name = "testdatabase";
static const std::string collection_name_1 = "testcollection_1";
static const std::string collection_name_2 = "testcollection_2";

TEST_CASE("integration::cpp::test_join") {
    auto config = test_create_config("/tmp/test_join/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();

    INFO("initialization") {
        auto session = otterbrix::session_id_t();
        {
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
            dispatcher->execute_sql(session, "CREATE TABLE " + database_name + "." + collection_name_1 + "();");
            dispatcher->execute_sql(session, "CREATE TABLE " + database_name + "." + collection_name_2 + "();");
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_1 << " (name, key_1, key_2) VALUES ";
            for (int num = 0, reversed = 100; num < 101; ++num, --reversed) {
                query << "('Name " << num << "', " << num << ", " << reversed << ")" << (reversed == 0 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_2 << " (value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "(" << (num + 25) * 2 * 10 << ", " << (num + 25) * 2 << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
    }

    INFO("inner join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (size_t num = 0; num < 26; ++num) {
                REQUIRE(cur->chunk_data().value(1, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2);
                REQUIRE(cur->chunk_data().value(4, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2);
                REQUIRE(cur->chunk_data().value(3, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(0, num).value<std::string_view>() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
    }

    INFO("left outer join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " LEFT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);

            for (size_t num = 0; num < 50; ++num) {
                REQUIRE(cur->chunk_data().value(1, num).value<int64_t>() == static_cast<int64_t>(num));
                REQUIRE(cur->chunk_data().value(4, num).is_null());
                REQUIRE(cur->chunk_data().value(3, num).is_null());
                REQUIRE(cur->chunk_data().value(0, num).value<std::string_view>() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->chunk_data().value(1, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(4, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(3, row).value<int64_t>() == (num + 50) * 10);
                REQUIRE(cur->chunk_data().value(0, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 50));
                ++row;
                REQUIRE(cur->chunk_data().value(1, row).value<int64_t>() == num + 51);
                REQUIRE(cur->chunk_data().value(4, row).is_null());
                REQUIRE(cur->chunk_data().value(3, row).is_null());
                REQUIRE(cur->chunk_data().value(0, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 51));
                ++row;
            }
            REQUIRE(cur->chunk_data().value(1, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(4, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(3, 100).value<int64_t>() == 1000);
            REQUIRE(cur->chunk_data().value(0, 100).value<std::string_view>() == "Name 100");
        }
    }

    INFO("right outer join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " RIGHT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);

            for (size_t num = 0; num < 26; ++num) {
                REQUIRE(cur->chunk_data().value(1, num).value<int64_t>() == static_cast<int64_t>(num) * 2 + 50);
                REQUIRE(cur->chunk_data().value(4, num).value<int64_t>() == static_cast<int64_t>(num) * 2 + 50);
                REQUIRE(cur->chunk_data().value(3, num).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 50) * 10);
                REQUIRE(cur->chunk_data().value(0, num).value<std::string_view>() ==
                        "Name " + std::to_string(num * 2 + 50));
            }
            for (size_t num = 0; num < 74; ++num) {
                size_t row = 26 + num;
                REQUIRE(cur->chunk_data().value(1, row).is_null());
                REQUIRE(cur->chunk_data().value(4, row).value<int64_t>() == static_cast<int64_t>(num) * 2 + 102);
                REQUIRE(cur->chunk_data().value(3, row).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                REQUIRE(cur->chunk_data().value(0, row).is_null());
            }
        }
    }

    INFO("full outer join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " FULL OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 175);

            for (size_t num = 0; num < 50; ++num) {
                REQUIRE(cur->chunk_data().value(1, num).value<int64_t>() == static_cast<int64_t>(num));
                REQUIRE(cur->chunk_data().value(4, num).is_null());
                REQUIRE(cur->chunk_data().value(3, num).is_null());
                REQUIRE(cur->chunk_data().value(0, num).value<std::string_view>() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->chunk_data().value(1, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(4, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(3, row).value<int64_t>() == (num + 50) * 10);
                REQUIRE(cur->chunk_data().value(0, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 50));
                ++row;
                REQUIRE(cur->chunk_data().value(1, row).value<int64_t>() == num + 51);
                REQUIRE(cur->chunk_data().value(4, row).is_null());
                REQUIRE(cur->chunk_data().value(3, row).is_null());
                REQUIRE(cur->chunk_data().value(0, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 51));
                ++row;
            }
            REQUIRE(cur->chunk_data().value(1, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(4, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(3, 100).value<int64_t>() == 1000);
            REQUIRE(cur->chunk_data().value(0, 100).value<std::string_view>() == "Name 100");
            for (size_t num = 0; num < 74; ++num) {
                row = 101 + num;
                REQUIRE(cur->chunk_data().value(1, row).is_null());
                REQUIRE(cur->chunk_data().value(4, row).value<int64_t>() == static_cast<int64_t>(num) * 2 + 102);
                REQUIRE(cur->chunk_data().value(3, row).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                REQUIRE(cur->chunk_data().value(0, row).is_null());
            }
        }
    }

    INFO("cross join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " CROSS JOIN " << database_name
                  << "." << collection_name_2 << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10100);
        }
    }

    INFO("two join predicates") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key AND " << collection_name_1 << ".key_2"
                  << " = " << collection_name_2 + ".key;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("two join predicates, with const") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key AND " << collection_name_2 << ".key"
                  << " > "
                  << "75;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);
        }
    }

    INFO("self join ") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_1 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_1 + ".key_2;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);
        }
    }

    INFO("inner join + group by + aggregates + order + limit") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT c1.name, COUNT(c2.value) AS cnt, AVG(c2.key) AS avg_key "
                                           "FROM testdatabase.testcollection_1 c1 "
                                           "INNER JOIN testdatabase.testcollection_2 c2 ON c1.key_1 = c2.key "
                                           "GROUP BY c1.name "
                                           "ORDER BY avg_key DESC "
                                           "LIMIT 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("triple inner join with shared-name keys in both joins") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".col_mid();");
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".col_end();");
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << ".col_mid (key_1, linker) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "(" << (num + 25) * 2 << ", " << (num + 25) * 2 * 10 << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }
        {
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO " + database_name +
                                                   ".col_end (linker, extra) VALUES "
                                                   "(500, 1), (700, 2), (1000, 3), (1500, 4), (2000, 5);");
            REQUIRE(cur->is_success());
        }

        // First join: 26 rows (key_1 in {50,52,..,100}, linker in {500,520,..,1000}).
        // Second join: intersect linker with {500,700,1000,1500,2000} → 3 rows.
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT testcollection_1.name, col_end.extra "
                                           "FROM testdatabase.testcollection_1 "
                                           "INNER JOIN testdatabase.col_mid "
                                           "  ON testcollection_1.key_1 = col_mid.key_1 "
                                           "INNER JOIN testdatabase.col_end "
                                           "  ON col_mid.linker = col_end.linker "
                                           "ORDER BY col_end.extra ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("triple inner join — aliases") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT camp.name, ord.extra "
                                           "FROM testdatabase.testcollection_1 camp "
                                           "INNER JOIN testdatabase.col_mid mid "
                                           "  ON camp.key_1 = mid.key_1 "
                                           "INNER JOIN testdatabase.col_end ord "
                                           "  ON mid.linker = ord.linker "
                                           "ORDER BY ord.extra ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("triple inner join — second JOIN references first table") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".col_aux();");
        {
            auto cur =
                dispatcher->execute_sql(session,
                                        "INSERT INTO " + database_name +
                                            ".col_aux (k, tag) VALUES (50, 'a'), (54, 'b'), (60, 'c'), (200, 'd');");
            REQUIRE(cur->is_success());
        }
        {
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT c.name, x.tag "
                                               "FROM testdatabase.testcollection_1 c "
                                               "INNER JOIN testdatabase.col_mid m "
                                               "  ON c.key_1 = m.key_1 "
                                               "INNER JOIN testdatabase.col_aux x "
                                               "  ON c.key_1 = x.k "
                                               "ORDER BY x.tag ASC;");
            REQUIRE(cur->is_success());
            // c.key_1 = {50,52,...,100}, x.k = {50,54,60,200} = {50,54,60} → 3 rows.
            REQUIRE(cur->size() == 3);
        }
    }
}