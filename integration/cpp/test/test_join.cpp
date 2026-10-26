#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <variant>

using namespace components;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const std::string database_name = "testdatabase";
static const std::string collection_name_1 = "testcollection_1";
static const std::string collection_name_2 = "testcollection_2";

// NOTE: JOIN conditions here use the range form `a >= b AND a <= b` rather than a
// plain `a = b`. For these (non-NULL, integer) keys the two are semantically
// identical, so every expected size/value below is unchanged — but a compound AND
// condition is not a single equi-comparison, so the optimizer keeps the nested-loop
// operator_join_t. This file therefore exercises the general join operator; the
// equi hash-join fast path is covered by test_hash_join.cpp.
TEST_CASE("integration::cpp::test_join") {
    auto config = test_create_config(integration_fixture_path("test_join/base"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();

    INFO("initialization");
    {
        auto session = otterbrix::session_id_t();
        {
            dispatcher->execute_sql(session, "CREATE SCHEMA " + database_name + ";");
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
            REQUIRE(cur->affected_rows() == 101);
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_2 << " (value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "(" << (num + 25) * 2 * 10 << ", " << (num + 25) * 2 << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 100);
        }
    }

    INFO("inner join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (size_t num = 0; num < 26; ++num) {
                auto cell0 = cur->get<int64_t>(1, num);
                REQUIRE_FALSE(cell0.has_error());
                REQUIRE(cell0.value() == (static_cast<int64_t>(num) + 25) * 2);
                auto cell1 = cur->get<int64_t>(4, num);
                REQUIRE_FALSE(cell1.has_error());
                REQUIRE(cell1.value() == (static_cast<int64_t>(num) + 25) * 2);
                auto cell2 = cur->get<int64_t>(3, num);
                REQUIRE_FALSE(cell2.has_error());
                REQUIRE(cell2.value() == (static_cast<int64_t>(num) + 25) * 2 * 10);
                auto cell3 = cur->get<std::string_view>(0, num);
                REQUIRE_FALSE(cell3.has_error());
                REQUIRE(cell3.value() == "Name " + std::to_string((num + 25) * 2));
            }
        }
    }

    INFO("left outer join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " LEFT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);

            for (size_t num = 0; num < 50; ++num) {
                auto cell4 = cur->get<int64_t>(1, num);
                REQUIRE_FALSE(cell4.has_error());
                REQUIRE(cell4.value() == static_cast<int64_t>(num));
                auto cell5 = cur->is_null(4, num);
                REQUIRE_FALSE(cell5.has_error());
                REQUIRE(cell5.value());
                auto cell6 = cur->is_null(3, num);
                REQUIRE_FALSE(cell6.has_error());
                REQUIRE(cell6.value());
                auto cell7 = cur->get<std::string_view>(0, num);
                REQUIRE_FALSE(cell7.has_error());
                REQUIRE(cell7.value() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                auto cell8 = cur->get<int64_t>(1, row);
                REQUIRE_FALSE(cell8.has_error());
                REQUIRE(cell8.value() == num + 50);
                auto cell9 = cur->get<int64_t>(4, row);
                REQUIRE_FALSE(cell9.has_error());
                REQUIRE(cell9.value() == num + 50);
                auto cell10 = cur->get<int64_t>(3, row);
                REQUIRE_FALSE(cell10.has_error());
                REQUIRE(cell10.value() == (num + 50) * 10);
                auto cell11 = cur->get<std::string_view>(0, row);
                REQUIRE_FALSE(cell11.has_error());
                REQUIRE(cell11.value() == "Name " + std::to_string(num + 50));
                ++row;
                auto cell12 = cur->get<int64_t>(1, row);
                REQUIRE_FALSE(cell12.has_error());
                REQUIRE(cell12.value() == num + 51);
                auto cell13 = cur->is_null(4, row);
                REQUIRE_FALSE(cell13.has_error());
                REQUIRE(cell13.value());
                auto cell14 = cur->is_null(3, row);
                REQUIRE_FALSE(cell14.has_error());
                REQUIRE(cell14.value());
                auto cell15 = cur->get<std::string_view>(0, row);
                REQUIRE_FALSE(cell15.has_error());
                REQUIRE(cell15.value() == "Name " + std::to_string(num + 51));
                ++row;
            }
            auto cell16 = cur->get<int64_t>(1, 100);
            REQUIRE_FALSE(cell16.has_error());
            REQUIRE(cell16.value() == 100);
            auto cell17 = cur->get<int64_t>(4, 100);
            REQUIRE_FALSE(cell17.has_error());
            REQUIRE(cell17.value() == 100);
            auto cell18 = cur->get<int64_t>(3, 100);
            REQUIRE_FALSE(cell18.has_error());
            REQUIRE(cell18.value() == 1000);
            auto cell19 = cur->get<std::string_view>(0, 100);
            REQUIRE_FALSE(cell19.has_error());
            REQUIRE(cell19.value() == "Name 100");
        }
    }

    INFO("right outer join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " RIGHT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);

            for (size_t num = 0; num < 26; ++num) {
                auto cell20 = cur->get<int64_t>(1, num);
                REQUIRE_FALSE(cell20.has_error());
                REQUIRE(cell20.value() == static_cast<int64_t>(num) * 2 + 50);
                auto cell21 = cur->get<int64_t>(4, num);
                REQUIRE_FALSE(cell21.has_error());
                REQUIRE(cell21.value() == static_cast<int64_t>(num) * 2 + 50);
                auto cell22 = cur->get<int64_t>(3, num);
                REQUIRE_FALSE(cell22.has_error());
                REQUIRE(cell22.value() == (static_cast<int64_t>(num) * 2 + 50) * 10);
                auto cell23 = cur->get<std::string_view>(0, num);
                REQUIRE_FALSE(cell23.has_error());
                REQUIRE(cell23.value() == "Name " + std::to_string(num * 2 + 50));
            }
            for (size_t num = 0; num < 74; ++num) {
                size_t row = 26 + num;
                auto cell24 = cur->is_null(1, row);
                REQUIRE_FALSE(cell24.has_error());
                REQUIRE(cell24.value());
                auto cell25 = cur->get<int64_t>(4, row);
                REQUIRE_FALSE(cell25.has_error());
                REQUIRE(cell25.value() == static_cast<int64_t>(num) * 2 + 102);
                auto cell26 = cur->get<int64_t>(3, row);
                REQUIRE_FALSE(cell26.has_error());
                REQUIRE(cell26.value() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                auto cell27 = cur->is_null(0, row);
                REQUIRE_FALSE(cell27.has_error());
                REQUIRE(cell27.value());
            }
        }
    }

    INFO("full outer join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " FULL OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 175);

            for (size_t num = 0; num < 50; ++num) {
                auto cell28 = cur->get<int64_t>(1, num);
                REQUIRE_FALSE(cell28.has_error());
                REQUIRE(cell28.value() == static_cast<int64_t>(num));
                auto cell29 = cur->is_null(4, num);
                REQUIRE_FALSE(cell29.has_error());
                REQUIRE(cell29.value());
                auto cell30 = cur->is_null(3, num);
                REQUIRE_FALSE(cell30.has_error());
                REQUIRE(cell30.value());
                auto cell31 = cur->get<std::string_view>(0, num);
                REQUIRE_FALSE(cell31.has_error());
                REQUIRE(cell31.value() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                auto cell32 = cur->get<int64_t>(1, row);
                REQUIRE_FALSE(cell32.has_error());
                REQUIRE(cell32.value() == num + 50);
                auto cell33 = cur->get<int64_t>(4, row);
                REQUIRE_FALSE(cell33.has_error());
                REQUIRE(cell33.value() == num + 50);
                auto cell34 = cur->get<int64_t>(3, row);
                REQUIRE_FALSE(cell34.has_error());
                REQUIRE(cell34.value() == (num + 50) * 10);
                auto cell35 = cur->get<std::string_view>(0, row);
                REQUIRE_FALSE(cell35.has_error());
                REQUIRE(cell35.value() == "Name " + std::to_string(num + 50));
                ++row;
                auto cell36 = cur->get<int64_t>(1, row);
                REQUIRE_FALSE(cell36.has_error());
                REQUIRE(cell36.value() == num + 51);
                auto cell37 = cur->is_null(4, row);
                REQUIRE_FALSE(cell37.has_error());
                REQUIRE(cell37.value());
                auto cell38 = cur->is_null(3, row);
                REQUIRE_FALSE(cell38.has_error());
                REQUIRE(cell38.value());
                auto cell39 = cur->get<std::string_view>(0, row);
                REQUIRE_FALSE(cell39.has_error());
                REQUIRE(cell39.value() == "Name " + std::to_string(num + 51));
                ++row;
            }
            auto cell40 = cur->get<int64_t>(1, 100);
            REQUIRE_FALSE(cell40.has_error());
            REQUIRE(cell40.value() == 100);
            auto cell41 = cur->get<int64_t>(4, 100);
            REQUIRE_FALSE(cell41.has_error());
            REQUIRE(cell41.value() == 100);
            auto cell42 = cur->get<int64_t>(3, 100);
            REQUIRE_FALSE(cell42.has_error());
            REQUIRE(cell42.value() == 1000);
            auto cell43 = cur->get<std::string_view>(0, 100);
            REQUIRE_FALSE(cell43.has_error());
            REQUIRE(cell43.value() == "Name 100");
            for (size_t num = 0; num < 74; ++num) {
                row = 101 + num;
                auto cell44 = cur->is_null(1, row);
                REQUIRE_FALSE(cell44.has_error());
                REQUIRE(cell44.value());
                auto cell45 = cur->get<int64_t>(4, row);
                REQUIRE_FALSE(cell45.has_error());
                REQUIRE(cell45.value() == static_cast<int64_t>(num) * 2 + 102);
                auto cell46 = cur->get<int64_t>(3, row);
                REQUIRE_FALSE(cell46.has_error());
                REQUIRE(cell46.value() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                auto cell47 = cur->is_null(0, row);
                REQUIRE_FALSE(cell47.has_error());
                REQUIRE(cell47.value());
            }
        }
    }

    INFO("cross join");
    {
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

    INFO("inner join ON true");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON TRUE;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10100);
        }
    }

    INFO("left join ON true");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " LEFT JOIN " << database_name
                  << "." << collection_name_2 << " ON TRUE;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10100);
        }
    }

    INFO("two join predicates");
    {
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

    INFO("two join predicates, with const");
    {
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

    INFO("self join ");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " AS a INNER JOIN "
                  << database_name << "." << collection_name_1 << " AS b"
                  << " ON a.key_1 >= b.key_2 AND a.key_1 <= b.key_2;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);
        }
    }

    INFO("inner join + group by + aggregates + order + limit");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT c1.name, COUNT(c2.value) AS cnt, AVG(c2.key) AS avg_key "
                                           "FROM testdatabase.testcollection_1 c1 "
                                           "INNER JOIN testdatabase.testcollection_2 c2 "
                                           "  ON c1.key_1 >= c2.key AND c1.key_1 <= c2.key "
                                           "GROUP BY c1.name "
                                           "ORDER BY avg_key DESC "
                                           "LIMIT 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("triple inner join with shared-name keys in both joins");
    {
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
                                           "  ON testcollection_1.key_1 >= col_mid.key_1 "
                                           "     AND testcollection_1.key_1 <= col_mid.key_1 "
                                           "INNER JOIN testdatabase.col_end "
                                           "  ON col_mid.linker >= col_end.linker "
                                           "     AND col_mid.linker <= col_end.linker "
                                           "ORDER BY col_end.extra ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("triple inner join — aliases");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT camp.name, ord.extra "
                                           "FROM testdatabase.testcollection_1 camp "
                                           "INNER JOIN testdatabase.col_mid mid "
                                           "  ON camp.key_1 >= mid.key_1 AND camp.key_1 <= mid.key_1 "
                                           "INNER JOIN testdatabase.col_end ord "
                                           "  ON mid.linker >= ord.linker AND mid.linker <= ord.linker "
                                           "ORDER BY ord.extra ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("triple inner join — second JOIN references first table");
    {
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
                                               "  ON c.key_1 >= m.key_1 AND c.key_1 <= m.key_1 "
                                               "INNER JOIN testdatabase.col_aux x "
                                               "  ON c.key_1 >= x.k AND c.key_1 <= x.k "
                                               "ORDER BY x.tag ASC;");
            REQUIRE(cur->is_success());
            // c.key_1 = {50,52,...,100}, x.k = {50,54,60,200} = {50,54,60} → 3 rows.
            REQUIRE(cur->size() == 3);
        }
    }
}