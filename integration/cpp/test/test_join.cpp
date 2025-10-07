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
            query << "INSERT INTO " << database_name << "." << collection_name_1
                  << " (_id, name, key_1, key_2) VALUES ";
            for (int num = 0, reversed = 100; num < 101; ++num, --reversed) {
                query << "('" << gen_id(num + 1, dispatcher->resource()) << "', "
                      << "'Name " << num << "', " << num << ", " << reversed << ")" << (reversed == 0 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_2 << " (_id, value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1001, dispatcher->resource()) << "', " << (num + 25) * 2 * 10 << ", "
                      << (num + 25) * 2 << ")" << (num == 99 ? ";" : ", ");
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
                  << "." << collection_name_2 << " ON " << database_name << "." << collection_name_1 << ".key_1"
                  << " = " << database_name << "." << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == (num + 25) * 2);
                REQUIRE(cur->get_document()->get_long("key") == (num + 25) * 2);
                REQUIRE(cur->get_document()->get_long("value") == (num + 25) * 2 * 10);
                REQUIRE(cur->get_document()->get_string("name") ==
                        std::pmr::string("Name " + std::to_string((num + 25) * 2)));
            }
        }
    }

    INFO("left outer join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " LEFT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << database_name << "." << collection_name_1
                  << ".key_1"
                  << " = " << database_name << "." << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);

            for (int num = 0; num < 50; ++num) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num);
                REQUIRE(cur->get_document()->get_long("key") == 0);
                REQUIRE(cur->get_document()->get_long("value") == 0);
                REQUIRE(cur->get_document()->get_string("name") == std::pmr::string("Name " + std::to_string(num)));
            }
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num + 50);
                REQUIRE(cur->get_document()->get_long("key") == num + 50);
                REQUIRE(cur->get_document()->get_long("value") == (num + 50) * 10);
                REQUIRE(cur->get_document()->get_string("name") ==
                        std::pmr::string("Name " + std::to_string(num + 50)));
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num + 51);
                REQUIRE(cur->get_document()->get_long("key") == 0);
                REQUIRE(cur->get_document()->get_long("value") == 0);
                REQUIRE(cur->get_document()->get_string("name") ==
                        std::pmr::string("Name " + std::to_string(num + 51)));
            }
            REQUIRE(cur->has_next());
            cur->next_document();
            REQUIRE(cur->get_document()->get_long("key_1") == 100);
            REQUIRE(cur->get_document()->get_long("key") == 100);
            REQUIRE(cur->get_document()->get_long("value") == 1000);
            REQUIRE(cur->get_document()->get_string("name") == std::pmr::string("Name " + std::to_string(100)));
        }
    }

    INFO("right outer join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " RIGHT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << database_name << "." << collection_name_1
                  << ".key_1"
                  << " = " << database_name << "." << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num * 2 + 50);
                REQUIRE(cur->get_document()->get_long("key") == num * 2 + 50);
                REQUIRE(cur->get_document()->get_long("value") == (num * 2 + 50) * 10);
                REQUIRE(cur->get_document()->get_string("name") ==
                        std::pmr::string("Name " + std::to_string(num * 2 + 50)));
            }
            for (int num = 0; num < 74; ++num) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == 0);
                REQUIRE(cur->get_document()->get_long("key") == num * 2 + 102);
                REQUIRE(cur->get_document()->get_long("value") == (num * 2 + 102) * 10);
                REQUIRE(cur->get_document()->get_string("name") == "");
            }
        }
    }

    INFO("full outer join") {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " FULL OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << database_name << "." << collection_name_1
                  << ".key_1"
                  << " = " << database_name << "." << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 175);

            for (int num = 0; num < 50; ++num) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num);
                REQUIRE(cur->get_document()->get_long("key") == 0);
                REQUIRE(cur->get_document()->get_long("value") == 0);
                REQUIRE(cur->get_document()->get_string("name") == std::pmr::string("Name " + std::to_string(num)));
            }
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num + 50);
                REQUIRE(cur->get_document()->get_long("key") == num + 50);
                REQUIRE(cur->get_document()->get_long("value") == (num + 50) * 10);
                REQUIRE(cur->get_document()->get_string("name") ==
                        std::pmr::string("Name " + std::to_string(num + 50)));
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == num + 51);
                REQUIRE(cur->get_document()->get_long("key") == 0);
                REQUIRE(cur->get_document()->get_long("value") == 0);
                REQUIRE(cur->get_document()->get_string("name") ==
                        std::pmr::string("Name " + std::to_string(num + 51)));
            }
            REQUIRE(cur->has_next());
            cur->next_document();
            REQUIRE(cur->get_document()->get_long("key_1") == 100);
            REQUIRE(cur->get_document()->get_long("key") == 100);
            REQUIRE(cur->get_document()->get_long("value") == 1000);
            REQUIRE(cur->get_document()->get_string("name") == std::pmr::string("Name " + std::to_string(100)));
            for (int num = 0; num < 74; ++num) {
                REQUIRE(cur->has_next());
                cur->next_document();
                REQUIRE(cur->get_document()->get_long("key_1") == 0);
                REQUIRE(cur->get_document()->get_long("key") == num * 2 + 102);
                REQUIRE(cur->get_document()->get_long("value") == (num * 2 + 102) * 10);
                REQUIRE(cur->get_document()->get_string("name") == "");
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
                  << "." << collection_name_2 << " ON " << database_name << "." << collection_name_1 << ".key_1"
                  << " = " << database_name << "." << collection_name_2 + ".key AND " << database_name << "."
                  << collection_name_1 << ".key_2"
                  << " = " << database_name << "." << collection_name_2 + ".key;";
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
                  << "." << collection_name_2 << " ON " << database_name << "." << collection_name_1 << ".key_1"
                  << " = " << database_name << "." << collection_name_2 + ".key AND " << database_name << "."
                  << collection_name_2 << ".key"
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
                  << "." << collection_name_1 << " ON " << database_name << "." << collection_name_1 << ".key_1"
                  << " = " << database_name << "." << collection_name_1 + ".key_2;";
        }
    }
}