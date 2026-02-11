#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
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
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (size_t num = 0; num < 26; ++num) {
                REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2);
                REQUIRE(cur->chunk_data().value(6, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2);
                REQUIRE(cur->chunk_data().value(5, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() ==
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
                REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == static_cast<int64_t>(num));
                REQUIRE(cur->chunk_data().value(6, num).is_null());
                REQUIRE(cur->chunk_data().value(5, num).is_null());
                REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->chunk_data().value(2, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(6, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(5, row).value<int64_t>() == (num + 50) * 10);
                REQUIRE(cur->chunk_data().value(1, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 50));
                ++row;
                REQUIRE(cur->chunk_data().value(2, row).value<int64_t>() == num + 51);
                REQUIRE(cur->chunk_data().value(6, row).is_null());
                REQUIRE(cur->chunk_data().value(5, row).is_null());
                REQUIRE(cur->chunk_data().value(1, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 51));
                ++row;
            }
            REQUIRE(cur->chunk_data().value(2, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(6, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(5, 100).value<int64_t>() == 1000);
            REQUIRE(cur->chunk_data().value(1, 100).value<std::string_view>() == "Name 100");
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
                REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == static_cast<int64_t>(num) * 2 + 50);
                REQUIRE(cur->chunk_data().value(6, num).value<int64_t>() == static_cast<int64_t>(num) * 2 + 50);
                REQUIRE(cur->chunk_data().value(5, num).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 50) * 10);
                REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() ==
                        "Name " + std::to_string(num * 2 + 50));
            }
            for (size_t num = 0; num < 74; ++num) {
                size_t row = 26 + num;
                REQUIRE(cur->chunk_data().value(2, row).is_null());
                REQUIRE(cur->chunk_data().value(6, row).value<int64_t>() == static_cast<int64_t>(num) * 2 + 102);
                REQUIRE(cur->chunk_data().value(5, row).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                REQUIRE(cur->chunk_data().value(1, row).is_null());
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
                REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == static_cast<int64_t>(num));
                REQUIRE(cur->chunk_data().value(6, num).is_null());
                REQUIRE(cur->chunk_data().value(5, num).is_null());
                REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->chunk_data().value(2, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(6, row).value<int64_t>() == num + 50);
                REQUIRE(cur->chunk_data().value(5, row).value<int64_t>() == (num + 50) * 10);
                REQUIRE(cur->chunk_data().value(1, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 50));
                ++row;
                REQUIRE(cur->chunk_data().value(2, row).value<int64_t>() == num + 51);
                REQUIRE(cur->chunk_data().value(6, row).is_null());
                REQUIRE(cur->chunk_data().value(5, row).is_null());
                REQUIRE(cur->chunk_data().value(1, row).value<std::string_view>() ==
                        "Name " + std::to_string(num + 51));
                ++row;
            }
            REQUIRE(cur->chunk_data().value(2, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(6, 100).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(5, 100).value<int64_t>() == 1000);
            REQUIRE(cur->chunk_data().value(1, 100).value<std::string_view>() == "Name 100");
            for (size_t num = 0; num < 74; ++num) {
                row = 101 + num;
                REQUIRE(cur->chunk_data().value(2, row).is_null());
                REQUIRE(cur->chunk_data().value(6, row).value<int64_t>() == static_cast<int64_t>(num) * 2 + 102);
                REQUIRE(cur->chunk_data().value(5, row).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                REQUIRE(cur->chunk_data().value(1, row).is_null());
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
}