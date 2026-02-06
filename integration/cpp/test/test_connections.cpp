#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <integration/cpp/connection.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";
constexpr size_t doc_num = 1000;
constexpr size_t num_threads = 4;
constexpr size_t work_per_thread = doc_num / num_threads;

TEST_CASE("integration::cpp::test_otterbrix_multithread") {
    auto config = test_create_config("/tmp/test_connectors");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    config.wal.sync_to_disk = false;
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
        // REQUIRE can behave wierdly with threading, but storing result and checking it later works fine
        std::array<bool, num_threads> results;

        std::function append_func = [&](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (size_t num = start; num < end; ++num) {
                query << "('" << gen_id(static_cast<int>(num + 1), dispatcher->resource()) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == end - 1 ? ";" : ", ");
            }
            auto session = otterbrix::session_id_t();
            auto c = dispatcher->execute_sql(session, query.str());
            //REQUIRE(c->size() == work_per_thread);
            results[id] = c->size() == work_per_thread;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 0);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(append_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == doc_num);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == doc_num);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == doc_num - 90 - 1);
        }
    }
}

TEST_CASE("integration::cpp::test_connectors") {
    auto config = test_create_config("/tmp/test_connectors");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    config.wal.sync_to_disk = false;
    auto otterbrix = otterbrix::make_otterbrix(config);

    INFO("initialization") {
        auto* dispatcher = otterbrix->dispatcher();
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
        // REQUIRE can behave wierdly with threading, but storing result and checking it later works fine
        std::array<bool, num_threads> results;

        std::array<std::unique_ptr<otterbrix::connection_t>, num_threads> connectors;
        for (size_t i = 0; i < num_threads; i++) {
            connectors[i] = std::make_unique<otterbrix::connection_t>(otterbrix);
        }

        std::function append_func = [&](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (size_t num = start; num < end; ++num) {
                query << "('" << gen_id(static_cast<int>(num + 1), otterbrix->dispatcher()->resource()) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == end - 1 ? ";" : ", ");
            }
            auto c = connectors[id]->execute(query.str());
            //REQUIRE(c->size() == work_per_thread);
            results[id] = c->size() == work_per_thread;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(otterbrix->dispatcher()->size(session, database_name, collection_name) == 0);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(append_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(otterbrix->dispatcher()->size(session, database_name, collection_name) == doc_num);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = otterbrix->dispatcher()->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == doc_num);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = otterbrix->dispatcher()->execute_sql(session,
                                                            "SELECT * FROM TestDatabase.TestCollection "
                                                            "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == doc_num - 90 - 1);
        }
    }
}