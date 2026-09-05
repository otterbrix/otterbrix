#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <integration/cpp/connection.hpp>
#include <stdexcept>
#include <unistd.h>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";
constexpr size_t doc_num = 1000;
constexpr size_t num_threads = 4;
constexpr size_t work_per_thread = doc_num / num_threads;

TEST_CASE("integration::cpp::test_otterbrix_multithread") {
    auto config = test_create_config(integration_fixture_path("test_otterbrix_multithread"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name);
        }
    }

    INFO("insert");
    {
        // REQUIRE can behave wierdly with threading, but storing result and checking it later works fine
        std::array<bool, num_threads> results;

        std::function append_func = [&](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (size_t num = start; num < end; ++num) {
                query << "('Name " << num << "'," << num << ")" << (num == end - 1 ? ";" : ", ");
            }
            auto session = otterbrix::session_id_t();
            auto c = dispatcher->execute_sql(session, query.str());
            //REQUIRE(c->size() == work_per_thread);
            results[id] = c->size() == work_per_thread;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(append_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }
    }

    INFO("find");
    {
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
    auto config = test_create_config(integration_fixture_path("test_connectors"));
    test_clear_directory(config);
    config.wal.on = false;
    auto otterbrix = otterbrix::make_otterbrix(config);

    INFO("initialization");
    {
        auto* dispatcher = otterbrix->dispatcher();
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name);
        }
    }

    INFO("insert");
    {
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
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (size_t num = start; num < end; ++num) {
                query << "('Name " << num << "'," << num << ")" << (num == end - 1 ? ";" : ", ");
            }
            auto c = connectors[id]->execute(query.str());
            //REQUIRE(c->size() == work_per_thread);
            results[id] = c->size() == work_per_thread;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(append_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }
    }

    INFO("find");
    {
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
// ===========================================================================
// EXECUTE AFTER CLOSE IS A REFUSAL, NOT A NULL DEREFERENCE.
//
// close() nulls the instance pointer that execute() then guards with a bare assert — an
// abort in Debug and a straight null dereference in Release. A use-after-close is an
// embedder bug, and the loud, catchable answer at this API boundary is an exception (the
// same channel base_spaces uses for its startup refusals), never undefined behaviour.
//
// BEFORE: this test died on the assert (Debug) / crashed on nullptr (Release).
// ===========================================================================
TEST_CASE("integration::cpp::connection::execute_after_close_refuses_loudly") {
    auto config = test_create_config(integration_fixture_path("test_connection_after_close") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.wal.on = false;
    auto otterbrix = otterbrix::make_otterbrix(config);

    otterbrix::connection_t connection(otterbrix);
    REQUIRE(connection.execute("SELECT 1;") != nullptr);

    connection.close();

    REQUIRE_THROWS_AS(connection.execute("SELECT 1;"), std::runtime_error);
}
