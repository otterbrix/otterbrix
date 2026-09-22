#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <string_view>

using namespace components::cursor;

namespace {

    constexpr std::int64_t A_BASE = 100'000;
    constexpr std::int64_t B_BASE = 200'000;
    constexpr std::int64_t D_BASE = 400'000;

    cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    // The statement and its error have to be reported from the assertion's own scope: a message
    // logged inside a helper is out of scope again before the caller's REQUIRE reports anything.
    std::string why(const cursor_t& cursor) {
        return cursor.is_error() ? std::string{cursor.get_error().what.begin(), cursor.get_error().what.end()}
                                 : std::string{"<no error: statement reported success>"};
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(run(dispatcher, session, "CREATE TABLE TestDatabase.t (a BIGINT, b BIGINT);")->is_success());
        REQUIRE(run(dispatcher,
                    session,
                    "INSERT INTO TestDatabase.t (a, b) VALUES (" + std::to_string(A_BASE) + ", " +
                        std::to_string(B_BASE) + ");")
                    ->is_success());
    }

    std::string plan_text(const cursor_t& plan) {
        std::string text;
        for (std::size_t row = 0; row < plan.size(); ++row) {
            text += std::string(plan.value(0, row).value<std::string_view>());
            text += '\n';
        }
        return text;
    }

    // A fresh session, so it reads what is committed rather than the writer's own work.
    bool another_session_sees_column_d(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto reader = otterbrix::session_id_t();
        return run(dispatcher, reader, "SELECT d FROM TestDatabase.t;")->is_success();
    }

} // namespace

TEST_CASE("integration::cpp::ddl_in_transaction") {
    auto config = test_create_config(integration_fixture_path("ddl_in_transaction"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    SECTION("an added column is visible to later statements") {
        auto writer = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
        REQUIRE(run(dispatcher, writer, "ALTER TABLE TestDatabase.t ADD COLUMN d BIGINT;")->is_success());

        INFO("the next statement in this transaction is planned against a layout that has d");
        {
            auto cur = run(dispatcher, writer, "SELECT d FROM TestDatabase.t;");
            INFO("SELECT d: " << why(*cur));
            REQUIRE(cur->is_success());
        }

        INFO("and it can be written, which needs d to exist in the storage, not just in the catalog");
        REQUIRE(run(dispatcher,
                    writer,
                    "INSERT INTO TestDatabase.t (a, b, d) VALUES (" + std::to_string(A_BASE + 1) + ", " +
                        std::to_string(B_BASE + 1) + ", " + std::to_string(D_BASE + 1) + ");")
                    ->is_success());
        {
            auto cur = run(dispatcher, writer, "SELECT a, d FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->value(0, 1).value<std::int64_t>() == A_BASE + 1);
            REQUIRE(cur->value(1, 1).value<std::int64_t>() == D_BASE + 1);
        }

        INFO("nobody else sees it until this transaction commits");
        REQUIRE_FALSE(another_session_sees_column_d(dispatcher));

        REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
        REQUIRE(another_session_sees_column_d(dispatcher));

        INFO("the row written inside the transaction kept its value through the commit");
        {
            auto reader = otterbrix::session_id_t();
            auto cur = run(dispatcher, reader, "SELECT a, d FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->value(1, 1).value<std::int64_t>() == D_BASE + 1);
        }
    }

    SECTION("a dropped column is invisible to later statements") {
        auto writer = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
        REQUIRE(run(dispatcher, writer, "ALTER TABLE TestDatabase.t DROP COLUMN b;")->is_success());

        // Naming b is probed after the commit, not here: a statement that fails inside BEGIN fails the
        // whole transaction, so asking for b would end the transaction this section is about.
        INFO("a still reads as a — the drop moved nothing");
        {
            auto cur = run(dispatcher, writer, "SELECT a FROM TestDatabase.t;");
            INFO("SELECT a: " << why(*cur));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<std::int64_t>() == A_BASE);
        }

        INFO("another session still sees b, because the drop has not committed");
        {
            auto reader = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, reader, "SELECT b FROM TestDatabase.t;")->is_success());
        }

        REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
        {
            auto reader = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, reader, "SELECT b FROM TestDatabase.t;")->is_error());
            auto cur = run(dispatcher, reader, "SELECT a FROM TestDatabase.t;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<std::int64_t>() == A_BASE);
        }
    }

    SECTION("a rolled back add column leaves nothing behind") {
        auto writer = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
        REQUIRE(run(dispatcher, writer, "ALTER TABLE TestDatabase.t ADD COLUMN d BIGINT;")->is_success());
        REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());

        INFO("the column is gone for everyone, including the session that added it");
        REQUIRE(run(dispatcher, writer, "SELECT d FROM TestDatabase.t;")->is_error());
        REQUIRE_FALSE(another_session_sees_column_d(dispatcher));

        INFO("and the table still writes and reads as it did before the rolled-back ALTER");
        REQUIRE(run(dispatcher,
                    writer,
                    "INSERT INTO TestDatabase.t (a, b) VALUES (" + std::to_string(A_BASE + 2) + ", " +
                        std::to_string(B_BASE + 2) + ");")
                    ->is_success());
        {
            auto cur = run(dispatcher, writer, "SELECT a, b FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->value(0, 1).value<std::int64_t>() == A_BASE + 2);
            REQUIRE(cur->value(1, 1).value<std::int64_t>() == B_BASE + 2);
        }
    }

    SECTION("a snapshot older than a committed drop still reads the column") {
        // The reader's transaction starts, and reads b, BEFORE anything drops it: from here on its answers
        // are fixed to that snapshot, which is the half of the stamp rule the other sections never exercise
        // -- they all watch a newer snapshot gain or lose a column, never an older one keep it.
        auto reader = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, reader, "BEGIN;")->is_success());
        {
            auto cur = run(dispatcher, reader, "SELECT b FROM TestDatabase.t;");
            INFO("SELECT b before the drop: " << why(*cur));
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<std::int64_t>() == B_BASE);
        }

        auto dropper = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, dropper, "ALTER TABLE TestDatabase.t DROP COLUMN b;")->is_success());

        INFO("the drop has committed, but not into this reader's snapshot");
        {
            auto cur = run(dispatcher, reader, "SELECT b FROM TestDatabase.t;");
            INFO("SELECT b after the drop committed: " << why(*cur));
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<std::int64_t>() == B_BASE);
        }

        REQUIRE(run(dispatcher, reader, "COMMIT;")->is_success());

        INFO("and its next transaction is on the far side of the drop");
        REQUIRE(run(dispatcher, reader, "SELECT b FROM TestDatabase.t;")->is_error());
        {
            auto cur = run(dispatcher, reader, "SELECT a FROM TestDatabase.t;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<std::int64_t>() == A_BASE);
        }
    }

    SECTION("an index created in a transaction holds the table's rows after COMMIT") {
        auto writer = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
        REQUIRE(run(dispatcher, writer, "CREATE INDEX ix_a ON TestDatabase.t (a);")->is_success());
        REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());

        const std::string point_query = "SELECT b FROM TestDatabase.t WHERE a = " + std::to_string(A_BASE) + ";";
        auto reader = otterbrix::session_id_t();
        INFO("the lookup must go through the index, or a sequential scan answers it whatever the index holds");
        {
            auto plan = run(dispatcher, reader, "EXPLAIN " + point_query);
            REQUIRE(plan->is_success());
            const auto text = plan_text(*plan);
            INFO("plan:\n" << text);
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        {
            auto cur = run(dispatcher, reader, point_query);
            INFO(point_query << ": " << why(*cur));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<std::int64_t>() == B_BASE);
        }
    }
}