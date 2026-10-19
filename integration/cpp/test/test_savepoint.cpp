#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {
    constexpr int64_t COMMITTED_ROWS = 1500;

    cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        auto cursor = dispatcher->execute_sql(session, sql);
        INFO("statement: " << sql);
        return cursor;
    }

    void create_marks(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(run(dispatcher, session, "CREATE TABLE TestDatabase.marks (id bigint);")->is_success());
        constexpr int64_t batch = 500;
        for (int64_t first = 0; first < COMMITTED_ROWS; first += batch) {
            std::stringstream query;
            query << "INSERT INTO TestDatabase.marks (id) VALUES ";
            for (int64_t id = first; id < first + batch; ++id) {
                query << "(" << id << ")" << (id + 1 == first + batch ? ";" : ", ");
            }
            REQUIRE(run(dispatcher, session, query.str())->is_success());
        }
    }

    // The ids the session sees, in order, are exactly first..last.
    bool marks_are_range(otterbrix::wrapper_dispatcher_t* dispatcher,
                         const otterbrix::session_id_t& session,
                         int64_t first,
                         int64_t last) {
        auto cursor = run(dispatcher, session, "SELECT id FROM TestDatabase.marks ORDER BY id;");
        REQUIRE(cursor->is_success());
        if (cursor->size() != static_cast<std::size_t>(last - first + 1)) {
            return false;
        }
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            const auto id = cursor->get<int64_t>(0, row);
            REQUIRE_FALSE(id.has_error());
            if (id.value() != first + static_cast<int64_t>(row)) {
                return false;
            }
        }
        return true;
    }
} // namespace

TEST_CASE("integration::cpp::savepoint::outside_a_transaction_block") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/outside_a_transaction_block"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_marks(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "SAVEPOINT nowhere;")->is_error());
    REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT nowhere;")->is_error());
    REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT nowhere;")->is_error());
    REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id < 10;")->is_success());
    REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 10, COMMITTED_ROWS - 1));
}

TEST_CASE("integration::cpp::savepoint::rollback_to") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/rollback_to"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_marks(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id < 100;")->is_success());

    SECTION("deletes, inserts and updates after the savepoint are undone") {
        REQUIRE(run(dispatcher, session, "SAVEPOINT before_changes;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1000;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (5000), (5001);")->is_success());
        REQUIRE(
            run(dispatcher, session, "UPDATE TestDatabase.marks SET id = id + 10000 WHERE id < 200;")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_changes;")->is_success());
        REQUIRE(marks_are_range(dispatcher, session, 100, COMMITTED_ROWS - 1));

        // The savepoint survives its rollback.
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id < 150;")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO before_changes;")->is_success());
        REQUIRE(marks_are_range(dispatcher, session, 100, COMMITTED_ROWS - 1));
    }

    SECTION("a released savepoint's changes belong to the enclosing one") {
        REQUIRE(run(dispatcher, session, "SAVEPOINT outer_point;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1400;")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT inner_point;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (5000);")->is_success());
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT inner_point;")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT outer_point;")->is_success());
        REQUIRE(marks_are_range(dispatcher, session, 100, COMMITTED_ROWS - 1));
    }

    SECTION("a repeated name is released one savepoint at a time") {
        REQUIRE(run(dispatcher, session, "SAVEPOINT repeated;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1400;")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT repeated;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id < 200;")->is_success());
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT repeated;")->is_success());
        // The older savepoint is still there and covers both deletes.
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT repeated;")->is_success());
        REQUIRE(marks_are_range(dispatcher, session, 100, COMMITTED_ROWS - 1));
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT repeated;")->is_success());
    }

    // Other sessions see nothing of the transaction before it commits.
    REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 0, COMMITTED_ROWS - 1));
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 100, COMMITTED_ROWS - 1));
}

// Probed from another session after COMMIT: a statement failing inside the transaction would end it.
TEST_CASE("integration::cpp::savepoint::ddl") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/ddl"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_marks(dispatcher);
    {
        auto setup = otterbrix::session_id_t();
        REQUIRE(
            run(dispatcher, setup, "CREATE TABLE TestDatabase.columns (id bigint, a bigint, b bigint);")->is_success());
        REQUIRE(
            run(dispatcher, setup, "INSERT INTO TestDatabase.columns (id, a, b) VALUES (1, 10, 100);")->is_success());
        REQUIRE(run(dispatcher, setup, "CREATE TABLE TestDatabase.dropped_before (id bigint);")->is_success());
    }
    const auto committed_ok = [&](const std::string& sql) {
        return run(dispatcher, otterbrix::session_id_t(), sql)->is_success();
    };

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());

    SECTION("added columns") {
        REQUIRE(run(dispatcher, session, "ALTER TABLE TestDatabase.columns ADD COLUMN kept bigint;")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT before_add;")->is_success());
        REQUIRE(run(dispatcher, session, "ALTER TABLE TestDatabase.columns ADD COLUMN gone bigint;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.columns (id, gone) VALUES (2, 7);")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_add;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

        REQUIRE(committed_ok("SELECT kept FROM TestDatabase.columns;"));
        REQUIRE_FALSE(committed_ok("SELECT gone FROM TestDatabase.columns;"));
        auto rows = run(dispatcher, otterbrix::session_id_t(), "SELECT id FROM TestDatabase.columns;");
        REQUIRE(rows->is_success());
        REQUIRE(rows->size() == 1);
    }

    SECTION("dropped columns") {
        REQUIRE(run(dispatcher, session, "ALTER TABLE TestDatabase.columns DROP COLUMN a;")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT before_drop;")->is_success());
        REQUIRE(run(dispatcher, session, "ALTER TABLE TestDatabase.columns DROP COLUMN b;")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_drop;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

        REQUIRE_FALSE(committed_ok("SELECT a FROM TestDatabase.columns;"));
        auto rows = run(dispatcher, otterbrix::session_id_t(), "SELECT b FROM TestDatabase.columns;");
        REQUIRE(rows->is_success());
        REQUIRE(rows->size() == 1);
        const auto id = rows->get<int64_t>(0, 0);
        REQUIRE_FALSE(id.has_error());
        REQUIRE(id.value() == 100);
    }

    SECTION("created tables") {
        REQUIRE(run(dispatcher, session, "CREATE TABLE TestDatabase.kept (id bigint);")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT before_create;")->is_success());
        REQUIRE(run(dispatcher, session, "CREATE TABLE TestDatabase.gone (id bigint);")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.gone (id) VALUES (1);")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_create;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

        REQUIRE(committed_ok("SELECT id FROM TestDatabase.kept;"));
        REQUIRE_FALSE(committed_ok("SELECT id FROM TestDatabase.gone;"));
        // The name is free again.
        REQUIRE(committed_ok("CREATE TABLE TestDatabase.gone (id bigint);"));
    }

    SECTION("dropped tables") {
        REQUIRE(run(dispatcher, session, "DROP TABLE TestDatabase.dropped_before;")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT before_drop;")->is_success());
        REQUIRE(run(dispatcher, session, "DROP TABLE TestDatabase.marks;")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_drop;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

        REQUIRE_FALSE(committed_ok("SELECT id FROM TestDatabase.dropped_before;"));
        REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 0, COMMITTED_ROWS - 1));
        // Still a working table, not just a readable one.
        REQUIRE(committed_ok("DELETE FROM TestDatabase.marks WHERE id < 10;"));
        REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 10, COMMITTED_ROWS - 1));
    }
}

// The rolled-back records are in the journal before the COMMIT; replay must not bring them back.
TEST_CASE("integration::cpp::savepoint::restart") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/restart_source"));
    test_clear_directory(config);
    const auto crash_dir = integration_fixture_path("test_savepoint/restart_crash");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        create_marks(dispatcher);

        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id < 100;")->is_success());
        REQUIRE(run(dispatcher, session, "SAVEPOINT before_changes;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1000;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (5000);")->is_success());
        REQUIRE(
            run(dispatcher, session, "UPDATE TestDatabase.marks SET id = id + 10000 WHERE id < 200;")->is_success());
        REQUIRE(run(dispatcher, session, "ALTER TABLE TestDatabase.marks ADD COLUMN gone bigint;")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_changes;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (6000);")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        test_helpers::copy_crash_image(config.main_path, crash_dir);
    }

    auto crash_config = test_create_config(crash_dir);
    test_spaces space(crash_config);
    auto* dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();
    auto below = run(dispatcher, session, "SELECT id FROM TestDatabase.marks WHERE id < 5000 ORDER BY id;");
    REQUIRE(below->is_success());
    REQUIRE(below->size() == static_cast<std::size_t>(COMMITTED_ROWS - 100));
    const auto below_first = below->get<int64_t>(0, 0);
    REQUIRE_FALSE(below_first.has_error());
    REQUIRE(below_first.value() == 100);
    const auto below_last = below->get<int64_t>(0, below->size() - 1);
    REQUIRE_FALSE(below_last.has_error());
    REQUIRE(below_last.value() == COMMITTED_ROWS - 1);
    auto above = run(dispatcher, session, "SELECT id FROM TestDatabase.marks WHERE id >= 5000;");
    REQUIRE(above->is_success());
    REQUIRE(above->size() == 1);
    const auto above_first = above->get<int64_t>(0, 0);
    REQUIRE_FALSE(above_first.has_error());
    REQUIRE(above_first.value() == 6000);
    REQUIRE(run(dispatcher, session, "SELECT gone FROM TestDatabase.marks;")->is_error());
}

TEST_CASE("integration::cpp::savepoint::after_a_failure") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/after_a_failure"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_marks(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id < 100;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT before_failure;")->is_success());
    REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1000;")->is_success());
    REQUIRE(run(dispatcher, session, "SELECT no_such_column FROM TestDatabase.marks;")->is_error());

    SECTION("rollback to makes the transaction usable again") {
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1400;")->is_error());
        REQUIRE(run(dispatcher, session, "SAVEPOINT refused;")->is_error());
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT before_failure;")->is_error());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_failure;")->is_success());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1400;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 100, 1399));
    }

    SECTION("rollback to an unknown savepoint keeps the failure") {
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT unknown;")->is_error());
        REQUIRE(run(dispatcher, session, "DELETE FROM TestDatabase.marks WHERE id >= 1400;")->is_error());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_failure;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 100, COMMITTED_ROWS - 1));
    }

    SECTION("rollback undoes everything") {
        REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());
        REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 0, COMMITTED_ROWS - 1));
    }

    SECTION("commit ends it as a rollback") {
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_error());
        REQUIRE(marks_are_range(dispatcher, otterbrix::session_id_t(), 0, COMMITTED_ROWS - 1));
    }
}

// A CHECK is validated after the INSERT has written, so the failing statement leaves rows behind; a write
// neither parked nor reverted would still show to its own transaction.
TEST_CASE("integration::cpp::savepoint::failed_statement") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/failed_statement"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto setup = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, setup, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(run(dispatcher, setup, "CREATE TABLE TestDatabase.checked (id bigint);")->is_success());
        REQUIRE(run(dispatcher, setup, "ALTER TABLE TestDatabase.checked ADD CONSTRAINT id_positive CHECK (id > 0);")
                    ->is_success());
    }
    const auto ids = [&](const otterbrix::session_id_t& session) {
        auto cursor = run(dispatcher, session, "SELECT id FROM TestDatabase.checked ORDER BY id;");
        REQUIRE(cursor->is_success());
        std::vector<int64_t> out;
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            const auto id = cursor->get<int64_t>(0, row);
            REQUIRE_FALSE(id.has_error());
            out.push_back(id.value());
        }
        return out;
    };

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.checked (id) VALUES (1), (2);")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT before_failure;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.checked (id) VALUES (10);")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.checked (id) VALUES (20), (-1);")->is_error());
    REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT before_failure;")->is_success());

    REQUIRE(ids(session) == std::vector<int64_t>{1, 2});
    REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.checked (id) VALUES (3);")->is_success());
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    REQUIRE(ids(otterbrix::session_id_t()) == std::vector<int64_t>{1, 2, 3});
}

TEST_CASE("integration::cpp::savepoint::unknown_name") {
    auto config = test_create_config(integration_fixture_path("test_savepoint/unknown_name"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_marks(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT known;")->is_success());
    SECTION("release") { REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT unknown;")->is_error()); }
    SECTION("rollback to") { REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT unknown;")->is_error()); }
    SECTION("released already") {
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT known;")->is_success());
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT known;")->is_error());
    }
    REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());
}
