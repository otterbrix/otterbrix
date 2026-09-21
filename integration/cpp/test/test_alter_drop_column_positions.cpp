#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <sstream>
#include <string>

namespace {

    // Deliberately not a multiple of DEFAULT_VECTOR_CAPACITY: a boundary-aligned load can hide a
    // shift that only shows in a partial batch.
    constexpr std::size_t ROWS = 1500;
    constexpr std::size_t INSERT_BATCH = 500;
    constexpr std::int64_t A_BASE = 100'000;
    constexpr std::int64_t B_BASE = 200'000;
    constexpr std::int64_t C_BASE = 300'000;

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("SQL: " << sql);
        REQUIRE(cur->is_success());
    }

    // Rows [from, from + count) of the full three-column table.
    void insert_all_columns(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t from, std::size_t count) {
        std::size_t done = 0;
        while (done < count) {
            const std::size_t batch = std::min(INSERT_BATCH, count - done);
            std::stringstream query;
            query << "INSERT INTO TestDatabase.t (a, b, c) VALUES ";
            for (std::size_t i = 0; i < batch; ++i) {
                const auto row = static_cast<std::int64_t>(from + done + i);
                query << "(" << (A_BASE + row) << ", " << (B_BASE + row) << ", " << (C_BASE + row) << ")"
                      << (i + 1 == batch ? ";" : ", ");
            }
            run_sql(dispatcher, query.str());
            done += batch;
        }
    }

    // Rows [from, from + count) once b is gone: the same a/c values the three-column form would write.
    void insert_surviving_columns(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t from, std::size_t count) {
        std::size_t done = 0;
        while (done < count) {
            const std::size_t batch = std::min(INSERT_BATCH, count - done);
            std::stringstream query;
            query << "INSERT INTO TestDatabase.t (a, c) VALUES ";
            for (std::size_t i = 0; i < batch; ++i) {
                const auto row = static_cast<std::int64_t>(from + done + i);
                query << "(" << (A_BASE + row) << ", " << (C_BASE + row) << ")" << (i + 1 == batch ? ";" : ", ");
            }
            run_sql(dispatcher, query.str());
            done += batch;
        }
    }

    void seed_three_columns(otterbrix::wrapper_dispatcher_t* dispatcher) {
        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.t (a BIGINT, b BIGINT, c BIGINT);");
        insert_all_columns(dispatcher, 0, ROWS);
    }

    // Every row of `a, c` in insert order, against the values its row number was written with.
    void check_surviving_columns(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t expected_rows) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, c FROM TestDatabase.t ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == expected_rows);
        for (std::size_t row = 0; row < expected_rows; ++row) {
            INFO("row " << row);
            REQUIRE(cur->value(0, row).value<std::int64_t>() == A_BASE + static_cast<std::int64_t>(row));
            REQUIRE(cur->value(1, row).value<std::int64_t>() == C_BASE + static_cast<std::int64_t>(row));
        }
    }

    std::string error_text(const components::cursor::cursor_t& cur) {
        // get_error() on a successful cursor throws, so read it only when there is one.
        return cur.is_error() ? std::string{cur.get_error().what.begin(), cur.get_error().what.end()}
                              : std::string{"<no error: statement reported success>"};
    }

    bool column_present(const components::cursor::cursor_t& cur, std::string_view name) {
        if (cur.chunks().empty()) {
            return false;
        }
        const auto& chunk = cur.chunks().front();
        for (std::uint64_t column = 0; column < chunk.column_count(); ++column) {
            if (chunk.data[column].type().alias() == name) {
                return true;
            }
        }
        return false;
    }

    // SELECT * must fan out the columns this transaction may read, and only those.
    void check_star_is_a_and_c(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.t;");
        INFO("error: " << error_text(*cur));
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 2);
        REQUIRE(column_present(*cur, "a"));
        REQUIRE(column_present(*cur, "c"));
        REQUIRE_FALSE(column_present(*cur, "b"));
    }

} // namespace

// A dropped column keeps its place in the storage and loses it in the transaction's schema, so the
// columns that outlive it must not slide into the hole it leaves.
TEST_CASE("integration::cpp::alter_drop_column_positions") {
    auto config = test_create_config(integration_fixture_path("alter_drop_column_positions"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    SECTION("a dropped middle column moves nothing") {
        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();

            seed_three_columns(dispatcher);
            run_sql(dispatcher, "ALTER TABLE TestDatabase.t DROP COLUMN b;");

            check_surviving_columns(dispatcher, ROWS);
            check_star_is_a_and_c(dispatcher);

            INFO("b is gone from the catalog, so nothing may name it");
            {
                auto session = otterbrix::session_id_t();
                auto cur = dispatcher->execute_sql(session, "SELECT b FROM TestDatabase.t;");
                REQUIRE_FALSE(cur->is_success());
            }

            INFO("rows written after the drop land in the same positions as the rows written before it");
            insert_surviving_columns(dispatcher, ROWS, ROWS);
            check_surviving_columns(dispatcher, 2 * ROWS);
            check_star_is_a_and_c(dispatcher);
        }

        INFO("a restart changes nothing a reader can see");
        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();

            check_surviving_columns(dispatcher, 2 * ROWS);
            check_star_is_a_and_c(dispatcher);

            INFO("and the positions still hold for rows written after the restart");
            insert_surviving_columns(dispatcher, 2 * ROWS, ROWS);
            check_surviving_columns(dispatcher, 3 * ROWS);
        }
    }

    SECTION("two drops leave the survivor alone") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        seed_three_columns(dispatcher);

        // Dropping the FIRST column too leaves c behind two tombstones, the case a positions-follow-the-
        // storage layout gets wrong by two places rather than one.
        run_sql(dispatcher, "ALTER TABLE TestDatabase.t DROP COLUMN b;");
        run_sql(dispatcher, "ALTER TABLE TestDatabase.t DROP COLUMN a;");

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT c FROM TestDatabase.t ORDER BY c;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == ROWS);
            for (std::size_t row = 0; row < ROWS; ++row) {
                INFO("row " << row);
                REQUIRE(cur->value(0, row).value<std::int64_t>() == C_BASE + static_cast<std::int64_t>(row));
            }
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.t;");
            INFO("error: " << error_text(*cur));
            REQUIRE(cur->is_success());
            REQUIRE(cur->column_count() == 1);
            REQUIRE(column_present(*cur, "c"));
        }
    }

    // An UPDATE rewrites the whole row (delete then append), so it is the write path that carries a full
    // payload built from the columns the transaction can see. With a tombstone sitting between a and c,
    // a payload placed by position rather than by identity writes c into b's slot: a keeps its value, c
    // comes back wrong, and nothing refuses it -- so this checks the VALUE of the column it did not name
    // as well as the one it did.
    SECTION("an update past a tombstone writes the right column") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        seed_three_columns(dispatcher);
        run_sql(dispatcher, "ALTER TABLE TestDatabase.t DROP COLUMN b;");

        // Row 7 only: the rows around it are the control, and they must come back untouched.
        constexpr std::int64_t TOUCHED = 7;
        run_sql(dispatcher,
                "UPDATE TestDatabase.t SET c = " + std::to_string(C_BASE + TOUCHED + 1) +
                    " WHERE a = " + std::to_string(A_BASE + TOUCHED) + ";");

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, c FROM TestDatabase.t ORDER BY a;");
        INFO("error: " << error_text(*cur));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == ROWS);
        for (std::size_t row = 0; row < ROWS; ++row) {
            INFO("row " << row);
            const auto expected_c =
                C_BASE + static_cast<std::int64_t>(row) + (static_cast<std::int64_t>(row) == TOUCHED ? 1 : 0);
            REQUIRE(cur->value(0, row).value<std::int64_t>() == A_BASE + static_cast<std::int64_t>(row));
            REQUIRE_FALSE(cur->value(1, row).is_null());
            REQUIRE(cur->value(1, row).value<std::int64_t>() == expected_c);
        }
    }
}