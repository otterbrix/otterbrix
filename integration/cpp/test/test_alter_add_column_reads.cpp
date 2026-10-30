#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return cur;
    }

    void make_table(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& rows) {
        run_ok(dispatcher, "CREATE SCHEMA TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES " + rows + ";");
    }

    void make_table_with_added_column(otterbrix::wrapper_dispatcher_t* dispatcher) {
        make_table(dispatcher, "(1)");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint;");
    }

    void require_both_legs_agree(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t rows) {
        std::string predicate;
        {
            auto projected = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
            REQUIRE(projected->size() == rows);
            auto cell0 = projected->is_null(0, 0);
            REQUIRE_FALSE(cell0.has_error());
            if (cell0.value()) {
                predicate = "extra IS NULL";
            } else {
                auto extra = projected->get<int64_t>(0, 0);
                REQUIRE_FALSE(extra.has_error());
                predicate = "extra = " + std::to_string(extra.value());
            }
        }
        INFO("the projection leg answers: " << predicate);
        auto matched = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE " + predicate + ";");
        INFO("the predicate leg kept " << matched->size() << " of " << rows << " row(s)");
        CHECK(matched->size() == rows);
    }

} // namespace

TEST_CASE("integration::cpp::alter_add_column_reads") {
    auto config = test_create_config(integration_fixture_path("alter_add_column_reads"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    SECTION("select reads null") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_added_column(dispatcher);

        auto cur = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
        REQUIRE(cur->size() == 1);
        auto cell1 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell1.has_error());
        CHECK(cell1.value());
    }

    SECTION("select star reads null") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_added_column(dispatcher);

        auto cur = run_ok(dispatcher, "SELECT * FROM TestDatabase.t;");
        REQUIRE(cur->size() == 1);
        INFO("columns in the SELECT * result: " << cur->column_count());
        REQUIRE(cur->column_count() == 2);
        auto cell2 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell2.has_error());
        CHECK(cell2.value() == 1);
        auto cell3 = cur->is_null(1, 0);
        REQUIRE_FALSE(cell3.has_error());
        CHECK(cell3.value());
    }

    // A predicate reference pushes the column down a different leg than the projection: it binds as scan graph input.
    SECTION("a predicate on the column") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_added_column(dispatcher);

        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NULL;");
            INFO("a column added after the rows were written is NULL in every row, so IS NULL keeps them all");
            REQUIRE(cur->size() == 1);
            auto cell4 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell4.has_error());
            CHECK(cell4.value() == 1);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NOT NULL;");
            CHECK(cur->size() == 0);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra = 1;");
            INFO("NULL = 1 is UNKNOWN, so the row is dropped");
            CHECK(cur->size() == 0);
        }
    }

    SECTION("aggregates over the column") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_added_column(dispatcher);

        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(*) FROM TestDatabase.t;");
            REQUIRE(cur->size() == 1);
            auto cell5 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell5.has_error());
            CHECK(cell5.value() == 1);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(extra) FROM TestDatabase.t;");
            INFO("COUNT of a column that is NULL in every row is 0");
            REQUIRE(cur->size() == 1);
            auto cell6 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell6.has_error());
            CHECK(cell6.value() == 0);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT SUM(extra) FROM TestDatabase.t;");
            INFO("SUM over no non-NULL input is NULL");
            REQUIRE(cur->size() == 1);
            auto cell7 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell7.has_error());
            CHECK(cell7.value());
        }
    }

    SECTION("order by and group by the column") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table(dispatcher, "(1), (2)");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint;");

        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t ORDER BY extra, a;");
            REQUIRE(cur->size() == 2);
            auto cell8 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell8.has_error());
            CHECK(cell8.value() == 1);
            auto cell9 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell9.has_error());
            CHECK(cell9.value() == 2);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT extra, COUNT(*) FROM TestDatabase.t GROUP BY extra;");
            INFO("every row shares the one NULL key, so there is a single group of 2");
            REQUIRE(cur->size() == 1);
            auto cell10 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell10.has_error());
            CHECK(cell10.value());
            auto cell11 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell11.has_error());
            CHECK(cell11.value() == 2);
        }
    }

    // The first materializing INSERT backfills predating rows with NULL; the new row keeps its own value.
    SECTION("a materializing insert keeps old rows null") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_added_column(dispatcher);

        // Read it BEFORE the materializing INSERT, so a fix for only the post-materialization side can't pass here.
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->size() == 1);
            auto cell12 = cur->is_null(1, 0);
            REQUIRE_FALSE(cell12.has_error());
            CHECK(cell12.value());
        }

        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (2, 777);");

        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->size() == 2);
            auto cell13 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell13.has_error());
            CHECK(cell13.value() == 1);
            INFO("the row that predates the column keeps the NULL it read before");
            auto cell14 = cur->is_null(1, 0);
            REQUIRE_FALSE(cell14.has_error());
            CHECK(cell14.value());
            auto cell15 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell15.has_error());
            CHECK(cell15.value() == 2);
            auto cell16 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell16.has_error());
            CHECK(cell16.value() == 777);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NULL;");
            REQUIRE(cur->size() == 1);
            auto cell17 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell17.has_error());
            CHECK(cell17.value() == 1);
        }
    }

    SECTION("the column survives a restart before the first insert") {
        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();
            make_table_with_added_column(dispatcher);
            run_ok(dispatcher, "CHECKPOINT;");
        }

        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();

            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t;");
            INFO("the pg_attribute row outlived the restart; the storage column was never born");
            REQUIRE(cur->size() == 1);
            auto cell18 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell18.has_error());
            CHECK(cell18.value() == 1);
            auto cell19 = cur->is_null(1, 0);
            REQUIRE_FALSE(cell19.has_error());
            CHECK(cell19.value());
        }
    }

    // The DEFAULT backfills the rows that predate the column (row_group_t::add_column), which mirrors
    // PostgreSQL (since PG 11) in not rewriting the table.
    SECTION("a default backfills old rows") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table(dispatcher, "(1)");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint DEFAULT 7;");

        {
            auto cur = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
            INFO("the same constant the materializing INSERT's backfill will write into this row");
            REQUIRE(cur->size() == 1);
            auto cell20 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell20.has_error());
            CHECK(cell20.value() == 7);
        }

        // A row inserted WITHOUT the column takes the DEFAULT — the write path's job.
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (2);");
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->size() == 2);
            auto cell21 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell21.has_error());
            CHECK(cell21.value() == 7);
            auto cell22 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell22.has_error());
            CHECK(cell22.value() == 7);
        }
    }

    SECTION("a default answers the predicate leg") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table(dispatcher, "(1), (2)");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint DEFAULT 7;");

        require_both_legs_agree(dispatcher, 2);

        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NOT NULL;");
            INFO("a published DEFAULT is not NULL, so IS NOT NULL keeps every row");
            CHECK(cur->size() == 2);
        }

        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (3, 7);");
        require_both_legs_agree(dispatcher, 3);
    }

    SECTION("a default survives a restart before the first insert") {
        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();
            make_table(dispatcher, "(1), (2)");
            run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint DEFAULT 7;");
            {
                auto cur = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
                REQUIRE(cur->size() == 2);
                auto cell23 = cur->get<int64_t>(0, 0);
                REQUIRE_FALSE(cell23.has_error());
                CHECK(cell23.value() == 7);
            }
            require_both_legs_agree(dispatcher, 2);
            run_ok(dispatcher, "CHECKPOINT;");
        }

        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();

            {
                auto cur = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
                INFO("the column read 7 before the restart; nothing materialized it in between");
                REQUIRE(cur->size() == 2);
                auto cell24 = cur->get<int64_t>(0, 0);
                REQUIRE_FALSE(cell24.has_error());
                CHECK(cell24.value() == 7);
            }
            require_both_legs_agree(dispatcher, 2);

            run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (3, 7);");
            {
                auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
                REQUIRE(cur->size() == 3);
                for (std::size_t row = 0; row < 3; ++row) {
                    INFO("row " << row);
                    auto cell25 = cur->get<int64_t>(1, row);
                    REQUIRE_FALSE(cell25.has_error());
                    CHECK(cell25.value() == 7);
                }
            }
            require_both_legs_agree(dispatcher, 3);
        }
    }

    SECTION("dml over the column") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table(dispatcher, "(1), (2), (3)");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint;");

        INFO("an UPDATE that only FILTERS on the column rewrites the rows and leaves it NULL");
        {
            auto cur = run_ok(dispatcher, "UPDATE TestDatabase.t SET a = 9 WHERE extra IS NULL;");
            CHECK(cur->affected_rows() == 3);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t;");
            REQUIRE(cur->size() == 3);
            for (std::size_t row = 0; row < 3; ++row) {
                INFO("row " << row);
                auto cell26 = cur->get<int64_t>(0, row);
                REQUIRE_FALSE(cell26.has_error());
                CHECK(cell26.value() == 9);
                auto cell27 = cur->is_null(1, row);
                REQUIRE_FALSE(cell27.has_error());
                CHECK(cell27.value());
            }
        }

        INFO("a DELETE that filters on the column matches nothing (NULL = 5 is UNKNOWN)");
        {
            auto cur = run_ok(dispatcher, "DELETE FROM TestDatabase.t WHERE extra = 5;");
            CHECK(cur->affected_rows() == 0);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(*) FROM TestDatabase.t;");
            REQUIRE(cur->size() == 1);
            auto cell28 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell28.has_error());
            CHECK(cell28.value() == 3);
        }

        INFO("an UPDATE that WRITES the column writes it");
        {
            auto cur = run_ok(dispatcher, "UPDATE TestDatabase.t SET extra = 42 WHERE a = 9;");
            CHECK(cur->affected_rows() == 3);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t;");
            REQUIRE(cur->size() == 3);
            for (std::size_t row = 0; row < 3; ++row) {
                INFO("row " << row);
                auto cell29 = cur->get<int64_t>(0, row);
                REQUIRE_FALSE(cell29.has_error());
                CHECK(cell29.value() == 9);
                auto cell30 = cur->get<int64_t>(1, row);
                REQUIRE_FALSE(cell30.has_error());
                CHECK(cell30.value() == 42);
            }
        }

        INFO("the table still takes INSERTs, and one that carries the column materializes it");
        {
            auto cur = run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (77, 5);");
            CHECK(cur->affected_rows() == 1);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra = 5;");
            REQUIRE(cur->size() == 1);
            auto cell31 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell31.has_error());
            CHECK(cell31.value() == 77);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(extra) FROM TestDatabase.t;");
            REQUIRE(cur->size() == 1);
            INFO("the three rows the UPDATE wrote, plus the row inserted with it");
            auto cell32 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell32.has_error());
            CHECK(cell32.value() == 4);
        }
    }

    SECTION("one insert materializes several added columns") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_ok(dispatcher, "CREATE SCHEMA TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (id bigint, name text);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (id, name) VALUES (1, 'one');");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN a bigint;");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN b bigint;");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (id, name, a, b) VALUES (2, 'two', 42, 43);");

        auto cur = run_ok(dispatcher, "SELECT id, a, b FROM TestDatabase.t ORDER BY id;");
        REQUIRE(cur->size() == 2);
        INFO("the row that predates both columns reads NULL for them");
        auto cell33 = cur->is_null(1, 0);
        REQUIRE_FALSE(cell33.has_error());
        CHECK(cell33.value());
        auto cell34 = cur->is_null(2, 0);
        REQUIRE_FALSE(cell34.has_error());
        CHECK(cell34.value());
        INFO("and each added column keeps its own value, not the other's");
        auto cell35 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell35.has_error());
        CHECK(cell35.value() == 2);
        auto cell36 = cur->get<int64_t>(1, 1);
        REQUIRE_FALSE(cell36.has_error());
        CHECK(cell36.value() == 42);
        auto cell37 = cur->get<int64_t>(2, 1);
        REQUIRE_FALSE(cell37.has_error());
        CHECK(cell37.value() == 43);
    }

    SECTION("rows written while an ADD was open replay as they were") {
        const auto crash_path = integration_fixture_path("alter_add_column_reads_crash");
        std::filesystem::remove_all(crash_path);

        auto run_scenario = [&](const std::string& end_of_add,
                                const std::string& select,
                                const std::vector<std::vector<int64_t>>& expected) {
            {
                test_spaces space(config);
                auto* dispatcher = space.dispatcher();
                run_ok(dispatcher, "CREATE SCHEMA TestDatabase;");
                run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint, b bigint);");
                run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, b) VALUES (1, 10);");
                run_ok(dispatcher, "CHECKPOINT;");

                auto adder = otterbrix::session_id_t();
                REQUIRE(dispatcher->execute_sql(adder, "BEGIN;")->is_success());
                REQUIRE(dispatcher->execute_sql(adder, "ALTER TABLE TestDatabase.t ADD COLUMN c bigint DEFAULT 77;")
                            ->is_success());
                run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, b) VALUES (2, 20);");
                run_ok(dispatcher, "UPDATE TestDatabase.t SET b = 11 WHERE a = 1;");
                REQUIRE(dispatcher->execute_sql(adder, end_of_add)->is_success());

                run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN d bigint DEFAULT 9;");
                run_ok(dispatcher,
                       end_of_add == "COMMIT;" ? "INSERT INTO TestDatabase.t (a, b, c, d) VALUES (3, 30, 33, 39);"
                                               : "INSERT INTO TestDatabase.t (a, b, d) VALUES (3, 30, 39);");
                std::filesystem::copy(config.main_path, crash_path, std::filesystem::copy_options::recursive);
            }

            auto crash_config = test_create_config(crash_path);
            crash_config.log.level = log_t::level::off;
            test_spaces space(crash_config);
            auto cur = run_ok(space.dispatcher(), select);
            REQUIRE(cur->size() == expected.size());
            for (std::size_t row = 0; row < expected.size(); ++row) {
                for (std::size_t column = 0; column < expected[row].size(); ++column) {
                    INFO("row " << row << ", column " << column);
                    const auto cell = cur->get<int64_t>(column, row);
                    REQUIRE_FALSE(cell.has_error());
                    CHECK(cell.value() == expected[row][column]);
                }
            }
        };

        SECTION("the ADD rolls back") {
            run_scenario("ROLLBACK;",
                         "SELECT a, b, d FROM TestDatabase.t ORDER BY a;",
                         {{1, 11, 9}, {2, 20, 9}, {3, 30, 39}});
        }

        SECTION("the ADD commits") {
            run_scenario("COMMIT;",
                         "SELECT a, b, c, d FROM TestDatabase.t ORDER BY a;",
                         {{1, 11, 77, 9}, {2, 20, 77, 9}, {3, 30, 33, 39}});
        }
        std::filesystem::remove_all(crash_path);
    }
}