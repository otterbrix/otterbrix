#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

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
        run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
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
            const auto& cell = projected->value(0, 0);
            predicate =
                cell.is_null() ? std::string{"extra IS NULL"} : "extra = " + std::to_string(cell.value<int64_t>());
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
        CHECK(cur->value(0, 0).is_null());
    }

    SECTION("select star reads null") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_added_column(dispatcher);

        auto cur = run_ok(dispatcher, "SELECT * FROM TestDatabase.t;");
        REQUIRE(cur->size() == 1);
        INFO("columns in the SELECT * result: " << cur->column_count());
        REQUIRE(cur->column_count() == 2);
        CHECK(cur->value(0, 0).value<int64_t>() == 1);
        CHECK(cur->value(1, 0).is_null());
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
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
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
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(extra) FROM TestDatabase.t;");
            INFO("COUNT of a column that is NULL in every row is 0");
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 0);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT SUM(extra) FROM TestDatabase.t;");
            INFO("SUM over no non-NULL input is NULL");
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).is_null());
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
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
            CHECK(cur->value(0, 1).value<int64_t>() == 2);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT extra, COUNT(*) FROM TestDatabase.t GROUP BY extra;");
            INFO("every row shares the one NULL key, so there is a single group of 2");
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).is_null());
            CHECK(cur->value(1, 0).value<int64_t>() == 2);
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
            CHECK(cur->value(1, 0).is_null());
        }

        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (2, 777);");

        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->size() == 2);
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
            INFO("the row that predates the column keeps the NULL it read before");
            CHECK(cur->value(1, 0).is_null());
            CHECK(cur->value(0, 1).value<int64_t>() == 2);
            CHECK(cur->value(1, 1).value<int64_t>() == 777);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NULL;");
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
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
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
            CHECK(cur->value(1, 0).is_null());
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
            CHECK(cur->value(0, 0).value<int64_t>() == 7);
        }

        // A row inserted WITHOUT the column takes the DEFAULT — the write path's job.
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (2);");
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
            REQUIRE(cur->size() == 2);
            CHECK(cur->value(1, 0).value<int64_t>() == 7);
            CHECK(cur->value(1, 1).value<int64_t>() == 7);
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
                CHECK(cur->value(0, 0).value<int64_t>() == 7);
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
                CHECK(cur->value(0, 0).value<int64_t>() == 7);
            }
            require_both_legs_agree(dispatcher, 2);

            run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (3, 7);");
            {
                auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
                REQUIRE(cur->size() == 3);
                for (std::size_t row = 0; row < 3; ++row) {
                    INFO("row " << row);
                    CHECK(cur->value(1, row).value<int64_t>() == 7);
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
            CHECK(cur->size() == 3);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t;");
            REQUIRE(cur->size() == 3);
            for (std::size_t row = 0; row < 3; ++row) {
                INFO("row " << row);
                CHECK(cur->value(0, row).value<int64_t>() == 9);
                CHECK(cur->value(1, row).is_null());
            }
        }

        INFO("a DELETE that filters on the column matches nothing (NULL = 5 is UNKNOWN)");
        {
            auto cur = run_ok(dispatcher, "DELETE FROM TestDatabase.t WHERE extra = 5;");
            CHECK(cur->size() == 0);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(*) FROM TestDatabase.t;");
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 3);
        }

        INFO("an UPDATE that WRITES the column writes it");
        {
            auto cur = run_ok(dispatcher, "UPDATE TestDatabase.t SET extra = 42 WHERE a = 9;");
            CHECK(cur->size() == 3);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t;");
            REQUIRE(cur->size() == 3);
            for (std::size_t row = 0; row < 3; ++row) {
                INFO("row " << row);
                CHECK(cur->value(0, row).value<int64_t>() == 9);
                CHECK(cur->value(1, row).value<int64_t>() == 42);
            }
        }

        INFO("the table still takes INSERTs, and one that carries the column materializes it");
        {
            auto cur = run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (77, 5);");
            CHECK(cur->size() == 1);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra = 5;");
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 77);
        }
        {
            auto cur = run_ok(dispatcher, "SELECT COUNT(extra) FROM TestDatabase.t;");
            REQUIRE(cur->size() == 1);
            INFO("the three rows the UPDATE wrote, plus the row inserted with it");
            CHECK(cur->value(0, 0).value<int64_t>() == 4);
        }
    }

    SECTION("one insert materializes several added columns") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (id bigint, name text);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (id, name) VALUES (1, 'one');");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN a bigint;");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN b bigint;");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (id, name, a, b) VALUES (2, 'two', 42, 43);");

        auto cur = run_ok(dispatcher, "SELECT id, a, b FROM TestDatabase.t ORDER BY id;");
        REQUIRE(cur->size() == 2);
        INFO("the row that predates both columns reads NULL for them");
        CHECK(cur->value(1, 0).is_null());
        CHECK(cur->value(2, 0).is_null());
        INFO("and each added column keeps its own value, not the other's");
        CHECK(cur->value(0, 1).value<int64_t>() == 2);
        CHECK(cur->value(1, 1).value<int64_t>() == 42);
        CHECK(cur->value(2, 1).value<int64_t>() == 43);
    }
}