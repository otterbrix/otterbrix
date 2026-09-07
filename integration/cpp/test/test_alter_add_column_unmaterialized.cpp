// ALTER TABLE ADD COLUMN writes a pg_attribute row and stops; the physical column materializes only on the
// first INSERT that carries it, so until then it reads as the column's DEFAULT (or NULL).

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

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

    void make_table_with_unmaterialized_column(otterbrix::wrapper_dispatcher_t* dispatcher) {
        run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1);");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint;");
    }

    // SELECT reads the column via the projection fill; WHERE reads it via the pushed-down predicate, a separate
    // leg run below the fill. Builds a predicate from the projection's answer and requires the predicate leg to agree.
    void require_both_legs_agree(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t rows) {
        std::string predicate;
        {
            auto projected = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
            REQUIRE(projected->size() == rows);
            const auto& cell = projected->value(0, 0);
            predicate = cell.is_null() ? std::string{"extra IS NULL"}
                                       : "extra = " + std::to_string(cell.value<int64_t>());
        }
        INFO("the projection leg answers: " << predicate);
        auto matched = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE " + predicate + ";");
        INFO("the predicate leg kept " << matched->size() << " of " << rows << " row(s)");
        CHECK(matched->size() == rows);
    }

} // namespace

TEST_CASE("integration::cpp::alter_add_column_unmaterialized::select_column_reads_null") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/select_column"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

    auto cur = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).is_null());
}

TEST_CASE("integration::cpp::alter_add_column_unmaterialized::select_star_reads_null") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/select_star"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

    auto cur = run_ok(dispatcher, "SELECT * FROM TestDatabase.t;");
    REQUIRE(cur->size() == 1);
    INFO("columns in the SELECT * result: " << cur->column_count());
    REQUIRE(cur->column_count() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(1, 0).is_null());
}

// A predicate reference pushes the column down a different leg than the projection: it binds as scan graph input.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::predicate_on_column") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/predicate"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

    {
        auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NULL;");
        INFO("an unmaterialized column is NULL in every row, so IS NULL keeps them all");
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

TEST_CASE("integration::cpp::alter_add_column_unmaterialized::aggregates_over_column") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/aggregates"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

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

TEST_CASE("integration::cpp::alter_add_column_unmaterialized::order_and_group_by_column") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/order_group"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
    run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
    run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1), (2);");
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
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::materializing_insert_keeps_old_rows_null") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/materialize"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

    // Read it BEFORE the materializing INSERT, so a fix for only the post-materialization side can't pass this case.
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

TEST_CASE("integration::cpp::alter_add_column_unmaterialized::survives_restart_before_first_insert") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/restart"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        make_table_with_unmaterialized_column(dispatcher);
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

// The DEFAULT backfills existing rows on both sides of materialization (fill_unmaterialized before it,
// row_group_t::add_column at it); mirrors PostgreSQL (since PG 11) in not rewriting the table.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::default_backfills_old_rows") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/with_default"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
    run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
    run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1);");
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

TEST_CASE("integration::cpp::alter_add_column_unmaterialized::default_answers_the_predicate_leg") {
    auto config =
        test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/default_predicate"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
    run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
    run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1), (2);");
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

// The pg_attribute row survives a restart but the DEFAULT publication does not; the column comes back NULL.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::default_survives_restart_before_first_insert") {
    auto config =
        test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/default_restart"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1), (2);");
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

// The read-side fix widens scanned chunks to the catalog's width, so an UPDATE payload is one column wider than
// any row group can hold; the write side narrows it, refusing a statement that would need a real (non-NULL) value there.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::dml_over_the_column") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/dml"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
    run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
    run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1), (2), (3);");
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

    INFO("an UPDATE that WRITES the column is refused as an error cursor — not an abort, and "
         "not a row written without the value");
    {
        auto cur = exec(dispatcher, "UPDATE TestDatabase.t SET extra = 42 WHERE a = 9;");
        REQUIRE(cur->is_error());
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
        INFO("only the row that was inserted with it is non-NULL");
        CHECK(cur->value(0, 0).value<int64_t>() == 1);
    }
}
