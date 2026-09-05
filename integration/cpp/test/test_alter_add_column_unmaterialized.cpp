// ALTER TABLE ... ADD COLUMN writes a pg_attribute row and stops; the physical column
// is materialized later, by the first INSERT that carries it (agent_disk stage 1b
// storage_append_inner, direct_add_column_sync on the replay leg). Deferred on purpose:
// test_alter_rename_column::rename_and_unmaterialized_add_column_are_distinguishable pins
// it by requiring the durable file to still hold TWO columns after an ADD COLUMN with no
// INSERT.
//
// So there's a legal window where the catalog names a column storage has never heard of.
// Before this fix the scan adapter dropped the projected ordinal it couldn't find, turning
// `SELECT extra FROM t` into a zero-column scan that tripped
// `assert(!column_ids_.empty())` in components/table/table_state.cpp (abort in Debug,
// silent wrong answer under NDEBUG).
//
// Cases below pin the answer: the column's DEFAULT (or NULL if none) in every existing
// row -- exactly what the materializing INSERT itself backfills via row_group_t::add_column,
// so the read does not change across materialization.

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

    // CREATE DATABASE + a one-column table holding a single row, then ALTER in a
    // second column and never insert into it. This is the whole reproduction: the
    // engine is now in the legal catalog-ahead-of-storage state.
    void make_table_with_unmaterialized_column(otterbrix::wrapper_dispatcher_t* dispatcher) {
        run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1);");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint;");
    }

    // `SELECT extra` reads the column via the projection (table_storage_adapter_t::
    // fill_unmaterialized); `WHERE extra ...` reads it via the pushed-down predicate
    // (row_group_t::evaluate_predicate), which runs below the projection fill. This asks
    // the first leg for the column's value, builds a predicate from it, and requires the
    // second leg to keep every row -- so it passes iff the two legs agree, whatever the value.
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

// The bare reproduction: naming the column alone is enough.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::select_column_reads_null") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/select_column"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

    auto cur = run_ok(dispatcher, "SELECT extra FROM TestDatabase.t;");
    REQUIRE(cur->size() == 1);
    // The content, not merely the survival: the catalog shows the column, so the
    // row has a cell for it, and that cell is NULL.
    CHECK(cur->value(0, 0).is_null());
}

// SELECT * has to widen to the catalog's shape, not the storage's.
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

// The column read as a PREDICATE, which pushes it down a different leg than the
// projection: the filter binds it as an input and the scan has to feed the graph.
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

// Aggregates: COUNT(col) ignores NULLs, COUNT(*) does not, SUM over all-NULL is NULL.
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

// ORDER BY on the column, and a GROUP BY that keys on it.
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

// The first INSERT that carries the column materializes it and backfills rows that
// predate it: those must keep reading NULL, the new row must read its own value.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::materializing_insert_keeps_old_rows_null") {
    auto config = test_create_config(integration_fixture_path("test_alter_add_column_unmaterialized/materialize"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    make_table_with_unmaterialized_column(dispatcher);

    // Read it BEFORE the materialization, so a regression that only fixes the
    // post-materialization side cannot pass this case.
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

// The catalog row is durable and the storage column is not, so a restart re-enters
// the same state rather than leaving it; the read must answer the same after it.
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

// The DEFAULT backfills existing rows on both sides of materialization: before it,
// table_storage_adapter_t::fill_unmaterialized returns the catalog's constant; at it,
// stage 1b stamps the same constant into the column_definition_t and
// row_group_t::add_column writes it into every pre-existing row. Either half alone would
// flip the answer at the first INSERT.
//
// Mirrors PostgreSQL (since PG 11): ADD COLUMN ... DEFAULT doesn't rewrite the table --
// the constant sits in pg_attribute.attmissingval and older rows read it from there. Our
// marker (added_column_type_t{type, default_spec}) rides the ALTER's commit to the owning
// agent the same way; default_spec is the text stored in pg_attribute.attdefspec.
//
// The write path is separate and unaffected: an INSERT omitting the column takes the
// DEFAULT via enrich_logical_plan::build_insert_fill_list. Restart persistence of the
// parked publication is covered by default_survives_restart_before_first_insert below.
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

// `SELECT extra` and `WHERE extra = ...` read the same column two ways and used to
// disagree: the projection took the DEFAULT from the publication while the pushed-down
// filter fed the graph an all-invalid vector. The predicate here is built from the
// projection's own answer, so the case can't pass unless both legs agree.
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

    // The no-default column is the same demand, answered NULL on both legs -- kept so a
    // "fill the predicate with the default" fix that forgets that half fails too.
    {
        auto cur = run_ok(dispatcher, "SELECT a FROM TestDatabase.t WHERE extra IS NOT NULL;");
        INFO("a published DEFAULT is not NULL, so IS NOT NULL keeps every row");
        CHECK(cur->size() == 2);
    }

    // The materialization must not move the answer either: the first INSERT that carries the
    // column backfills the same constant, so both legs keep saying what they said.
    run_ok(dispatcher, "INSERT INTO TestDatabase.t (a, extra) VALUES (3, 7);");
    require_both_legs_agree(dispatcher, 3);
}

// The pg_attribute row is durable but the publication carrying the default is not, so the
// load path re-derives it -- type only. The column comes back published without its
// default, reads NULL, and the first INSERT then backfills that NULL permanently.
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

        // The first INSERT materializes the column and backfills pre-existing rows from the
        // publication -- a lost default writes NULL there permanently, the point of no return.
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

// The read-side fix widens scanned chunks to the CATALOG's width, so an UPDATE's payload
// chunk comes back one column wider than any row group can hold; the write side must narrow
// it again. An all-NULL trailing column is dropped and the row written (plain
// filter-only UPDATEs); a real value can only be stored by materializing the column, which
// only the append path's schema-growth stage can do, so that statement is refused instead of
// silently dropping the value. Before this fix the drop case aborted in collection_t::append
// on `chunk.column_count() == types_.size()`.
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
