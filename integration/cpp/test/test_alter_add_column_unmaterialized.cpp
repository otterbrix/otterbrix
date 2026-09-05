// ============================================================================
// READING A COLUMN THAT THE CATALOG HAS AND THE STORAGE DOES NOT.
//
// ALTER TABLE ... ADD COLUMN writes a pg_attribute row and STOPS. The physical
// column is materialized later, by the first INSERT that carries it (agent_disk
// stage 1b `storage_append_inner`, and `direct_add_column_sync` on the replay
// leg). That deferral is deliberate — test_alter_rename_column's
// `rename_and_unmaterialized_add_column_are_distinguishable` pins it by REQUIRE-ing
// that the durable file still holds TWO columns after an ADD COLUMN with no INSERT
// behind it.
//
// So there is a LEGAL window in which the catalog names a column the storage has
// never heard of, and every reader has to survive it. It did not: the scan
// adapter dropped a projected ordinal it could not find in the storage, which
// turned `SELECT extra FROM t` into a scan of ZERO columns and tripped
// `assert(!column_ids_.empty())` in components/table/table_state.cpp — an abort on
// the READ path, i.e. a host process killed by a plain SELECT, and under NDEBUG a
// zero-column scan answering silently.
//
// THE ANSWER THESE CASES PIN: NULL for every existing row — which is exactly what
// the materializing INSERT itself backfills those rows with (row_group_t::add_column
// fills pre-existing rows with the column's default, and the definition the
// materialization stage builds carries none). The read therefore does not change
// when the column is finally materialized, and the last case asserts that boundary
// directly: old rows stay NULL, the new one carries its value.
// ============================================================================

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

    // CREATE DATABASE + a one-column table holding a single row, then ALTER in a
    // second column and never insert into it. This is the whole reproduction: the
    // engine is now in the legal catalog-ahead-of-storage state.
    void make_table_with_unmaterialized_column(otterbrix::wrapper_dispatcher_t* dispatcher) {
        run_ok(dispatcher, "CREATE DATABASE TestDatabase;");
        run_ok(dispatcher, "CREATE TABLE TestDatabase.t (a bigint);");
        run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (1);");
        run_ok(dispatcher, "ALTER TABLE TestDatabase.t ADD COLUMN extra bigint;");
    }

} // namespace

// The bare reproduction: naming the column alone is enough.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::select_column_reads_null") {
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/select_column");
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
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/select_star");
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
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/predicate");
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
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/aggregates");
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
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/order_group");
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

// THE BOUNDARY. The first INSERT that carries the column materializes it and
// backfills the rows that predate it. Those rows must keep answering NULL, and the
// new row must answer with its value — i.e. the pre-materialization read above was
// not a different answer from the post-materialization one.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::materializing_insert_keeps_old_rows_null") {
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/materialize");
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

// The catalog row is durable and the storage column is not, so a restart RE-ENTERS
// the same state rather than leaving it. The read must answer the same after it.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::survives_restart_before_first_insert") {
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/restart");
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

// A DEFAULT does not change the answer for rows that predate the column: the
// materializing INSERT backfills them with NULL (the definition stage 1b builds
// carries no default), so the read has to say NULL too or it would flip the moment
// the column is materialized. The DEFAULT is expanded on the WRITE path, for rows
// inserted without the column — enrich_logical_plan's build_insert_fill_list is
// the single oracle for that — and the second half of this case pins that split.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::default_does_not_backfill_old_rows") {
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/with_default");
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
        INFO("same answer the materializing INSERT's backfill would give this row");
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).is_null());
    }

    // A row inserted WITHOUT the column takes the DEFAULT — the write path's job.
    run_ok(dispatcher, "INSERT INTO TestDatabase.t (a) VALUES (2);");
    {
        auto cur = run_ok(dispatcher, "SELECT a, extra FROM TestDatabase.t ORDER BY a;");
        REQUIRE(cur->size() == 2);
        CHECK(cur->value(1, 0).is_null());
        CHECK(cur->value(1, 1).value<int64_t>() == 7);
    }
}

// DML OVER A TABLE THAT HAS ONE. The read fix widens the chunks the scan produces to the
// CATALOG's width, and an UPDATE's payload is one of those chunks — it comes back one column
// wider than any row group can hold. So the write side has to narrow it again, and the two
// halves of that are on opposite sides of rule 6:
//   * the trailing column carries nothing -> drop it and write the row (this is every UPDATE
//     that merely FILTERS on the new column, which is what the first case does);
//   * it carries a value -> that value can only be stored by materializing the column, which
//     only the append path's schema-growth stage can do. Writing the row without it would lose
//     it silently, so the statement is refused and nothing changes.
// Before the fix the first case aborted in collection_t::append on
// `chunk.column_count() == types_.size()`.
TEST_CASE("integration::cpp::alter_add_column_unmaterialized::dml_over_the_column") {
    auto config = test_create_config("/tmp/test_alter_add_column_unmaterialized/dml");
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
