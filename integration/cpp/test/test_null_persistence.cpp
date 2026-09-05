#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// NULL values must survive a checkpoint + restart.
//
// They do not. A checkpoint flushes only each column's MAIN segments; the validity bitmap
// is never written. On reopen, `column_data_t::initialize_column_validity`
// (components/table/column_data.cpp) MANUFACTURES one validity segment per data pointer,
// and `column_segment_t`'s constructor 0xFF-fills it — all-valid. The comment there states
// the consequence outright: "reloaded validity reads all-valid". So every NULL in a
// checkpointed table silently becomes a non-NULL zero/empty value.
//
// Why nothing catches this today:
//   * most tables are still IN_MEMORY, so the .otbx load path is rarely taken;
//   * restart tests that DO pass mostly replay the WAL, which rebuilds rows from records
//     and therefore preserves NULLs — the loss only appears once a CHECKPOINT has folded
//     the rows into the .otbx and the WAL no longer carries them;
//   * the existing persistence tests assert on non-NULL values and on row COUNTS, and the
//     count is right either way. That is exactly what makes the corruption silent.
//
// This matters far more after B1a (disk becomes the only storage mode): at that point every
// NULL in every table is subject to it.
//
// The checks below deliberately probe THREE distinct observations of the same fact, because
// a partial fix could satisfy one and not the others: IS NULL as a predicate, the cursor's
// own is_null() on the read-back cell, and a COUNT that ignores NULLs.

TEST_CASE("integration::cpp::test_null_persistence::nulls_survive_checkpoint_and_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_null_persistence/basic");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
    config.log.level = log_t::level::off;

    INFO("phase 1: disk table with NULLs in a nullable column, verified, then CHECKPOINT");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id BIGINT, v BIGINT, s STRING) ;")->is_success());
        REQUIRE(exec("INSERT INTO b.t (id, v, s) VALUES (1, 10, 'a'), (2, NULL, NULL), (3, 30, 'c');")
                    ->is_success());

        // Before the restart the engine has this right — so the phase-2 failure is the
        // restart, not the insert.
        {
            auto cur = exec("SELECT id FROM b.t WHERE v IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
        }
        {
            auto cur = exec("SELECT v FROM b.t ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->value(0, 1).is_null());
        }
        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    INFO("phase 2: restart — the NULLs must still be NULL");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        // The row count is right either way — this is the silent part.
        {
            auto cur = exec("SELECT id FROM b.t;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto cur = exec("SELECT id FROM b.t WHERE v IS NULL;");
            INFO("the NULL BIGINT must still satisfy IS NULL after the restart");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        {
            auto cur = exec("SELECT v FROM b.t ORDER BY id;");
            INFO("the read-back cell itself must report null");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            CHECK(cur->value(0, 1).is_null());
        }
        {
            auto cur = exec("SELECT id FROM b.t WHERE s IS NULL;");
            INFO("a NULL STRING must survive too — the string path has its own segment layout");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        // A NULL must not be counted as a value: COUNT(v) skips NULLs, COUNT(*) does not.
        {
            auto cur = exec("SELECT COUNT(v) FROM b.t;");
            INFO("COUNT(v) must skip the NULL row");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 2);
        }
    }
}

// NULLs INSIDE nested columns must survive too. A LIST/ARRAY/STRUCT column carries
// validity at TWO levels: the top-level cell (the whole list/struct is NULL) and the
// interior (an element / a field is NULL). Both levels live in validity bitmaps that a
// checkpoint must persist; the nested column DATA already round-trips, so a lost bitmap
// here is the same silent corruption as in a flat column — the reloaded cell reads as a
// present zero/empty value.
TEST_CASE("integration::cpp::test_null_persistence::nested_nulls_survive_checkpoint_and_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_null_persistence/nested");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
    config.log.level = log_t::level::off;

    INFO("phase 1: LIST + ARRAY + STRUCT columns with whole-cell and interior NULLs, then CHECKPOINT");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TYPE np_pair AS (a BIGINT, b BIGINT);")->is_success());
        REQUIRE(exec("CREATE TABLE b.n (id BIGINT, l BIGINT[], arr BIGINT[3], p np_pair) "
                     ";")
                    ->is_success());
        REQUIRE(exec("INSERT INTO b.n (id, l, arr, p) VALUES "
                     "(1, ARRAY[10, 20, 30], ARRAY[10, 20, 30], ROW(1, 2));")
                    ->is_success());
        REQUIRE(exec("INSERT INTO b.n (id, l, arr, p) VALUES (2, NULL, NULL, NULL);")->is_success());
        REQUIRE(exec("INSERT INTO b.n (id, l, arr, p) VALUES "
                     "(3, ARRAY[40, NULL, 60], ARRAY[40, NULL, 60], ROW(3, NULL));")
                    ->is_success());

        // Pre-restart the engine has all of this right (proven here), so any phase-2
        // failure is the reload.
        {
            auto cur = exec("SELECT id FROM b.n WHERE l IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
        }
        {
            auto cur = exec("SELECT l FROM b.n WHERE id = 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto v = cur->value(0, 0);
            REQUIRE(v.children().size() == 3);
            REQUIRE(v.children()[0].value<int64_t>() == 40);
            REQUIRE(v.children()[1].is_null());
            REQUIRE(v.children()[2].value<int64_t>() == 60);
        }
        {
            auto cur = exec("SELECT arr FROM b.n WHERE id = 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto v = cur->value(0, 0);
            REQUIRE(v.children().size() == 3);
            REQUIRE(v.children()[1].is_null());
        }
        {
            auto cur = exec("SELECT (p).b FROM b.n WHERE id = 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).is_null());
        }
        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    INFO("phase 2: restart — nested NULLs at both levels must still be NULL");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto cur = exec("SELECT id FROM b.n;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto cur = exec("SELECT id FROM b.n WHERE l IS NULL;");
            INFO("the whole-cell NULL LIST must still satisfy IS NULL");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        {
            auto cur = exec("SELECT id FROM b.n WHERE arr IS NULL;");
            INFO("the whole-cell NULL ARRAY must still satisfy IS NULL");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        {
            auto cur = exec("SELECT id FROM b.n WHERE p IS NULL;");
            INFO("the whole-cell NULL STRUCT must still satisfy IS NULL");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        {
            auto cur = exec("SELECT l FROM b.n WHERE id = 3;");
            INFO("the NULL LIST element must still be NULL after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto v = cur->value(0, 0);
            REQUIRE(v.children().size() == 3);
            CHECK(v.children()[0].value<int64_t>() == 40);
            CHECK(v.children()[1].is_null());
            CHECK(v.children()[2].value<int64_t>() == 60);
        }
        {
            auto cur = exec("SELECT arr FROM b.n WHERE id = 3;");
            INFO("the NULL ARRAY element must still be NULL after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto v = cur->value(0, 0);
            REQUIRE(v.children().size() == 3);
            CHECK(v.children()[1].is_null());
        }
        {
            auto cur = exec("SELECT (p).b FROM b.n WHERE id = 3;");
            INFO("the NULL STRUCT field must still be NULL after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).is_null());
        }
        {
            // (p).b is NULL for id=2 (whole struct NULL) AND id=3 (field NULL).
            auto cur = exec("SELECT id FROM b.n WHERE (p).b IS NULL;");
            INFO("IS NULL over a struct field must see both NULL levels");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 2);
        }
    }
}

// NULLs BEYOND the first row group (1024 rows) and beyond the first vector must survive
// as well. This branch has a documented history of bugs that only appear past the first
// 1024 rows (per-vector / per-row-group state that the first block masks), so a 3-row
// round-trip is not proof. 3000 rows span three row groups; the NULL pattern (every
// 100th id) puts NULLs in ALL of them, including past row 2048.
TEST_CASE("integration::cpp::test_null_persistence::nulls_survive_past_first_row_group") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_null_persistence/multi_rg");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
    config.log.level = log_t::level::off;

    constexpr int64_t ROWS = 3000;    // > 2 row groups of 1024
    constexpr int64_t NULL_STEP = 100; // ids 100, 200, ..., 3000 carry NULLs
    constexpr int64_t NULLS = ROWS / NULL_STEP;

    INFO("phase 1: 3000-row disk table, NULLs every 100th row, verified, then CHECKPOINT");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.big (id BIGINT, v BIGINT, s STRING) ;")->is_success());

        for (int64_t base = 1; base <= ROWS; base += NULL_STEP) {
            std::string sql = "INSERT INTO b.big (id, v, s) VALUES ";
            for (int64_t id = base; id < base + NULL_STEP && id <= ROWS; ++id) {
                if (id != base) {
                    sql += ", ";
                }
                if (id % NULL_STEP == 0) {
                    sql += "(" + std::to_string(id) + ", NULL, NULL)";
                } else {
                    sql += "(" + std::to_string(id) + ", " + std::to_string(id * 2) + ", 's" + std::to_string(id) +
                           "')";
                }
            }
            sql += ";";
            REQUIRE(exec(sql)->is_success());
        }

        {
            auto cur = exec("SELECT COUNT(*) FROM b.big WHERE v IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<int64_t>() == NULLS);
        }
        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    INFO("phase 2: restart — NULLs in EVERY row group must still be NULL");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto cur = exec("SELECT COUNT(*) FROM b.big;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<int64_t>() == ROWS);
        }
        {
            auto cur = exec("SELECT COUNT(*) FROM b.big WHERE v IS NULL;");
            INFO("every 100th BIGINT must still be NULL, across all three row groups");
            REQUIRE(cur->is_success());
            CHECK(cur->value(0, 0).value<int64_t>() == NULLS);
        }
        {
            auto cur = exec("SELECT COUNT(*) FROM b.big WHERE s IS NULL;");
            INFO("every 100th STRING must still be NULL, across all three row groups");
            REQUIRE(cur->is_success());
            CHECK(cur->value(0, 0).value<int64_t>() == NULLS);
        }
        {
            // Rows past the SECOND row-group boundary (id > 2048): the documented
            // past-the-first-vector failure mode.
            auto cur = exec("SELECT COUNT(*) FROM b.big WHERE v IS NULL AND id > 2048;");
            INFO("NULLs past row 2048 (third row group) must survive");
            REQUIRE(cur->is_success());
            CHECK(cur->value(0, 0).value<int64_t>() == (ROWS - 2100) / NULL_STEP + 1); // ids 2100..3000
        }
        {
            auto cur = exec("SELECT v FROM b.big WHERE id = 2900;");
            INFO("a specific NULL cell in the third row group reads back as NULL");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).is_null());
        }
        {
            auto cur = exec("SELECT v FROM b.big WHERE id = 2901;");
            INFO("its non-NULL neighbour keeps its value");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 5802);
        }
        {
            auto cur = exec("SELECT COUNT(v) FROM b.big;");
            INFO("COUNT(v) must skip every NULL in every row group");
            REQUIRE(cur->is_success());
            CHECK(cur->value(0, 0).value<int64_t>() == ROWS - NULLS);
        }
    }
}
