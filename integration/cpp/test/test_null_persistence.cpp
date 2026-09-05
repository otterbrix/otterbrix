#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// NULL values must survive a checkpoint + restart. Hazard: a checkpoint flushing only MAIN
// segments leaves the validity bitmap unwritten, so reload manufactures an all-valid
// (0xFF-filled) one per data pointer, turning every checkpointed NULL into a non-NULL
// zero/empty value. Fix: the persisted validity child (column_data.hpp: checkpoint_children —
// child_columns[0] is always the validity child).
//
// Restart tests mostly replay the WAL (no checkpoint involved) and persistence tests assert
// only non-NULL values and row COUNTS, both right either way, so nothing else catches this.
//
// Each case probes THREE independent observations (IS NULL, cursor is_null(), a
// NULL-ignoring COUNT), since a partial fix could satisfy one and not the others.

TEST_CASE("integration::cpp::test_null_persistence::nulls_survive_checkpoint_and_restart") {
    auto config = test_create_config(integration_fixture_path("test_null_persistence/basic"));
    test_clear_directory(config);
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

        // Before the restart the engine has this right, so any phase-2 failure is the
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

// NULLs INSIDE nested columns must survive too: a LIST/ARRAY/STRUCT column carries validity
// at TWO levels (the whole cell, and an interior element/field), each its own bitmap subject
// to the same checkpoint hazard as a flat column.
TEST_CASE("integration::cpp::test_null_persistence::nested_nulls_survive_checkpoint_and_restart") {
    auto config = test_create_config(integration_fixture_path("test_null_persistence/nested"));
    test_clear_directory(config);
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

// NULLs BEYOND the first row group (1024 rows) must survive too -- per-vector/per-row-group
// state can mask bugs that only show past the first block, so a 3-row round-trip isn't proof.
// 3000 rows span three row groups; NULLs every 100th id land in all three, including past 2048.
TEST_CASE("integration::cpp::test_null_persistence::nulls_survive_past_first_row_group") {
    auto config = test_create_config(integration_fixture_path("test_null_persistence/multi_rg"));
    test_clear_directory(config);
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
            // Rows past the second row-group boundary (id > 2048).
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
