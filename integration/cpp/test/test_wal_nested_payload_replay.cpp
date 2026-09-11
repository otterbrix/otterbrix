#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/table/test/fault_injection_file.hpp>

#include <algorithm>
#include <sstream>
#include <string>

// WAL replay's chunk codec sizes nested columns (LIST/ARRAY/STRUCT) via fixed_type_size()
// (data_chunk_binary.cpp), which returns 0 for them: type, null mask and row count round-trip
// while every element is silently replaced by zero. Each case here reads every element, since a
// row-count or NOT-NULL check alone would still pass on an all-zero column.

namespace {

    constexpr std::size_t CHECKPOINTED_ROWS = 3072;
    constexpr std::size_t JOURNAL_ROWS = 1024;
    constexpr std::size_t TOTAL_ROWS = CHECKPOINTED_ROWS + JOURNAL_ROWS;
    constexpr std::size_t ARRAY_LENGTH = 40;
    constexpr std::size_t INSERT_BATCH = 512;

    int64_t array_element(std::size_t row, std::size_t index) {
        return static_cast<int64_t>(row * 100 + index);
    }

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("SQL: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    void insert_array_rows(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t first, std::size_t count) {
        std::size_t done = 0;
        while (done < count) {
            const std::size_t batch = std::min(INSERT_BATCH, count - done);
            std::stringstream q;
            q << "INSERT INTO TestDatabase.wide (a, payload) VALUES ";
            for (std::size_t i = 0; i < batch; ++i) {
                const std::size_t row = first + done + i;
                q << "(" << row << ", ARRAY[";
                for (std::size_t j = 0; j < ARRAY_LENGTH; ++j) {
                    q << array_element(row, j) << (j + 1 == ARRAY_LENGTH ? "" : ",");
                }
                q << "])" << (i + 1 == batch ? ";" : ", ");
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            done += batch;
        }
    }

} // namespace

// On the unfixed build, `a` reads correctly for all 4096 rows while every `payload` element in
// the last 1024 (journal-only) reads 0 -- type carried, payload dropped.
TEST_CASE("integration::cpp::test_wal_nested_payload_replay::array_payload_survives_replay_of_the_journal") {
    auto config = test_create_config(integration_fixture_path("test_wal_nested_payload_replay/array"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    INFO("phase 1: the first rows, made durable by an explicit CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.wide (a bigint, payload bigint[40]);");
        insert_array_rows(dispatcher, 0, CHECKPOINTED_ROWS);
        run_sql(dispatcher, "CHECKPOINT;");
    }

    INFO("phase 2: more rows, then KILL before any checkpoint — they exist only in the journal");
    {
        // Declared before the engine so the interposer is installed before block managers open files.
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_array_rows(dispatcher, CHECKPOINTED_ROWS, JOURNAL_ROWS);

        plan.fail_writes_from = 1;
    } // ← the destructor's CHECKPOINT runs here and can commit nothing.

    INFO("phase 3: restart — the last rows are rebuilt from the journal, payload included");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, payload FROM TestDatabase.wide ORDER BY a;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == TOTAL_ROWS);

        for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
            INFO("scalar column, row " << i);
            REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
        }

        std::size_t mismatched_cells = 0;
        std::size_t first_bad_row = TOTAL_ROWS;
        std::size_t first_bad_index = 0;
        int64_t first_bad_value = 0;
        int64_t first_bad_expected = 0;
        std::size_t wrong_cardinality_rows = 0;
        std::size_t null_cells = 0;

        for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
            auto cell = cur->value(1, i);
            if (cell.children().size() != ARRAY_LENGTH) {
                ++wrong_cardinality_rows;
                continue;
            }
            for (std::size_t j = 0; j < ARRAY_LENGTH; ++j) {
                const auto& element = cell.children()[j];
                if (element.is_null()) {
                    ++null_cells;
                    continue;
                }
                const int64_t got = element.value<int64_t>();
                const int64_t want = array_element(i, j);
                if (got != want) {
                    ++mismatched_cells;
                    if (first_bad_row == TOTAL_ROWS) {
                        first_bad_row = i;
                        first_bad_index = j;
                        first_bad_value = got;
                        first_bad_expected = want;
                    }
                }
            }
        }

        INFO("first divergence: row " << first_bad_row << " element " << first_bad_index << " read "
                                      << first_bad_value << ", written " << first_bad_expected);
        INFO("mismatched elements: " << mismatched_cells << " of " << (TOTAL_ROWS * ARRAY_LENGTH));
        INFO("rows with the wrong element count: " << wrong_cardinality_rows);
        INFO("elements that came back NULL: " << null_cells);
        CHECK(wrong_cardinality_rows == 0);
        CHECK(null_cells == 0);
        CHECK(mismatched_cells == 0);
    }
}

// ARRAY, LIST and STRUCT are three different physical layouts, so each is gated separately rather
// than inferred from the ARRAY case. Interior NULLs are gated too: a codec that rebuilds content
// while flattening null-ness would be a second, silent corruption.
TEST_CASE("integration::cpp::test_wal_nested_payload_replay::list_and_struct_payload_survive_replay_of_the_journal") {
    auto config = test_create_config(integration_fixture_path("test_wal_nested_payload_replay/list_struct"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    INFO("phase 1: checkpointed rows, so the durable half is known-good and isolated");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TYPE np_pair AS (a BIGINT, b BIGINT);");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.n (id bigint, l bigint[], p np_pair);");
        run_sql(dispatcher,
                "INSERT INTO TestDatabase.n (id, l, p) VALUES "
                "(1, ARRAY[10, 20, 30], ROW(11, 12)), "
                "(2, ARRAY[40, 50], ROW(21, 22));");
        run_sql(dispatcher, "CHECKPOINT;");
    }

    INFO("phase 2: journal-only rows — varying list lengths, interior NULLs — then KILL");
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        run_sql(dispatcher,
                "INSERT INTO TestDatabase.n (id, l, p) VALUES "
                "(3, ARRAY[60, 70, 80, 90, 100], ROW(31, 32));");
        run_sql(dispatcher, "INSERT INTO TestDatabase.n (id, l, p) VALUES (4, ARRAY[110, NULL, 130], ROW(41, NULL));");
        run_sql(dispatcher, "INSERT INTO TestDatabase.n (id, l, p) VALUES (5, NULL, NULL);");

        plan.fail_writes_from = 1;
    } // ← the destructor's CHECKPOINT runs here and can commit nothing.

    INFO("phase 3: restart — the journal rebuilds lists and structs with their contents");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id, l, p FROM TestDatabase.n ORDER BY id;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);

        for (std::size_t i = 0; i < 5; ++i) {
            INFO("scalar column, row " << i);
            REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i + 1));
        }

        INFO("LIST, checkpointed row 1 — the durable path, the control");
        {
            auto cell = cur->value(1, 0);
            REQUIRE(cell.children().size() == 3);
            CHECK(cell.children()[0].value<int64_t>() == 10);
            CHECK(cell.children()[1].value<int64_t>() == 20);
            CHECK(cell.children()[2].value<int64_t>() == 30);
        }

        INFO("LIST, journal-only row 3 — five elements, longer than anything checkpointed");
        {
            auto cell = cur->value(1, 2);
            REQUIRE(cell.children().size() == 5);
            CHECK(cell.children()[0].value<int64_t>() == 60);
            CHECK(cell.children()[1].value<int64_t>() == 70);
            CHECK(cell.children()[2].value<int64_t>() == 80);
            CHECK(cell.children()[3].value<int64_t>() == 90);
            CHECK(cell.children()[4].value<int64_t>() == 100);
        }

        INFO("LIST, journal-only row 4 — content around an interior NULL");
        {
            auto cell = cur->value(1, 3);
            REQUIRE(cell.children().size() == 3);
            CHECK(cell.children()[0].value<int64_t>() == 110);
            CHECK(cell.children()[1].is_null());
            CHECK(cell.children()[2].value<int64_t>() == 130);
        }

        INFO("STRUCT, journal-only row 3 — both fields carry their values");
        {
            auto cell = cur->value(2, 2);
            REQUIRE(cell.children().size() == 2);
            CHECK(cell.children()[0].value<int64_t>() == 31);
            CHECK(cell.children()[1].value<int64_t>() == 32);
        }

        // Projects fields instead of reading the struct value: a NULL field would type the struct
        // <BIGINT, NA>, tripping vector_t::value()'s type-identity assert -- unrelated to the journal.
        INFO("STRUCT, journal-only row 4 — a present field beside a NULL one");
        {
            auto fields = dispatcher->execute_sql(session, "SELECT (p).a, (p).b FROM TestDatabase.n WHERE id = 4;");
            INFO("error: " << (fields->is_error() ? fields->get_error().what : "none"));
            REQUIRE(fields->is_success());
            REQUIRE(fields->size() == 1);
            CHECK(fields->value(0, 0).value<int64_t>() == 41);
            CHECK(fields->value(1, 0).is_null());
        }

        INFO("the whole-cell NULLs of journal-only row 5 are still NULL");
        {
            auto null_list = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.n WHERE l IS NULL;");
            REQUIRE(null_list->is_success());
            REQUIRE(null_list->size() == 1);
            CHECK(null_list->value(0, 0).value<int64_t>() == 5);
        }
        {
            auto null_struct = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.n WHERE p IS NULL;");
            REQUIRE(null_struct->is_success());
            REQUIRE(null_struct->size() == 1);
            CHECK(null_struct->value(0, 0).value<int64_t>() == 5);
        }
    }
}
