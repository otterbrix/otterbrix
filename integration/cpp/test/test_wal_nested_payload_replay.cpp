#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/table/test/fault_injection_file.hpp>

#include <algorithm>
#include <sstream>
#include <string>

// A NESTED COLUMN'S PAYLOAD MUST SURVIVE THE JOURNAL, not only the checkpoint.
//
// Recovery has two sources and they are not interchangeable. Rows made durable by a CHECKPOINT
// come back out of the `.otbx` through the column-tree loader, which has always carried nested
// payload recursively (`[validity, ...children]` block pointers). Rows written AFTER the last
// checkpoint come back out of the WAL, through the chunk codec in
// components/vector/data_chunk_binary.cpp — and that codec sized every column by
// `fixed_type_size()`, which answers 0 for LIST, ARRAY and STRUCT. Writer and reader agreed on
// that zero: the writer emitted `data_size = 0` and no bytes, the reader memcpy'd 0 bytes into
// a correctly-SHAPED but zero-filled nested column. The type, the null mask and the row count
// all round-tripped while the CONTENT of every list element, array element and struct field was
// silently replaced by zero.
//
// So the defect is invisible to every test that ends its scope cleanly:
// base_otterbrix_t::~base_otterbrix_t issues a CHECKPOINT, which moves the rows to the `.otbx`
// path that works and hides the journal path that does not. THE CRASH IS THE TEST, taken only
// through the fault-injection seam — arming `fail_writes_from = 1` makes every later `.otbx`
// write fail, so the destructor's checkpoint commits nothing and the post-checkpoint rows stay
// WAL-only. The WAL is a different file and does not go through the block manager's interposer,
// so the records themselves survive.
//
// THE GATE IS THE CONTENT, ELEMENT BY ELEMENT. A row count, a NOT-NULL check and a cardinality
// check all pass on a column whose every element has been zeroed — those were exactly the
// assertions that let this sit. Each case reads every element of every replayed cell.

namespace {

    constexpr std::size_t CHECKPOINTED_ROWS = 3072; // 1.5 row groups, made durable in the .otbx
    constexpr std::size_t JOURNAL_ROWS = 1024;      // written after that checkpoint — WAL-only
    constexpr std::size_t TOTAL_ROWS = CHECKPOINTED_ROWS + JOURNAL_ROWS;
    constexpr std::size_t ARRAY_LENGTH = 40;
    constexpr std::size_t INSERT_BATCH = 512;

    // Content-addressed, so a cell that came back as a present zero is a mismatch rather than a
    // row count that happens to agree.
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

// CASE 1 — ARRAY, the shape the defect was first seen on. Rows 0..3071 are checkpointed and
// come back through the `.otbx`; rows 3072..4095 are journal-only and come back through the
// chunk codec. On the unfixed build the scalar column `a` is right for all 4096 rows and every
// one of the 40 elements of `payload` reads 0 for the last 1024 — the exact signature of a
// codec that carried the TYPE and dropped the PAYLOAD.
TEST_CASE("integration::cpp::test_wal_nested_payload_replay::array_payload_survives_replay_of_the_journal") {
    auto config = test_create_config(integration_fixture_path("test_wal_nested_payload_replay/array"));
    test_clear_directory(config);
    config.wal.on = true;
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
        // Declared before the engine so the interposer is installed when the block managers open
        // their files (wrap() runs once per open) and is still installed while the engine tears
        // down. Every knob stays off until the kill is armed, so phase 2 runs normally.
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        insert_array_rows(dispatcher, CHECKPOINTED_ROWS, JOURNAL_ROWS);

        // KILL. fail_writes_from is compared with >=, so 1 fails every write from here on
        // without the test having to count them. The engine is idle between statements, so this
        // write to the shared plan cannot race an in-flight one.
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

        // The scalar column first: it is carried by the SAME record and the same codec, so a
        // failure here would mean the record never replayed at all rather than that the nested
        // payload was dropped. Keeping the two claims apart is what makes the second one legible.
        for (std::size_t i = 0; i < TOTAL_ROWS; ++i) {
            INFO("scalar column, row " << i);
            REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
        }

        // ELEMENT BY ELEMENT, every row, both halves. Mismatches are accumulated so the report
        // names the FIRST divergence and how far it spreads, instead of stopping at row 3072
        // with no indication of whether the rest is wrong too.
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

// CASE 2 — LIST and STRUCT, on the same crash window. ARRAY, LIST and STRUCT are three
// different physical layouts (a stride into a flat child, a {offset,length} pair into a
// separately-sized child, and one row-aligned child per field) and the codec sized all three at
// zero, so each has to be gated on its own rather than inferred from the ARRAY case.
//
// The interior NULLs are here on purpose. Validity has its own history of being lost across
// this boundary, and a payload codec that rebuilds elements while flattening their null-ness
// is a second silent corruption wearing the first one's clothes: the gate is the CONTENT AND
// THE NULLS TOGETHER, on rows that exist only in the journal.
TEST_CASE("integration::cpp::test_wal_nested_payload_replay::list_and_struct_payload_survive_replay_of_the_journal") {
    auto config = test_create_config(integration_fixture_path("test_wal_nested_payload_replay/list_struct"));
    test_clear_directory(config);
    config.wal.on = true;
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

        // Row 3 carries a longer list than anything checkpointed, so a reader that rebuilt the
        // child from the row count rather than from the written span comes up short.
        run_sql(dispatcher,
                "INSERT INTO TestDatabase.n (id, l, p) VALUES "
                "(3, ARRAY[60, 70, 80, 90, 100], ROW(31, 32));");
        // Row 4 puts a NULL inside both a list and a struct.
        run_sql(dispatcher, "INSERT INTO TestDatabase.n (id, l, p) VALUES (4, ARRAY[110, NULL, 130], ROW(41, NULL));");
        // Row 5 makes the whole nested cells NULL, which is the OTHER validity level.
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

        // Read through a FIELD PROJECTION, not as a whole struct value: reconstructing a
        // struct value derives its type from the field VALUES, so a NULL field yields a struct
        // typed <BIGINT, NA> and trips vector_t::value()'s type-identity assert. That is a
        // pre-existing limitation of the struct read-back, unrelated to the journal.
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
