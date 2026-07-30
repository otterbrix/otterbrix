// ============================================================================
// WHAT COERCES AN INSERT WRITE-SET VALUE TO ITS COLUMN'S DECLARED TYPE
//
// A VALUES literal arrives typed by the SQL transformer, not by the catalog: an
// integer literal is BIGINT, a quoted literal is STRING_LITERAL, ROW(...) is a
// STRUCT of those literal types. Something has to turn each of them into the
// column's declared type before storage allocates a buffer from it.
//
// Two places in the dispatcher look like that something, and only ONE of them is
// live:
//
//   * validate_types (services/dispatcher/validate_logical_plan.cpp) HAD four
//     coercion arms -- DATE/duration, DECIMAL, STRUCT, ENUM -- behind a gate that
//     asked whether a TYPE was registered under the CATALOG COLUMN TYPE's alias.
//     operator_resolve_table_t stamps that alias with the COLUMN's name, so for a
//     builtin/decimal column the gate asked "is a type registered whose name is
//     `amount`?" and for a UDT it additionally wanted a resolve-type leaf that the
//     transformer emits only for CREATE TABLE / CREATE TYPE / casts. Measured over
//     the whole suite the gate ran 15691 times and every arm had hit count 0. The
//     arms were removed; this file is what says so out loud.
//
//   * enrich_insert_sync (services/dispatcher/enrich_logical_plan.cpp) rebuilds
//     EVERY write-set column of a non-computing table with the declared type and
//     copies each row through logical_value_t::cast_as. No gate, no opt-in. This is
//     where the work is actually done, and the cases below pin what it can and
//     cannot do.
//
// What cast_as can do bounds the whole feature, because validate_schema's
// is_convertable_to() runs BEFORE enrich and refuses anything cast_as would then
// have to invent:
//
//   int literal    -> DECIMAL   accepted, scaled correctly      (cast_as has it)
//   ROW(...)       -> STRUCT    accepted, children narrowed     (cast_as recurses)
//   'label'        -> ENUM      accepted, resolved to ordinal   (cast_as has it)
//   'text'         -> DECIMAL   REJECTED by is_convertable_to   (cast_as lacks it)
//   '2024-06-01'   -> DATE      REJECTED by is_convertable_to   (cast_as lacks it)
//
// The two rejections are the capability the deleted DATE/duration and DECIMAL arms
// uniquely had: core::date::parse_date/parse_timestamp/... has no counterpart
// anywhere in cast_as. Reinstating string->temporal INSERTs is a SEMANTICS change
// (PostgreSQL accepts them) with a known live consequence -- see
// test_check_constraint_types.cpp, whose date_column_is_unreachable_from_sql case is
// written to go red the day it happens -- so it is deliberately left rejected here
// and pinned, not quietly enabled.
//
// The unknown-ENUM-label case is different in kind and is NOT a pin: cast_as answers
// an unmatched label with an NA VALUE rather than an error, enrich has no error
// channel and treats a non-error as success, so the row used to be stored as NULL
// with nothing reported. That is the one live silent-wrong-answer among the four,
// and it is now refused at validate.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <components/types/types.hpp>
#include <string>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;
using components::types::logical_type;

namespace {

    std::string error_text(const cursor_t_ptr& c) { return c->is_error() ? std::string(c->get_error().what) : ""; }

} // namespace

// A quoted literal into a DATE (and TIMESTAMP) column is refused before any row is
// built. Nothing on the INSERT path parses a string into a temporal value: cast_as
// has no STRING_LITERAL -> DATE branch, and the one parser that did
// (core::date::parse_date, in the deleted validate_types arm) never ran.
TEST_CASE("integration::cpp::write_set_coercion::temporal_string_insert_is_rejected") {
    auto config = make_test_config(test_temp_path("write_set_coercion/temporal"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE C;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE C.t_date (id bigint, d date);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE C.t_ts (id bigint, ts timestamp);")->is_success());

    auto date_ins = exec(dispatcher, "INSERT INTO C.t_date (id, d) VALUES (1, '2024-06-01');");
    INFO("date insert: " << error_text(date_ins));
    REQUIRE(date_ins->is_error());
    CHECK(error_text(date_ins) == "insert_node: can not convert data column[1] type to table type");

    auto ts_ins = exec(dispatcher, "INSERT INTO C.t_ts (id, ts) VALUES (1, '2024-06-01 10:00:00');");
    INFO("timestamp insert: " << error_text(ts_ins));
    REQUIRE(ts_ins->is_error());
    CHECK(error_text(ts_ins) == "insert_node: can not convert data column[1] type to table type");

    // Refused means refused: no row was half-built and stored.
    auto after = exec(dispatcher, "SELECT id, d FROM C.t_date;");
    REQUIRE(after->is_success());
    CHECK(after->size() == 0);
}

// An integer literal into a DECIMAL column IS coerced, and coerced correctly: the
// stored value carries the declared width/scale and the scaled payload (42 -> 4200
// at scale 2). A string literal into the same column is refused -- cast_as has no
// STRING_LITERAL -> DECIMAL branch either.
TEST_CASE("integration::cpp::write_set_coercion::decimal_from_numeric_literal") {
    auto config = make_test_config(test_temp_path("write_set_coercion/decimal"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE C;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE C.t_dec (id bigint, amount decimal(10,2));")->is_success());

    auto ins = exec(dispatcher, "INSERT INTO C.t_dec (id, amount) VALUES (1, 42);");
    INFO("decimal insert: " << error_text(ins));
    REQUIRE(ins->is_success());

    auto sel = exec(dispatcher, "SELECT id, amount FROM C.t_dec;");
    REQUIRE(sel->is_success());
    REQUIRE(sel->size() == 1);
    auto stored = sel->value(1, 0);
    REQUIRE(stored.type().type() == logical_type::DECIMAL);
    const auto* dec =
        static_cast<const components::types::decimal_logical_type_extension*>(stored.type().extension());
    CHECK(dec->width() == 10);
    CHECK(dec->scale() == 2);
    // The value survived the rebuild: had the rebuilt column lost its catalog
    // identity, the append matcher would have filled the column with NULLs.
    CHECK(stored.value<int64_t>() == 4200);

    auto from_string = exec(dispatcher, "INSERT INTO C.t_dec (id, amount) VALUES (2, '7.25');");
    INFO("decimal-from-string insert: " << error_text(from_string));
    REQUIRE(from_string->is_error());
    CHECK(error_text(from_string) == "insert_node: can not convert data column[1] type to table type");
}

// ROW(1,2) is a STRUCT of BIGINT literals; the declared composite type's fields are
// INTEGER. The stored value carries the DECLARED field types, so the per-row
// rebuild recursed into the children and did not merely relabel the top level.
TEST_CASE("integration::cpp::write_set_coercion::struct_row_literal_is_narrowed") {
    auto config = make_test_config(test_temp_path("write_set_coercion/struct"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE C;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TYPE point_t AS (px int, py int);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE C.t_struct (id bigint, p point_t);")->is_success());

    auto ins = exec(dispatcher, "INSERT INTO C.t_struct (id, p) VALUES (1, ROW(1,2));");
    INFO("struct insert: " << error_text(ins));
    REQUIRE(ins->is_success());

    auto sel = exec(dispatcher, "SELECT id, p FROM C.t_struct;");
    REQUIRE(sel->is_success());
    REQUIRE(sel->size() == 1);
    auto stored = sel->value(1, 0);
    REQUIRE(stored.type().type() == logical_type::STRUCT);
    REQUIRE(stored.type().child_types().size() == 2);
    CHECK(stored.type().child_types()[0].field_name() == "px");
    CHECK(stored.type().child_types()[1].field_name() == "py");
    CHECK(stored.type().child_types()[0].type() == logical_type::INTEGER);
    CHECK(stored.type().child_types()[1].type() == logical_type::INTEGER);
}

// The one thing validate_types still does to a write-set: on a schemaless
// computing table (relkind='g'), a column whose every value is NULL is NA-typed
// and carries no storable type, so it is an ABSENT key rather than a real column
// and is dropped from the chunk. Handing an all-NA column to storage crashes the
// append, so the drop is load-bearing. Pinned here because the surrounding
// per-column loop was removed with the dead coercion arms and this is what was
// left standing.
TEST_CASE("integration::cpp::write_set_coercion::computing_table_drops_all_null_column") {
    auto config = make_test_config(test_temp_path("write_set_coercion/computing"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE C;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE C.g();")->is_success());

    // `b` is NULL in every row of the write-set: an NA-typed column.
    auto ins = exec(dispatcher, "INSERT INTO C.g (a, b) VALUES (1, NULL), (2, NULL);");
    INFO("all-null-column insert: " << error_text(ins));
    REQUIRE(ins->is_success());

    auto sel = exec(dispatcher, "SELECT a FROM C.g;");
    REQUIRE(sel->is_success());
    CHECK(sel->size() == 2);

    // A LEADING all-NULL column exercises the same drop with a following column to
    // shift down -- the case the old in-loop erase mutated the container it was
    // iterating.
    auto ins2 = exec(dispatcher, "INSERT INTO C.g (c, d) VALUES (NULL, 7), (NULL, 8);");
    INFO("leading-all-null-column insert: " << error_text(ins2));
    REQUIRE(ins2->is_success());

    auto sel2 = exec(dispatcher, "SELECT d FROM C.g WHERE d > 0;");
    REQUIRE(sel2->is_success());
    CHECK(sel2->size() == 2);
}

// A known label is resolved to its ordinal. An UNKNOWN label must be REFUSED: it
// used to be accepted and silently stored as NULL, because cast_as answers an
// unmatched label with an NA value rather than an error and enrich_insert_sync has
// no error channel to notice. The same unknown label in a WHERE clause has always
// errored ("enum value 'purple' not found in ENUM column"), so the write path was
// the lenient one of the pair.
TEST_CASE("integration::cpp::write_set_coercion::enum_label_coercion") {
    auto config = make_test_config(test_temp_path("write_set_coercion/enum"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE C;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TYPE oddness_t AS ENUM ('even', 'odd');")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE C.t_enum (id bigint, o oddness_t);")->is_success());

    auto good = exec(dispatcher, "INSERT INTO C.t_enum (id, o) VALUES (1, 'odd');");
    INFO("known-label insert: " << error_text(good));
    REQUIRE(good->is_success());

    {
        auto sel = exec(dispatcher, "SELECT id, o FROM C.t_enum;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 1);
        auto stored = sel->value(1, 0);
        REQUIRE(stored.type().type() == logical_type::ENUM);
        CHECK(stored.value<int32_t>() == 1);
    }

    auto bad = exec(dispatcher, "INSERT INTO C.t_enum (id, o) VALUES (2, 'purple');");
    INFO("unknown-label insert: " << (bad->is_error() ? error_text(bad)
                                                      : std::string("SUCCEEDED -- unknown ENUM label accepted")));
    REQUIRE(bad->is_error());
    CHECK(error_text(bad) == "insert_node: enum 'oddness_t' does not contain value: 'purple'");

    // And it left nothing behind.
    {
        auto sel = exec(dispatcher, "SELECT id, o FROM C.t_enum;");
        REQUIRE(sel->is_success());
        CHECK(sel->size() == 1);
    }

    // A multi-row VALUES is refused as a whole when any row carries an unknown
    // label -- the check walks every row, not just the first.
    auto mixed = exec(dispatcher, "INSERT INTO C.t_enum (id, o) VALUES (3, 'even'), (4, 'mauve');");
    INFO("mixed insert: " << error_text(mixed));
    REQUIRE(mixed->is_error());
    {
        auto sel = exec(dispatcher, "SELECT id, o FROM C.t_enum;");
        REQUIRE(sel->is_success());
        CHECK(sel->size() == 1);
    }
}
