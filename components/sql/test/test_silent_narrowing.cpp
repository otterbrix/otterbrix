// The contract: a clause the executor does not implement is REFUSED at the
// transformer, never silently dropped, and a literal under a declared cast carries
// the DECLARED type, never the one the literal happened to parse as.
//
// Unrefused, every case here is a statement that reports success while quietly
// answering a different question:
//   - a window function runs as a plain aggregate (one value per group);
//   - SELECT ... INTO returns rows and creates no table;
//   - FOR UPDATE locks nothing;
//   - a duplicate WITH name keeps the FIRST body and drops the second;
//   - CREATE TABLE swallows EXCLUDE and every constraint ATTRIBUTE;
//   - CREATE SEQUENCE swallows CYCLE / CACHE / OWNED BY / RESTART;
//   - CAST(1.5 AS INT) answers a DOUBLE, CAST('123' AS BIGINT) a string.

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

using v = components::types::logical_value_t;

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = transformer.transform(transform::pg_cell_to_node_cast(select));                                  \
        REQUIRE(std::string_view{result.get_error().what} == RESULT);                                                  \
    }

#define TEST_TRANSFORMER_OK(QUERY)                                                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = transformer.transform(transform::pg_cell_to_node_cast(select));                                  \
        REQUIRE_FALSE(result.get_error().contains_error());                                                            \
    }

// A single-parameter WHERE clause: the one bound constant must equal VALUE —
// type included. logical_value_t::operator== is exact, so a double 2.0 does
// NOT pass for an int32 2.
#define TEST_WHERE_PARAM(QUERY, VALUE)                                                                                 \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto wrap = transformer.transform(transform::pg_cell_to_node_cast(select)).finalize();                         \
        REQUIRE(!wrap.has_error());                                                                                    \
        auto result = wrap.value();                                                                                    \
        auto params = result.parameters;                                                                               \
        REQUIRE(params->parameters().parameters.size() == 1);                                                          \
        const auto& bound = params->parameter(core::parameter_id_t(uint16_t(0)));                                      \
        /* type first: operator== over two different types is an ASSERT, not false */                                  \
        REQUIRE(bound.type() == (VALUE).type());                                                                       \
        REQUIRE(bound == (VALUE));                                                                                     \
    }

TEST_CASE("components::sql::narrowing::window_functions_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_ERROR(
        "SELECT sum(number) OVER () FROM db.tbl;",
        R"_(window function OVER is not supported yet: sum(...) would have been computed as a plain aggregate)_");
    TEST_TRANSFORMER_ERROR(
        "SELECT number, avg(number) OVER (PARTITION BY name) FROM db.tbl;",
        R"_(window function OVER is not supported yet: avg(...) would have been computed as a plain aggregate)_");
    // The named-window form: the WINDOW clause itself is dropped by the
    // transformer, with or without an OVER referencing it.
    TEST_TRANSFORMER_ERROR("SELECT sum(number) FROM db.tbl WINDOW w AS (PARTITION BY name);",
                           R"_(the WINDOW clause is not supported yet)_");
    // An OVER buried in an expression, not at the top of the select list.
    TEST_TRANSFORMER_ERROR(
        "SELECT number + sum(number) OVER () FROM db.tbl;",
        R"_(window function OVER is not supported yet: sum(...) would have been computed as a plain aggregate)_");
    // Aggregate-internal ORDER BY / WITHIN GROUP share the same seam: the
    // ordering the user asked for is read by nobody.
    TEST_TRANSFORMER_ERROR(
        "SELECT array_agg(name ORDER BY number) FROM db.tbl;",
        R"_(aggregate ORDER BY / WITHIN GROUP is not supported yet: the ordering would have been dropped)_");
    // And VARIADIC: func_variadic is read by nobody, so f(VARIADIC arr) would
    // quietly run as f(arr) — a different call.
    TEST_TRANSFORMER_ERROR(
        "SELECT concat_ws(VARIADIC name) FROM db.tbl;",
        R"_(VARIADIC is not supported yet: the argument would have been passed unexpanded)_");
}

TEST_CASE("components::sql::narrowing::select_shape_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_ERROR(
        "SELECT * INTO db.tbl2 FROM db.tbl;",
        R"_(SELECT ... INTO is not supported yet: rows would have come back and no table would have been created)_");
    TEST_TRANSFORMER_ERROR(
        "SELECT * FROM db.tbl FOR UPDATE;",
        R"_(the locking clause (FOR UPDATE / FOR SHARE) is not supported yet: rows would not have been locked)_");
    TEST_TRANSFORMER_ERROR(
        "SELECT * FROM db.tbl FOR SHARE;",
        R"_(the locking clause (FOR UPDATE / FOR SHARE) is not supported yet: rows would not have been locked)_");
}

TEST_CASE("components::sql::narrowing::duplicate_with_names_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // A silent emplace no-op on the second `c` runs the query against the FIRST
    // body and reports success.
    TEST_TRANSFORMER_ERROR(
        "WITH c AS (SELECT a FROM db.big), c AS (SELECT b FROM db.big) SELECT * FROM c;",
        R"_(WITH query name "c" specified more than once)_");
    // A name arriving from ANOTHER WITH of the same statement: with one flat
    // registration map the reference would silently resolve to whichever body
    // registered FIRST — for a shadowing inner WITH, the wrong one.
    TEST_TRANSFORMER_ERROR(
        "WITH c AS (SELECT a FROM db.big) SELECT * FROM (WITH c AS (SELECT b FROM db.big) SELECT * FROM c) s;",
        R"_(WITH query name "c" is already defined in this statement: WITH scoping is not supported yet)_");
    // Distinct names stay accepted.
    TEST_TRANSFORMER_OK("WITH c AS (SELECT a FROM db.big), d AS (SELECT b FROM db.big) SELECT * FROM c;");
}

TEST_CASE("components::sql::narrowing::create_table_constraint_kinds_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // Table-level EXCLUDE parses, and a `default: continue` in
    // extract_table_constraints makes it vanish — the table created without it.
    TEST_TRANSFORMER_ERROR(
        "CREATE TABLE db.tbl (a INT, EXCLUDE (a WITH =));",
        R"_(EXCLUDE constraints are not supported yet: the constraint would have been silently dropped)_");
    TEST_TRANSFORMER_ERROR(
        "CREATE TABLE db.tbl (a INT, CONSTRAINT ex EXCLUDE (a WITH =));",
        R"_(EXCLUDE constraints are not supported yet: the constraint would have been silently dropped)_");
    // Column-level constraint ATTRIBUTES (deferrability) ride the same seam in
    // extract_column_constraints.
    TEST_TRANSFORMER_ERROR(
        "CREATE TABLE db.tbl (a INT UNIQUE DEFERRABLE);",
        R"_(the DEFERRABLE constraint attribute is not supported yet: it would have been silently dropped)_");
    TEST_TRANSFORMER_ERROR(
        "CREATE TABLE db.tbl (a INT UNIQUE INITIALLY DEFERRED);",
        R"_(the INITIALLY DEFERRED constraint attribute is not supported yet: it would have been silently dropped)_");
    // Table-level deferrability travels as FIELDS on the constraint node
    // (processCASbits in gram.y), not as separate ATTR entries — a second
    // carrier for the same dropped attribute.
    TEST_TRANSFORMER_ERROR(
        "CREATE TABLE db.tbl (a INT, UNIQUE (a) DEFERRABLE);",
        R"_(the DEFERRABLE constraint attribute is not supported yet: it would have been silently dropped)_");
    TEST_TRANSFORMER_ERROR(
        "CREATE TABLE db.tbl (a INT, UNIQUE (a) DEFERRABLE INITIALLY DEFERRED);",
        R"_(the INITIALLY DEFERRED constraint attribute is not supported yet: it would have been silently dropped)_");
    // NOT DEFERRABLE / INITIALLY IMMEDIATE restate the default: accepted.
    TEST_TRANSFORMER_OK("CREATE TABLE db.tbl (a INT UNIQUE NOT DEFERRABLE);");
    TEST_TRANSFORMER_OK("CREATE TABLE db.tbl (a INT UNIQUE INITIALLY IMMEDIATE);");
    // An explicit NULL column marker restates the default: accepted.
    TEST_TRANSFORMER_OK("CREATE TABLE db.tbl (a INT NULL);");
}

TEST_CASE("components::sql::narrowing::create_sequence_options_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_ERROR(
        "CREATE SEQUENCE db.seq CYCLE;",
        R"_(CREATE SEQUENCE ... CYCLE is not supported yet: the sequence would have been created NO CYCLE)_");
    TEST_TRANSFORMER_ERROR(
        "CREATE SEQUENCE db.seq CACHE 10;",
        R"_(CREATE SEQUENCE ... CACHE is not supported yet (only CACHE 1, the default, is accepted))_");
    TEST_TRANSFORMER_ERROR(
        "CREATE SEQUENCE db.seq OWNED BY db.tbl.col;",
        R"_(CREATE SEQUENCE ... OWNED BY is not supported yet: the ownership dependency would have been dropped)_");
    TEST_TRANSFORMER_ERROR("CREATE SEQUENCE db.seq RESTART;",
                           R"_(RESTART is not supported in CREATE SEQUENCE)_");
    TEST_TRANSFORMER_ERROR("CREATE SEQUENCE db.seq RESTART WITH 5;",
                           R"_(RESTART is not supported in CREATE SEQUENCE)_");
    // The default-restating spellings stay accepted.
    TEST_TRANSFORMER_OK("CREATE SEQUENCE db.seq NO CYCLE;");
    TEST_TRANSFORMER_OK("CREATE SEQUENCE db.seq CACHE 1;");
    TEST_TRANSFORMER_OK("CREATE SEQUENCE db.seq OWNED BY NONE;");
}

TEST_CASE("components::sql::narrowing::cast_targets_honoured") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // Numeric literal, integer target: the DECLARED type answers, fractions
    // round half away from zero (PostgreSQL numeric -> int).
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(1.5 AS INT);", v(&resource, int32_t{2}));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(2.4 AS SMALLINT);", v(&resource, int16_t{2}));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(1 AS BIGINT);", v(&resource, int64_t{1}));
    // Numeric literal, float target: 1 becomes 1.0, not an int64.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(1 AS DOUBLE PRECISION);", v(&resource, double{1.0}));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(1 AS REAL);", v(&resource, float{1.0f}));
    // String literal, numeric target: parsed, not passed through as a string.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST('123' AS BIGINT);", v(&resource, int64_t{123}));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST('9223372036854775807' AS BIGINT);",
                     v(&resource, std::numeric_limits<int64_t>::max()));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST('12.5' AS DOUBLE PRECISION);", v(&resource, double{12.5}));
    // BOOLEAN accepts PostgreSQL's full literal set, not just 't'.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST('yes' AS BOOLEAN);", v(&resource, true));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST('off' AS BOOLEAN);", v(&resource, false));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(2 AS BOOLEAN);", v(&resource, true));
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(0 AS BOOLEAN);", v(&resource, false));
    // What cannot be honoured is refused, never handed back untyped.
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST('abc' AS BIGINT);",
                           R"_(invalid input for a cast to BIGINT: 'abc')_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST('1.5' AS INT);",
                           R"_(invalid input for a cast to INTEGER: '1.5')_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST(300 AS TINYINT);",
                           R"_(value out of range for a cast to TINYINT: 300)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST('nope' AS BOOLEAN);",
                           R"_(invalid input for a cast to BOOLEAN: 'nope')_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST(1.5 AS BOOLEAN);",
                           R"_(invalid input for a cast to BOOLEAN: 1.5)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST('x' AS UUID);",
                           R"_(a literal cast to UUID is not supported yet)_");
    // A refused TARGET TYPE is a refusal, not a silent string — NUMERIC without
    // (width, scale) is the case.
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = CAST(1.5 AS NUMERIC);",
                           R"_(Incorrect modifiers for DECIMAL, width and scale required)_");
}

TEST_CASE("components::sql::narrowing::decimal_literal_exact") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::types::complex_logical_type;
    using components::types::int128_t;

    // 123456789.12345678901234567890 at scale 20:
    // 123456789 * 10^20 + 12345678901234567890 — 29 significant digits, more
    // than a double can carry — the shape atof mangles.
    const int128_t ten_to_10 = int128_t{10000000000LL};
    const int128_t scaled = int128_t{123456789} * ten_to_10 * ten_to_10 + int128_t{1234567890123456789LL} * 10 +
                            int128_t{0}; // 12345678901234567890 assembled inside int128
    auto dec_type_res = complex_logical_type::create_decimal(38, 20);
    REQUIRE(!dec_type_res.has_error());
    const auto expected = v::create_decimal(&resource, dec_type_res.value(), scaled);

    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST('123456789.12345678901234567890' AS NUMERIC(38,20));",
                     expected);
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(123456789.12345678901234567890 AS NUMERIC(38,20));",
                     expected);

    // Excess fractional digits round half away from zero, like PostgreSQL.
    {
        auto t32_res = complex_logical_type::create_decimal(3, 2);
        REQUIRE(!t32_res.has_error());
        TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(1.999 AS NUMERIC(3,2));",
                         v::create_decimal(&resource, t32_res.value(), int64_t{200}));
    }

    // A value that needs more integer digits than the declared width has left
    // over is an overflow, refused — not truncated, not rounded away.
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('100' AS NUMERIC(2,1));",
                           R"_(numeric field overflow: 100 does not fit NUMERIC(2, 1))_");

    // The INSERT path: the VALUES chunk holds the DECIMAL value itself — the
    // scaled int128, digit for digit — not a double approximation of it.
    SECTION("INSERT VALUES carries the exact decimal") {
        auto select = linitial(raw_parser(
            &arena_resource,
            "INSERT INTO db.tbl (d) VALUES (CAST('123456789.12345678901234567890' AS NUMERIC(38,20)));"));
        auto wrap = transformer.transform(transform::pg_cell_to_node_cast(select)).finalize();
        REQUIRE(!wrap.has_error());
        auto result = wrap.value();
        auto node = result.sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        auto stored = chunk.value(0, 0);
        REQUIRE(stored.type().type() == components::types::logical_type::DECIMAL);
        stored.set_alias("");
        REQUIRE(stored == expected);
    }
}

TEST_CASE("components::sql::narrowing::subscript_read_by_tag") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // A subscript wider than int32 leaves the scanner as a T_Float carrying its
    // digits; reading `ival` without looking at the tag renders the BIT PATTERN OF
    // A POINTER into the column path.
    SECTION("INSERT INTO db.tbl (arr[3000000000]) VALUES (5);") {
        auto select =
            linitial(raw_parser(&arena_resource, "INSERT INTO db.tbl (arr[3000000000]) VALUES (5);"));
        auto wrap = transformer.transform(transform::pg_cell_to_node_cast(select)).finalize();
        REQUIRE(!wrap.has_error());
        auto result = wrap.value();
        auto node = result.sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        auto* ins = static_cast<components::logical_plan::node_insert_t*>(node.get());
        REQUIRE(ins->key_translation().size() == 1);
        REQUIRE(ins->key_translation().front().as_string() == "arr/3000000000");
    }
    // A fractional subscript is not an index at all: refuse it by name.
    TEST_TRANSFORMER_ERROR("UPDATE db.tbl SET arr[1.5] = 3;",
                           R"_(an array subscript must be an integer literal, got: 1.5)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO db.tbl (arr[1.5]) VALUES (5);",
                           R"_(an array subscript must be an integer literal, got: 1.5)_");
}

TEST_CASE("components::sql::narrowing::get_type_refuses_an_absent_typename") {
    auto resource = core::pmr::otterbrix_resource();
    // A null TypeName must not answer a default-constructed NA type: that is a
    // failure reported as a value.
    auto res = transform::get_type(&resource, nullptr);
    REQUIRE(res.has_error());
    REQUIRE(std::string_view{res.error().what} == R"_(cannot determine a type: the TypeName is absent)_");
}
