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
//   - CAST(1.5 AS INT) answers a DOUBLE, CAST('123' AS BIGINT) a string;
//   - `numeric(38,20) DEFAULT 0.1234...` stores the nearest double, not the digits;
//   - `1e400` becomes +Infinity, because floatVal() is atof() and atof says nothing.

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <string>

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
    auto dec_type_res = complex_logical_type::create_decimal(&resource, 38, 20);
    REQUIRE(!dec_type_res.has_error());
    const auto expected = v::create_decimal(&resource, dec_type_res.value(), scaled);

    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST('123456789.12345678901234567890' AS NUMERIC(38,20));",
                     expected);
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(123456789.12345678901234567890 AS NUMERIC(38,20));",
                     expected);

    // Excess fractional digits round half away from zero, like PostgreSQL.
    {
        auto t32_res = complex_logical_type::create_decimal(&resource, 3, 2);
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

// The DEFAULT clause of a column definition is the ONE place the transformer
// already holds the target type — the column is being DECLARED on the same line —
// and it used to throw that knowledge away and read the literal by its own
// spelling. For a DECIMAL column that is lossy before anything downstream can
// help: `numeric(38,20) DEFAULT 0.12345678901234567890` went through
// numeric_literal_value's double tail and landed on 0.12345678901234567168 —
// 12345678901234567168 against the written 12345678901234567890 once scaled,
// short by 722 at the 20th decimal place — and no later cast to the column's type
// can put back digits the double never carried.
//
// Everything that is NOT a DECIMAL column keeps reading its default exactly as
// before: the integer ladder in numeric_literal_value is already exact to 128
// bits, and a DOUBLE column wants the double. That control is pinned below,
// because widening this to every declared type would turn `c integer DEFAULT 7`
// into an INTEGER at the transformer and flip the ALTER-vs-CREATE divergence that
// services/collection/executor.cpp's convert_column_defaults already performs, once, for BOTH
// spellings (pinned by "alter_add_column_default_is_coerced_like_create_table").
TEST_CASE("components::sql::narrowing::decimal_column_default_exact") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::types::complex_logical_type;
    using components::types::int128_t;
    using components::types::logical_type;

    // The default of the single column of a single-column CREATE TABLE.
    auto only_default = [&](const char* query) {
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto wrap = transformer.transform(transform::pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!wrap.has_error());
        auto plan = wrap.value();
        auto node = plan.sub_queries.back();
        REQUIRE(node->type() == components::logical_plan::node_type::create_collection_t);
        auto* create = static_cast<components::logical_plan::node_create_collection_t*>(node.get());
        REQUIRE(create->column_definitions().size() == 1);
        REQUIRE(create->column_definitions().front().has_default_value());
        return create->column_definitions().front().default_value();
    };

    // 0.12345678901234567890 at scale 20 is the 20 written digits, verbatim.
    // Assembled inside int128 because 12345678901234567890 is past int64's max.
    const int128_t written = int128_t{1234567890123456789LL} * 10;
    auto dec_res = complex_logical_type::create_decimal(&resource, 38, 20);
    REQUIRE(!dec_res.has_error());

    SECTION("a fractional default keeps every written digit") {
        auto stored = only_default("CREATE TABLE db.tbl (d numeric(38,20) DEFAULT 0.12345678901234567890);");
        REQUIRE(stored.type().type() == logical_type::DECIMAL);
        stored.set_alias("");
        REQUIRE(stored == v::create_decimal(&resource, dec_res.value(), written));
    }

    SECTION("the sign travels with the digits") {
        auto stored = only_default("CREATE TABLE db.tbl (d numeric(38,20) DEFAULT -0.12345678901234567890);");
        REQUIRE(stored.type().type() == logical_type::DECIMAL);
        stored.set_alias("");
        REQUIRE(stored == v::create_decimal(&resource, dec_res.value(), -written));
    }

    SECTION("an integer literal under a DECIMAL column scales, it does not stay an integer") {
        auto stored = only_default("CREATE TABLE db.tbl (d numeric(10,2) DEFAULT 7);");
        REQUIRE(stored.type().type() == logical_type::DECIMAL);
        auto dec_10_2 = complex_logical_type::create_decimal(&resource, 10, 2);
        REQUIRE(!dec_10_2.has_error());
        stored.set_alias("");
        REQUIRE(stored == v::create_decimal(&resource, dec_10_2.value(), int64_t{700}));
    }

    SECTION("a default that does not fit the declared width is refused, not truncated") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE db.tbl (d numeric(2,1) DEFAULT 100.0);"));
        auto result = transformer.transform(transform::pg_cell_to_node_cast(stmt));
        REQUIRE(std::string_view{result.get_error().what} ==
                R"_(numeric field overflow: 100.0 does not fit NUMERIC(2, 1))_");
    }

    // --- controls: no other declared type changes reading.
    SECTION("a BIGINT column still reads its default as the integer ladder produced it") {
        auto stored = only_default("CREATE TABLE db.tbl (c bigint DEFAULT 7);");
        REQUIRE(stored.type().type() == logical_type::BIGINT);
        REQUIRE(stored.value<int64_t>() == 7);
    }
    SECTION("an INTEGER column still reads its default as BIGINT — the ALTER/CREATE split stands") {
        auto stored = only_default("CREATE TABLE db.tbl (c integer DEFAULT 7);");
        REQUIRE(stored.type().type() == logical_type::BIGINT);
        REQUIRE(stored.value<int64_t>() == 7);
    }
    SECTION("a DOUBLE column still reads its default as a double") {
        auto stored = only_default("CREATE TABLE db.tbl (c double precision DEFAULT 1.5);");
        REQUIRE(stored.type().type() == logical_type::DOUBLE);
        REQUIRE(stored.value<double>() == 1.5);
    }
}

// AN EXPONENT IS A SPELLING, NOT A LOSS OF PRECISION. PostgreSQL's numeric input
// reads one: `1e-5::numeric(10,6)` is 0.000010, `1.5e3::numeric(10,2)` is 1500.00,
// `1e3::int` is 1000. The exact reader that replaced the double tail under a
// DECLARED target refused every one of them with "not a decimal number", so the
// half of the precision fix that lives in this component turned legal statements
// into errors: `numeric(10,6) DEFAULT 1e-5` stopped parsing at all, where before
// the exact reader existed it stored the nearest double. A ceiling on precision is
// not a ban on a spelling.
//
// The exponent is applied by MOVING THE POINT through the written digits, so the
// reading stays exact — multiplying by 10^exp would put back the rounding the
// exact path exists to remove. What must NOT change is what the reader refuses:
// an empty or non-numeric exponent is still malformed, and a shift that carries
// the value past the declared width is still an overflow.
TEST_CASE("components::sql::narrowing::decimal_literal_exponent") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::types::complex_logical_type;

    auto dec = [&](uint8_t width, uint8_t scale) {
        auto res = complex_logical_type::create_decimal(&resource, width, scale);
        REQUIRE(!res.has_error());
        return res.value();
    };

    // 1e-5 at scale 6 is 10; the point moved five places left, no double involved.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(1e-5 AS NUMERIC(10,6));",
                     v::create_decimal(&resource, dec(10, 6), int64_t{10}));
    // The same digits written as a string literal read the same way.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST('1e-5' AS NUMERIC(10,6));",
                     v::create_decimal(&resource, dec(10, 6), int64_t{10}));
    // A positive exponent moves the point right: 1.5e3 = 1500.00 at scale 2.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(1.5e3 AS NUMERIC(10,2));",
                     v::create_decimal(&resource, dec(10, 2), int64_t{150000}));
    // Capital E, explicit sign.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(1.5E+3 AS NUMERIC(10,2));",
                     v::create_decimal(&resource, dec(10, 2), int64_t{150000}));
    // Rounding survives the shift: 1.999e0 at scale 2 rounds half away from zero.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST(1.999e0 AS NUMERIC(3,2));",
                     v::create_decimal(&resource, dec(3, 2), int64_t{200}));
    // A shift far past the scale is zero, not a refusal and not garbage.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE d = CAST('1e-40' AS NUMERIC(10,6));",
                     v::create_decimal(&resource, dec(10, 6), int64_t{0}));
    // The integer ladder reads the same spelling through the same parser: an
    // exponent literal under an INTEGER target is PostgreSQL's 1e3::int == 1000.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = CAST(1e3 AS INTEGER);", v(&resource, int32_t{1000}));

    // --- what the shift must NOT swallow.
    // A shift that carries the value past the declared width is an overflow, and
    // it says so with the literal as written.
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST(1e3 AS NUMERIC(3,1));",
                           R"_(numeric field overflow: 1e3 does not fit NUMERIC(3, 1))_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('1e400' AS NUMERIC(10,6));",
                           R"_(numeric field overflow: 1e400 does not fit NUMERIC(10, 6))_");
    // An exponent with no digits, or with something that is not a digit, is still
    // malformed — the reader never reads a prefix and calls it a number.
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('1e' AS NUMERIC(10,2));",
                           R"_(not a decimal number: 1e)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('1e+' AS NUMERIC(10,2));",
                           R"_(not a decimal number: 1e+)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('1e2x' AS NUMERIC(10,2));",
                           R"_(not a decimal number: 1e2x)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('e5' AS NUMERIC(10,2));",
                           R"_(not a decimal number: e5)_");
    // Two exponents are not two shifts.
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE d = CAST('1e2e3' AS NUMERIC(10,2));",
                           R"_(not a decimal number: 1e2e3)_");

    // --- the shift is bounded BY THE MANTISSA, not by a constant.
    // Both literals below carry a mantissa longer than any fixed clamp on the exponent,
    // which is how a constant bound stops being sound: it drags written digits back
    // inside the declared scale, answering a number for a value that is zero, or stops
    // the mantissa short of the width, answering a value for one that overflows. The
    // digits are built here rather than spelled out so the query and the refusal it must
    // quote back cannot drift apart.
    SECTION("a mantissa longer than any constant clamp still shifts down to zero") {
        // 10^149 shifted down by 1000 is 10^-851: zero at every declarable scale.
        std::string huge = "1";
        huge.append(149, '0');
        huge += "e-1000";
        const std::string query = "SELECT * FROM db.tbl WHERE d = CAST('" + huge + "' AS NUMERIC(38,6));";
        auto stmt = linitial(raw_parser(&arena_resource, query.c_str()));
        auto wrap = transformer.transform(transform::pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!wrap.has_error());
        auto params = wrap.value().parameters;
        REQUIRE(params->parameters().parameters.size() == 1);
        const auto& bound = params->parameter(core::parameter_id_t(uint16_t(0)));
        // NUMERIC(38, ...) is stored as int128, so the expected payload is written as
        // one: an int64 zero is a different physical value and would not compare equal.
        const auto zero = v::create_decimal(&resource, dec(38, 6), components::types::int128_t{0});
        REQUIRE(bound.type() == zero.type());
        REQUIRE(bound == zero);
    }
    SECTION("and a mantissa longer than any constant clamp still shifts up to an overflow") {
        // 10^-150 shifted up by 1000 is 10^850: past every declarable width.
        std::string tiny = "0.";
        tiny.append(149, '0');
        tiny += "1e1000";
        const std::string query = "SELECT * FROM db.tbl WHERE d = CAST('" + tiny + "' AS NUMERIC(38,0));";
        auto stmt = linitial(raw_parser(&arena_resource, query.c_str()));
        auto result = transformer.transform(transform::pg_cell_to_node_cast(stmt));
        REQUIRE(std::string{result.get_error().what.c_str()} ==
                "numeric field overflow: " + tiny + " does not fit NUMERIC(38, 0)");
    }
}

// The DEFAULT clause reads its literal through the very same parser, so the
// spelling that a CAST accepts is the spelling a column declaration accepts.
TEST_CASE("components::sql::narrowing::decimal_column_default_exponent") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::types::complex_logical_type;
    using components::types::logical_type;

    auto only_default = [&](const char* query) {
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto wrap = transformer.transform(transform::pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!wrap.has_error());
        auto plan = wrap.value();
        auto node = plan.sub_queries.back();
        REQUIRE(node->type() == components::logical_plan::node_type::create_collection_t);
        auto* create = static_cast<components::logical_plan::node_create_collection_t*>(node.get());
        REQUIRE(create->column_definitions().size() == 1);
        REQUIRE(create->column_definitions().front().has_default_value());
        return create->column_definitions().front().default_value();
    };

    auto dec_10_6 = complex_logical_type::create_decimal(&resource, 10, 6);
    REQUIRE(!dec_10_6.has_error());

    SECTION("a DECIMAL column takes an exponent default") {
        auto stored = only_default("CREATE TABLE db.tbl (d numeric(10,6) DEFAULT 1e-5);");
        REQUIRE(stored.type().type() == logical_type::DECIMAL);
        stored.set_alias("");
        REQUIRE(stored == v::create_decimal(&resource, dec_10_6.value(), int64_t{10}));
    }

    SECTION("and reads it exactly the way the written-out spelling reads") {
        auto shifted = only_default("CREATE TABLE db.tbl (d numeric(10,6) DEFAULT 1e-5);");
        auto spelled = only_default("CREATE TABLE db.tbl (d numeric(10,6) DEFAULT 0.00001);");
        shifted.set_alias("");
        spelled.set_alias("");
        REQUIRE(shifted == spelled);
    }
}

// floatVal() is atof(): it answers +/-inf for a literal past the double range and
// 0.0 for text it cannot read, in both cases WITHOUT saying so. An unadorned
// `1e400` reaching a plan as +Infinity is a value no column holds and no
// comparison orders — the same silent wrong answer the integer ladder above this
// tail already refuses to give past uint128.
TEST_CASE("components::sql::narrowing::double_literal_out_of_range_is_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = 1e400;",
                           R"_(numeric literal out of range: 1e400 does not fit a double)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM db.tbl WHERE x = -1e400;",
                           R"_(numeric literal out of range: -1e400 does not fit a double)_");
    // The rung just below the ceiling still reads, so the refusal is a ceiling and
    // not a ban on exponents.
    TEST_WHERE_PARAM("SELECT * FROM db.tbl WHERE x = 1e308;", v(&resource, 1e308));
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
