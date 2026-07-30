#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <string>

// FLOAT (SQL REAL / float4) columns and the type of an arithmetic projection.
//
// Two divergences this pins down:
//
//  * The result type of `float_col <op> x`. The planner types the projection with
//    types::arithmetic_result_type, which promotes FLOAT op INTEGER to FLOAT (PostgreSQL and
//    DuckDB float4 semantics). The execution kernel used to demote that FLOAT to DOUBLE right
//    after asking the same function, so the plan said FLOAT and the chunk carried DOUBLE.
//    Nothing in the repo caught it: there was no test anywhere with a REAL/float4 column.
//
//  * The result type over ZERO rows. compute_*_arithmetic hard-coded DOUBLE when count == 0,
//    whatever the operands were, and the arithmetic arm of operator_select is the one column
//    kind that does not fall back to the plan-resolved col.result_type. A 0-row chunk is real
//    input (an empty table, or a fully filtered batch), not a drain sentinel.

namespace {
    using test_helpers::exec;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    // logical_type of column `col` in the first result chunk.
    template<typename D>
    components::types::logical_type column_type(D* d, const std::string& sql, uint64_t col = 0) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        REQUIRE_FALSE(c->chunks().empty());
        REQUIRE(c->chunks().front().column_count() > col);
        return c->chunks().front().data[col].type().type();
    }
} // namespace

TEST_CASE("integration::cpp::float_arithmetic::real_column_keeps_float") {
    auto config = test_helpers::make_test_config(test_temp_path("float_arithmetic/real_column"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, r REAL, dbl DOUBLE);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, r, dbl) VALUES (1, 1.5, 1.5),(2, 2.5, 2.5),(3, 4.0, 4.0);"));

    using components::types::logical_type;

    INFO("a REAL column reads back as FLOAT");
    REQUIRE(column_type(d, "SELECT r FROM m.t ORDER BY id;") == logical_type::FLOAT);

    INFO("FLOAT op INTEGER stays FLOAT");
    REQUIRE(column_type(d, "SELECT r * 2 FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r + 1 FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r - 1 FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r / 2 FROM m.t ORDER BY id;") == logical_type::FLOAT);

    INFO("FLOAT op FLOAT stays FLOAT");
    REQUIRE(column_type(d, "SELECT r * r FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r + r FROM m.t ORDER BY id;") == logical_type::FLOAT);

    // A literal operand goes through the vector-scalar kernel; two columns go through the
    // vector-vector one. Only the latter reaches the L=float/R=int32 mix that the demotion
    // used to turn into a double store into a 4-byte FLOAT slot.
    INFO("FLOAT column op INTEGER column stays FLOAT");
    REQUIRE(column_type(d, "SELECT r * id FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT id * r FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r + id FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r - id FROM m.t ORDER BY id;") == logical_type::FLOAT);
    REQUIRE(column_type(d, "SELECT r / id FROM m.t ORDER BY id;") == logical_type::FLOAT);

    INFO("FLOAT op DOUBLE widens to DOUBLE");
    REQUIRE(column_type(d, "SELECT r * dbl FROM m.t ORDER BY id;") == logical_type::DOUBLE);

    INFO("unary minus preserves FLOAT");
    REQUIRE(column_type(d, "SELECT -r FROM m.t ORDER BY id;") == logical_type::FLOAT);

    INFO("the values are the ones float arithmetic produces");
    {
        auto c = exec(d, "SELECT r * 2 FROM m.t ORDER BY id;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 3);
        const auto& column = c->chunks().front().data[0];
        REQUIRE(column.get_value<float>(0) == 3.0f);
        REQUIRE(column.get_value<float>(1) == 5.0f);
        REQUIRE(column.get_value<float>(2) == 8.0f);
    }

    INFO("column op column produces the same float values, at float width");
    {
        auto c = exec(d, "SELECT r * id FROM m.t ORDER BY id;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 3);
        const auto& column = c->chunks().front().data[0];
        REQUIRE(column.type().type() == logical_type::FLOAT);
        REQUIRE(column.get_value<float>(0) == 1.5f);
        REQUIRE(column.get_value<float>(1) == 5.0f);
        REQUIRE(column.get_value<float>(2) == 12.0f);
    }
    {
        auto c = exec(d, "SELECT r + r FROM m.t ORDER BY id;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 3);
        const auto& column = c->chunks().front().data[0];
        REQUIRE(column.type().type() == logical_type::FLOAT);
        REQUIRE(column.get_value<float>(0) == 3.0f);
        REQUIRE(column.get_value<float>(1) == 5.0f);
        REQUIRE(column.get_value<float>(2) == 8.0f);
    }
}

TEST_CASE("integration::cpp::float_arithmetic::empty_table_projection_is_typed") {
    auto config = test_helpers::make_test_config(test_temp_path("float_arithmetic/empty_table"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.empty_t (a INT, b BIGINT, r REAL);"));

    using components::types::logical_type;

    INFO("no rows inserted: the projection is still typed from the operands, not DOUBLE");
    {
        auto c = exec(d, "SELECT a + b FROM m.empty_t;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 0);
        REQUIRE_FALSE(c->chunks().empty());
        REQUIRE(c->chunks().front().data[0].type().type() == logical_type::BIGINT);
    }
    {
        auto c = exec(d, "SELECT a + a FROM m.empty_t;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->chunks().front().data[0].type().type() == logical_type::INTEGER);
    }
    {
        auto c = exec(d, "SELECT r * 2 FROM m.empty_t;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->chunks().front().data[0].type().type() == logical_type::FLOAT);
    }

    INFO("a fully filtered non-empty table is the same 0-row case");
    {
        REQUIRE(okq(d, "INSERT INTO m.empty_t (a, b, r) VALUES (1, 2, 1.5);"));
        auto c = exec(d, "SELECT a + b FROM m.empty_t WHERE a > 100;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 0);
        REQUIRE_FALSE(c->chunks().empty());
        REQUIRE(c->chunks().front().data[0].type().type() == logical_type::BIGINT);
    }
}
