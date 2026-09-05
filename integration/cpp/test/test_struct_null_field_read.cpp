#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// Selecting a WHOLE struct cell that has a NULL field killed the process.
//
// vector_t::value() rebuilt the cell's type from its field VALUES. A NULL field carries
// logical_type::NA -- that is how logical_value_t spells "no value" -- so a declared
// STRUCT<BIGINT, BIGINT> came back typed STRUCT<BIGINT, NA> and tripped the type-identity
// assert at the bottom of value(). Under NDEBUG the assert is gone and the cell is simply
// mistyped: written back into a vector of the declared type it is rejected by the
// cast guard in set_value, which returns without writing and without touching validity --
// the row silently reads as a struct of zeros.
//
// Field projection -- SELECT (p).a, (p).b -- reads the field vectors directly and never
// built the whole-cell value, which is why the existing nested-NULL tests route around
// this and it stayed hidden.
TEST_CASE("integration::cpp::test_struct_null_field_read::select_whole_struct_cell_with_a_null_field") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_struct_null_field_read/select");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE snf;")->is_success());
    REQUIRE(exec("CREATE TYPE snf_pair AS (a BIGINT, b BIGINT);")->is_success());
    REQUIRE(exec("CREATE TABLE snf.t (id BIGINT, p snf_pair);")->is_success());
    REQUIRE(exec("INSERT INTO snf.t (id, p) VALUES (1, ROW(11, 12));")->is_success());
    REQUIRE(exec("INSERT INTO snf.t (id, p) VALUES (2, ROW(21, NULL));")->is_success());
    REQUIRE(exec("INSERT INTO snf.t (id, p) VALUES (3, NULL);")->is_success());

    INFO("a fully populated cell still reads as before");
    {
        auto cur = exec("SELECT p FROM snf.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE_FALSE(v.is_null());
        REQUIRE(v.type().type() == components::types::logical_type::STRUCT);
        REQUIRE(v.children().size() == 2);
        REQUIRE(v.children()[0].value<int64_t>() == 11);
        REQUIRE(v.children()[1].value<int64_t>() == 12);
    }

    INFO("a NULL FIELD leaves the cell present, keeps the sibling field, and keeps the declared type");
    {
        auto cur = exec("SELECT p FROM snf.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE_FALSE(v.is_null());
        REQUIRE(v.type().type() == components::types::logical_type::STRUCT);
        REQUIRE(v.type().child_types().size() == 2);
        REQUIRE(v.type().child_types()[0].type() == components::types::logical_type::BIGINT);
        // The declared field type survives the NULL: NA is the absence of a value, not a type.
        REQUIRE(v.type().child_types()[1].type() == components::types::logical_type::BIGINT);
        REQUIRE(v.children().size() == 2);
        REQUIRE(v.children()[0].value<int64_t>() == 21);
        REQUIRE(v.children()[1].is_null());
    }

    INFO("a NULL CELL is still a NULL cell, not a struct of NULL fields");
    {
        auto cur = exec("SELECT p FROM snf.t WHERE id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.is_null());
        REQUIRE(v.children().empty());
    }

    INFO("field projection agrees with the whole-cell read");
    {
        auto cur = exec("SELECT (p).a, (p).b FROM snf.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 21);
        REQUIRE(cur->value(1, 0).is_null());
    }

    INFO("the whole result set reads in one pass");
    {
        auto cur = exec("SELECT id, p FROM snf.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(1, 0).children()[1].value<int64_t>() == 12);
        REQUIRE(cur->value(1, 1).children()[1].is_null());
        REQUIRE(cur->value(1, 2).is_null());
    }
}
