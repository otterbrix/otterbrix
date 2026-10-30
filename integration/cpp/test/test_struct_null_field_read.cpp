#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// Selecting a WHOLE struct cell with a NULL field killed the process: vector_t::value() rebuilt
// the cell's type from field VALUES, so a NULL field (typed NA) turned STRUCT<BIGINT, BIGINT> into
// STRUCT<BIGINT, NA> and tripped the type-identity assert -- or, under NDEBUG, silently read back
// as a struct of zeros via set_value's cast guard. Field projection (SELECT (p).a, (p).b) reads
// field vectors directly and never builds the whole-cell value, which is why existing nested-NULL
// tests routed around this.
TEST_CASE("integration::cpp::test_struct_null_field_read::select_whole_struct_cell_with_a_null_field") {
    auto config = test_create_config(integration_fixture_path("test_struct_null_field_read/select"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE SCHEMA snf;")->is_success());
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
        auto cell3 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE_FALSE(cell3.value());
        REQUIRE(cur->type_data()[0].type() == components::types::logical_type::STRUCT);
        REQUIRE(cur->type_data()[0].child_types().size() == 2);
        auto cell5 = cur->get<int64_t>(0, 0, {0});
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == 11);
        auto cell6 = cur->get<int64_t>(0, 0, {1});
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == 12);
    }

    INFO("a NULL FIELD leaves the cell present, keeps the sibling field, and keeps the declared type");
    {
        auto cur = exec("SELECT p FROM snf.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell7 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell7.has_error());
        REQUIRE_FALSE(cell7.value());
        REQUIRE(cur->type_data()[0].type() == components::types::logical_type::STRUCT);
        REQUIRE(cur->type_data()[0].child_types().size() == 2);
        REQUIRE(cur->type_data()[0].child_types()[0].type() == components::types::logical_type::BIGINT);
        // The declared field type survives the NULL: NA is the absence of a value, not a type.
        REQUIRE(cur->type_data()[0].child_types()[1].type() == components::types::logical_type::BIGINT);
        auto cell9 = cur->get<int64_t>(0, 0, {0});
        REQUIRE_FALSE(cell9.has_error());
        REQUIRE(cell9.value() == 21);
        auto cell10 = cur->is_null(0, 0, {1});
        REQUIRE_FALSE(cell10.has_error());
        REQUIRE(cell10.value());
    }

    INFO("a NULL CELL is still a NULL cell, not a struct of NULL fields");
    {
        auto cur = exec("SELECT p FROM snf.t WHERE id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell_null = cur->is_null(0, 0);
        REQUIRE_FALSE(cell_null.has_error());
        REQUIRE(cell_null.value());
    }

    INFO("field projection agrees with the whole-cell read");
    {
        auto cur = exec("SELECT (p).a, (p).b FROM snf.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell0 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell0.has_error());
        REQUIRE(cell0.value() == 21);
        auto cell1 = cur->is_null(1, 0);
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value());
    }

    INFO("the whole result set reads in one pass");
    {
        auto cur = exec("SELECT id, p FROM snf.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell11 = cur->get<int64_t>(1, 0, {1});
        REQUIRE_FALSE(cell11.has_error());
        REQUIRE(cell11.value() == 12);
        auto cell12 = cur->is_null(1, 1, {1});
        REQUIRE_FALSE(cell12.has_error());
        REQUIRE(cell12.value());
        auto cell2 = cur->is_null(1, 2);
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value());
    }
}
