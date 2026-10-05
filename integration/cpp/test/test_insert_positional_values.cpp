// INSERT without a column list, as PostgreSQL 18 (analyze.c transformInsertStmt / checkInsertTargets /
// transformInsertRow): the values fill the table's columns in order; fewer values leave the rest to their DEFAULT
// (or NULL); more values are refused; every VALUES row has the same length.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace {

    struct fixture_t {
        explicit fixture_t(const char* dir)
            : config(test_create_config(integration_fixture_path(dir))) {
            test_clear_directory(config);
            space.emplace(config);
            ok("CREATE DATABASE d;");
            ok("CREATE TABLE d.t (a BIGINT, b TEXT, c BIGINT DEFAULT 7, e BIGINT);");
        }

        components::cursor::cursor_t_ptr run(const std::string& sql) {
            return space->dispatcher()->execute_sql(session, sql);
        }

        components::cursor::cursor_t_ptr ok(const std::string& sql) {
            auto cursor = run(sql);
            INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
            REQUIRE(cursor->is_success());
            return cursor;
        }

        std::string refused(const std::string& sql) {
            auto cursor = run(sql);
            INFO(sql);
            REQUIRE(cursor->is_error());
            return std::string{cursor->get_error().what};
        }

        // (a, b, c, e) of every row ordered by a; NULL reads as "null".
        std::vector<std::string> rows() {
            auto cursor = ok("SELECT a, b, c, e FROM d.t ORDER BY a;");
            std::vector<std::string> out;
            for (std::size_t row = 0; row < cursor->size(); ++row) {
                std::string line;
                for (std::size_t col = 0; col < 4; ++col) {
                    const auto cell = cursor->value(col, row);
                    line += col == 0 ? "" : ",";
                    if (cell.is_null()) {
                        line += "null";
                    } else if (col == 1) {
                        line += std::string{cell.value<std::string_view>()};
                    } else {
                        line += std::to_string(cell.value<int64_t>());
                    }
                }
                out.push_back(std::move(line));
            }
            return out;
        }

        configuration::config config;
        std::optional<test_spaces> space;
        otterbrix::session_id_t session;
    };

} // namespace

TEST_CASE("integration::cpp::insert_positional_values::every_column_in_order") {
    fixture_t db("test_insert_positional_values/every_column");
    auto inserted = db.ok("INSERT INTO d.t VALUES (1, 'x', 3, 4), (2, 'y', 5, 6);");
    CHECK(inserted->affected_rows() == 2);
    CHECK(db.rows() == std::vector<std::string>{"1,x,3,4", "2,y,5,6"});
}

TEST_CASE("integration::cpp::insert_positional_values::fewer_values_default_the_rest") {
    fixture_t db("test_insert_positional_values/fewer");
    db.ok("INSERT INTO d.t VALUES (1, 'x');");
    db.ok("INSERT INTO d.t VALUES (2);");
    CHECK(db.rows() == std::vector<std::string>{"1,x,7,null", "2,null,7,null"});
}

TEST_CASE("integration::cpp::insert_positional_values::values_are_cast_to_the_column_types") {
    fixture_t db("test_insert_positional_values/cast");
    db.ok("INSERT INTO d.t VALUES (CAST(1 AS INTEGER), 'x', CAST(3 AS SMALLINT), 4);");
    CHECK(db.rows() == std::vector<std::string>{"1,x,3,4"});
}

TEST_CASE("integration::cpp::insert_positional_values::parameters_by_position") {
    fixture_t db("test_insert_positional_values/params");
    auto* resource = db.space->dispatcher()->resource();
    std::vector<std::pair<size_t, components::types::logical_value_t>> params{
        {1, components::types::logical_value_t{resource, static_cast<int64_t>(9)}},
        {2, components::types::logical_value_t{resource, std::string{"p"}}},
    };
    auto cursor =
        db.space->dispatcher()->execute_sql_with_params(db.session, "INSERT INTO d.t VALUES ($1, $2);", params);
    INFO((cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
    REQUIRE(cursor->is_success());
    CHECK(db.rows() == std::vector<std::string>{"9,p,7,null"});
}

TEST_CASE("integration::cpp::insert_positional_values::refusals_as_postgresql") {
    fixture_t db("test_insert_positional_values/refusals");
    CHECK(db.refused("INSERT INTO d.t VALUES (1, 'x', 3, 4, 5);") == "INSERT has more expressions than target columns");
    CHECK(db.refused("INSERT INTO d.t VALUES (1, 'x'), (2);") == "VALUES lists must all be the same length");
    CHECK(db.refused("INSERT INTO d.t (a, b) VALUES (1);") == "INSERT has more target columns than expressions");
    CHECK(db.refused("INSERT INTO d.t (a) VALUES (1, 'x');") == "INSERT has more expressions than target columns");
    CHECK(db.rows().empty());
}

TEST_CASE("integration::cpp::insert_positional_values::insert_select_without_a_list") {
    fixture_t db("test_insert_positional_values/select");
    db.ok("CREATE TABLE d.src (a BIGINT, b TEXT);");
    db.ok("INSERT INTO d.src (a, b) VALUES (1, 'x'), (2, 'y');");
    db.ok("INSERT INTO d.t SELECT a, b FROM d.src;");
    CHECK(db.rows() == std::vector<std::string>{"1,x,7,null", "2,y,7,null"});
}

// A dynamic-schema table has no columns to fill in order: its INSERT names them.
TEST_CASE("integration::cpp::insert_positional_values::dynamic_schema_needs_a_column_list") {
    fixture_t db("test_insert_positional_values/dynamic");
    db.ok("CREATE TABLE d.g ();");
    CHECK(db.refused("INSERT INTO d.g VALUES (1, 'x');") ==
          "INSERT into dynamic-schema table \"g\" needs a column list: its columns are named by the INSERT");
}
