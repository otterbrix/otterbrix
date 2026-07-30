#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/generaty.hpp>
#include <components/tests/temp_dir.hpp>
#include <components/types/logical_value.hpp>

#include <array>
#include <optional>
#include <set>

using namespace components;
using namespace components::cursor;

static const std::string database_name = "testdatabase";

namespace {

    // (id, n, generate_series) with std::nullopt marking a SQL NULL (LEFT JOIN pad).
    using lat_row_t = std::array<std::optional<int64_t>, 3>;

    // Collect every result row as an (id, n, series) tuple, mapping columns by name
    // so the check is independent of physical column order. Order-insensitive: a
    // join/scan need not preserve outer-row order, so compare as a multiset.
    std::multiset<lat_row_t> collect_rows(const cursor_t& cur, uint64_t c_id, uint64_t c_n, uint64_t c_series) {
        std::multiset<lat_row_t> rows;
        for (uint64_t r = 0; r < cur.size(); ++r) {
            auto read = [&](uint64_t col) -> std::optional<int64_t> {
                auto cell = cur.value(col, r);
                if (cell.is_null()) {
                    return std::nullopt;
                }
                return std::optional<int64_t>{cell.value<int64_t>()};
            };
            rows.insert(lat_row_t{read(c_id), read(c_n), read(c_series)});
        }
        return rows;
    }

} // namespace

TEST_CASE("integration::cpp::table_function::generate_series") {
    auto config = test_create_config(test_temp_path("test_table_function"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + database_name + ";");
    }

    INFO("FROM generate_series(1, 5) — inclusive series of 5 rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM generate_series(1, 5);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        REQUIRE(cur->column_count() == 1);
        REQUIRE(test_column_index(*cur, "generate_series") == 0);
        // Single table-function source emits in order — assert exact values.
        for (uint64_t i = 0; i < 5; ++i) {
            REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i + 1));
        }
    }

    INFO("FROM generate_series(1, 10, 2) — stepped series");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM generate_series(1, 10, 2);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5); // 1,3,5,7,9
        const int64_t expected[] = {1, 3, 5, 7, 9};
        for (uint64_t i = 0; i < 5; ++i) {
            REQUIRE(cur->value(0, i).value<int64_t>() == expected[i]);
        }
    }

    INFO("FROM generate_series with empty range yields no rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM generate_series(5, 1);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::table_function::lateral_generate_series") {
    auto config = test_create_config(test_temp_path("test_table_function_lateral"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + database_name + ";");
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".t (id BIGINT, n BIGINT);");
        dispatcher->execute_sql(session, "INSERT INTO " + database_name + ".t (id, n) VALUES (1, 2), (2, 3);");
    }

    // Assert the full (id, n, series) tuple set, not just the row count, so a
    // correlation-binding bug that yields the right cardinality but wrong values
    // (or mislabelled columns) is caught.
    auto check_columns = [](const cursor_t& cur, uint64_t& id_i, uint64_t& n_i, uint64_t& series_i) {
        REQUIRE(cur.column_count() == 3);
        id_i = test_column_index(cur, "id");
        n_i = test_column_index(cur, "n");
        series_i = test_column_index(cur, "generate_series");
        REQUIRE(id_i != test_column_not_found);
        REQUIRE(n_i != test_column_not_found);
        REQUIRE(series_i != test_column_not_found);
    };

    INFO("FROM t, generate_series(1, t.n) — correlated expansion");
    {
        auto session = otterbrix::session_id_t();
        // row id=1,n=2 -> series 1,2 (2 rows); row id=2,n=3 -> series 1,2,3 (3 rows) => 5 rows
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + database_name + ".t, generate_series(1, t.n);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        uint64_t id_i, n_i, series_i;
        check_columns(*cur, id_i, n_i, series_i);
        std::multiset<lat_row_t> expected{{{1, 2, 1}}, {{1, 2, 2}}, {{2, 3, 1}}, {{2, 3, 2}}, {{2, 3, 3}}};
        REQUIRE(collect_rows(*cur, id_i, n_i, series_i) == expected);
    }

    INFO("FROM t JOIN LATERAL generate_series(1, t.n) ON true — explicit inner form");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + database_name +
                                               ".t JOIN LATERAL generate_series(1, t.n) ON true;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        uint64_t id_i, n_i, series_i;
        check_columns(*cur, id_i, n_i, series_i);
        std::multiset<lat_row_t> expected{{{1, 2, 1}}, {{1, 2, 2}}, {{2, 3, 1}}, {{2, 3, 2}}, {{2, 3, 3}}};
        REQUIRE(collect_rows(*cur, id_i, n_i, series_i) == expected);
    }

    INFO("FROM t LEFT JOIN LATERAL generate_series(1, t.n) ON true — outer row kept when empty");
    {
        auto session = otterbrix::session_id_t();
        // Add a row whose series is empty (generate_series(1, 0) -> 0 rows). LEFT JOIN
        // must keep that outer row NULL-padded: 5 (from id=1,2) + 1 (id=3 empty) = 6.
        dispatcher->execute_sql(session, "INSERT INTO " + database_name + ".t (id, n) VALUES (3, 0);");
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + database_name +
                                               ".t LEFT JOIN LATERAL generate_series(1, t.n) ON true;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
        uint64_t id_i, n_i, series_i;
        check_columns(*cur, id_i, n_i, series_i);
        // The (3, 0) outer row survives with a NULL series column.
        std::multiset<lat_row_t> expected{{{1, 2, 1}},
                                          {{1, 2, 2}},
                                          {{2, 3, 1}},
                                          {{2, 3, 2}},
                                          {{2, 3, 3}},
                                          {{3, 0, std::nullopt}}};
        REQUIRE(collect_rows(*cur, id_i, n_i, series_i) == expected);
    }

    INFO("FROM t, generate_series(t.id, t.n) — multiple correlated args");
    {
        auto session = otterbrix::session_id_t();
        // Table now holds (1,2),(2,3),(3,0). id=1,n=2 -> series 1,2; id=2,n=3 -> series 2,3;
        // id=3,n=0 -> empty (implicit cross drops it) => 4 rows.
        auto cur =
            dispatcher->execute_sql(session, "SELECT * FROM " + database_name + ".t, generate_series(t.id, t.n);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        uint64_t id_i, n_i, series_i;
        check_columns(*cur, id_i, n_i, series_i);
        std::multiset<lat_row_t> expected{{{1, 2, 1}}, {{1, 2, 2}}, {{2, 3, 2}}, {{2, 3, 3}}};
        REQUIRE(collect_rows(*cur, id_i, n_i, series_i) == expected);
    }
}