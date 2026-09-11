#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>

#include <array>
#include <optional>
#include <set>

using namespace components;
using namespace components::cursor;

namespace {

    int find_column(const cursor_t& cur, std::string_view name) {
        for (uint64_t i = 0; i < cur.column_count(); ++i) {
            if (cur.chunks().front().data[i].type().alias() == name) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    using row3_t = std::array<std::optional<int64_t>, 3>;

    // std::nullopt marks a SQL NULL; comparison is order-insensitive (multiset).
    std::multiset<row3_t> collect(const cursor_t& cur, int c0, int c1, int c2) {
        std::multiset<row3_t> rows;
        for (uint64_t r = 0; r < cur.size(); ++r) {
            auto read = [&](int col) -> std::optional<int64_t> {
                auto cell = cur.value(static_cast<uint64_t>(col), r);
                if (cell.is_null()) {
                    return std::nullopt;
                }
                return std::optional<int64_t>{cell.value<int64_t>()};
            };
            rows.insert(row3_t{read(c0), read(c1), read(c2)});
        }
        return rows;
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE s;");
        dispatcher->execute_sql(session, "CREATE TABLE s.outer_t (id BIGINT, n BIGINT);");
        dispatcher->execute_sql(session, "INSERT INTO s.outer_t (id, n) VALUES (1, 10), (2, 20);");
        dispatcher->execute_sql(session, "CREATE TABLE s.inner_t (k BIGINT, v BIGINT);");
        dispatcher->execute_sql(session, "INSERT INTO s.inner_t (k, v) VALUES (1, 100), (1, 101), (2, 200), (3, 300);");
    }

} // namespace

TEST_CASE("integration::cpp::lateral_subquery::correlated_where") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_where"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.id) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(v_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, 100}}, {{1, 10, 101}}, {{2, 20, 200}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::order_by_in_correlated_body") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_order_by"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session,
                                       "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE "
                                       "inner_t.k = outer_t.id ORDER BY inner_t.v) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    int id_index = find_column(*cur, "id");
    int n_index = find_column(*cur, "n");
    int v_index = find_column(*cur, "v");
    REQUIRE(id_index >= 0);
    REQUIRE(n_index >= 0);
    REQUIRE(v_index >= 0);
    // ORDER BY reorders each outer row's own inner rows; it never changes which pairs exist.
    std::multiset<row3_t> expected{{{1, 10, 100}}, {{1, 10, 101}}, {{2, 20, 200}}};
    REQUIRE(collect(*cur, id_index, n_index, v_index) == expected);

    std::optional<int64_t> current_id;
    std::optional<int64_t> previous_value;
    for (uint64_t row = 0; row < cur->size(); ++row) {
        const int64_t id = cur->value(static_cast<uint64_t>(id_index), row).value<int64_t>();
        const int64_t value = cur->value(static_cast<uint64_t>(v_index), row).value<int64_t>();
        if (current_id != id) {
            current_id = id;
            previous_value = std::nullopt;
        }
        if (previous_value.has_value()) {
            REQUIRE(*previous_value <= value);
        }
        previous_value = value;
    }
}

// Regression for a batched-join_builder UAF/row-mixup: the lazy builder buffered a raw pointer to each outer
// row's freed inner result and reused one left_chunk_ pointer across chunks; the eager builder now copies each row.
TEST_CASE("integration::cpp::lateral_subquery::correlated_where_multichunk") {
    constexpr int64_t N = 1100; // > 1024 so the outer input spans >= 2 chunks
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_multichunk"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.outer_big (id BIGINT);");
    dispatcher->execute_sql(session, "CREATE TABLE s.inr (k BIGINT, v BIGINT);");
    auto insert_batched = [&](const std::string& head, auto tuple_for) {
        std::string sql;
        int in_batch = 0;
        for (int64_t i = 0; i < N; ++i) {
            if (in_batch == 0) {
                sql = head;
            }
            sql += tuple_for(i);
            if (++in_batch == 200 || i == N - 1) {
                sql += ";";
                dispatcher->execute_sql(session, sql);
                in_batch = 0;
            } else {
                sql += ",";
            }
        }
    };
    insert_batched("INSERT INTO s.outer_big (id) VALUES ", [](int64_t i) { return "(" + std::to_string(i) + ")"; });
    insert_batched("INSERT INTO s.inr (k, v) VALUES ",
                   [](int64_t i) { return "(" + std::to_string(i) + "," + std::to_string(i + 100000) + ")"; });

    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_big, LATERAL (SELECT inr.v FROM s.inr WHERE inr.k = outer_big.id) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == static_cast<uint64_t>(N));
    int id_i = find_column(*cur, "id");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(v_i >= 0);
    std::set<int64_t> seen_ids;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        auto id_cell = cur->value(static_cast<uint64_t>(id_i), r);
        auto v_cell = cur->value(static_cast<uint64_t>(v_i), r);
        REQUIRE_FALSE(id_cell.is_null());
        REQUIRE_FALSE(v_cell.is_null());
        const int64_t id = id_cell.value<int64_t>();
        const int64_t v = v_cell.value<int64_t>();
        REQUIRE(v == id + 100000);
        seen_ids.insert(id);
    }
    REQUIRE(seen_ids.size() == static_cast<size_t>(N));
}

TEST_CASE("integration::cpp::lateral_subquery::left_join_empty") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_left"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "INSERT INTO s.outer_t (id, n) VALUES (5, 50);");
    }
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t LEFT JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
        "outer_t.id) sub ON true;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 4);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(v_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, 100}}, {{1, 10, 101}}, {{2, 20, 200}}, {{5, 50, std::nullopt}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::correlated_in_arithmetic") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_arith"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // Predicate value getters read correlation parameters live per row, not frozen at the first one.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.v > outer_t.id * 150) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(v_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, 200}}, {{1, 10, 300}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::right_full_lateral_rejected") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_rightfull"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    // A LATERAL reference can only sit on the inner side of a join, so RIGHT/FULL JOIN LATERAL is ill-defined; the
    // lateral join operator only NULL-extends for LEFT, so without a guard these would silently return a wrong answer.
    for (const char* sql :
         {"SELECT * FROM s.outer_t RIGHT JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
          "outer_t.id) sub ON true;",
          "SELECT * FROM s.outer_t FULL JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
          "outer_t.id) sub ON true;"}) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("sql: " << sql);
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::lateral_subquery::inner_join_on_predicate") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_on"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // The lateral join's own ON predicate is a separate filter from the subquery's WHERE.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.id) "
        "sub ON sub.v > 100;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(v_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, 101}}, {{2, 20, 200}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::left_join_on_predicate_null_pads") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_on_left"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t LEFT JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
        "outer_t.id) sub ON sub.v > 250;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(v_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, std::nullopt}}, {{2, 20, std::nullopt}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::correlated_function_argument") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_fn"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE s;");
        dispatcher->execute_sql(s, "CREATE TABLE s.os (id BIGINT, tag TEXT);");
        dispatcher->execute_sql(s, "INSERT INTO s.os (id, tag) VALUES (1, 'a'), (2, 'bb');");
        dispatcher->execute_sql(s, "CREATE TABLE s.inner_t (k BIGINT, v BIGINT);");
        dispatcher->execute_sql(s, "INSERT INTO s.inner_t (k, v) VALUES (1, 100), (1, 101), (2, 200), (3, 300);");
    }

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.os, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = length(os.tag)) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    int id_i = find_column(*cur, "id");
    int v_i = find_column(*cur, "v");
    REQUIRE(id_i >= 0);
    REQUIRE(v_i >= 0);
    std::multiset<std::pair<int64_t, int64_t>> got;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        got.emplace(cur->value(static_cast<uint64_t>(id_i), r).value<int64_t>(),
                    cur->value(static_cast<uint64_t>(v_i), r).value<int64_t>());
    }
    std::multiset<std::pair<int64_t, int64_t>> expected{{1, 100}, {1, 101}, {2, 200}};
    REQUIRE(got == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::projects_correlated_arithmetic") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_projarith"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur =
        dispatcher->execute_sql(session, "SELECT * FROM s.outer_t, LATERAL (SELECT outer_t.id * 10 AS ten) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int ten_i = find_column(*cur, "ten");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(ten_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, 10}}, {{2, 20, 20}}};
    REQUIRE(collect(*cur, id_i, n_i, ten_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::projects_correlated_outer_column") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_proj"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT * FROM s.outer_t, LATERAL (SELECT outer_t.id AS x) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    int id_i = find_column(*cur, "id");
    int n_i = find_column(*cur, "n");
    int x_i = find_column(*cur, "x");
    REQUIRE(id_i >= 0);
    REQUIRE(n_i >= 0);
    REQUIRE(x_i >= 0);
    std::multiset<row3_t> expected{{{1, 10, 1}}, {{2, 20, 2}}};
    REQUIRE(collect(*cur, id_i, n_i, x_i) == expected);
}

// DML ... FROM/USING LATERAL: the FROM/USING clause is a source sub-plan that may itself correlate two source
// items (never the DML target, outside the source's join scope); the source materializes as the join's RIGHT side.
TEST_CASE("integration::cpp::dml_lateral::delete_using_lateral_generate_series") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_delete"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (lo BIGINT, hi BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.src (lo, hi) VALUES (1, 3), (6, 8);");

    auto cur = dispatcher->execute_sql(
        session,
        "DELETE FROM s.tgt USING s.src, LATERAL generate_series(src.lo, src.hi) WHERE tgt.id = generate_series;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 6);

    auto check = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(check->is_success());
    REQUIRE(check->size() == 4);
    std::multiset<int64_t> survivors;
    for (uint64_t r = 0; r < check->size(); ++r) {
        survivors.insert(check->value(0, r).value<int64_t>());
    }
    REQUIRE(survivors == std::multiset<int64_t>{4, 5, 9, 10});
}

TEST_CASE("integration::cpp::dml_lateral::update_from_lateral_correlated_subquery") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_update"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT, val BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id, val) VALUES (1, 0), (2, 0), (3, 0);");
    dispatcher->execute_sql(session, "CREATE TABLE s.a (k BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.a (k) VALUES (1), (2);");
    dispatcher->execute_sql(session, "CREATE TABLE s.innr (ik BIGINT, iv BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.innr (ik, iv) VALUES (1, 111), (2, 222);");

    auto cur = dispatcher->execute_sql(
        session,
        "UPDATE s.tgt SET val = iv FROM s.a, LATERAL (SELECT innr.iv FROM s.innr WHERE innr.ik = a.k) sub "
        "WHERE tgt.id = a.k;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);

    auto check = dispatcher->execute_sql(session, "SELECT id, val FROM s.tgt;");
    REQUIRE(check->is_success());
    REQUIRE(check->size() == 3);
    std::multiset<std::array<int64_t, 2>> rows;
    for (uint64_t r = 0; r < check->size(); ++r) {
        rows.insert(std::array<int64_t, 2>{check->value(0, r).value<int64_t>(), check->value(1, r).value<int64_t>()});
    }
    REQUIRE(rows == std::multiset<std::array<int64_t, 2>>{{{1, 111}}, {{2, 222}}, {{3, 0}}});
}

TEST_CASE("integration::cpp::dml_lateral::delete_using_no_where_respects_source") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_del_nowhere"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (1),(2),(3),(4),(5);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (x BIGINT);");

    // DELETE ... USING with no WHERE is a cross-join filter: an empty source joins nothing, so nothing is deleted.
    auto empty = dispatcher->execute_sql(session, "DELETE FROM s.tgt USING s.src;");
    INFO("error: " << (empty->is_error() ? empty->get_error().what.c_str() : "none"));
    REQUIRE(empty->is_success());
    REQUIRE(empty->size() == 0);
    auto survived = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(survived->size() == 5);

    dispatcher->execute_sql(session, "INSERT INTO s.src (x) VALUES (99);");
    auto all = dispatcher->execute_sql(session, "DELETE FROM s.tgt USING s.src;");
    INFO("error: " << (all->is_error() ? all->get_error().what.c_str() : "none"));
    REQUIRE(all->is_success());
    REQUIRE(all->size() == 5);
    auto remaining = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(remaining->size() == 0);
}

TEST_CASE("integration::cpp::dml_lateral::delete_using_lateral_empty_join_preserves_rows") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_del_empty"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (100),(200),(300);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (lo BIGINT, hi BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.src (lo, hi) VALUES (1, 3), (6, 8);");

    auto cur = dispatcher->execute_sql(
        session,
        "DELETE FROM s.tgt USING s.src, LATERAL generate_series(src.lo, src.hi) WHERE tgt.id = generate_series;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
    auto check = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(check->size() == 3);
}

TEST_CASE("integration::cpp::dml_lateral::delete_using_lateral_duplicate_matches_delete_once") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_del_dup"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (1),(2),(3);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (lo BIGINT, hi BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.src (lo, hi) VALUES (1, 3), (2, 3);");

    // DELETE ... USING is a semi-join: a target row is deleted exactly once no matter how many source rows it joins.
    auto cur = dispatcher->execute_sql(
        session,
        "DELETE FROM s.tgt USING s.src, LATERAL generate_series(src.lo, src.hi) WHERE tgt.id = generate_series;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto check = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(check->size() == 0);
}

TEST_CASE("integration::cpp::dml_lateral::update_from_lateral_empty_join_no_change") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_upd_empty"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT, val BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id, val) VALUES (1, 0), (2, 0), (3, 0);");
    dispatcher->execute_sql(session, "CREATE TABLE s.a (k BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.a (k) VALUES (7), (8);");
    dispatcher->execute_sql(session, "CREATE TABLE s.innr (ik BIGINT, iv BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.innr (ik, iv) VALUES (7, 111), (8, 222);");

    auto cur = dispatcher->execute_sql(
        session,
        "UPDATE s.tgt SET val = iv FROM s.a, LATERAL (SELECT innr.iv FROM s.innr WHERE innr.ik = a.k) sub "
        "WHERE tgt.id = a.k;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
    auto check = dispatcher->execute_sql(session, "SELECT id, val FROM s.tgt;");
    REQUIRE(check->size() == 3);
    for (uint64_t r = 0; r < check->size(); ++r) {
        REQUIRE(check->value(1, r).value<int64_t>() == 0);
    }
}

TEST_CASE("integration::cpp::dml_lateral::update_from_lateral_duplicate_matches_update_once") {
    auto config = test_create_config(integration_fixture_path("test_dml_lateral_upd_dup"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT, val BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id, val) VALUES (1, 0), (2, 0), (3, 0);");
    dispatcher->execute_sql(session, "CREATE TABLE s.a (k BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.a (k) VALUES (1), (2);");
    dispatcher->execute_sql(session, "CREATE TABLE s.innr (ik BIGINT, iv BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.innr (ik, iv) VALUES (1, 111), (1, 111), (2, 222);");

    // UPDATE ... FROM is a semi-join: a target row is updated exactly once despite multiple matching source rows.
    auto cur = dispatcher->execute_sql(
        session,
        "UPDATE s.tgt SET val = iv FROM s.a, LATERAL (SELECT innr.iv FROM s.innr WHERE innr.ik = a.k) sub "
        "WHERE tgt.id = a.k;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto check = dispatcher->execute_sql(session, "SELECT id, val FROM s.tgt;");
    REQUIRE(check->size() == 3);
    std::multiset<std::array<int64_t, 2>> rows;
    for (uint64_t r = 0; r < check->size(); ++r) {
        rows.insert(std::array<int64_t, 2>{check->value(0, r).value<int64_t>(), check->value(1, r).value<int64_t>()});
    }
    REQUIRE(rows == std::multiset<std::array<int64_t, 2>>{{{1, 111}}, {{2, 222}}, {{3, 0}}});
}

// A LATERAL body is evaluated independently for each outer row, whatever the body
// contains: the rows one outer row produces never depend on an earlier outer row.
TEST_CASE("integration::cpp::lateral_subquery::body_evaluated_per_outer_row") {
    auto config = test_create_config(integration_fixture_path("test_lateral_subquery_body_per_row"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.owners (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.owners (id) VALUES (1), (2);");
    dispatcher->execute_sql(session, "CREATE TABLE s.items (owner_id BIGINT, amount BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.items (owner_id, amount) VALUES (1, 7), (1, 7), (2, 7), (2, 8);");

    auto count_rows = [&](const std::string& sql) -> uint64_t {
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("sql: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        return cur->size();
    };

    SECTION("distinct") {
        // owner 1 -> {7}; owner 2 -> {7, 8}
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT DISTINCT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id) sub;") == 3);
    }
    SECTION("limit") {
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id LIMIT 1) sub;") == 2);
    }
    SECTION("union") {
        // owner 1 -> {7}; owner 2 -> {7, 8}
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id UNION SELECT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id) sub;") == 3);
    }
    SECTION("union_all") {
        // owner 1 -> 2 + 2 rows; owner 2 -> 2 + 2
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id UNION ALL SELECT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id) sub;") == 8);
    }
    SECTION("order_by_with_limit") {
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.amount FROM s.items "
                           "WHERE items.owner_id = owners.id ORDER BY items.amount LIMIT 1) sub;") == 2);
    }
    SECTION("grouped_aggregate") {
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.owner_id, sum(items.amount) FROM s.items "
                           "WHERE items.owner_id = owners.id GROUP BY items.owner_id) sub;") == 2);
    }
    SECTION("grouped_aggregate_with_having") {
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.owner_id, sum(items.amount) FROM s.items "
                           "WHERE items.owner_id = owners.id GROUP BY items.owner_id "
                           "HAVING sum(items.amount) > 0) sub;") == 2);
    }
    SECTION("grouped_by_computed_key") {
        // owner 1 -> one parity group; owner 2 -> two
        REQUIRE(count_rows("SELECT * FROM s.owners, LATERAL (SELECT items.amount % 2 AS parity, sum(items.amount) "
                           "FROM s.items WHERE items.owner_id = owners.id GROUP BY items.amount % 2) sub;") == 3);
    }
}
