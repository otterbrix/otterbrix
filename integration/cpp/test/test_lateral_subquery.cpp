#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/generaty.hpp>
#include <components/tests/temp_dir.hpp>
#include <components/types/logical_value.hpp>

#include <array>
#include <optional>
#include <set>
#include <string>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    using row3_t = std::array<std::optional<int64_t>, 3>;

    // Collect the result as (col0, col1, col2) tuples mapped by the given column
    // indices; std::nullopt marks a SQL NULL. Order-insensitive multiset compare.
    std::multiset<row3_t> collect(const cursor_t& cur, uint64_t c0, uint64_t c1, uint64_t c2) {
        std::multiset<row3_t> rows;
        for (uint64_t r = 0; r < cur.size(); ++r) {
            auto read = [&](uint64_t col) -> std::optional<int64_t> {
                auto cell = cur.value(col, r);
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
    auto config = test_create_config(test_temp_path("test_lateral_subquery_where"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // The subquery filters an inner table by an outer column: inner_t.k = outer_t.id.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.id) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    // outer (1,10) -> inner v in {100,101}; outer (2,20) -> inner v {200}.
    std::multiset<row3_t> expected{{{1, 10, 100}}, {{1, 10, 101}}, {{2, 20, 200}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

// Regression for the batched-join_builder use-after-free / row-mixup under
// LATERAL. The lazy builder buffered a raw pointer to each outer row's per-iteration
// inner result (destroyed each iteration) and a SINGLE left_chunk_ pointer overwritten
// across outer chunks; with > DEFAULT_VECTOR_CAPACITY (1024) outer rows the correlated
// inner is re-run per row, so the post-loop flush() gathered from freed inner chunks
// (heap-use-after-free under ASan) and, when a flush window spanned two outer chunks,
// paired left rows with the wrong outer chunk. The eager LATERAL builder copies each row
// at emit time, fixing both. This asserts the full, correct 1:1 output at scale.
TEST_CASE("integration::cpp::lateral_subquery::correlated_where_multichunk") {
    constexpr int64_t N = 1100; // > 1024 so the outer input spans >= 2 chunks
    auto config = test_create_config(test_temp_path("test_lateral_subquery_multichunk"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.outer_big (id BIGINT);");
    dispatcher->execute_sql(session, "CREATE TABLE s.inr (k BIGINT, v BIGINT);");
    // Each outer id 0..N-1 matches exactly one inner row (v = id + 100000). Insert in
    // batches to keep individual INSERT statements small.
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
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    // Every outer id must appear exactly once, paired with its own inner v (id + 100000).
    // A left/right chunk mixup would break the pairing; a UAF would abort under ASan.
    std::set<int64_t> seen_ids;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        auto id_cell = cur->value(id_i, r);
        auto v_cell = cur->value(v_i, r);
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
    auto config = test_create_config(test_temp_path("test_lateral_subquery_left"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    {
        auto session = otterbrix::session_id_t();
        // (5,50) has no matching inner_t.k=5 — LEFT JOIN must keep it NULL-padded.
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
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    std::multiset<row3_t> expected{{{1, 10, 100}}, {{1, 10, 101}}, {{2, 20, 200}}, {{5, 50, std::nullopt}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::correlated_in_arithmetic") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_arith"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // Correlation nested inside an arithmetic operand (outer_t.id * 150). Predicate
    // value getters read parameters live per row, so the threshold is recomputed for
    // each outer row rather than frozen at the first one.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.v > outer_t.id * 150) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    // id=1 -> v>150 -> {200,300}; id=2 -> v>300 -> {}.
    REQUIRE(cur->size() == 2);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    std::multiset<row3_t> expected{{{1, 10, 200}}, {{1, 10, 300}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::right_full_lateral_rejected") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_rightfull"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    // A LATERAL reference can only sit on the inner side of a join, so RIGHT/FULL JOIN
    // LATERAL is ill-defined. The lateral join operator only NULL-extends for LEFT, so
    // without a guard these would fall through to plain inner semantics and return a
    // silently wrong answer. Validation must reject them cleanly instead.
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
    auto config = test_create_config(test_temp_path("test_lateral_subquery_on"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // The lateral join's own ON predicate filters inner rows: sub.v > 100 drops v=100.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.id) "
        "sub ON sub.v > 100;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    // outer (1,10): v in {100,101}, keep v>100 -> {101}; outer (2,20): v {200} -> {200}.
    REQUIRE(cur->size() == 2);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    std::multiset<row3_t> expected{{{1, 10, 101}}, {{2, 20, 200}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::left_join_on_predicate_null_pads") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_on_left"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // ON sub.v > 250 filters out every inner row for both outer rows; LEFT JOIN must
    // still keep each outer row, NULL-padded.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.outer_t LEFT JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
        "outer_t.id) sub ON sub.v > 250;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    std::multiset<row3_t> expected{{{1, 10, std::nullopt}}, {{2, 20, std::nullopt}}};
    REQUIRE(collect(*cur, id_i, n_i, v_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::correlated_function_argument") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_fn"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    // Correlated column as a function argument: length(os.tag) is recomputed per outer
    // row and drives the inner filter.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT * FROM s.os, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = length(os.tag)) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t v_i = test_column_index(*cur, "v");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(v_i != test_column_not_found);
    // length('a')=1 -> k=1 -> v{100,101}; length('bb')=2 -> k=2 -> v{200}.
    std::multiset<std::pair<int64_t, int64_t>> got;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        got.emplace(cur->value(id_i, r).value<int64_t>(), cur->value(v_i, r).value<int64_t>());
    }
    std::multiset<std::pair<int64_t, int64_t>> expected{{1, 100}, {1, 101}, {2, 200}};
    REQUIRE(got == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::projects_correlated_arithmetic") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_projarith"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // A correlated column inside a SELECT-list arithmetic expression: ten = id * 10,
    // recomputed per outer row.
    auto cur =
        dispatcher->execute_sql(session, "SELECT * FROM s.outer_t, LATERAL (SELECT outer_t.id * 10 AS ten) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t ten_i = test_column_index(*cur, "ten");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(ten_i != test_column_not_found);
    std::multiset<row3_t> expected{{{1, 10, 10}}, {{2, 20, 20}}};
    REQUIRE(collect(*cur, id_i, n_i, ten_i) == expected);
}

TEST_CASE("integration::cpp::lateral_subquery::projects_correlated_outer_column") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_proj"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    auto session = otterbrix::session_id_t();
    // The subquery projects a correlated outer column; operator_select reads the
    // correlation parameter live per outer row, so x tracks each outer id.
    auto cur = dispatcher->execute_sql(session, "SELECT * FROM s.outer_t, LATERAL (SELECT outer_t.id AS x) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    uint64_t id_i = test_column_index(*cur, "id");
    uint64_t n_i = test_column_index(*cur, "n");
    uint64_t x_i = test_column_index(*cur, "x");
    REQUIRE(id_i != test_column_not_found);
    REQUIRE(n_i != test_column_not_found);
    REQUIRE(x_i != test_column_not_found);
    std::multiset<row3_t> expected{{{1, 10, 1}}, {{2, 20, 2}}};
    REQUIRE(collect(*cur, id_i, n_i, x_i) == expected);
}

// A LATERAL correlation is captured by TABLE QUALIFIER alone (outer_t.<anything>
// lowers to a correlation parameter), so a reference to a column the outer relation
// does not have reaches the plan as a parameter no schema check ever sees. Validation
// must refuse it: it is a name that resolves to nothing, and only the lateral join
// operator would notice — mid-execution, and never at all for EXPLAIN, which then
// reports a plan for a query that cannot run.
TEST_CASE("integration::cpp::lateral_subquery::unmatched_correlation_rejected") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_badcorr"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    const std::string sql =
        "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.nosuch) sub;";

    {
        auto session = otterbrix::session_id_t();
        auto explained = dispatcher->execute_sql(session, "EXPLAIN " + sql);
        INFO("explain error: " << (explained->is_error() ? explained->get_error().what.c_str() : "none"));
        REQUIRE(explained->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        REQUIRE(cur->is_error());
        INFO("error: " << cur->get_error().what.c_str());
        // Refused by the schema validator (the name does not exist), not by the
        // operator's runtime binding step.
        REQUIRE(cur->get_error().type == core::error_code_t::field_not_exists);
    }

    // A correlation that DOES name an outer column still validates and runs.
    {
        auto session = otterbrix::session_id_t();
        auto ok = dispatcher->execute_sql(
            session,
            "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.id) sub;");
        INFO("control error: " << (ok->is_error() ? ok->get_error().what.c_str() : "none"));
        REQUIRE(ok->is_success());
        REQUIRE(ok->size() == 3);
    }
}

// -------------------------------------------------------------------------
// DML ... FROM/USING LATERAL: the FROM/USING clause is a source sub-plan that
// may itself contain a LATERAL correlation between two source items (never to
// the DML target, which is not in the source's join scope). The whole source is
// materialized as the RIGHT side of the DML join; the WHERE predicate joins the
// target (LEFT) against it.
// -------------------------------------------------------------------------

TEST_CASE("integration::cpp::dml_lateral::delete_using_lateral_generate_series") {
    auto config = test_create_config(test_temp_path("test_dml_lateral_delete"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (lo BIGINT, hi BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.src (lo, hi) VALUES (1, 3), (6, 8);");

    // USING src, LATERAL generate_series(src.lo, src.hi): the table function
    // correlates to the sibling USING table src (not the target). Its output is the
    // union of [1..3] and [6..8]; tgt rows whose id lands in that set are deleted.
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
    auto config = test_create_config(test_temp_path("test_dml_lateral_update"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // FROM a, LATERAL (SELECT innr.iv WHERE innr.ik = a.k): the derived table
    // correlates to sibling source a. The materialized source is {(k=1,iv=111),
    // (k=2,iv=222)}; joined to tgt on tgt.id = a.k, SET val = the correlated iv.
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
    auto config = test_create_config(test_temp_path("test_dml_lateral_del_nowhere"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (1),(2),(3),(4),(5);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (x BIGINT);");

    // DELETE ... USING with no WHERE is a cross-join filter: an EMPTY source joins
    // nothing, so no target row may be deleted (the source must not be silently
    // dropped and the delete degraded to delete-all).
    auto empty = dispatcher->execute_sql(session, "DELETE FROM s.tgt USING s.src;");
    INFO("error: " << (empty->is_error() ? empty->get_error().what.c_str() : "none"));
    REQUIRE(empty->is_success());
    REQUIRE(empty->size() == 0);
    auto survived = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(survived->size() == 5);

    // With a non-empty source, every target row cross-joins a source row, so the
    // no-WHERE form deletes them all.
    dispatcher->execute_sql(session, "INSERT INTO s.src (x) VALUES (99);");
    auto all = dispatcher->execute_sql(session, "DELETE FROM s.tgt USING s.src;");
    INFO("error: " << (all->is_error() ? all->get_error().what.c_str() : "none"));
    REQUIRE(all->is_success());
    REQUIRE(all->size() == 5);
    auto remaining = dispatcher->execute_sql(session, "SELECT id FROM s.tgt;");
    REQUIRE(remaining->size() == 0);
}

TEST_CASE("integration::cpp::dml_lateral::delete_using_lateral_empty_join_preserves_rows") {
    auto config = test_create_config(test_temp_path("test_dml_lateral_del_empty"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (100),(200),(300);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (lo BIGINT, hi BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.src (lo, hi) VALUES (1, 3), (6, 8);");

    // The source produces rows (series {1,2,3,6,7,8}) but none join the target ids
    // {100,200,300}: nothing is deleted, every target row survives.
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
    auto config = test_create_config(test_temp_path("test_dml_lateral_del_dup"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id) VALUES (1),(2),(3);");
    dispatcher->execute_sql(session, "CREATE TABLE s.src (lo BIGINT, hi BIGINT);");
    // Overlapping ranges: series = {1,2,3} + {2,3} — ids 2 and 3 are produced by BOTH
    // source rows, so each joins two source rows.
    dispatcher->execute_sql(session, "INSERT INTO s.src (lo, hi) VALUES (1, 3), (2, 3);");

    // DELETE ... USING is a semi-join: a target row is deleted exactly once no matter
    // how many source rows it joins. All three targets are deleted (3, not 5).
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
    auto config = test_create_config(test_temp_path("test_dml_lateral_upd_empty"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // The source materializes {(k=7,iv=111),(k=8,iv=222)} but no target id joins k,
    // so nothing is updated and every val stays 0.
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
    auto config = test_create_config(test_temp_path("test_dml_lateral_upd_dup"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.tgt (id BIGINT, val BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.tgt (id, val) VALUES (1, 0), (2, 0), (3, 0);");
    dispatcher->execute_sql(session, "CREATE TABLE s.a (k BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.a (k) VALUES (1), (2);");
    dispatcher->execute_sql(session, "CREATE TABLE s.innr (ik BIGINT, iv BIGINT);");
    // ik=1 has TWO rows (both iv=111), so the lateral yields two source rows for k=1;
    // tgt id=1 then joins two source rows.
    dispatcher->execute_sql(session, "INSERT INTO s.innr (ik, iv) VALUES (1, 111), (1, 111), (2, 222);");

    // UPDATE ... FROM is a semi-join: tgt id=1 is updated once (val=111) despite the two
    // matching source rows; id=2 -> 222; id=3 untouched. Two rows affected, not three.
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

namespace {
    // The result's column names, in output order.
    std::vector<std::string> result_column_names(const cursor_t& cur) {
        std::vector<std::string> names;
        names.reserve(cur.columns().size());
        for (const auto& column : cur.columns()) {
            names.emplace_back(column.name.data(), column.name.size());
        }
        return names;
    }
} // namespace

// A LATERAL join merges outer and inner into ONE chunk, and the merged chunk records
// the split nowhere — the column NAME is the only user-visible trace of which side a
// column came from. Unlike the other two joins it lays the output out from its
// PLAN-TIME side schemas rather than from the input chunks (the inner sub-plan may
// yield no chunk at all, for every outer row), so this pins the names its plan-time
// currency has to carry, in all three layouts: (outer ++ inner) for the comma form,
// the same under LEFT with an outer row whose inner side is empty, and the OUTER-only
// layout of a semi join, where the inner columns are absent entirely.
TEST_CASE("integration::cpp::lateral_subquery::output_column_names") {
    auto config = test_create_config(test_temp_path("test_lateral_subquery_names"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);
    {
        auto session = otterbrix::session_id_t();
        // (5,50) has no matching inner_t.k — the LEFT form must keep it, NULL-padded.
        dispatcher->execute_sql(session, "INSERT INTO s.outer_t (id, n) VALUES (5, 50);");
    }

    const std::vector<std::string> outer_and_inner{"id", "n", "v"};

    INFO("comma LATERAL — (outer ++ inner)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = outer_t.id) sub;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(result_column_names(*cur) == outer_and_inner);
    }

    INFO("LEFT JOIN LATERAL — the NULL-padded outer row keeps the inner column named");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM s.outer_t LEFT JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
            "outer_t.id) sub ON true;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(result_column_names(*cur) == outer_and_inner);
    }

    // A CORRELATED EXISTS in WHERE lowers to a lateral SEMI join (transform_select.cpp),
    // whose output is the outer schema alone — the one layout where the two side
    // schemas do not simply concatenate.
    INFO("correlated EXISTS — the semi layout is the OUTER schema alone");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM s.outer_t WHERE EXISTS (SELECT 1 FROM s.inner_t WHERE inner_t.k = outer_t.id);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        const std::vector<std::string> outer_only{"id", "n"};
        REQUIRE(result_column_names(*cur) == outer_only);
    }
}
