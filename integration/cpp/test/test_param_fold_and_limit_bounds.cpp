#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/vector_builders.hpp>

#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// PostgreSQL 18 semantics (from memory, not re-verified against a server):
//   LIMIT NULL  = no limit,  OFFSET NULL = OFFSET 0,
//   LIMIT -1 / OFFSET -1 (literal or parameter) = error "LIMIT must not be negative" / "OFFSET must not be negative".
// And a parameter slot is read-only input: folding `$1 + 1` must not change what a second `$1` or a rebind sees.

using namespace test_helpers;

namespace {
    using param_t = std::pair<size_t, components::vector::vector_t>;
    using params_t = std::vector<param_t>;

    components::vector::vector_t i64(std::pmr::memory_resource* resource, int64_t v) {
        return components::vector::make_scalar(resource, v);
    }

    components::vector::vector_t null_value(std::pmr::memory_resource* resource) {
        return components::vector::make_null(
            resource,
            components::types::complex_logical_type{components::types::logical_type::NA});
    }

    std::string describe(const components::cursor::cursor_t& cur) {
        if (cur.is_error()) {
            return std::string{cur.get_error().what};
        }
        return "accepted, rows=" + std::to_string(cur.size());
    }

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto cell0 = cur->get<std::string_view>(0, r);
            REQUIRE_FALSE(cell0.has_error());
            out += std::string(cell0.value());
            out += '\n';
        }
        return out;
    }

    template<typename D>
    void seed_rows(D* d, int rows) {
        REQUIRE(ok(d, "CREATE SCHEMA m;"));
        REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
        std::string sql = "INSERT INTO m.t (x) VALUES ";
        for (int i = 1; i <= rows; ++i) {
            sql += (i == 1 ? "(" : ", (") + std::to_string(i) + ")";
        }
        REQUIRE(ok(d, sql + ";"));
    }

    std::set<int64_t> column_values(const components::cursor::cursor_t& cur) {
        std::set<int64_t> out;
        for (uint64_t r = 0; r < cur.size(); ++r) {
            auto cell1 = cur.get<int64_t>(0, r);
            REQUIRE_FALSE(cell1.has_error());
            out.insert(cell1.value());
        }
        return out;
    }

    template<typename D>
    void expect_rows(D* d, const std::string& sql, uint64_t expected_rows) {
        auto cur = exec(d, sql);
        INFO(sql << " -> " << describe(*cur));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == expected_rows);
    }

    template<typename D>
    void expect_index_scan(D* d, const std::string& sql, uint64_t expected_rows) {
        auto plan = exec(d, "EXPLAIN " + sql);
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for " << sql << ":\n" << text);
        CHECK(text.find("Index Scan") != std::string::npos);
        expect_rows(d, sql, expected_rows);
    }

    template<typename D>
    void expect_ids(D* d, const std::string& sql, const std::set<int64_t>& expected) {
        auto cur = exec(d, sql);
        INFO(sql << " -> " << describe(*cur));
        REQUIRE(cur->is_success());
        CHECK(column_values(*cur) == expected);
    }

    template<typename D>
    void expect_rows_ok(D* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO(sql << " -> " << describe(*cur));
        CHECK(cur->is_success());
    }

    template<typename D>
    void expect_error(D* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO(sql << " -> " << describe(*cur));
        CHECK(cur->is_error());
    }

    template<typename D>
    void expect_param_rows(D* d, const std::string& sql, params_t params, uint64_t expected_rows) {
        auto cur = d->execute_sql_with_params(otterbrix::session_id_t(), sql, params);
        INFO(sql << " -> " << describe(*cur));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == expected_rows);
    }

    template<typename D>
    void expect_param_error(D* d, const std::string& sql, params_t params) {
        auto cur = d->execute_sql_with_params(otterbrix::session_id_t(), sql, params);
        INFO(sql << " -> " << describe(*cur));
        CHECK(cur->is_error());
    }
} // namespace

TEST_CASE("integration::cpp::limit_bounds::literal_limit_null_means_no_limit") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/literal_limit_null"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_rows(d, "SELECT x FROM m.t LIMIT NULL;", 5);
}

TEST_CASE("integration::cpp::limit_bounds::literal_offset_null_means_offset_zero") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/literal_offset_null"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_rows(d, "SELECT x FROM m.t OFFSET NULL;", 5);
}

TEST_CASE("integration::cpp::limit_bounds::literal_negative_limit_is_an_error") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/literal_negative_limit"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error(d, "SELECT x FROM m.t LIMIT -1;");
}

TEST_CASE("integration::cpp::limit_bounds::literal_negative_offset_is_an_error") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/literal_negative_offset"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error(d, "SELECT x FROM m.t OFFSET -1;");
}

TEST_CASE("integration::cpp::limit_bounds::literal_negative_limit_other_than_minus_one_is_an_error") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/literal_negative_limit_5"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error(d, "SELECT x FROM m.t LIMIT -5;");
}

TEST_CASE("integration::cpp::limit_bounds::parameter_limit_null_means_no_limit") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_limit_null"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_param_rows(d, "SELECT x FROM m.t LIMIT $1;", {{1, null_value(d->resource())}}, 5);
}

TEST_CASE("integration::cpp::limit_bounds::parameter_offset_null_means_offset_zero") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_offset_null"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_param_rows(d, "SELECT x FROM m.t OFFSET $1;", {{1, null_value(d->resource())}}, 5);
}

TEST_CASE("integration::cpp::limit_bounds::parameter_negative_limit_is_an_error") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_negative_limit"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_param_error(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), -1)}});
}

TEST_CASE("integration::cpp::limit_bounds::parameter_negative_limit_other_than_minus_one_is_an_error") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_negative_limit_5"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_param_error(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), -5)}});
}

TEST_CASE("integration::cpp::limit_bounds::parameter_negative_offset_is_an_error") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_negative_offset"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_param_error(d, "SELECT x FROM m.t OFFSET $1;", {{1, i64(d->resource(), -1)}});
}

// $1 + 1 folds into a single constant. A second use of $1 in the same statement must still read the caller's value.
TEST_CASE("integration::cpp::param_fold::shared_parameter_is_not_overwritten_by_folding") {
    auto config = make_test_config(integration_fixture_path("param_fold/shared_parameter"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (a BIGINT, b BIGINT);"));
    // (a, b): (6, 5) is the correct answer for $1 = 5 (a = 6, b = 5); (6, 6) is what a clobbered $1 would select.
    REQUIRE(ok(d, "INSERT INTO m.t (a, b) VALUES (6, 5), (6, 6), (5, 5), (7, 7);"));

    auto cur = d->execute_sql_with_params(otterbrix::session_id_t(),
                                          "SELECT a, b FROM m.t WHERE a = $1 + 1 AND b = $1;",
                                          {{1, i64(d->resource(), 5)}});
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell2 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell2.has_error());
    CHECK(cell2.value() == 6);
    auto cell3 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell3.has_error());
    CHECK(cell3.value() == 5);
}

TEST_CASE("integration::cpp::param_fold::shared_parameter_is_not_overwritten_when_used_first") {
    auto config = make_test_config(integration_fixture_path("param_fold/shared_parameter_first"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (a BIGINT, b BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (a, b) VALUES (6, 5), (6, 6), (5, 5), (7, 7);"));

    auto cur = d->execute_sql_with_params(otterbrix::session_id_t(),
                                          "SELECT a, b FROM m.t WHERE b = $1 AND a = $1 + 1;",
                                          {{1, i64(d->resource(), 5)}});
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell4 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell4.has_error());
    CHECK(cell4.value() == 6);
    auto cell5 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell5.has_error());
    CHECK(cell5.value() == 5);
}

// execute, bind a new value, execute again: the second run must see the NEW value in every place it is used.
// A transform_result is single-shot, so the second bind goes through a new transform.
TEST_CASE("integration::cpp::param_fold::rebind_after_execute_recomputes_the_folded_expression") {
    namespace sql = components::sql;
    auto config = make_test_config(integration_fixture_path("param_fold/rebind_after_execute"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 10);

    auto* resource = d->resource();
    std::pmr::monotonic_buffer_resource arena(resource);
    const std::string query = "SELECT x FROM m.t WHERE x = $1 + 1;";
    auto* raw = parse_ok(&arena, query.c_str());
    REQUIRE(raw != nullptr);

    const auto run_with = [&](int64_t value) {
        sql::transform::transformer transformer(resource, query.c_str());
        auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(linitial(raw)));
        REQUIRE_FALSE(binder.has_error());
        binder.bind(1, i64(resource, value));
        auto plan = binder.finalize();
        REQUIRE_FALSE(plan.has_error());
        auto cur = d->execute_plan(otterbrix::session_id_t(), std::move(plan.value()));
        REQUIRE(cur->is_success());
        return column_values(*cur);
    };

    CHECK(run_with(1) == std::set<int64_t>{2});
    CHECK(run_with(5) == std::set<int64_t>{6});
}

// Nothing has read a $N before finalize, so the last bind wins, whatever its type. The validator then judges the
// type the statement ended up with.
TEST_CASE("integration::cpp::param_storage::rebinding_a_parameter_with_another_type_is_a_clean_error") {
    auto config = make_test_config(integration_fixture_path("param_storage/rebind_other_type"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);

    auto cur = d->execute_sql_with_params(
        otterbrix::session_id_t(),
        "SELECT x FROM m.t WHERE x = $1;",
        {{1, i64(d->resource(), 1)}, {1, components::vector::make_scalar(d->resource(), std::string("abc"))}});
    INFO(describe(*cur));
    REQUIRE(cur->is_error());
}

// The compatible direction: the first bind is text, the last one is the integer the column wants. The last bind
// wins, so the statement runs as if only the integer had been bound.
TEST_CASE("integration::cpp::param_storage::rebinding_a_text_parameter_with_an_integer_runs_with_the_integer") {
    auto config = make_test_config(integration_fixture_path("param_storage/rebind_text_then_integer"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);

    auto cur = d->execute_sql_with_params(
        otterbrix::session_id_t(),
        "SELECT x FROM m.t WHERE x = $1;",
        {{1, components::vector::make_scalar(d->resource(), std::string("abc"))}, {1, i64(d->resource(), 3)}});
    INFO(describe(*cur));
    REQUIRE(cur->is_success());
    CHECK(column_values(*cur) == std::set<int64_t>{3});
}

TEST_CASE("integration::cpp::param_storage::rebinding_a_parameter_with_the_same_type_keeps_the_last_value") {
    auto config = make_test_config(integration_fixture_path("param_storage/rebind_same_type"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);

    auto cur = d->execute_sql_with_params(otterbrix::session_id_t(),
                                          "SELECT x FROM m.t WHERE x = $1;",
                                          {{1, i64(d->resource(), 1)}, {1, i64(d->resource(), 4)}});
    INFO(describe(*cur));
    REQUIRE(cur->is_success());
    CHECK(column_values(*cur) == std::set<int64_t>{4});
}

// Every literal of an IN list is a parameter of its own, and a statement can hold more of them than 16 bits count.
TEST_CASE("integration::cpp::param_storage::a_statement_with_seventy_thousand_literals") {
    auto config = make_test_config(integration_fixture_path("param_storage/seventy_thousand_literals"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x) VALUES (5), (69999), (100000), (-3);"));

    std::string sql = "SELECT x FROM m.t WHERE x IN (";
    for (int i = 1; i <= 70000; ++i) {
        sql += (i == 1 ? "" : ", ") + std::to_string(i);
    }
    sql += ");";
    auto cur = exec(d, sql);
    INFO(describe(*cur));
    REQUIRE(cur->is_success());
    CHECK(column_values(*cur) == std::set<int64_t>{5, 69999});
}

// A cast of a constant, and a unary minus, are folded before index selection, which needs a bare key
// against a bare constant.
TEST_CASE("integration::cpp::param_fold::index_scan_on_double_column_equal_integer_literal") {
    auto config = make_test_config(integration_fixture_path("param_fold/index_double"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, d DOUBLE);"));
    REQUIRE(ok(d, "INSERT INTO m.t (id, d) VALUES (1, 3.0), (2, 4.5), (3, 3.0), (4, -3.0), (5, 7.5);"));
    REQUIRE(ok(d, "CREATE INDEX d_idx ON m.t (d);"));
    expect_index_scan(d, "SELECT id FROM m.t WHERE d = 3;", 2);
    expect_index_scan(d, "SELECT id FROM m.t WHERE d = -3;", 1);
    expect_index_scan(d, "SELECT id FROM m.t WHERE d = 3 * 2.5;", 1);
}

TEST_CASE("integration::cpp::param_fold::index_scan_on_timestamp_column_equal_date_literal") {
    auto config = make_test_config(integration_fixture_path("param_fold/index_timestamp"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, ts TIMESTAMP);"));
    REQUIRE(ok(d,
               "INSERT INTO m.t (id, ts) VALUES (1, '2024-01-01 00:00:00'), (2, '2024-01-01 10:00:00'), "
               "(3, '2024-01-02 00:00:00');"));
    REQUIRE(ok(d, "CREATE INDEX ts_idx ON m.t (ts);"));
    expect_index_scan(d, "SELECT id FROM m.t WHERE ts = '2024-01-01'::date;", 1);
}

TEST_CASE("integration::cpp::param_fold::index_scan_on_decimal_column_equal_integer_literal") {
    auto config = make_test_config(integration_fixture_path("param_fold/index_decimal"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, dec DECIMAL(10,2));"));
    REQUIRE(ok(d, "INSERT INTO m.t (id, dec) VALUES (1, 5.00), (2, 5.50), (3, 5.00), (4, 6.00);"));
    {
        auto cur = exec(d, "CREATE INDEX dec_idx ON m.t USING hash (dec);");
        INFO(describe(*cur));
        REQUIRE(cur->is_success());
    }
    expect_index_scan(d, "SELECT id FROM m.t WHERE dec = 5;", 2);
}

// The cast reads the session time zone, so the folded value must be the one the runtime would produce.
TEST_CASE("integration::cpp::param_fold::timezone_pin_cast_folded_with_the_session_zone") {
    auto config = make_test_config(integration_fixture_path("param_fold/timezone_pin"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto session = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(session, "SET TIMEZONE TO '+03:00';")->is_success());
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, tstz TIMESTAMPTZ);"));
    REQUIRE(ok(d, "INSERT INTO m.t (id, tstz) VALUES (1, '2024-01-01 07:00:00+00'), (2, '2024-01-01 10:00:00+00');"));
    // 10:00 local at +03:00 is 07:00 UTC: row 1.
    auto cur = d->execute_sql(session, "SELECT id FROM m.t WHERE tstz = '2024-01-01 10:00:00'::timestamp;");
    INFO(describe(*cur));
    REQUIRE(cur->is_success());
    CHECK(column_values(*cur) == std::set<int64_t>{1});
}

// The control: under UTC the same literal is row 2, so the pin above can only pass if the session zone was used.
TEST_CASE("integration::cpp::param_fold::timezone_control_utc_picks_the_other_row") {
    auto config = make_test_config(integration_fixture_path("param_fold/timezone_control"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, tstz TIMESTAMPTZ);"));
    REQUIRE(ok(d, "INSERT INTO m.t (id, tstz) VALUES (1, '2024-01-01 07:00:00+00'), (2, '2024-01-01 10:00:00+00');"));
    expect_ids(d, "SELECT id FROM m.t WHERE tstz = '2024-01-01 10:00:00'::timestamp;", {2});
}

// A cast the kernel refuses is not folded; the runtime raises it only for a row that reaches it.
TEST_CASE("integration::cpp::param_fold::refused_cast_is_lazy_on_an_empty_table") {
    auto config = make_test_config(integration_fixture_path("param_fold/lazy_cast"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, dec DECIMAL(4,2));"));
    expect_rows(d, "SELECT id FROM m.t WHERE dec = 1000;", 0);
}

// Row check only: EXPLAIN renders a pushed cast and a pushed folded constant alike, so the plan cannot tell them apart.
TEST_CASE("integration::cpp::param_fold::folded_constants_give_the_right_rows_on_a_disk_scan") {
    auto config = make_test_config(integration_fixture_path("param_fold/disk_scan"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (id BIGINT, d DOUBLE);"));
    REQUIRE(ok(d, "INSERT INTO m.t (id, d) VALUES (1, 3.0), (2, 4.5), (3, 3.0), (4, -3.0);"));
    expect_rows(d, "SELECT id FROM m.t WHERE d = 3;", 2);
    expect_rows(d, "SELECT id FROM m.t WHERE d = -3;", 1);
}

// A parameter or a literal under a prefix operator keeps the operator (the old promotion dropped it).
TEST_CASE("integration::cpp::param_fold::prefix_operator_over_a_parameter_or_literal_is_kept") {
    auto config = make_test_config(integration_fixture_path("param_fold/prefix_operator_kept"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x) VALUES (3), (-3), (5), (-6);"));
    auto cur = d->execute_sql_with_params(otterbrix::session_id_t(),
                                          "SELECT x FROM m.t WHERE x = -$1;",
                                          {{1, i64(d->resource(), 3)}});
    INFO(describe(*cur));
    REQUIRE(cur->is_success());
    CHECK(column_values(*cur) == std::set<int64_t>{-3});
    expect_ids(d, "SELECT x FROM m.t WHERE x = ~5;", {-6});
}

// A one-operand operator the fold refused still applies: it must not be replaced by its operand.
TEST_CASE("integration::cpp::param_fold::refused_unary_minus_is_not_dropped") {
    auto config = make_test_config(integration_fixture_path("param_fold/unary_not_dropped"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT, u UBIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x, u) VALUES (3, 5), (-3, 6), (5, 7), (-6, 8);"));
    expect_ids(d, "SELECT x FROM m.t WHERE x = -(SELECT 3);", {-3});
    expect_ids(d, "SELECT x FROM m.t WHERE x = ~(5::bigint);", {-6});
    expect_error(d, "SELECT x FROM m.t WHERE x = -(5::ubigint);");
    expect_error(d, "SELECT x FROM m.t WHERE u = -(5::ubigint);");
}

// A TRY_CAST that answers NULL for a non-NULL input stays a cast: folded it would be an all_unknown leaf in an OR.
TEST_CASE("integration::cpp::param_fold::try_cast_to_null_is_not_folded") {
    auto config = make_test_config(integration_fixture_path("param_fold/try_cast_null"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT, s SMALLINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x, s) VALUES (6, 1), (7, 2);"));
    auto cur = d->execute_sql_with_params(otterbrix::session_id_t(),
                                          "SELECT x FROM m.t WHERE s = TRY_CAST($1 AS SMALLINT) OR x = 6;",
                                          {{1, i64(d->resource(), 100000)}});
    INFO(describe(*cur));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell6 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell6.has_error());
    CHECK(cell6.value() == 6);
}

// ---------------------------------------------------------------------------------------------------------------
// PostgreSQL 18 nodeLimit.c recompute_limits: the count and the offset are validated when the Limit node starts
// to run, never earlier. OFFSET is checked first, then LIMIT. Only a statement that executes can fail.

namespace {
    constexpr const char* limit_text = "LIMIT must not be negative";
    constexpr const char* offset_text = "OFFSET must not be negative";

    std::string error_text(const components::cursor::cursor_t& cur) {
        return cur.is_error() ? std::string{cur.get_error().what} : std::string{};
    }

    template<typename D>
    void expect_error_text(D* d, const std::string& sql, const char* text) {
        auto cur = exec(d, sql);
        INFO(sql << " -> " << describe(*cur));
        REQUIRE(cur->is_error());
        CHECK(error_text(*cur) == text);
    }

    template<typename D>
    void expect_param_error_text(D* d, const std::string& sql, params_t params, const char* text) {
        auto cur = d->execute_sql_with_params(otterbrix::session_id_t(), sql, params);
        INFO(sql << " -> " << describe(*cur));
        REQUIRE(cur->is_error());
        CHECK(error_text(*cur) == text);
    }
} // namespace

TEST_CASE("integration::cpp::limit_bounds::the_error_texts_are_the_postgres_ones") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/texts"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error_text(d, "SELECT x FROM m.t LIMIT -1;", limit_text);
    expect_error_text(d, "SELECT x FROM m.t LIMIT -5;", limit_text);
    expect_error_text(d, "SELECT x FROM m.t OFFSET -1;", offset_text);
    expect_param_error_text(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), -1)}}, limit_text);
    expect_param_error_text(d, "SELECT x FROM m.t OFFSET $1;", {{1, i64(d->resource(), -3)}}, offset_text);
}

TEST_CASE("integration::cpp::limit_bounds::the_offset_is_checked_before_the_limit") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/offset_first"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error_text(d, "SELECT x FROM m.t LIMIT -1 OFFSET -1;", offset_text);
    expect_error_text(d, "SELECT x FROM m.t ORDER BY x LIMIT 5 OFFSET -1;", offset_text);
    expect_error_text(d, "SELECT x FROM m.t ORDER BY x LIMIT -1 OFFSET 1;", limit_text);
}

TEST_CASE("integration::cpp::limit_bounds::the_window_values_that_are_valid") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/valid_window"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_rows(d, "SELECT x FROM m.t LIMIT 0;", 0);
    expect_rows(d, "SELECT x FROM m.t LIMIT ALL;", 5);
    expect_rows(d, "SELECT x FROM m.t LIMIT NULL;", 5);
    expect_rows(d, "SELECT x FROM m.t OFFSET 0;", 5);
    expect_rows(d, "SELECT x FROM m.t LIMIT NULL OFFSET 2;", 3);
    expect_rows(d, "SELECT x FROM m.t LIMIT ALL OFFSET 2;", 3);
    expect_rows(d, "SELECT x FROM m.t LIMIT 3 OFFSET NULL;", 3);
    expect_rows(d, "SELECT x FROM m.t ORDER BY x LIMIT 0 OFFSET 2;", 0);
    expect_rows(d, "SELECT x FROM m.t ORDER BY x LIMIT NULL OFFSET 4;", 1);
    expect_rows(d, "SELECT x FROM m.t LIMIT 2 OFFSET 4;", 1);
    expect_rows(d, "SELECT x FROM m.t LIMIT 2 OFFSET 9;", 0);
}

TEST_CASE("integration::cpp::limit_bounds::a_parameter_is_validated_per_execution") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_per_execution"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_param_rows(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), 0)}}, 0);
    expect_param_rows(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), 2)}}, 2);
    expect_param_rows(d,
                      "SELECT x FROM m.t LIMIT $1 OFFSET $2;",
                      {{1, i64(d->resource(), 3)}, {2, i64(d->resource(), 4)}},
                      1);
    expect_param_error_text(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), -1)}}, limit_text);
    // a negative value does not poison the next execution of the same text
    expect_param_rows(d, "SELECT x FROM m.t LIMIT $1;", {{1, i64(d->resource(), 4)}}, 4);
}

TEST_CASE("integration::cpp::limit_bounds::a_parameter_of_the_wrong_type_is_refused_before_execution") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/param_wrong_type"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    const auto oops = [d] { return components::vector::make_scalar(d->resource(), std::string("oops")); };
    expect_param_error_text(d,
                            "SELECT x FROM m.t LIMIT $1;",
                            {{1, oops()}},
                            "argument of LIMIT must be type bigint, not type text");
    expect_param_error_text(d,
                            "SELECT x FROM m.t OFFSET $1;",
                            {{1, oops()}},
                            "argument of OFFSET must be type bigint, not type text");
    // a UINT64 past INT64_MAX is neither NULL (no limit) nor a count
    const auto too_big = [d] {
        return components::vector::make_scalar(d->resource(), std::numeric_limits<uint64_t>::max());
    };
    expect_param_error_text(d, "SELECT x FROM m.t LIMIT $1;", {{1, too_big()}}, "bigint out of range");
    expect_param_error_text(d, "SELECT x FROM m.t OFFSET $1;", {{1, too_big()}}, "bigint out of range");
}

TEST_CASE("integration::cpp::limit_bounds::explain_without_analyze_does_not_run_the_limit") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/explain"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_rows_ok(d, "EXPLAIN SELECT x FROM m.t LIMIT -1;");
    expect_rows_ok(d, "EXPLAIN SELECT x FROM m.t OFFSET -1;");
    expect_rows_ok(d, "EXPLAIN SELECT x FROM m.t ORDER BY x LIMIT -1;");
    expect_error_text(d, "EXPLAIN ANALYZE SELECT x FROM m.t LIMIT -1;", limit_text);
    expect_error_text(d, "EXPLAIN ANALYZE SELECT x FROM m.t OFFSET -1;", offset_text);

    auto plan_only = d->execute_sql_with_params(otterbrix::session_id_t(),
                                                "EXPLAIN SELECT x FROM m.t LIMIT $1;",
                                                {{1, i64(d->resource(), -1)}});
    INFO(describe(*plan_only));
    CHECK(plan_only->is_success());
}

TEST_CASE("integration::cpp::limit_bounds::a_view_is_validated_when_it_is_used") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/view"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    REQUIRE(ok(d, "CREATE VIEW m.v AS SELECT x FROM m.t LIMIT -1;"));
    expect_error_text(d, "SELECT x FROM m.v;", limit_text);
    REQUIRE(ok(d, "CREATE VIEW m.w AS SELECT x FROM m.t OFFSET -1;"));
    expect_error_text(d, "SELECT x FROM m.w;", offset_text);
}

TEST_CASE("integration::cpp::limit_bounds::a_sub_query_is_validated_when_it_runs") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/sub_query"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error_text(d, "SELECT * FROM (SELECT x FROM m.t LIMIT -1) s;", limit_text);
    expect_error_text(d, "WITH c AS (SELECT x FROM m.t LIMIT -1) SELECT * FROM c;", limit_text);
    expect_error_text(d, "SELECT x FROM m.t UNION ALL SELECT x FROM (SELECT x FROM m.t LIMIT -1) s;", limit_text);
    expect_rows(d, "SELECT * FROM (SELECT x FROM m.t LIMIT NULL) s;", 5);
    expect_rows(d, "SELECT * FROM (SELECT x FROM m.t LIMIT ALL OFFSET 1) s;", 4);
    expect_param_rows(d, "SELECT * FROM (SELECT x FROM m.t LIMIT $1) s;", {{1, null_value(d->resource())}}, 5);
}

TEST_CASE("integration::cpp::limit_bounds::a_lateral_over_an_empty_outer_never_starts_its_limit") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/lateral_empty_outer"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    REQUIRE(ok(d, "CREATE TABLE m.e (id BIGINT);"));
    expect_rows(d, "SELECT e.id, s.x FROM m.e e, LATERAL (SELECT x FROM m.t WHERE x = e.id LIMIT -1) s;", 0);
    REQUIRE(ok(d, "INSERT INTO m.e (id) VALUES (1);"));
    expect_error_text(d,
                      "SELECT e.id, s.x FROM m.e e, LATERAL (SELECT x FROM m.t WHERE x = e.id LIMIT -1) s;",
                      limit_text);
}

TEST_CASE("integration::cpp::limit_bounds::an_empty_table_still_starts_the_limit") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/empty_table"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
    expect_error_text(d, "SELECT x FROM m.t LIMIT -1;", limit_text);
    expect_error_text(d, "SELECT x FROM m.t OFFSET -1;", offset_text);
    expect_rows(d, "SELECT x FROM m.t LIMIT 3;", 0);
}

TEST_CASE("integration::cpp::limit_bounds::the_limit_is_checked_before_the_rows_are_computed") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/before_rows"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x) VALUES (0), (1);"));
    // the division by zero is never reached: the Limit node refuses first
    expect_error_text(d, "SELECT 1 / x FROM m.t LIMIT -1;", limit_text);
}

TEST_CASE("integration::cpp::limit_bounds::dml_limit_is_validated_before_anything_changes") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/dml"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_rows(d, 5);
    expect_error_text(d, "DELETE FROM m.t WHERE x > 0 LIMIT -1;", limit_text);
    expect_error_text(d, "UPDATE m.t SET x = x + 10 WHERE x > 0 LIMIT -1;", limit_text);
    expect_rows(d, "SELECT x FROM m.t;", 5);
    expect_rows(d, "SELECT x FROM m.t WHERE x > 5;", 0);
    expect_rows(d, "DELETE FROM m.t WHERE x > 0 LIMIT 2 RETURNING x;", 2);
}

// PostgreSQL starts the topmost Limit before it asks anything below for a row (ExecLimit LIMIT_INITIAL), so the
// LIMIT error wins over an error a deeper piece of the plan would raise while computing its rows.
TEST_CASE("integration::cpp::limit_bounds::the_limit_is_refused_before_a_deeper_piece_runs") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/top_down"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x) VALUES (0), (1);"));
    // control: a single piece
    expect_error_text(d, "SELECT 1 / x FROM m.t LIMIT -1;", limit_text);
    expect_error_text(d, "SELECT * FROM (SELECT 1 / x AS y FROM m.t) s JOIN m.t u ON s.y = u.x LIMIT -1;", limit_text);
    expect_error_text(d, "SELECT * FROM (SELECT 1 / x AS y FROM m.t) s, m.t u LIMIT -1;", limit_text);
    expect_error_text(d, "SELECT 1 / x FROM m.t UNION ALL SELECT x FROM m.t LIMIT -1;", limit_text);
}

// Only the Limit nodes of the piece that is about to run and of its ancestors are started first. A piece beside it
// is not: the right piece of a set operation runs first here, whereas PostgreSQL's Append goes left to right and
// would report the LIMIT of the left arm in the first query. The divergence is accepted.
TEST_CASE("integration::cpp::limit_bounds::a_limit_in_a_sibling_piece_is_not_started_early") {
    auto config = make_test_config(integration_fixture_path("limit_bounds/sibling_piece"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE SCHEMA m;"));
    REQUIRE(ok(d, "CREATE TABLE m.t (x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO m.t (x) VALUES (0), (1);"));
    // the right arm runs first and divides by zero before the left arm's Limit is reached
    expect_error_text(d,
                      "SELECT x FROM (SELECT x FROM m.t LIMIT -1) s UNION ALL SELECT 1 / x FROM m.t;",
                      "division by zero");
    // the right arm is the limited one and runs first
    expect_error_text(d, "SELECT 1 / x FROM m.t UNION ALL SELECT x FROM (SELECT x FROM m.t LIMIT -1) s;", limit_text);
}
