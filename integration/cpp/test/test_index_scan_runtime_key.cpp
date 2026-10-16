// An index scan reads its key when it opens, not when the plan is built: the key may be a sub-query result or a
// LATERAL outer cell, neither of which exists at plan time.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <services/index/manager_index.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <set>
#include <string>
#include <string_view>
#include <utility>

namespace {

    using components::cursor::cursor_t_ptr;

    std::string plan_text(const cursor_t_ptr& cur) {
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
    bool seed(D* d, const std::string& index_clause = "") {
        auto run = [&](const std::string& sql) { return d->execute_sql(otterbrix::session_id_t(), sql)->is_success(); };
        return run("CREATE DATABASE s;") && run("CREATE TABLE s.o (id BIGINT);") &&
               run("INSERT INTO s.o (id) VALUES (NULL), (1), (2);") && run("CREATE TABLE s.n (k BIGINT, v BIGINT);") &&
               run("INSERT INTO s.n (k, v) VALUES (NULL, 300), (1, 100), (2, 200);") &&
               run("CREATE INDEX ik ON s.n " + index_clause + " (k);");
    }

    template<typename D>
    void require_index_scan(D* d, const std::string& sql) {
        for (const auto* prefix : {"EXPLAIN ", "EXPLAIN ANALYZE "}) {
            auto plan = d->execute_sql(otterbrix::session_id_t(), prefix + sql);
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO(prefix << sql << "\n" << text);
            REQUIRE(text.find("Index Scan on n") != std::string::npos);
        }
    }

    std::set<std::pair<int64_t, int64_t>> pairs_of(const cursor_t_ptr& cur) {
        std::set<std::pair<int64_t, int64_t>> rows;
        for (uint64_t r = 0; r < cur->size(); ++r) {
            std::pair<int64_t, int64_t> row{-1, -1};
            auto null0 = cur->is_null(0, r);
            REQUIRE_FALSE(null0.has_error());
            if (!null0.value()) {
                auto cell0 = cur->get<int64_t>(0, r);
                REQUIRE_FALSE(cell0.has_error());
                row.first = cell0.value();
            }
            auto null1 = cur->is_null(1, r);
            REQUIRE_FALSE(null1.has_error());
            if (!null1.value()) {
                auto cell1 = cur->get<int64_t>(1, r);
                REQUIRE_FALSE(cell1.has_error());
                row.second = cell1.value();
            }
            rows.insert(row);
        }
        return rows;
    }

} // namespace

TEST_CASE("integration::cpp::index_scan_runtime_key::a_scalar_sub_query_key_uses_the_index") {
    auto config = test_create_config(integration_fixture_path("index_scan_runtime_key/sub_query_key"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(seed(d));

    const std::string sql = "SELECT v FROM s.n WHERE k = (SELECT max(id) FROM s.o);";
    require_index_scan(d, sql);
    auto cur = d->execute_sql(otterbrix::session_id_t(), sql);
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell2 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell2.has_error());
    CHECK(cell2.value() == 200);
}

TEST_CASE("integration::cpp::index_scan_runtime_key::a_null_or_empty_key_selects_nothing") {
    auto config = test_create_config(integration_fixture_path("index_scan_runtime_key/null_key"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(seed(d));

    const std::string null_key = "(SELECT id FROM s.o WHERE id IS NULL)";
    const std::string empty_key = "(SELECT id FROM s.o WHERE id > 100)";
    for (const auto& predicate : {"k = " + null_key,
                                  "k = " + empty_key,
                                  "k < " + null_key,
                                  "k <= " + null_key,
                                  "k > " + null_key,
                                  "k >= " + empty_key}) {
        const std::string sql = "SELECT v FROM s.n WHERE " + predicate + ";";
        require_index_scan(d, sql);
        auto cur = d->execute_sql(otterbrix::session_id_t(), sql);
        INFO(sql << " -> " << (cur->is_error() ? cur->get_error().what : "ok"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::index_scan_runtime_key::a_parameter_bound_to_null_selects_nothing") {
    auto config = test_create_config(integration_fixture_path("index_scan_runtime_key/null_parameter"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(seed(d));

    auto cur = d->execute_sql_with_params(otterbrix::session_id_t(),
                                          "SELECT v FROM s.n WHERE k = $1;",
                                          {{1,
                                            components::vector::make_null(d->resource(),
                                                                          components::types::complex_logical_type{
                                                                              components::types::logical_type::NA})}});
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);
}

TEST_CASE("integration::cpp::index_scan_runtime_key::a_lateral_probe_per_outer_row") {
    auto config = test_create_config(integration_fixture_path("index_scan_runtime_key/lateral_probe"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(seed(d));

    const std::string inner = "LATERAL (SELECT v FROM s.n WHERE k = o.id) l";
    auto plan = d->execute_sql(otterbrix::session_id_t(), "EXPLAIN SELECT o.id, l.v FROM s.o o, " + inner + ";");
    REQUIRE(plan->is_success());
    const auto text = plan_text(plan);
    INFO(text);
    CHECK(text.find("Index Scan on n") != std::string::npos);

    services::index::reset_index_agent_reads();
    auto cur = d->execute_sql(otterbrix::session_id_t(), "SELECT o.id, l.v FROM s.o o, " + inner + ";");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(pairs_of(cur) == std::set<std::pair<int64_t, int64_t>>{{1, 100}, {2, 200}});
    INFO("outer rows 1 and 2 probe the index; the NULL row probes nothing");
    CHECK(services::index::index_agent_reads() == 2);
}

TEST_CASE("integration::cpp::index_scan_runtime_key::an_integer_outer_column_probes_a_bigint_hash_index") {
    auto config = test_create_config(integration_fixture_path("index_scan_runtime_key/integer_outer"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(seed(d, "USING hash"));
    REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CREATE TABLE s.oi (id INTEGER);")->is_success());
    REQUIRE(d->execute_sql(otterbrix::session_id_t(), "INSERT INTO s.oi (id) VALUES (1), (2), (3);")->is_success());

    auto cur = d->execute_sql(otterbrix::session_id_t(),
                              "SELECT oi.id, l.v FROM s.oi oi, LATERAL (SELECT v FROM s.n WHERE k = oi.id) l;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(pairs_of(cur) == std::set<std::pair<int64_t, int64_t>>{{1, 100}, {2, 200}});
}

TEST_CASE("integration::cpp::index_scan_runtime_key::an_integer_key_probes_a_bigint_btree_index") {
    auto config = test_create_config(integration_fixture_path("index_scan_runtime_key/integer_key_btree"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(seed(d, "USING btree"));
    REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CREATE TABLE s.oi (id INTEGER);")->is_success());
    REQUIRE(d->execute_sql(otterbrix::session_id_t(), "INSERT INTO s.oi (id) VALUES (1), (2), (3);")->is_success());

    using expected_t = std::set<std::pair<int64_t, int64_t>>;
    const std::pair<std::string, expected_t> probes[] = {
        {"SELECT oi.id, l.v FROM s.oi oi, LATERAL (SELECT v FROM s.n WHERE k = oi.id) l;", {{1, 100}, {2, 200}}},
        {"SELECT v, k FROM s.n WHERE k = (SELECT max(id) FROM s.oi WHERE id < 3);", {{200, 2}}},
        {"SELECT v, k FROM s.n WHERE k >= (SELECT min(id) FROM s.oi WHERE id > 1);", {{200, 2}}},
    };
    for (const auto& [sql, expected] : probes) {
        auto plan = d->execute_sql(otterbrix::session_id_t(), "EXPLAIN " + sql);
        REQUIRE(plan->is_success());
        INFO(sql << "\n" << plan_text(plan));
        CHECK(plan_text(plan).find("Index Scan") == std::string::npos);

        auto cur = d->execute_sql(otterbrix::session_id_t(), sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(pairs_of(cur) == expected);
    }
}
