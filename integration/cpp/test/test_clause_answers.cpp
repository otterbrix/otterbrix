#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <string>
#include <utility>
#include <vector>

// GROUP BY, HAVING, ORDER BY, LIMIT and JOIN answered over named tables, with and without an index and with the
// aggregate pushed to the owning agent: what the statement returns, whatever the clause nodes carry.

using namespace test_helpers;

namespace {

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE cl;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE cl.t (g bigint, v bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE cl.u (g bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO cl.t (g, v) VALUES (1, 10), (1, 20), (2, 30), (2, 50), (2, 40), (3, 5);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO cl.u (g, name) VALUES (1, 'a'), (2, 'b'), (4, 'd');")->is_success());
    }

    std::vector<std::pair<int64_t, int64_t>> pairs_of(const components::cursor::cursor_t_ptr& cur) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        std::vector<std::pair<int64_t, int64_t>> out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.emplace_back(cur->value(0, row).value<int64_t>(), cur->value(1, row).value<int64_t>());
        }
        return out;
    }

    std::vector<int64_t> column_of(const components::cursor::cursor_t_ptr& cur) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        std::vector<int64_t> out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(0, row).value<int64_t>());
        }
        return out;
    }

    std::string plan_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, "EXPLAIN " + sql);
        REQUIRE(cur->is_success());
        std::string text;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            text += std::string(cur->value(0, row).value<std::string_view>());
            text += '\n';
        }
        return text;
    }

    using pairs = std::vector<std::pair<int64_t, int64_t>>;
    using values = std::vector<int64_t>;

} // namespace

TEST_CASE("integration::cpp::clause_answers::group_having_order_limit_over_a_table") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/table")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    INFO("GROUP BY ... ORDER BY, the aggregate answered by the owning agent");
    services::disk::reset_pushdown_reply_rows();
    CHECK(pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t GROUP BY g ORDER BY g;")) ==
          pairs{{1, 30}, {2, 120}, {3, 5}});
    CHECK(services::disk::pushdown_reply_rows() > 0);

    INFO("HAVING keeps the groups it admits, ORDER BY DESC orders them");
    CHECK(
        pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t GROUP BY g HAVING SUM(v) > 20 ORDER BY g DESC;")) ==
        pairs{{2, 120}, {1, 30}});

    INFO("ORDER BY ... LIMIT ... OFFSET over the rows");
    CHECK(column_of(exec(dispatcher, "SELECT v FROM cl.t ORDER BY v DESC LIMIT 2 OFFSET 1;")) == values{40, 30});

    INFO("LIMIT ... OFFSET over the groups");
    CHECK(pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t GROUP BY g ORDER BY g LIMIT 1 OFFSET 1;")) ==
          pairs{{2, 120}});

    INFO("WHERE, GROUP BY, ORDER BY and LIMIT together");
    CHECK(pairs_of(exec(dispatcher,
                        "SELECT g, COUNT(*) AS c FROM cl.t WHERE v >= 20 GROUP BY g ORDER BY g DESC LIMIT 1;")) ==
          pairs{{2, 3}});
}

TEST_CASE("integration::cpp::clause_answers::an_index_answers_the_same") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/index")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE INDEX t_g ON cl.t (g);")->is_success());

    const std::string ordered = "SELECT v FROM cl.t WHERE g = 2 ORDER BY v LIMIT 2;";
    const auto plan = plan_of(dispatcher, ordered);
    INFO("plan:\n" << plan);
    CHECK(plan.find("Index Scan") != std::string::npos);
    CHECK(plan.find("Limit") != std::string::npos);

    services::index::reset_index_agent_reads();
    CHECK(column_of(exec(dispatcher, ordered)) == values{30, 40});
    CHECK(services::index::index_agent_reads() >= 1);

    CHECK(pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t WHERE g = 2 GROUP BY g HAVING COUNT(*) > 1;")) ==
          pairs{{2, 120}});
}

TEST_CASE("integration::cpp::clause_answers::a_join_groups_orders_and_limits") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/join")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    CHECK(pairs_of(exec(dispatcher,
                        "SELECT t.g, COUNT(*) AS c FROM cl.t JOIN cl.u ON t.g = u.g GROUP BY t.g ORDER BY t.g;")) ==
          pairs{{1, 2}, {2, 3}});
    CHECK(pairs_of(exec(dispatcher,
                        "SELECT t.g, SUM(t.v) AS s FROM cl.t JOIN cl.u ON t.g = u.g GROUP BY t.g HAVING SUM(t.v) > 50 "
                        "ORDER BY t.g DESC LIMIT 1;")) == pairs{{2, 120}});
}

TEST_CASE("integration::cpp::clause_answers::insert_from_a_grouped_select") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/insert_select")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE TABLE cl.s (g bigint, total bigint);")->is_success());

    auto inserted = exec(dispatcher, "INSERT INTO cl.s (g, total) SELECT g, SUM(v) FROM cl.t GROUP BY g ORDER BY g;");
    INFO("error: " << (inserted->is_error() ? inserted->get_error().what : "none"));
    REQUIRE(inserted->is_success());
    CHECK(pairs_of(exec(dispatcher, "SELECT g, total FROM cl.s ORDER BY g;")) == pairs{{1, 30}, {2, 120}, {3, 5}});
}
