// The disk is offered every block over its tables, as a host's backend is (docs/embedding-host-api.md, "Pushdown"):
// it takes the WHERE of a table's SELECT block, or its GROUP BY of the reduce form, and runs it while it reads.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/tests/wait_ready.hpp>
#include <services/disk/agent_disk.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <atomic>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace test_helpers;

namespace {

    std::string plan_of(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, "EXPLAIN " + sql);
        INFO("EXPLAIN " << sql << ": " << (cur->is_error() ? cur->get_error().what.c_str() : "<ok>"));
        REQUIRE(cur->is_success());
        std::string out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto line = cur->get<std::string_view>(0, row);
            REQUIRE_FALSE(line.has_error());
            out += std::string(line.value());
            out += '\n';
        }
        return out;
    }

    bool has(const std::string& text, const std::string& needle) { return text.find(needle) != std::string::npos; }

    int64_t cell(const components::cursor::cursor_t_ptr& cur, uint64_t column, uint64_t row) {
        auto value = cur->get<int64_t>(column, row);
        REQUIRE_FALSE(value.has_error());
        return value.value();
    }

    std::multiset<int64_t> column_of(const components::cursor::cursor_t_ptr& cur, uint64_t column) {
        std::multiset<int64_t> values;
        for (uint64_t row = 0; row < cur->size(); ++row) {
            auto cell = cur->get<int64_t>(column, row);
            REQUIRE_FALSE(cell.has_error());
            values.insert(cell.value());
        }
        return values;
    }

    // t: id 0..19, g = id % 4, v = id * 10.
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE SCHEMA dp;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE dp.t (id bigint, g bigint, v bigint);")->is_success());
        REQUIRE(seed_rows(d, "dp.t", "id, g, v", 20, [](unsigned i) {
                    std::stringstream s;
                    s << "(" << i << ", " << (i % 4) << ", " << (i * 10) << ")";
                    return s.str();
                })->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::disk_pushdown::a_where_over_a_disk_table_is_taken") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/where"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const std::string sql = "SELECT v FROM dp.t WHERE id >= 15;";
    const auto plan = plan_of(d, sql);
    INFO(plan);
    CHECK(has(plan, "Seq Scan on t"));
    CHECK_FALSE(has(plan, "Filter"));

    services::disk::reset_disk_block_rows();
    auto cur = exec(d, sql);
    REQUIRE(cur->is_success());
    CHECK(column_of(cur, 0) == std::multiset<int64_t>{150, 160, 170, 180, 190});
    CHECK(services::disk::disk_block_rows() == 5);
}

TEST_CASE("integration::cpp::disk_pushdown::a_group_by_over_a_disk_table_is_taken") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/group"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const std::string sql = "SELECT g, SUM(v) AS s FROM dp.t WHERE id < 12 GROUP BY g ORDER BY g;";
    const auto plan = plan_of(d, sql);
    INFO(plan);
    CHECK(has(plan, "Pushed Aggregate"));

    services::disk::reset_disk_block_rows();
    auto taken = exec(d, sql);
    REQUIRE(taken->is_success());
    CHECK(services::disk::disk_block_rows() == 4);
    // HAVING keeps the group in otterbrix: the same answer over the rows of the taken WHERE.
    auto kept = exec(d, "SELECT g, SUM(v) AS s FROM dp.t WHERE id < 12 GROUP BY g HAVING COUNT(*) > 0 ORDER BY g;");
    REQUIRE(kept->is_success());
    REQUIRE(taken->size() == 4);
    REQUIRE(kept->size() == 4);
    for (uint64_t row = 0; row < 4; ++row) {
        CHECK(cell(taken, 0, row) == cell(kept, 0, row));
        CHECK(cell(taken, 1, row) == cell(kept, 1, row));
    }
    // g = 0: ids 0, 4, 8 -> v 0 + 40 + 80.
    CHECK(cell(taken, 1, 0) == 120);
}

TEST_CASE("integration::cpp::disk_pushdown::a_join_of_disk_tables_takes_each_side") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/join"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE TABLE dp.u (k bigint, w bigint);")->is_success());
    REQUIRE(exec(d, "INSERT INTO dp.u (k, w) VALUES (1, 7), (2, 8), (3, 9), (17, 5);")->is_success());

    const std::string sql = "SELECT a.v, b.w FROM (SELECT id, v FROM dp.t WHERE id > 1) a "
                            "JOIN (SELECT k, w FROM dp.u WHERE w < 9) b ON a.id = b.k;";
    const auto plan = plan_of(d, sql);
    INFO(plan);
    CHECK(has(plan, "Hash Join"));
    CHECK(has(plan, "Seq Scan on t"));
    CHECK(has(plan, "Seq Scan on u"));
    CHECK_FALSE(has(plan, "Filter"));

    services::disk::reset_disk_block_rows();
    auto cur = exec(d, sql);
    REQUIRE(cur->is_success());
    // a: ids 2..19 (18 rows); b: k 1, 2, 17 (3 rows); joined: k 2 and 17.
    CHECK(column_of(cur, 0) == std::multiset<int64_t>{20, 170});
    CHECK(services::disk::disk_block_rows() == 18 + 3);
}

TEST_CASE("integration::cpp::disk_pushdown::a_per_side_where_of_a_join_is_taken") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/join_where"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE TABLE dp.u (k bigint, w bigint);")->is_success());
    REQUIRE(exec(d, "INSERT INTO dp.u (k, w) VALUES (1, 7), (2, 8), (3, 9), (17, 5);")->is_success());

    const std::string sql = "SELECT a.v, b.w FROM dp.t a JOIN dp.u b ON a.id = b.k WHERE a.id > 1 AND b.w < 9;";
    const auto plan = plan_of(d, sql);
    INFO(plan);
    CHECK(has(plan, "Hash Join"));
    CHECK(has(plan, "Seq Scan on t"));
    CHECK(has(plan, "Seq Scan on u"));
    CHECK_FALSE(has(plan, "Filter"));

    services::disk::reset_disk_block_rows();
    auto cur = exec(d, sql);
    REQUIRE(cur->is_success());
    // t: ids 2..19 (18 rows); u: w < 9 and the derived k > 1 leave k 2, 17 (2 rows); joined: k 2 and 17.
    CHECK(column_of(cur, 0) == std::multiset<int64_t>{20, 170});
    CHECK(services::disk::disk_block_rows() == 18 + 2);
}

TEST_CASE("integration::cpp::disk_pushdown::an_indexed_where_is_not_offered") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/index"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE INDEX t_id ON dp.t (id);")->is_success());

    const std::string sql = "SELECT v FROM dp.t WHERE id = 7;";
    const auto plan = plan_of(d, sql);
    INFO(plan);
    CHECK(has(plan, "Index Scan"));

    services::disk::reset_disk_block_rows();
    auto cur = exec(d, sql);
    REQUIRE(cur->is_success());
    CHECK(column_of(cur, 0) == std::multiset<int64_t>{70});
    CHECK(services::disk::disk_block_rows() == 0);
}

TEST_CASE("integration::cpp::disk_pushdown::an_indexed_where_of_a_join_side_is_an_index_scan") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/join_index"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE TABLE dp.u (k bigint, w bigint);")->is_success());
    REQUIRE(exec(d, "INSERT INTO dp.u (k, w) VALUES (1, 7), (2, 8), (3, 9), (17, 5);")->is_success());
    REQUIRE(exec(d, "CREATE INDEX t_id ON dp.t (id);")->is_success());

    const std::string sql = "SELECT a.v, b.w FROM dp.t a JOIN dp.u b ON a.id = b.k WHERE a.id = 17;";
    const auto plan = plan_of(d, sql);
    INFO(plan);
    CHECK(has(plan, "Index Scan"));

    services::disk::reset_disk_block_rows();
    auto cur = exec(d, sql);
    REQUIRE(cur->is_success());
    CHECK(column_of(cur, 0) == std::multiset<int64_t>{170});
    // The index answers t's side; only u's derived WHERE is taken.
    CHECK(services::disk::disk_block_rows() == 1);
}

TEST_CASE("integration::cpp::disk_pushdown::explain_takes_without_running") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/explain"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    services::disk::reset_disk_block_rows();
    const auto plan = plan_of(d, "SELECT g, COUNT(*) FROM dp.t WHERE v > 0 GROUP BY g;");
    INFO(plan);
    CHECK(has(plan, "Pushed Aggregate"));
    CHECK(services::disk::disk_block_rows() == 0);
}

// A LATERAL inner the disk took: decorrelated, the disk groups the inner by its key once; correlated beyond that, the
// take runs again on every drive with the outer row's values, and holds until the statement ends.
TEST_CASE("integration::cpp::disk_pushdown::a_lateral_inner_reruns_per_drive") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/lateral"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE SCHEMA s;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE s.outer_t (id BIGINT, n BIGINT);")->is_success());
    REQUIRE(exec(d, "INSERT INTO s.outer_t (id, n) VALUES (1, 10), (2, 20);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE s.inner_t (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(exec(d, "INSERT INTO s.inner_t (k, v) VALUES (1, 100), (1, 101), (2, 200), (3, 300);")->is_success());
    const auto sums = [](const components::cursor::cursor_t_ptr& cur) {
        std::multiset<std::pair<int64_t, int64_t>> got;
        for (uint64_t row = 0; row < cur->size(); ++row) {
            got.emplace(cell(cur, 0, row), cell(cur, 1, row));
        }
        return got;
    };

    INFO("by the key alone: one grouping of the inner, joined by hash");
    {
        const std::string sql = "SELECT o.id, sub.s FROM s.outer_t o, "
                                "LATERAL (SELECT sum(inner_t.v) AS s FROM s.inner_t WHERE inner_t.k = o.id) sub;";
        const auto plan = plan_of(d, sql);
        INFO(plan);
        CHECK(has(plan, "Hash Join"));
        CHECK(has(plan, "Seq Scan on inner_t"));
        CHECK(has(plan, "Pushed Aggregate: sum by k"));
        services::disk::reset_disk_block_rows();
        auto cur = exec(d, sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        // The groups k = 1, 2, 3.
        CHECK(services::disk::disk_block_rows() == 3);
        CHECK(sums(cur) == std::multiset<std::pair<int64_t, int64_t>>{{1, 201}, {2, 200}});
    }
    INFO("by the key and another outer column: the take runs once per drive");
    {
        const std::string sql = "SELECT o.id, sub.s FROM s.outer_t o, "
                                "LATERAL (SELECT sum(inner_t.v) AS s FROM s.inner_t "
                                "WHERE inner_t.k = o.id AND inner_t.v > o.n) sub;";
        const auto plan = plan_of(d, sql);
        INFO(plan);
        CHECK(has(plan, "Nested Loop"));
        CHECK(has(plan, "Seq Scan on inner_t"));
        CHECK(has(plan, "Pushed Aggregate: sum"));
        services::disk::reset_disk_block_rows();
        auto cur = exec(d, sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        // One sum per drive.
        CHECK(services::disk::disk_block_rows() == 2);
        CHECK(sums(cur) == std::multiset<std::pair<int64_t, int64_t>>{{1, 201}, {2, 200}});
        CHECK(test_helpers::wait_until([] { return services::disk::disk_held_takes() == 0; }));
    }
    INFO("a WHERE on the outer table is taken by the disk, the lateral inner is driven by the rows left");
    {
        const std::string sql = "SELECT o.id, sub.s FROM s.outer_t o, "
                                "LATERAL (SELECT sum(inner_t.v) AS s FROM s.inner_t WHERE inner_t.k = o.id) sub "
                                "WHERE o.n > 15;";
        auto cur = exec(d, sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
        REQUIRE(cur->is_success());
        CHECK(sums(cur) == std::multiset<std::pair<int64_t, int64_t>>{{2, 200}});
    }
}

namespace {

    struct pause_gate_t final : services::disk::scan_advance_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        bool hold(components::catalog::oid_t table_oid, uint64_t /*cursor_id*/) override {
            if (!armed.load(std::memory_order_acquire) ||
                static_cast<uint32_t>(table_oid) < static_cast<uint32_t>(components::catalog::FIRST_USER_OID)) {
                return false;
            }
            reached.store(true, std::memory_order_release);
            return !released.load(std::memory_order_acquire);
        }
    };

} // namespace

// The read of a taken WHERE pauses between its pages at the DEV gate, as a table's scan does.
TEST_CASE("integration::cpp::disk_pushdown::a_gate_between_pages_holds_a_taken_scan") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/gate"));
    test_spaces space(config);
    pause_gate_t gate;
    services::disk::dev_set_scan_advance_gate(&gate);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE SCHEMA dp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE dp.big (id bigint);")->is_success());
    for (unsigned done = 0; done < 3072; done += 512) {
        REQUIRE(seed_rows(d, "dp.big", "id", 512, [done](unsigned i) {
                    return "(" + std::to_string(done + i) + ")";
                })->is_success());
    }

    gate.armed.store(true, std::memory_order_release);
    components::cursor::cursor_t_ptr reader;
    std::thread reading(
        [&] { reader = d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM dp.big WHERE id >= 0;"); });
    const bool parked = test_helpers::wait_until([&] { return gate.reached.load(); });
    gate.released.store(true, std::memory_order_release);
    reading.join();
    services::disk::dev_set_scan_advance_gate(nullptr);

    REQUIRE(parked);
    REQUIRE(reader != nullptr);
    REQUIRE(reader->is_success());
    CHECK(reader->size() == 3072);
}

// A take lives one statement: when it ends (run, explained, or refused), the disk holds none of its takes.
TEST_CASE("integration::cpp::disk_pushdown::a_statement_releases_its_takes") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/release"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    const auto none_held = [] {
        return test_helpers::wait_until([] { return services::disk::disk_held_takes() == 0; });
    };

    REQUIRE(exec(d, "SELECT v FROM dp.t WHERE id >= 15;")->is_success());
    CHECK(none_held());
    plan_of(d, "SELECT g, SUM(v) FROM dp.t WHERE id < 12 GROUP BY g;");
    CHECK(none_held());
    REQUIRE(exec(d, "SELECT v / (id - id) FROM dp.t WHERE id >= 15;")->is_error());
    CHECK(none_held());
    // EXECUTE plans and offers its statement again on every run.
    const auto session = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(session, "PREPARE p(bigint) AS SELECT v FROM dp.t WHERE id >= $1;")->is_success());
    REQUIRE(d->execute_sql(session, "EXECUTE p(15);")->size() == 5);
    CHECK(none_held());
    REQUIRE(d->execute_sql(session, "EXECUTE p(18);")->size() == 2);
    CHECK(none_held());
}

namespace {

    // SELECT v FROM dp.t WHERE id = <branch % 20> for each branch in [first, last), as a balanced UNION ALL.
    std::string union_of(int first, int last) {
        if (last - first == 1) {
            return "SELECT v FROM dp.t WHERE id = " + std::to_string(first % 20);
        }
        const int middle = first + (last - first) / 2;
        return "(" + union_of(first, middle) + ") UNION ALL (" + union_of(middle, last) + ")";
    }

} // namespace

// A statement may take more blocks than any fixed number: each runs.
TEST_CASE("integration::cpp::disk_pushdown::a_statement_of_many_takes_runs_them_all") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/many"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    const std::string sql = union_of(0, 70);
    std::multiset<int64_t> expected;
    for (int branch = 0; branch < 70; ++branch) {
        expected.insert((branch % 20) * 10);
    }
    services::disk::reset_disk_block_rows();
    auto cur = exec(d, sql + ";");
    INFO((cur->is_error() ? cur->get_error().what.c_str() : "<ok>"));
    REQUIRE(cur->is_success());
    CHECK(column_of(cur, 0) == expected);
    CHECK(services::disk::disk_block_rows() == 70);
}

// A NULL parameter selects no row of a taken WHERE: col = NULL is never true.
TEST_CASE("integration::cpp::disk_pushdown::a_null_parameter_selects_no_row") {
    auto config = make_test_config(integration_fixture_path("test_disk_pushdown/null_parameter"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    const std::vector<std::pair<size_t, components::vector::vector_t>> null{
        {1,
         components::vector::make_null(d->resource(),
                                       components::types::complex_logical_type{components::types::logical_type::NA})}};
    auto counted =
        d->execute_sql_with_params(otterbrix::session_id_t(), "SELECT count(*) FROM dp.t WHERE id = $1;", null);
    INFO((counted->is_error() ? counted->get_error().what.c_str() : "<ok>"));
    REQUIRE(counted->is_success());
    REQUIRE(counted->size() == 1);
    CHECK(cell(counted, 0, 0) == 0);
}
