// e2e of a backend's tables as sources and targets: the backend actor of connection ext answers otterbrix's scans
// and writes of the tables of its database remote.

#include "host_fixture.hpp"
#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <atomic>
#include <core/tests/wait_ready.hpp>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using namespace components;
using host_fixture::strings;
using rows_t = host_actor::rows_t;

namespace {

    // The engine with the database remote of connection ext.
    struct ext_host_t {
        explicit ext_host_t(const std::string& leaf)
            : host(leaf, nullptr, {"ext"}) {
            host.register_database("ext", "remote");
        }

        host_actor::backend_t& backend() { return host.backend("ext"); }

        cursor::cursor_t_ptr execute_sql(const std::string& sql) { return host.db.run(otterbrix::session_id_t(), sql); }

        // ext.remote.public.<name> (BIGINT columns) holding `rows`.
        void table(const std::string& name, const strings& columns, const rows_t& rows) {
            std::string sql = "CREATE TABLE ext.remote.public." + name + " (";
            for (std::size_t i = 0; i < columns.size(); ++i) {
                sql += (i == 0 ? "" : ", ") + columns[i] + " BIGINT";
            }
            host.db.ok(otterbrix::session_id_t(), sql + ");");
            backend().set_rows("ext.remote.public." + name, rows);
            backend().clear_journal();
        }

        host_fixture::host_t host;
    };

    // A hung executor must fail the run, not wedge it: the dispatcher wait has no deadline of its own.
    cursor::cursor_t_ptr execute_within_deadline(ext_host_t& space, const std::string& sql) {
        std::atomic<bool> done{false};
        cursor::cursor_t_ptr cursor;
        std::thread worker([&] {
            cursor = space.execute_sql(sql);
            done.store(true, std::memory_order_release);
        });
        if (!test_helpers::wait_until([&] { return done.load(std::memory_order_acquire); })) {
            std::fprintf(stderr,
                         "extension_source: query exceeded %llds, executor hung\n",
                         static_cast<long long>(test_helpers::reply_deadline.count()));
            std::abort();
        }
        worker.join();
        return cursor;
    }

    std::string explain(ext_host_t& space, const std::string& sql) {
        auto cursor = space.execute_sql("EXPLAIN " + sql);
        REQUIRE(cursor->is_success());
        std::string out;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                out += std::string(chunk.data[0].get_value<std::string_view>(row));
                out += '\n';
            }
        }
        return out;
    }

    rows_t sorted_rows(const cursor::cursor_t_ptr& cursor) {
        rows_t rows;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                std::vector<int64_t> values;
                for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
                    values.push_back(chunk.data[col].get_value<int64_t>(row));
                }
                rows.push_back(std::move(values));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    // `count` tables <prefix>0 .. (key, v<i>) holding (1, 10 + i), (2, 20 + i).
    void pair_tables(ext_host_t& space, const std::string& prefix, int count) {
        for (int i = 0; i < count; ++i) {
            const auto n = static_cast<int64_t>(i);
            space.table(prefix + std::to_string(i), {"key", "v" + std::to_string(i)}, {{1, 10 + n}, {2, 20 + n}});
        }
    }

    std::string n_way_join(const std::string& prefix, int count) {
        std::string select = "SELECT s0.v0";
        std::string from = " FROM ext.remote.public." + prefix + "0 AS s0";
        for (int i = 1; i < count; ++i) {
            const auto n = std::to_string(i);
            select += ", s" + n + ".v" + n;
            from += " JOIN ext.remote.public." + prefix + n + " AS s" + n + " ON s0.key = s" + n + ".key";
        }
        return select + from + ";";
    }

} // namespace

// Every answer of the backend is awaited: the backend holds it until otterbrix waits for it, and none is dropped.
TEST_CASE("integration::cpp::extension_source::single_leaf") {
    ext_host_t space("test_ext_sync/base");
    space.table("t1", {"key", "val"}, {{7, 70}});
    space.backend().set_answer_when_awaited(true);
    auto cursor = execute_within_deadline(space, "SELECT * FROM ext.remote.public.t1;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    CHECK(space.backend().answers_dropped_unawaited() == 0);
}

TEST_CASE("integration::cpp::extension_source::several_pages") {
    ext_host_t space("test_ext_async/base");
    space.table("t1", {"key", "val"}, {{1, 10}, {2, 20}, {3, 30}});
    space.backend().set_batches("ext.remote.public.t1", {1, 1, 1});
    space.backend().set_answer_when_awaited(true);
    auto cursor = execute_within_deadline(space, "SELECT * FROM ext.remote.public.t1;");
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{1, 10}, {2, 20}, {3, 30}});
    CHECK(space.backend().answers_dropped_unawaited() == 0);
}

// A statement that fails on its second page leaves the scan before its end: the close of the cursor is awaited as
// every page was.
TEST_CASE("integration::cpp::extension_source::a_scan_left_early_awaits_its_close") {
    ext_host_t space("test_ext_close/base");
    space.table("t1", {"key", "val"}, {{1, 10}, {2, 20}, {3, 30}});
    space.backend().set_batches("ext.remote.public.t1", {1, 1, 1});
    space.backend().set_answer_when_awaited(true);
    auto cursor = execute_within_deadline(space, "SELECT 1 / (val - 20) AS q FROM ext.remote.public.t1 WHERE key > 0;");
    REQUIRE(cursor->is_error());
    CHECK(space.backend().cursor_closes() == 1);
    CHECK(space.backend().answers_dropped_unawaited() == 0);
}

TEST_CASE("integration::cpp::extension_source::empty_result") {
    ext_host_t space("test_ext_empty/base");
    space.table("t1", {"key", "val"}, {});
    auto cursor = execute_within_deadline(space, "SELECT * FROM ext.remote.public.t1;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 0);
}

TEST_CASE("integration::cpp::extension_source::join_two_storages") {
    ext_host_t space("test_ext_join2/base");
    space.table("t1", {"key", "name"}, {{1, 11}, {2, 22}, {3, 33}});
    space.table("t2", {"key", "value"}, {{1, 100}, {3, 300}, {9, 900}});
    auto cursor = execute_within_deadline(
        space,
        "SELECT l.name, r.value FROM ext.remote.public.t1 AS l JOIN ext.remote.public.t2 AS r ON l.key = r.key;");
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{11, 100}, {33, 300}});
}

TEST_CASE("integration::cpp::extension_source::group_by") {
    ext_host_t space("test_ext_group/base");
    space.table("t1", {"grp", "val"}, {{1, 10}, {1, 15}, {2, 20}, {2, 5}, {3, 1}});
    auto cursor = execute_within_deadline(space, "SELECT grp, SUM(val) AS s FROM ext.remote.public.t1 GROUP BY grp;");
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{1, 25}, {2, 25}, {3, 1}});
}

TEST_CASE("integration::cpp::extension_source::barrier_where_above_join") {
    ext_host_t space("test_ext_barrier/base");
    space.table("t1", {"key", "name"}, {{1, 11}, {2, 22}, {3, 33}});
    space.table("t2", {"key", "value"}, {{1, 100}, {2, 200}, {3, 300}});
    auto cursor = execute_within_deadline(
        space,
        "SELECT l.name, r.value FROM ext.remote.public.t1 AS l JOIN ext.remote.public.t2 AS r ON l.key = r.key "
        "WHERE r.value > 150;");
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{22, 200}, {33, 300}});
    // One scan per table: the filter stays above the backend's scan.
    CHECK(space.backend().journal("scan").size() == 2);
}

TEST_CASE("integration::cpp::extension_source::join_with_local_table") {
    ext_host_t space("test_ext_local/base");
    REQUIRE(space.execute_sql("CREATE SCHEMA extdb;")->is_success());
    REQUIRE(space.execute_sql("CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);")->is_success());
    REQUIRE(space.execute_sql("INSERT INTO extdb.local_t (key, amount) VALUES (1, 1000), (2, 2000), (5, 5000);")
                ->is_success());
    space.table("t1", {"key", "name"}, {{1, 11}, {2, 22}, {3, 33}});
    auto cursor = execute_within_deadline(
        space,
        "SELECT e.name, t.amount FROM ext.remote.public.t1 AS e JOIN extdb.local_t AS t ON e.key = t.key;");
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{11, 1000}, {22, 2000}});
}

// A page that does not have the table's columns breaks the contract: the statement fails, not the engine.
TEST_CASE("integration::cpp::extension_source::a_page_of_other_columns_errors_not_crash") {
    ext_host_t space("test_ext_norule/base");
    REQUIRE(space.execute_sql("CREATE SCHEMA extdb;")->is_success());
    REQUIRE(space.execute_sql("CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);")->is_success());
    space.table("t1", {"key", "name"}, {{1, 11}});
    space.backend().set_short_pages("ext.remote.public.t1");
    for (const std::string& sql :
         {"SELECT e.name, t.amount FROM ext.remote.public.t1 AS e JOIN extdb.local_t AS t ON e.key = t.key;",
          "SELECT key, count(name) FROM ext.remote.public.t1 GROUP BY key;"}) {
        INFO(sql);
        auto cursor = execute_within_deadline(space, sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::protocol_violation);
        CHECK(std::string{cursor->get_error().what} ==
              "the storage of table \"ext.remote.public.t1\" answered a page of other columns");
    }
    REQUIRE(space.execute_sql("SELECT 1;")->is_success());
}

TEST_CASE("integration::cpp::extension_source::explain_shows_backend") {
    ext_host_t space("test_ext_explain/base");
    space.table("t1", {"key", "name"}, {{1, 11}});
    space.table("t2", {"key", "value"}, {{1, 100}});
    auto text = explain(
        space,
        "SELECT l.name, r.value FROM ext.remote.public.t1 AS l JOIN ext.remote.public.t2 AS r ON l.key = r.key;");
    INFO(text);
    CHECK(text.find("Foreign Scan on ext.remote.public.t1") != std::string::npos);
    CHECK(text.find("Foreign Scan on ext.remote.public.t2") != std::string::npos);
    CHECK(space.backend().journal().empty());
}

TEST_CASE("integration::cpp::extension_source::sink_writes_backend") {
    ext_host_t space("test_ext_sink/base");
    REQUIRE(space.execute_sql("CREATE SCHEMA sdb;")->is_success());
    REQUIRE(space.execute_sql("CREATE TABLE sdb.local_src (key BIGINT, val BIGINT);")->is_success());
    REQUIRE(space.execute_sql("INSERT INTO sdb.local_src (key, val) VALUES (1,10),(2,20),(3,30);")->is_success());
    space.table("sink_target", {"key", "val"}, {});
    auto cursor =
        execute_within_deadline(space, "INSERT INTO ext.remote.public.sink_target SELECT key, val FROM sdb.local_src;");
    REQUIRE(cursor->is_success());
    auto written = space.backend().rows("ext.remote.public.sink_target");
    std::sort(written.begin(), written.end());
    REQUIRE(written == rows_t{{1, 10}, {2, 20}, {3, 30}});
}

// The executor opens every scan of the plan before it pumps any: the backend answers the opens only once all of them
// arrived. Opened one by one, the first open would never be answered.
TEST_CASE("integration::cpp::extension_source::sources_open_in_parallel") {
    ext_host_t space("test_ext_parallel_open/base");
    constexpr int kSources = 4;
    pair_tables(space, "s", kSources);
    space.backend().answer_opens_together(kSources);
    auto cursor = execute_within_deadline(space, n_way_join("s", kSources));
    INFO(session_sql::error_of(cursor));
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{10, 11, 12, 13}, {20, 21, 22, 23}});
    CHECK(space.backend().opens() == kSources);
    CHECK(space.backend().peak_waiting_opens() == kSources);
}

// Whichever scan fails to open, the query fails with its error and no other scan is left open at the backend.
TEST_CASE("integration::cpp::extension_source::failed_open_awaits_the_rest") {
    ext_host_t space("test_ext_failed_open/base");
    constexpr int kSources = 4;
    for (int failing = 0; failing < kSources; ++failing) {
        const std::string prefix = "f" + std::to_string(failing) + "_";
        pair_tables(space, prefix, kSources);
        space.backend().set_failing_open("ext.remote.public." + prefix + std::to_string(failing));
        space.backend().answer_opens_together(kSources);
        auto cursor = execute_within_deadline(space, n_way_join(prefix, kSources));
        INFO("failing source " << failing);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what} == "backend unavailable");
        CHECK(space.backend().open_cursors() == 0);
    }
}

// The backend answers the opens in the reverse order of their arrival: each scan reads its own table.
TEST_CASE("integration::cpp::extension_source::uneven_open_latencies") {
    ext_host_t space("test_ext_uneven_open/base");
    pair_tables(space, "u", 3);
    space.backend().answer_opens_together(3);
    auto cursor = execute_within_deadline(space, n_way_join("u", 3));
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{10, 11, 12}, {20, 21, 22}});
    CHECK(space.backend().opens() == 3);
}

// A LATERAL inner is a private sub-plan the up-front open never reaches: it opens its scan on every re-drive.
TEST_CASE("integration::cpp::extension_source::lateral_inner_source_opened_per_drive") {
    ext_host_t space("test_ext_lateral_open/base");
    REQUIRE(space.execute_sql("CREATE SCHEMA extdb;")->is_success());
    REQUIRE(space.execute_sql("CREATE TABLE extdb.outer_t (id BIGINT);")->is_success());
    REQUIRE(space.execute_sql("INSERT INTO extdb.outer_t (id) VALUES (1), (2), (3);")->is_success());
    pair_tables(space, "lat", 1);
    auto cursor =
        execute_within_deadline(space,
                                "SELECT o.id, s.v0 FROM extdb.outer_t AS o, "
                                "LATERAL (SELECT e.v0 FROM ext.remote.public.lat0 AS e WHERE e.key = o.id) s;");
    INFO(session_sql::error_of(cursor));
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == rows_t{{1, 10}, {2, 20}});
    CHECK(space.backend().opens() == 3);
    CHECK(space.backend().open_cursors() == 0);
}

// The engine takes at most DEFAULT_VECTOR_CAPACITY rows per source batch; slicing a wider backend page is the
// backend's job, so a wider page is refused whatever the query does with it. In Debug a page wider than 1024 rows
// cannot be built (data_chunk_t asserts its cardinality), so only release reaches the refusal.
#ifdef NDEBUG
TEST_CASE("integration::cpp::extension_source::chunk_over_vector_capacity") {
    ext_host_t space("test_ext_wide_chunk/base");
    constexpr int64_t n = 2500;
    rows_t rows;
    rows.reserve(n);
    for (int64_t i = 0; i < n; ++i) {
        rows.push_back({i, i % 3});
    }
    space.table("wide", {"key", "grp"}, rows);
    auto expect_refused = [&](const std::string& query) {
        auto cursor = execute_within_deadline(space, query);
        INFO(query);
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_success());
        CHECK(cursor->get_error().type == core::error_code_t::protocol_violation);
        CHECK(std::string{cursor->get_error().what}.find(std::to_string(vector::DEFAULT_VECTOR_CAPACITY)) !=
              std::string::npos);
    };

    SECTION("scan") { expect_refused("SELECT * FROM ext.remote.public.wide;"); }
    SECTION("filter") { expect_refused("SELECT key FROM ext.remote.public.wide WHERE key >= 2000;"); }
    SECTION("projection") { expect_refused("SELECT key + 1 AS k FROM ext.remote.public.wide;"); }
    SECTION("group_by") { expect_refused("SELECT grp, COUNT(*) AS c FROM ext.remote.public.wide GROUP BY grp;"); }
    SECTION("scalar_aggregate") { expect_refused("SELECT SUM(key) AS s FROM ext.remote.public.wide;"); }
    auto seed_local = [&] {
        REQUIRE(space.execute_sql("CREATE SCHEMA widedb;")->is_success());
        REQUIRE(space.execute_sql("CREATE TABLE widedb.t (key BIGINT, amount BIGINT);")->is_success());
        REQUIRE(space.execute_sql("INSERT INTO widedb.t (key, amount) VALUES (1, 10), (1500, 20), (2499, 30);")
                    ->is_success());
    };
    SECTION("join_wide_on_the_left") {
        seed_local();
        expect_refused("SELECT w.grp, t.amount FROM ext.remote.public.wide AS w JOIN widedb.t AS t ON w.key = t.key;");
    }
    SECTION("join_wide_on_the_right") {
        seed_local();
        expect_refused("SELECT w.grp, t.amount FROM widedb.t AS t JOIN ext.remote.public.wide AS w ON t.key = w.key;");
    }
}
#endif

// count(*) without WHERE over a backend's table counts the backend's rows: no pushdown turns the aggregate into a
// disk reduce of a table that has no disk storage.
TEST_CASE("integration::cpp::extension_source::count_star_over_a_storage_table") {
    ext_host_t space("test_ext_count_named/base");
    space.table("c", {"key", "val"}, {{1, 10}, {2, 20}, {3, 30}});
    auto cursor = execute_within_deadline(space, "SELECT count(*) AS c FROM ext.remote.public.c;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    auto cell0 = cursor->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    REQUIRE(cell0.value() == 3);
    CHECK(space.backend().journal("scan") == strings{"scan ext.remote.public.c"});
}

// A backend that takes count(*) of its table answers it as one block: no scan reaches it.
TEST_CASE("integration::cpp::extension_source::count_star_taken_by_the_backend") {
    ext_host_t space("test_ext_count_pass/base");
    space.table("p", {"key", "val"}, {{1, 10}, {2, 20}, {3, 30}});
    space.backend().set_take_policy({true, false});
    auto cursor = execute_within_deadline(space, "SELECT count(*) AS c FROM ext.remote.public.p;");
    INFO(session_sql::error_of(cursor));
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    auto cell1 = cursor->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell1.has_error());
    REQUIRE(cell1.value() == 3);
    CHECK(space.backend().offered() == strings{"$aggregate: {$group: {c}}"});
    CHECK(space.backend().journal("scan").empty());
    CHECK(space.backend().journal("run") == strings{"run count ext.remote.public.p"});
}
