// operator_match_t is the filter operator the planner builds for a non-pure-compare WHERE
// predicate (e.g. LIKE); a pure compare is pushed into the scan instead, giving a
// match(streaming)->full_scan(source) chain that streams one batch at a time. Tests check
// streaming_pipeline_runs() bumps (stubbing role() back to none makes that RED).

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024) so match's push() must filter across batches.
    constexpr unsigned kRowCount = 5000;

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_match::like_filter_streams_and_lands") {
    auto config = test_create_config(integration_fixture_path("test_streaming_match_like"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchDb.t (id bigint, name text);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO MatchDb.t (id, name) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            const char* prefix = (i % 2 == 0) ? "match_" : "other_";
            q << "(" << i << ", '" << prefix << i << "')" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    const unsigned kExpectedMatches = kRowCount / 2;

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "SELECT id, name FROM MatchDb.t WHERE name LIKE 'match%';");
        INFO("LIKE filter error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kExpectedMatches);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM MatchDb.t WHERE name LIKE 'match%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kExpectedMatches));
    }
}

TEST_CASE("integration::cpp::streaming_match::like_filter_with_limit_caps_across_batches") {
    auto config = test_create_config(integration_fixture_path("test_streaming_match_limit"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchDb.t (id bigint, name text);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO MatchDb.t (id, name) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", 'row_" << i << "')" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // kLimit > DEFAULT_VECTOR_CAPACITY forces multiple batches; a per-batch reset would over-emit.
    constexpr unsigned kLimit = 2000;
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "SELECT id, name FROM MatchDb.t WHERE name LIKE 'row%' LIMIT " + std::to_string(kLimit) + ";");
        INFO("LIKE+LIMIT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kLimit);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);
}

// A match over a SINK (GROUP BY / JOIN) carries no real row_ids, unlike over a scan. Two
// defects bite only there: filter_batch_ forwarding zero-filled row_ids to a DML/index
// consumer (bogus id 0), and the cached predicate's arena dangling once the finalize chunk differs.

namespace {
    void setup_match_over_sink_db(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE SinkDb;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE SinkDb.emp (id bigint, dept_id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE SinkDb.dept (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher,
                     "INSERT INTO SinkDb.emp (id, dept_id, name) VALUES "
                     "(1,1,'alice'), (2,1,'amy'), (3,1,'bob'), "
                     "(4,2,'carol'), (5,2,'amanda'), "
                     "(6,3,'dave');")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO SinkDb.dept (id, name) VALUES (1,'eng'), (2,'sales'), (3,'hr');")
                    ->is_success());
    }
} // namespace

TEST_CASE("integration::cpp::streaming_match::having_count_filter_returns_correct_rows") {
    auto config = test_create_config(integration_fixture_path("test_streaming_match_having"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    auto cur = exec(dispatcher, "SELECT dept_id, COUNT(*) AS c FROM SinkDb.emp GROUP BY dept_id HAVING COUNT(*) > 1;");
    INFO("HAVING error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
}

TEST_CASE("integration::cpp::streaming_match::join_with_nonpushdown_filter_returns_correct_rows") {
    auto config = test_create_config(integration_fixture_path("test_streaming_match_join_filter"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    auto cur = exec(dispatcher,
                    "SELECT e.id, e.name FROM SinkDb.emp e "
                    "JOIN SinkDb.dept d ON e.dept_id = d.id "
                    "WHERE e.name LIKE 'a%';");
    INFO("JOIN+LIKE error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
}

TEST_CASE("integration::cpp::streaming_match::delete_where_in_group_subquery_lands") {
    // DML over a GROUP BY sink-derived subquery: exercises defect (a) (no real row_ids).
    auto config = test_create_config(integration_fixture_path("test_streaming_match_delete_group"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    {
        auto cur = exec(dispatcher,
                        "DELETE FROM SinkDb.emp WHERE dept_id IN "
                        "(SELECT dept_id FROM SinkDb.emp GROUP BY dept_id HAVING COUNT(*) > 1);");
        INFO("DELETE IN group-subquery error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM SinkDb.emp;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM SinkDb.emp WHERE dept_id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 6);
    }
}

TEST_CASE("integration::cpp::streaming_match::delete_using_large_build_side_does_not_overflow") {
    // REGRESSION: a build side > DEFAULT_VECTOR_CAPACITY rows used to merge_chunks() into one
    // data_chunk_t, aborting on the capacity<=1024 assert; it's now iterated per chunk.
    auto config = test_create_config(integration_fixture_path("test_streaming_match_delete_using_large"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE BigDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE BigDb.target (id bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE BigDb.using_tbl (k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO BigDb.target (id, k) VALUES (0,0), (1,1), (2,2);")->is_success());
    constexpr unsigned kBuildRows = 2000;
    for (unsigned i = 0; i < kBuildRows; ++i) {
        REQUIRE(exec(dispatcher, "INSERT INTO BigDb.using_tbl (k) VALUES (1);")->is_success());
    }
    {
        auto cur = exec(dispatcher,
                        "DELETE FROM BigDb.target USING BigDb.using_tbl "
                        "WHERE BigDb.target.k = BigDb.using_tbl.k;");
        INFO("DELETE USING large-build error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1); // only the k=1 target row, deleted once (semi-join)
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM BigDb.target;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
    }
}

TEST_CASE("integration::cpp::streaming_match::delete_using_with_nonpushdown_filter_lands") {
    // DML over a JOIN sink with a non-pushdown filter: exercises defect (a) (no real row_ids).
    auto config = test_create_config(integration_fixture_path("test_streaming_match_delete_using"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    {
        auto cur = exec(dispatcher,
                        "DELETE FROM SinkDb.emp USING SinkDb.dept "
                        "WHERE SinkDb.emp.dept_id = SinkDb.dept.id AND SinkDb.emp.name LIKE 'a%';");
        INFO("DELETE USING+LIKE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM SinkDb.emp;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM SinkDb.emp WHERE name LIKE 'a%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 0);
    }
}

TEST_CASE("integration::cpp::streaming_match::like_all_null_element_disk_three_valued") {
    // DISK-mode three-valued LIKE ALL pushes the pattern set into the scan as a conjunction of
    // regex_filter_t leaves; a NULL pattern makes ALL UNKNOWN for every row (PostgreSQL: 0
    // rows) — the filter builder used to exclude regex from that NULL-element collapse.
    auto config = test_create_config(integration_fixture_path("test_streaming_match_like_all_null_disk"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchDb.t (id bigint, s text);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchDb.pat (p text);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO MatchDb.t (id, s) VALUES (1, 'ab'), (2, 'abc'), (3, 'zz');")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO MatchDb.pat (p) VALUES ('a%'), (NULL);")->is_success());

    {
        auto cur = exec(dispatcher, "SELECT id FROM MatchDb.t WHERE s LIKE ALL (SELECT p FROM MatchDb.pat);");
        INFO("LIKE ALL error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0); // the NULL pattern keeps every row at UNKNOWN
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM MatchDb.t WHERE s NOT LIKE ALL (SELECT p FROM MatchDb.pat);");
        INFO("NOT LIKE ALL error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0); // 'zz' fails 'a%' but the NULL keeps the ALL at UNKNOWN
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM MatchDb.t WHERE s LIKE ANY (SELECT p FROM MatchDb.pat);");
        INFO("LIKE ANY error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2); // ab, abc match 'a%'; the NULL adds only UNKNOWN
    }
}

// row_group_t::templated_scan gathers surviving rows via col_data.fetch_row; a buffer-pin OOM
// used to leave column_fetch_state::fetch_error unset instead of aborting the scan, silently
// emitting garbage cells. The OOM itself isn't triggerable from this harness (fixed 4 GiB buffer
// pool cap, no memory-limit knob), so this pins the happy-path gather code the fix touched.
TEST_CASE("integration::cpp::streaming_match::late_mat_gather_selective_disk_values_land") {
    auto config = test_create_config(integration_fixture_path("test_streaming_match_late_mat_gather"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kGatherRows = 4000;
    const std::string wide_pad(120, 'x');

    REQUIRE(exec(dispatcher, "CREATE DATABASE GatherDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE GatherDb.t (id bigint, s text);")->is_success());
    {
        std::stringstream q;
        q << "INSERT INTO GatherDb.t (id, s) VALUES ";
        for (unsigned i = 0; i < kGatherRows; ++i) {
            q << "(" << i << ", 'payload_" << i << "_" << wide_pad << "')" << (i + 1 == kGatherRows ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kGatherRows);
    }

    {
        auto cur = exec(dispatcher, "SELECT id, s FROM GatherDb.t WHERE id < 50;");
        INFO("head gather error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50);
    }

    {
        auto cur = exec(dispatcher, "SELECT id, s FROM GatherDb.t WHERE id >= 3950;");
        INFO("tail gather error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50);
    }

    // A swallowed fetch error would surface here as a garbage `s` cell next to a plausible row count.
    {
        auto cur = exec(dispatcher, "SELECT s FROM GatherDb.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const auto cell = cur->value(0, 0);
        REQUIRE(cell.value<std::string_view>() == std::string("payload_7_") + wide_pad);
    }
    {
        auto cur = exec(dispatcher, "SELECT s FROM GatherDb.t WHERE id = 3999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const auto cell = cur->value(0, 0);
        REQUIRE(cell.value<std::string_view>() == std::string("payload_3999_") + wide_pad);
    }
}
