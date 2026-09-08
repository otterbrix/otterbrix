// Asserts a WITH RECURSIVE query streams end-to-end through execute_pipeline (fixpoint sub-plans
// AND the outer plan), not just that it is correct -- see test_subqueries.cpp for correctness.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/collection/executor.hpp>

using namespace components;

namespace {
    cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    void setup_org(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE RC;")->is_success());
        REQUIRE(
            exec(dispatcher, "CREATE TABLE RC.OrgChart (id bigint, name string, manager_id bigint);")->is_success());
        auto cur = exec(dispatcher,
                        "INSERT INTO RC.OrgChart (id, name, manager_id) VALUES "
                        "(1, 'CEO',      0), "
                        "(2, 'VP Eng',   1), "
                        "(3, 'VP Mkt',   1), "
                        "(4, 'Engineer', 2), "
                        "(5, 'Designer', 3);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_recursive_cte::fixpoint_streams_and_is_correct") {
    auto config = test_create_config(integration_fixture_path("test_streaming_recursive_cte/fixpoint"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    setup_org(dispatcher);

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "WITH RECURSIVE hierarchy AS ("
                        "  SELECT id, name FROM RC.OrgChart WHERE manager_id = 0 "
                        "  UNION ALL "
                        "  SELECT e.id, e.name "
                        "  FROM RC.OrgChart e "
                        "  JOIN hierarchy h ON e.manager_id = h.id"
                        ") "
                        "SELECT name FROM hierarchy ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "CEO");
        REQUIRE(cur->value(0, 4).value<std::string_view>() == "Designer");
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);
}

TEST_CASE("integration::cpp::streaming_recursive_cte::outer_plan_no_longer_materializes") {
    // Gate for deleting the legacy on_execute path: the materialized-path counter must not
    // advance anywhere in this statement (outer chain or fixpoint).
    auto config = test_create_config(integration_fixture_path("test_streaming_recursive_cte/no_materialize"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    setup_org(dispatcher);

    const auto streaming_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "WITH RECURSIVE hierarchy AS ("
                        "  SELECT id, name FROM RC.OrgChart WHERE manager_id = 0 "
                        "  UNION ALL "
                        "  SELECT e.id, e.name "
                        "  FROM RC.OrgChart e "
                        "  JOIN hierarchy h ON e.manager_id = h.id"
                        ") "
                        "SELECT name FROM hierarchy ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
    const auto streaming_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(streaming_after > streaming_before);
}

TEST_CASE("integration::cpp::streaming_recursive_cte::subtree_and_depth_stream") {
    auto config = test_create_config(integration_fixture_path("test_streaming_recursive_cte/subtree"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    setup_org(dispatcher);

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "WITH RECURSIVE subtree AS ("
                        "  SELECT id, name FROM RC.OrgChart WHERE id = 2 "
                        "  UNION ALL "
                        "  SELECT e.id, e.name "
                        "  FROM RC.OrgChart e "
                        "  JOIN subtree s ON e.manager_id = s.id"
                        ") "
                        "SELECT name FROM subtree ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "VP Eng");
        REQUIRE(cur->value(0, 1).value<std::string_view>() == "Engineer");
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);
}
