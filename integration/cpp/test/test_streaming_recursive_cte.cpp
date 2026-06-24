// ============================================================================
// Recursive-CTE streaming-path coverage.
//
// operator_recursive_cte_t is a SOURCELESS async-finalize SINK: its anchor and
// recursive-term sub-plans are held as PRIVATE members (not left_/right_), so
// left()==nullptr and is_streaming_pipeline sees it as a sourceless sink root
// (role()==sink + needs_async_finalize()==true). The fixpoint runs inside
// await_async_and_resume, which co_awaits ctx->runner->run_subplan once per
// iteration over the anchor and the recursive term.
//
// WHERE THE STREAMING ACTUALLY HAPPENS — the per-iteration run_subplan calls.
// The recursive term lowers to JOIN(scan, cte_scan): the scan is a SOURCE, the
// cte_scan is a SOURCE over the working set, the join a SINK, so each recursive
// pass is a streaming pipeline driven by execute_pipeline. The anchor (a filtered
// base scan) likewise streams. So a WITH RECURSIVE query bumps
// streaming_pipeline_runs() at least once per fixpoint pass — proving the fixpoint
// is driven by the push-based streaming executor, not a bespoke materialize loop.
//
// At the TOP level the outer `SELECT ... FROM cte ...` lowers to
// [select(streaming) -> sort(sink) -> match(streaming) -> recursive_cte(sink,
// sourceless)]. is_streaming_pipeline now admits this whole chain: its deepest op
// (recursive_cte) is a SOURCELESS sink (left()==nullptr), regardless of the
// streaming/sink ancestors above it. execute_pipeline drives the bottom's
// await_async_and_resume FIRST (the fixpoint that PRODUCES its rows), then pumps
// those rows UP through match -> sort -> select (sort emits them sorted in FLUSH).
// So the OUTER plan now streams through execute_pipeline too — it no longer needs
// the legacy materialized path. (Before this change the outer plan fell to the
// materialized on_execute path because is_streaming_pipeline required an all-sink
// sourceless chain, which the streaming match/select ancestors violated.)
//
// The correctness oracle stays test_subqueries.cpp; this file adds the
// streaming-path assertion (streaming_pipeline_runs() bumps for a recursive CTE,
// AND the outer plan itself now routes through execute_pipeline).
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>

using namespace components;

namespace {
    cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    void setup_org(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE RC;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE RC.OrgChart (id bigint, name string, manager_id bigint);")
                    ->is_success());
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
    auto config = test_create_config("/tmp/test_streaming_recursive_cte/fixpoint");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    setup_org(dispatcher);

    // PATH: a WITH RECURSIVE traversal drives its anchor + every recursive pass
    // through execute_pipeline (cte_scan SOURCE + scan SOURCE feeding a JOIN SINK),
    // so streaming_pipeline_runs() must climb across the fixpoint. CORRECTNESS is
    // the same 5-row hierarchy the oracle (test_subqueries) pins.
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "CEO");
        REQUIRE(cur->chunk_data().value(0, 4).value<std::string_view>() == "Designer");
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    // The anchor sub-plan + each recursive-term pass are streamed sub-plans, AND the
    // outer [select->sort->match->recursive_cte] chain now streams too (the sourceless
    // recursive_cte sink bottom is driven first, then its rows pump up), so the counter
    // advances.
    REQUIRE(runs_after > runs_before);
}

TEST_CASE("integration::cpp::streaming_recursive_cte::outer_plan_no_longer_materializes") {
    // GATE for deleting the legacy on_execute path: prove the TOP-LEVEL WITH RECURSIVE
    // plan now routes FULLY through execute_pipeline (streaming) and NEVER falls to the
    // materialized path. is_streaming_pipeline now admits its sourceless recursive_cte
    // sink bottom regardless of the streaming match/select ancestors, and
    // execute_pipeline drives that producing sink FIRST then pumps its rows up. So the
    // materialized-path counter must NOT advance across the whole statement (neither the
    // outer chain nor any fixpoint sub-plan materializes).
    auto config = test_create_config("/tmp/test_streaming_recursive_cte/no_materialize");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    setup_org(dispatcher);

    const auto streaming_before = services::collection::executor::streaming_pipeline_runs();
    const auto materialized_before = services::collection::executor::materialized_plan_runs();
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
    const auto materialized_after = services::collection::executor::materialized_plan_runs();
    // The outer plan + every fixpoint sub-plan streamed.
    REQUIRE(streaming_after > streaming_before);
    // And NOTHING fell to the legacy materialize path — the proof that the outer
    // WITH RECURSIVE plan no longer needs on_execute (the deletion gate).
    REQUIRE(materialized_after == materialized_before);
}

TEST_CASE("integration::cpp::streaming_recursive_cte::subtree_and_depth_stream") {
    auto config = test_create_config("/tmp/test_streaming_recursive_cte/subtree");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    setup_org(dispatcher);

    // Subtree rooted at VP Eng (id=2): VP Eng + Engineer.
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "VP Eng");
        REQUIRE(cur->chunk_data().value(0, 1).value<std::string_view>() == "Engineer");
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);
}
