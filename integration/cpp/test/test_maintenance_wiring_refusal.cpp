// ============================================================================
// MAINTENANCE OPERATORS OVER AN UNWIRED TOPOLOGY MUST REFUSE, NOT REPORT SUCCESS.
//
// Every production topology wires the index and disk managers unconditionally
// (base_spaces spawns both before the executor exists), so a maintenance operator
// that finds an EMPTY address is not running in a lighter mode — it is running in a
// topology where its statement cannot do any of its work. Two operators answered
// that state with quiet success:
//
//   * index_scan yielded an EMPTY window and no error — the planner promised an
//     index and the reader got a silently short result set, indistinguishable from
//     "no row matches the predicate" (the exact case the same function's
//     search-error branch refuses);
//   * operator_drop_index_t, given nothing to scrub (no delete specs, or no disk
//     actor to send them to), skipped its own "at least one identity row went"
//     verdict and reported a DROP INDEX that dropped nothing as success.
//
// operator_create_index_backfill_t's quiet no-op over an empty index address is
// deliberately NOT converted (and not asserted here): the dispatcher differential
// harness (services/dispatcher/tests/test_variant_e3_differential.cpp) syncs an
// EMPTY index address by design and pins CREATE INDEX success there — it compares
// the CATALOG half of the statement, and the branch is its seam. index_scan and
// drop_index have no such pin.
//
// These tests drive the operators DIRECTLY with a bare pipeline context (all
// addresses empty). Each refusal path completes without a single cross-actor send,
// so the futures are ready synchronously — no scheduler, no space.
// ============================================================================

#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_drop_index.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/types/logical_value.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

namespace {

    namespace ops = components::operators;
    namespace lp = components::logical_plan;
    using components::catalog::oid_t;

    constexpr oid_t some_table_oid = 16401;
    constexpr oid_t some_index_oid = 16402;

} // namespace

// A DROP INDEX with nothing to scrub can never fire its own no-identity-row
// verdict, so "success" would mean an engine teardown over a catalog that may
// still describe the index. The planner never emits this shape (it refuses an
// unresolved index and always sends the full spec list), so reaching it is an
// invariant violation, and the operator has to say so.
TEST_CASE("integration::cpp::maintenance_wiring::drop_index_with_nothing_to_scrub_refuses") {
    auto* res = std::pmr::new_delete_resource();

    boost::intrusive_ptr<ops::operator_t> op{
        new ops::operator_drop_index_t(res, log_t{}, some_table_oid, some_index_oid, {})};

    components::pipeline::context_t ctx{lp::storage_parameters{res}};
    auto fut = op->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    INFO("no catalog delete specs and no disk actor: nothing this statement could verify as dropped");
    CHECK_FALSE(op->is_executed());
    REQUIRE(op->has_error());
    CHECK(op->get_error().type == core::error_code_t::index_not_exists);
}

// index_scan is built ONLY when the planner proved an index exists, so an
// unwired index service is the same planner-invariant violation manager_index
// answers index_not_exists for ("no engine for the oid" / "no index on the
// predicate key"). The old early return handed back an EMPTY window and
// no_error() — a silently short result set. It must refuse through the same
// channel the search-error branch already uses.
TEST_CASE("integration::cpp::maintenance_wiring::index_scan_without_index_service_refuses") {
    auto* res = std::pmr::new_delete_resource();

    components::expressions::key_t key{res, "count"};
    components::types::logical_value_t value{res, std::int64_t{42}};
    ops::index_scan op{res,
                       log_t{},
                       some_table_oid,
                       key,
                       value,
                       components::expressions::compare_type::eq,
                       lp::index_type::no_valid,
                       lp::limit_t::unlimit(),
                       {}};

    components::pipeline::context_t ctx{lp::storage_parameters{res}};
    auto fut = op.source_next(&ctx);
    REQUIRE(fut.is_ready());
    auto first = std::move(fut).take_ready();

    INFO("an unwired index service must be an error on the source, not an empty drain");
    REQUIRE(first.has_error());
    CHECK(first.error().type == core::error_code_t::index_not_exists);
    CHECK(op.has_error());
    CHECK_FALSE(op.is_executed());
}
