// Maintenance operators over an unwired topology must refuse, not silently succeed: an unwired index_scan
// returning an empty window is indistinguishable from "no match", and a DROP INDEX with nothing to scrub
// bypasses its no-identity-row verdict. operator_create_index_backfill_t is excluded on purpose:
// test_variant_e3_differential.cpp pins its empty-index sync as success.

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

// The planner never emits a DROP INDEX with an empty spec list, so reaching this path is a planner-invariant
// violation.
TEST_CASE("integration::cpp::maintenance_wiring::drop_index_with_nothing_to_scrub_refuses") {
    auto* res = std::pmr::new_delete_resource();

    boost::intrusive_ptr<ops::operator_t> op{
        new ops::operator_drop_index_t(res, log_t{}, some_table_oid, some_index_oid, {})};

    components::pipeline::context_t ctx{lp::storage_parameters{res},
                                        components::pipeline::no_mailbox(),
                                        components::pipeline::no_mailbox(),
                                        components::pipeline::no_mailbox()};
    auto fut = op->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    INFO("no catalog delete specs and no disk actor: nothing this statement could verify as dropped");
    CHECK_FALSE(op->is_executed());
    REQUIRE(op->has_error());
    CHECK(op->get_error().type == core::error_code_t::index_not_exists);
}

// index_scan is only built once the planner has proved the index exists, so this path is the same
// invariant violation.
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

    components::pipeline::context_t ctx{lp::storage_parameters{res},
                                        components::pipeline::no_mailbox(),
                                        components::pipeline::no_mailbox(),
                                        components::pipeline::no_mailbox()};
    auto fut = op.source_next(&ctx);
    REQUIRE(fut.is_ready());
    auto first = std::move(fut).take_ready();

    INFO("an unwired index service must be an error on the source, not an empty drain");
    REQUIRE(first.has_error());
    CHECK(first.error().type == core::error_code_t::index_not_exists);
    CHECK(op.has_error());
    CHECK_FALSE(op.is_executed());
}
