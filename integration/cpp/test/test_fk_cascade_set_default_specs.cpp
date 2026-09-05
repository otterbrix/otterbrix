// ============================================================================
// SET DEFAULT WITH A SPEC LIST SHORTER THAN THE COLUMN LIST IS A REFUSAL, NOT
// A SET NULL.
//
// operator_fk_cascade_t's ON DELETE SET DEFAULT leg reads
// child_col_default_specs[ci] for every child_col_schema_indices[ci] — guarded
// only by `ci < child_col_default_specs.size()`. A spec vector SHORTER than the
// position vector therefore did not fail: the tail columns silently fell into
// the SET NULL arm, substituting one referential action for another. The one
// producer (enrich) fills both vectors in a single loop, so the skew is not
// reachable through SQL today — this guard is the floor under an fk_info_t
// that arrives by another road, and this test drives the operator DIRECTLY
// with the poisoned descriptor.
//
// The guard sits BEFORE the first disk send, which is what lets this test run
// without a disk actor: before the fix the coroutine sailed past the arity
// check into a send on an empty address (abort); with the guard it refuses
// cleanly, naming the actual defect.
// ============================================================================

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/catalog/fk_info.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_fk_cascade.hpp>

#include <memory_resource>
#include <string>

using namespace components;

namespace {

    // Same stand-in as test_unique_constraint_operator.cpp: exposes a fixed
    // write-set (the deleted parent rows) as constraint_input().
    class cascade_source_operator_t final : public operators::read_only_operator_t {
    public:
        cascade_source_operator_t(std::pmr::memory_resource* resource, operators::operator_data_ptr data)
            : operators::read_only_operator_t(resource, log_t{}, operators::operator_type::empty) {
            constraint_input_ = std::move(data);
        }
    };

} // namespace

TEST_CASE("fk cascade: SET DEFAULT with fewer default specs than columns is refused before any send",
          "[fk_cascade_specs]") {
    auto resource = core::pmr::otterbrix_resource();

    // One deleted parent row with its key column, so the cascade has work to do.
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("id");
    vector::data_chunk_t parent_rows(&resource, cols, 1);
    parent_rows.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    parent_rows.set_cardinality(1);

    catalog::fk_info_t fk;
    fk.child_col_names = {"pid"};
    fk.parent_col_names = {"id"};
    fk.child_col_indices = {0};
    fk.parent_col_indices = {0};
    fk.child_col_schema_indices = {1};
    fk.child_col_default_specs = {}; // SHORTER than the position list — the poison.
    fk.child_table_oid = catalog::oid_t{16385};
    fk.parent_table_oid = catalog::oid_t{16386};
    fk.del_action = 'd'; // SET DEFAULT

    operators::operator_ptr op(new operators::operator_fk_cascade_t(&resource, log_t{}, std::move(fk)));
    op->set_children(operators::operator_ptr(
        new cascade_source_operator_t(&resource, operators::make_operator_data(&resource, std::move(parent_rows)))));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    auto fut = op->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    INFO("a default-spec list shorter than the column list must refuse, not SET NULL the tail");
    REQUIRE(op->has_error());
    const std::string err{op->get_error().what};
    INFO("error: " << err);
    CHECK(err.find("default") != std::string::npos);
}
