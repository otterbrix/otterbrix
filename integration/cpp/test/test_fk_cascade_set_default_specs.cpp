// A default-spec list shorter than the position list must refuse rather than silently fall
// through to SET NULL, substituting one referential action for another; unreachable via SQL, so
// this drives operator_fk_cascade_t directly with the poisoned descriptor.
// The refusal must run before the first disk send, or an empty mailbox address would abort
// instead of erroring cleanly -- which is also why this test needs no disk actor.

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

    // Same stand-in as test_unique_constraint_operator.cpp: exposes a fixed write-set as
    // constraint_input().
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
    fk.child_col_default_specs = {}; // shorter than the position list — the poison.
    fk.child_table_oid = catalog::oid_t{16385};
    fk.parent_table_oid = catalog::oid_t{16386};
    fk.del_action = 'd'; // SET DEFAULT

    operators::operator_ptr op(new operators::operator_fk_cascade_t(&resource, log_t{}, std::move(fk)));
    op->set_children(operators::operator_ptr(
        new cascade_source_operator_t(&resource, operators::make_operator_data(&resource, std::move(parent_rows)))));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource},
                            pipeline::no_mailbox(),
                            pipeline::no_mailbox(),
                            pipeline::no_mailbox());
    auto fut = op->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    INFO("a default-spec list shorter than the column list must refuse, not SET NULL the tail");
    REQUIRE(op->has_error());
    const std::string err{op->get_error().what};
    INFO("error: " << err);
    CHECK(err.find("default") != std::string::npos);
}
