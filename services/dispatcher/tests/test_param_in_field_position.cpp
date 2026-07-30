#include <catch2/catch_test_macros.hpp>

#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <memory_resource>
#include <vector>

// ============================================================================
// A `get_field` scalar whose operand is a bound parameter instead of a column
// key. The operand of a field reference names a COLUMN; a parameter names a
// value, and the two are different alternatives of the same slot. The validator
// used to read the slot as a key unconditionally, which is a throw on a
// std::variant and a read of the wrong union member once the slot stops being
// one.
//
// SQL no longer builds this shape — a projected `$n` is a constant, like the
// literal it stands in for. These are therefore the DEFENSIVE contract: a
// hand-built or extension-built plan of this shape gets a clean error, not a
// crash. The throw is not survivable where it happens: validation runs inside
// executor_t::execute_plan_full, an actor-zeta coroutine whose
// unhandled_exception() aborts with assertions on and abandons the future
// without them (see services/index/manager_index.cpp for the same failure).
// ============================================================================

using namespace components;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;

namespace {

    // One BIGINT column named "a", one row.
    vector::data_chunk_t one_column_chunk(std::pmr::memory_resource* res) {
        std::pmr::vector<complex_logical_type> types(res);
        types.emplace_back(logical_type::BIGINT);
        types.back().set_field_name("a");
        vector::data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        REQUIRE_FALSE(chunk.set_value(uint64_t{0}, uint64_t{0}, logical_value_t{res, int64_t{1}}).contains_error());
        return chunk;
    }

    // `get_field` carrying a bound parameter where a column key belongs.
    expressions::expression_ptr poisoned_field(std::pmr::memory_resource* res,
                                               logical_plan::parameter_node_t* params,
                                               expressions::scalar_type type) {
        auto scalar = expressions::make_scalar_expression(res, type, expressions::key_t{res, "a"});
        auto id = params->add_parameter(logical_value_t{res, int64_t{1}});
        scalar->append_param(id);
        return expressions::expression_ptr{scalar};
    }

} // namespace

TEST_CASE("services::dispatcher::param_in_field_position::select_projection_is_an_error") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    auto params = logical_plan::make_parameter_node(res);

    auto select = logical_plan::make_node_select(res, core::dbname_t{}, core::relname_t{});
    select->append_expression(poisoned_field(res, params.get(), expressions::scalar_type::get_field));

    auto agg = logical_plan::make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(logical_plan::make_node_raw_data(res, one_column_chunk(res)));
    agg->append_child(select);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params->parameters());

    REQUIRE(validated.has_error());
    INFO("error: " << validated.error().what);
    // The GUARD's own message, not merely some error: this harness's column names never
    // resolve, so an ordinary resolution failure would also satisfy has_error() — and the
    // regression this pin exists for (deleting the operand-shape check) would go unseen.
    CHECK(validated.error().what == "field reference operand is not a column");
}

TEST_CASE("services::dispatcher::param_in_field_position::group_key_is_an_error") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    auto params = logical_plan::make_parameter_node(res);

    std::vector<expressions::expression_ptr> group_exprs{
        poisoned_field(res, params.get(), expressions::scalar_type::get_field)};
    auto group = logical_plan::make_node_group(res, core::dbname_t{}, core::relname_t{}, group_exprs);

    auto agg = logical_plan::make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(logical_plan::make_node_raw_data(res, one_column_chunk(res)));
    agg->append_child(group);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params->parameters());

    REQUIRE(validated.has_error());
    INFO("error: " << validated.error().what);
    // The GUARD's own message, not merely some error: this harness's column names never
    // resolve, so an ordinary resolution failure would also satisfy has_error() — and the
    // regression this pin exists for (deleting the operand-shape check) would go unseen.
    CHECK(validated.error().what == "field reference operand is not a column");
}

// The third reader of this shape, resolve_returning_columns, is guarded the same way but
// is not pinned here: it is reached only when the target table's schema came from the
// catalog, and this harness deliberately runs with idx == nullptr. A test that built the
// plan without a catalog would pass for an unrelated reason.
