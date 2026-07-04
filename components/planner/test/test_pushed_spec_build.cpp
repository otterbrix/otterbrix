// Aggregate-pushdown POD spec-build (services::planner::impl::build_pushed_spec).
//
// A pushed aggregate rides a POD pushed_aggregate_spec_t built by build_pushed_spec; this
// file asserts:
//   (a) DISJOINTNESS — the POD carries NO node_ptr / expression_ptr; each string/path is an
//       independent std::pmr copy (mailbox-safe by construction), NOT an alias of the source.
//   (b) FIELD FIDELITY — group keys (name + path), aggregates (function / uid / arg-path /
//       alias / distinct) and output_types round-trip exactly.
//   (c) COMPLETENESS — a non-representable shape (HAVING / coalesce key / distinct or multi-arg
//       aggregate) is REJECTED (build returns false => the coordinator aggregate stands), never
//       silently mis-encoded.

#include <catch2/catch.hpp>

#include <components/compute/function.hpp>
#include <components/compute/tests/pushdown_sum_uid.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp>
#include <components/physical_plan_generator/impl/create_plan_aggregate.hpp>

#include "pushdown_plan_builders.hpp"

#include <memory_resource>

using namespace components::logical_plan;
using namespace components::expressions;
namespace types = components::types;
namespace ops = components::operators;
using key = components::expressions::key_t;
using pushdown_test::sum_uid; // a real builtin "sum" uid so the aggregate uid gate passes

namespace {

    using planner_test::dbn;
    using planner_test::reln;

    // A resolved column key: name + a single-element resolved path.
    key col(std::pmr::memory_resource* r, const char* name, size_t idx) {
        key k(r, name, side_t::left);
        std::pmr::vector<size_t> p{r};
        p.push_back(idx);
        k.set_path(std::move(p));
        return k;
    }

    // aggregate_t(output_types, distinct) -> group_t(pushdown, exprs, having). The pushdown
    // group is built here; the aggregate-node wrapping (oid / distinct / output_types) is the
    // shared planner_test::make_agg.
    node_ptr make_agg(std::pmr::memory_resource* r,
                      const std::vector<expression_ptr>& group_exprs,
                      expression_ptr having,
                      std::pmr::vector<types::complex_logical_type> out_types,
                      bool agg_distinct = false) {
        auto group = make_node_group(r, dbn(), reln(), group_exprs, having);
        group->set_pushdown(true);
        group->set_table_oid(components::catalog::oid_t{123});
        return planner_test::make_agg(r, group, components::catalog::oid_t{123}, std::move(out_types), agg_distinct);
    }

    const node_group_t* group_of(const node_ptr& agg) {
        return static_cast<const node_group_t*>(agg->children()[0].get());
    }

} // namespace

// ================================================================
// (b) FIELD FIDELITY — round-trip of every carried field.
// ================================================================
TEST_CASE("pushed_spec::field_fidelity") {
    std::pmr::monotonic_buffer_resource node_res;
    std::pmr::monotonic_buffer_resource spec_res;

    // GROUP BY g(col 1), SUM(v col 2) AS s_out (non-distinct); output types BIGINT, BIGINT.
    auto grp = make_scalar_expression(&node_res, scalar_type::group_field, col(&node_res, "g", 1));
    auto sum = make_aggregate_expression(&node_res, "sum", key(&node_res, "s_out", side_t::left));
    sum->add_function_uid(sum_uid(&node_res));
    sum->append_param(param_storage{col(&node_res, "v", 2)});

    std::pmr::vector<types::complex_logical_type> ot{&node_res};
    ot.emplace_back(types::logical_type::BIGINT);
    ot.emplace_back(types::logical_type::BIGINT);

    std::vector<expression_ptr> exprs{expression_ptr(grp), expression_ptr(sum)};
    auto agg = make_agg(&node_res, exprs, nullptr, std::move(ot));

    ops::pushed_aggregate_spec_t spec{&spec_res};
    REQUIRE(services::planner::impl::build_pushed_spec(group_of(agg), agg, &spec_res, spec));
    REQUIRE(spec.active());

    // group keys.
    REQUIRE(spec.group_keys.size() == 1);
    REQUIRE(spec.group_keys[0].name == "g");
    REQUIRE(spec.group_keys[0].path.size() == 1);
    REQUIRE(spec.group_keys[0].path[0] == 1);

    // aggregates.
    REQUIRE(spec.aggregates.size() == 1);
    REQUIRE(spec.aggregates[0].function_name == "sum");
    REQUIRE(spec.aggregates[0].func_uid == sum_uid(&node_res));
    REQUIRE_FALSE(spec.aggregates[0].distinct);
    REQUIRE(spec.aggregates[0].alias == "s_out");
    REQUIRE(spec.aggregates[0].arg_col_path.size() == 1);
    REQUIRE(spec.aggregates[0].arg_col_path[0] == 2);

    // output types round-trip (keys first, then aggregate values).
    REQUIRE(spec.output_types.size() == 2);
    REQUIRE(spec.output_types[0].type() == types::logical_type::BIGINT);
    REQUIRE(spec.output_types[1].type() == types::logical_type::BIGINT);
}

// ================================================================
// (a) DISJOINTNESS — the POD copied onto its OWN resource, no source aliasing.
// ================================================================
TEST_CASE("pushed_spec::disjointness") {
    std::pmr::monotonic_buffer_resource node_res;
    std::pmr::monotonic_buffer_resource spec_res;

    auto grp = make_scalar_expression(&node_res, scalar_type::group_field, col(&node_res, "g", 1));
    std::vector<expression_ptr> exprs{expression_ptr(grp)};
    auto agg = make_agg(&node_res, exprs, nullptr, std::pmr::vector<types::complex_logical_type>{&node_res});

    ops::pushed_aggregate_spec_t spec{&spec_res};
    REQUIRE(services::planner::impl::build_pushed_spec(group_of(agg), agg, &spec_res, spec));
    REQUIRE(spec.group_keys.size() == 1);
    REQUIRE(spec.group_keys[0].name == "g");

    // The POD carries an INDEPENDENT copy of the key name (distinct storage), so nothing
    // from the source expression tree crosses the mailbox by reference.
    const auto& src_name =
        static_cast<const scalar_expression_t*>(group_of(agg)->expressions()[0].get())->key().storage().back();
    REQUIRE(static_cast<const void*>(spec.group_keys[0].name.data()) !=
            static_cast<const void*>(src_name.data()));
}

// ================================================================
// (c) COMPLETENESS — a non-representable shape is REJECTED, never mis-encoded.
// ================================================================
TEST_CASE("pushed_spec::rejects_having") {
    std::pmr::monotonic_buffer_resource r;
    auto grp = make_scalar_expression(&r, scalar_type::group_field, col(&r, "g", 1));
    auto having = make_compare_expression(&r, compare_type::gt, key(&r, "cnt", side_t::left), key(&r, "lim", side_t::left));
    std::vector<expression_ptr> exprs{expression_ptr(grp)};
    auto agg = make_agg(&r, exprs, expression_ptr(having), std::pmr::vector<types::complex_logical_type>{&r});

    ops::pushed_aggregate_spec_t spec{&r};
    REQUIRE_FALSE(services::planner::impl::build_pushed_spec(group_of(agg), agg, &r, spec));
}

TEST_CASE("pushed_spec::rejects_coalesce_key") {
    std::pmr::monotonic_buffer_resource r;
    auto grp = make_scalar_expression(&r, scalar_type::coalesce, key(&r, "g", side_t::left));
    std::vector<expression_ptr> exprs{expression_ptr(grp)};
    auto agg = make_agg(&r, exprs, nullptr, std::pmr::vector<types::complex_logical_type>{&r});

    ops::pushed_aggregate_spec_t spec{&r};
    REQUIRE_FALSE(services::planner::impl::build_pushed_spec(group_of(agg), agg, &r, spec));
}

TEST_CASE("pushed_spec::rejects_distinct_aggregate") {
    std::pmr::monotonic_buffer_resource r;
    auto sum = make_aggregate_expression(&r, "sum", key(&r, "s", side_t::left));
    sum->add_function_uid(sum_uid(&r));
    sum->set_distinct(true);
    sum->append_param(param_storage{col(&r, "v", 2)});
    std::vector<expression_ptr> exprs{expression_ptr(sum)};
    auto agg = make_agg(&r, exprs, nullptr, std::pmr::vector<types::complex_logical_type>{&r});

    ops::pushed_aggregate_spec_t spec{&r};
    REQUIRE_FALSE(services::planner::impl::build_pushed_spec(group_of(agg), agg, &r, spec));
}

TEST_CASE("pushed_spec::rejects_multiarg_aggregate") {
    std::pmr::monotonic_buffer_resource r;
    auto sum = make_aggregate_expression(&r, "sum", key(&r, "s", side_t::left));
    sum->add_function_uid(sum_uid(&r));
    sum->append_param(param_storage{col(&r, "a", 1)});
    sum->append_param(param_storage{col(&r, "b", 2)}); // second arg => not POD-representable
    std::vector<expression_ptr> exprs{expression_ptr(sum)};
    auto agg = make_agg(&r, exprs, nullptr, std::pmr::vector<types::complex_logical_type>{&r});

    ops::pushed_aggregate_spec_t spec{&r};
    REQUIRE_FALSE(services::planner::impl::build_pushed_spec(group_of(agg), agg, &r, spec));
}
