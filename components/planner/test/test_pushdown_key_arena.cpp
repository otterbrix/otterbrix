
#include <catch2/catch_test_macros.hpp>
#include <components/vector/vector_builders.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/optimizer/rules/pushdown_filter.hpp>
#include <components/types/types.hpp>
#include <core/counting_resource.hpp>
#include <core/date/date_types.hpp>

#include <memory_resource>

using namespace components::logical_plan;
using namespace components::expressions;

namespace {
    core::dbname_t adb() { return core::dbname_t{std::string{"database"}}; }
    core::relname_t arel() { return core::relname_t{std::string{"collection"}}; }

    using core::pmr::default_resource_counter_t;

    // path()[0] is pre-stamped to `idx`, matching what validate_schema stamps at runtime.
    components::expressions::key_t akey(std::pmr::memory_resource* r, const char* name, size_t idx, side_t side) {
        components::expressions::key_t k(r, name, side);
        std::pmr::vector<size_t> p{r};
        p.push_back(idx);
        k.set_path(std::move(p));
        return k;
    }

    node_aggregate_ptr ascan(std::pmr::memory_resource* r, std::initializer_list<const char*> cols) {
        auto agg = make_node_aggregate(r, qualified_name_t{adb(), arel()});
        std::pmr::vector<components::types::complex_logical_type> out(r);
        for (const char* c : cols) {
            out.emplace_back(components::types::logical_type::BIGINT, c);
        }
        agg->set_output_types(std::move(out));
        return agg;
    }

    // t1={a,k}, t2={b,k2} joined ON t1.k=t2.k2, WHERE <col>=5; where_on_key selects WHERE t1.k=5 (derives
    // t2.k2=5) vs WHERE t1.a=5 (pushes only, derives nothing).
    size_t default_allocations_of_run(join_type jt, std::pmr::memory_resource* plan_arena, bool where_on_key) {
        auto params = make_parameter_node(plan_arena);
        auto p5 = params->add_parameter(components::vector::make_scalar(plan_arena, int64_t(5)),
                                        components::vector::parameter_kind::literal);

        auto left = ascan(plan_arena, {"a", "k"});
        auto right = ascan(plan_arena, {"b", "k2"});
        auto join = make_node_join(plan_arena, jt);
        join->append_child(left);
        join->append_child(right);
        join->append_expression(make_compare_expression(plan_arena,
                                                        compare_type::eq,
                                                        akey(plan_arena, "k", 1, side_t::left),
                                                        akey(plan_arena, "k2", 1, side_t::right)));

        auto where =
            where_on_key
                ? make_compare_expression(plan_arena, compare_type::eq, akey(plan_arena, "k", 1, side_t::left), p5)
                : make_compare_expression(plan_arena, compare_type::eq, akey(plan_arena, "a", 0, side_t::left), p5);

        auto outer = make_node_aggregate(plan_arena, qualified_name_t{adb(), arel()});
        outer->append_child(join);
        outer->append_child(make_node_match(plan_arena, qualified_name_t{adb(), arel()}, where));

        size_t measured = 0;
        {
            default_resource_counter_t counter;
            node_ptr out = components::planner::optimizer::pushdown_filter(plan_arena, outer);
            measured = counter.allocations();
            REQUIRE(out != nullptr);
        }
        return measured;
    }
} // namespace

// The deriving run costs exactly 3 (2 from the compare expression's copy of the derived key, 1 from
// relocalize_key_path); it cost 5 while the derived key reached the compare as a key_t, 12 before this rule
// placed its own copies.
TEST_CASE("components::planner::pushdown_filter::derivation_allocates_on_the_named_arena") {
    auto resource = core::pmr::otterbrix_resource();

    // LEFT gates the derivation off: a null-padded partner would wrongly drop preserved rows.
    CHECK(default_allocations_of_run(join_type::left, &resource, true) == 0);

    CHECK(default_allocations_of_run(join_type::inner, &resource, false) == 0);

    size_t one_plain_key_copy = 0;
    {
        auto k = akey(&resource, "k2", 3, side_t::right);
        default_resource_counter_t counter;
        components::expressions::key_t unplaced_copy = k;
        one_plain_key_copy = counter.allocations();
        CHECK(unplaced_copy.as_string() == "k2");
    }
    CHECK(one_plain_key_copy == 2);

    size_t one_compare_expression = 0;
    {
        auto params = make_parameter_node(&resource);
        auto p5 = params->add_parameter(components::vector::make_scalar(&resource, int64_t(5)),
                                        components::vector::parameter_kind::literal);
        auto k = akey(&resource, "k2", 3, side_t::right);
        default_resource_counter_t counter;
        auto expr = make_compare_expression(&resource, compare_type::eq, k, p5);
        one_compare_expression = counter.allocations();
        REQUIRE(expr != nullptr);
    }
    CHECK(one_compare_expression == 2 * one_plain_key_copy);

    const size_t deriving = default_allocations_of_run(join_type::inner, &resource, true);
    CHECK(deriving == 3);
    REQUIRE(deriving <= one_compare_expression + 1);
}
