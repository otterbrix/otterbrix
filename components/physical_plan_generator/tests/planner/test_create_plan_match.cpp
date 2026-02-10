#include <actor-zeta.hpp>
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/physical_plan/tests/operators/test_operator_generaty.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using namespace services::planner;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() { return {database_name, collection_name}; }

TEST_CASE("components::physical_plan_generator::match") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    {
        auto node_match = make_node_match(&resource, get_name(), nullptr);
        services::context_storage_t context;
        context.emplace(get_name(), d(collection));
        auto plan = create_plan(context, node_match, components::logical_plan::limit_t::unlimit());
        plan->on_execute(nullptr);
        REQUIRE(plan->output()->size() == 100);
    }
    {
        auto node_match = make_node_match(&resource,
                                          get_name(),
                                          make_compare_expression(&resource,
                                                                  compare_type::eq,
                                                                  key(&resource, "key", side_t::left),
                                                                  core::parameter_id_t(1)));
        services::context_storage_t context;
        context.emplace(get_name(), d(collection));
        auto plan = create_plan(context, node_match, components::logical_plan::limit_t::unlimit());
        REQUIRE(node_match->to_string() == R"_($match: {"key": {$eq: #1}})_");
    }
}
