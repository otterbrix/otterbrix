// Host optimizer rules registered at every stage: each sees the tree the built-in rules before it left.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>

#include <map>

using namespace components;

namespace {

    struct shape_t {
        bool inner_join{false};
        bool hash_join{false};
        bool read_cap{false};
        bool aggregate_pushdown{false};
        bool projected_cols{false};
    };

    std::map<planner::optimizer_stage, shape_t>& seen() {
        static std::map<planner::optimizer_stage, shape_t> shapes;
        return shapes;
    }

    void collect(const logical_plan::node_ptr& node, shape_t& shape) {
        switch (node->type()) {
            case logical_plan::node_type::join_t: {
                const auto* join = static_cast<const logical_plan::node_join_t*>(node.get());
                shape.inner_join |= join->type() == logical_plan::join_type::inner;
                shape.hash_join |= join->algo() == logical_plan::node_join_t::join_algo::hash;
                break;
            }
            case logical_plan::node_type::aggregate_t: {
                const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
                shape.read_cap |= agg->read_cap().limit() != logical_plan::limit_t::unlimit().limit();
                shape.projected_cols |= !agg->projected_cols().empty();
                break;
            }
            case logical_plan::node_type::group_t:
                shape.aggregate_pushdown |= static_cast<const logical_plan::node_group_t*>(node.get())->pushdown();
                break;
            default:
                break;
        }
        for (const auto& child : node->children()) {
            collect(child, shape);
        }
    }

    template<planner::optimizer_stage Stage>
    logical_plan::node_ptr record(std::pmr::memory_resource*, logical_plan::node_ptr node) {
        shape_t shape;
        collect(node, shape);
        seen()[Stage] = shape;
        return node;
    }

    using planner::optimizer_stage;
    constexpr planner::optimizer_rule_t rules[] = {
        {optimizer_stage::after_simplify, &record<optimizer_stage::after_simplify>},
        {optimizer_stage::after_filters_and_joins, &record<optimizer_stage::after_filters_and_joins>},
        {optimizer_stage::after_limit, &record<optimizer_stage::after_limit>},
        {optimizer_stage::after_aggregate_pushdown, &record<optimizer_stage::after_aggregate_pushdown>},
        {optimizer_stage::last, &record<optimizer_stage::last>},
    };

    void run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        INFO(sql);
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), sql)->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::host_optimizer_stages::each_stage_sees_its_shape") {
    auto config = test_create_config(integration_fixture_path("test_host_optimizer_stages/base"));
    test_clear_directory(config);
    test_spaces space(config, services::engine::primitives_t{rules, {}});
    auto* dispatcher = space.dispatcher();
    run_ok(dispatcher, "CREATE DATABASE sdb;");
    run_ok(dispatcher, "CREATE TABLE sdb.a (k BIGINT, v BIGINT);");
    run_ok(dispatcher, "CREATE TABLE sdb.b (k BIGINT, w BIGINT);");
    run_ok(dispatcher, "INSERT INTO sdb.a (k, v) VALUES (1, 10), (2, 20);");
    run_ok(dispatcher, "INSERT INTO sdb.b (k, w) VALUES (1, 5);");

    SECTION("comma join: promoted to INNER first, hash-selected with the filters") {
        seen().clear();
        run_ok(dispatcher, "SELECT a.v FROM sdb.a AS a, sdb.b AS b WHERE a.k = b.k;");
        CHECK(seen()[optimizer_stage::after_simplify].inner_join);
        CHECK_FALSE(seen()[optimizer_stage::after_simplify].hash_join);
        CHECK(seen()[optimizer_stage::after_filters_and_joins].hash_join);
    }
    SECTION("LIMIT: the read cap appears at after_limit") {
        seen().clear();
        run_ok(dispatcher, "SELECT k FROM sdb.a LIMIT 1;");
        CHECK_FALSE(seen()[optimizer_stage::after_filters_and_joins].read_cap);
        CHECK(seen()[optimizer_stage::after_limit].read_cap);
    }
    SECTION("aggregate: the pushdown stamp appears at after_aggregate_pushdown") {
        seen().clear();
        run_ok(dispatcher, "SELECT sum(v) AS s FROM sdb.a;");
        CHECK_FALSE(seen()[optimizer_stage::after_limit].aggregate_pushdown);
        CHECK(seen()[optimizer_stage::after_aggregate_pushdown].aggregate_pushdown);
    }
    SECTION("projection: pruned columns appear only at last") {
        seen().clear();
        run_ok(dispatcher, "SELECT k FROM sdb.a;");
        CHECK_FALSE(seen()[optimizer_stage::after_aggregate_pushdown].projected_cols);
        CHECK(seen()[optimizer_stage::last].projected_cols);
    }
}
