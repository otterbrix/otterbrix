#include <actor-zeta.hpp>
#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/planner.hpp>
#include <components/tests/generaty.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() { return {database_name, collection_name}; }

TEST_CASE("logical_plan::create_database") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_create_database(&resource, {database_name, {}});
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    REQUIRE(node->to_string() == R"_($create_database: database)_");
}

TEST_CASE("logical_plan::drop_database") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_drop_database(&resource, {database_name, {}});
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    REQUIRE(node->to_string() == R"_($drop_database: database)_");
}

TEST_CASE("logical_plan::create_collection") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_create_collection(&resource, {database_name, collection_name});
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    REQUIRE(node->to_string() == R"_($create_collection: database.collection)_");
}

TEST_CASE("logical_plan::drop_collection") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_drop_collection(&resource, {database_name, collection_name});
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    REQUIRE(node->to_string() == R"_($drop_collection: database.collection)_");
}

TEST_CASE("logical_plan::match") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto node_match = make_node_match(&resource,
                                      get_name(),
                                      make_compare_expression(&resource,
                                                              compare_type::eq,
                                                              key(&resource, "key", side_t::left),
                                                              core::parameter_id_t(1)));
    REQUIRE(node_match->to_string() == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("logical_plan::group") {
    auto resource = std::pmr::synchronized_pool_resource();
    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "_id"));
        scalar_expr->append_param(key(&resource, "date"));
        expressions.emplace_back(std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(&resource, aggregate_type::sum, key(&resource, "total"));
        auto expr_multiply = make_scalar_expression(&resource, scalar_type::multiply);
        expr_multiply->append_param(key(&resource, "price"));
        expr_multiply->append_param(key(&resource, "quantity"));
        agg_expr->append_param(std::move(expr_multiply));
        expressions.emplace_back(std::move(agg_expr));
        agg_expr = make_aggregate_expression(&resource, aggregate_type::avg, key(&resource, "avg_quantity"));
        agg_expr->append_param(key(&resource, "quantity"));
        expressions.emplace_back(std::move(agg_expr));
        auto node_group = make_node_group(&resource, get_name(), expressions);
        REQUIRE(
            node_group->to_string() ==
            R"_($group: {_id: "$date", total: {$sum: {$multiply: ["$price", "$quantity"]}}, avg_quantity: {$avg: "$quantity"}})_");
    }
    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "_id"));
        scalar_expr->append_param(key(&resource, "date"));
        expressions.emplace_back(std::move(scalar_expr));
        scalar_expr = make_scalar_expression(&resource, scalar_type::multiply, key(&resource, "count_4"));
        scalar_expr->append_param(core::parameter_id_t(1));
        scalar_expr->append_param(key(&resource, "count"));
        expressions.emplace_back(std::move(scalar_expr));
        auto node_group = make_node_group(&resource, get_name(), expressions);
        REQUIRE(node_group->to_string() == R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}})_");
    }
}

TEST_CASE("logical_plan::sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{key(&resource, "key"), sort_order::asc});
        auto node_sort = make_node_sort(&resource, get_name(), expressions);
        REQUIRE(node_sort->to_string() == R"_($sort: {key: 1})_");
    }
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{key(&resource, "key1"), sort_order::asc});
        expressions.emplace_back(new sort_expression_t{key(&resource, "key2"), sort_order::desc});
        auto node_sort = make_node_sort(&resource, get_name(), expressions);
        REQUIRE(node_sort->to_string() == R"_($sort: {key1: 1, key2: -1})_");
    }
}

TEST_CASE("logical_plan::aggregate") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto aggregate = make_node_aggregate(&resource, {database_name, collection_name});

    aggregate->append_child(make_node_match(&resource,
                                            {database_name, collection_name},
                                            make_compare_expression(&resource,
                                                                    compare_type::eq,
                                                                    key(&resource, "key", side_t::left),
                                                                    core::parameter_id_t(1))));

    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "_id"));
        scalar_expr->append_param(key(&resource, "date"));
        expressions.emplace_back(std::move(scalar_expr));
        scalar_expr = make_scalar_expression(&resource, scalar_type::multiply, key(&resource, "count_4"));
        scalar_expr->append_param(core::parameter_id_t(1));
        scalar_expr->append_param(key(&resource, "count"));
        expressions.emplace_back(std::move(scalar_expr));
        aggregate->append_child(make_node_group(&resource, {database_name, collection_name}, expressions));
    }
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{key(&resource, "name"), sort_order::asc});
        expressions.emplace_back(new sort_expression_t{key(&resource, "count"), sort_order::desc});
        aggregate->append_child(make_node_sort(&resource, {database_name, collection_name}, expressions));
    }

    components::planner::planner_t planner;
    auto node_aggregate = planner.create_plan(&resource, aggregate);

    REQUIRE(node_aggregate->to_string() == R"_($aggregate: {)_"
                                           R"_($match: {"key": {$eq: #1}}, )_"
                                           R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}}, )_"
                                           R"_($sort: {name: 1, count: -1})_"
                                           R"_(})_");
}

TEST_CASE("logical_plan::insert") {
    auto resource = std::pmr::synchronized_pool_resource();
    {
        std::pmr::vector<components::document::document_ptr> documents = {};
        auto plan = make_node_insert(&resource, {database_name, collection_name}, std::move(documents));
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        REQUIRE(node->to_string() == R"_($insert: {$raw_data: {$rows: 0}})_");
    }
    {
        std::pmr::vector<components::document::document_ptr> documents = {gen_doc(1, &resource)};
        auto plan = make_node_insert(&resource, {database_name, collection_name}, std::move(documents));
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        REQUIRE(node->to_string() == R"_($insert: {$raw_data: {$rows: 1}})_");
    }
    {
        std::pmr::vector<components::document::document_ptr> documents = {gen_doc(1, &resource),
                                                                          gen_doc(2, &resource),
                                                                          gen_doc(3, &resource),
                                                                          gen_doc(4, &resource),
                                                                          gen_doc(5, &resource)};
        auto plan = make_node_insert(&resource, {database_name, collection_name}, std::move(documents));
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        REQUIRE(node->to_string() == R"_($insert: {$raw_data: {$rows: 5}})_");
    }
    {
        auto plan = make_node_insert(&resource, {database_name, collection_name}, {gen_doc(1, &resource)});
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        REQUIRE(node->to_string() == R"_($insert: {$raw_data: {$rows: 1}})_");
    }
}

TEST_CASE("logical_plan::limit") {
    auto resource = std::pmr::synchronized_pool_resource();
    {
        auto limit = limit_t::limit_one();
        auto node_limit = make_node_limit(&resource, get_name(), limit);
        REQUIRE(node_limit->to_string() == R"_($limit: 1)_");
    }
    {
        auto limit = limit_t::unlimit();
        auto node_limit = make_node_limit(&resource, get_name(), limit);
        REQUIRE(node_limit->to_string() == R"_($limit: -1)_");
    }
    {
        auto limit = limit_t(5);
        auto node_limit = make_node_limit(&resource, get_name(), limit);
        REQUIRE(node_limit->to_string() == R"_($limit: 5)_");
    }
}

TEST_CASE("logical_plan::delete") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto match = make_node_match(&resource,
                                 {database_name, collection_name},
                                 make_compare_expression(&resource,
                                                         compare_type::eq,
                                                         key(&resource, "key", side_t::left),
                                                         core::parameter_id_t(1)));
    components::logical_plan::storage_parameters parameters{&resource};
    {
        auto node = make_node_delete_many(&resource, {database_name, collection_name}, match);
        components::planner::planner_t planner;
        auto node_delete = planner.create_plan(&resource, node);
        REQUIRE(node_delete->to_string() == R"_($delete: {$match: {"key": {$eq: #1}}, $limit: -1})_");
    }
    {
        auto node = make_node_delete_one(&resource, {database_name, collection_name}, match);
        components::planner::planner_t planner;
        auto node_delete = planner.create_plan(&resource, node);
        REQUIRE(node_delete->to_string() == R"_($delete: {$match: {"key": {$eq: #1}}, $limit: 1})_");
    }
}

TEST_CASE("logical_plan::update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto match = make_node_match(&resource,
                                 {database_name, collection_name},
                                 make_compare_expression(&resource,
                                                         compare_type::eq,
                                                         key(&resource, "key", side_t::left),
                                                         core::parameter_id_t(1)));

    update_expr_ptr update = new update_expr_set_t(components::expressions::key_t{&resource, "count"});
    update->left() = new update_expr_get_const_value_t(core::parameter_id_t(0));

    components::logical_plan::storage_parameters parameters{&resource};
    {
        auto node = make_node_update_many(&resource, {database_name, collection_name}, match, {update}, true);
        components::planner::planner_t planner;
        auto node_update = planner.create_plan(&resource, node);
        REQUIRE(node_update->to_string() == R"_($update: {$upsert: 1, $match: {"key": {$eq: #1}}, $limit: -1})_");
    }
    {
        auto node = make_node_update_one(&resource, {database_name, collection_name}, match, {update}, false);
        components::planner::planner_t planner;
        auto node_update = planner.create_plan(&resource, node);
        REQUIRE(node_update->to_string() == R"_($update: {$upsert: 0, $match: {"key": {$eq: #1}}, $limit: 1})_");
    }
}
