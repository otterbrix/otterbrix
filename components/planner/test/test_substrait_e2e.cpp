#include <catch2/catch.hpp>

#include <components/catalog/catalog.hpp>
#include <components/catalog/schema.hpp>
#include <components/catalog/table_id.hpp>
#include <components/catalog/table_metadata.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/substrait_adapter/from_substrait.hpp>
#include <components/logical_plan/substrait_adapter/to_substrait.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

using namespace components::expressions;
using namespace components::logical_plan;
using key = components::expressions::key_t;

namespace {
    struct e2e_roundtrip_result_t {
        node_ptr json;
        node_ptr binary;
    };

    collection_full_name_t orders_collection() { return {"db", "orders"}; }
    collection_full_name_t users_collection() { return {"db", "users"}; }
    collection_full_name_t archive_collection() { return {"db", "orders_archive"}; }

    void register_table(components::catalog::catalog& catalog,
                        std::pmr::memory_resource* resource,
                        const collection_full_name_t& name,
                        std::vector<components::types::complex_logical_type> fields) {
        std::vector<components::table::column_definition_t> columns;
        std::vector<components::types::field_description> descriptions;
        columns.reserve(fields.size());
        descriptions.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            auto column_name = fields[i].has_alias() ? fields[i].alias() : ("col_" + std::to_string(i));
            columns.emplace_back(std::move(column_name), std::move(fields[i]));
            descriptions.emplace_back(i + 1);
        }
        std::pmr::vector<components::catalog::field_id_t> primary_key(resource);
        primary_key.emplace_back(1);
        components::catalog::schema schema(resource, columns, descriptions, primary_key);
        auto err = catalog.create_table({resource, name}, {resource, schema});
        REQUIRE_FALSE(err.contains_error());
    }

    void populate_orders_catalog(components::catalog::catalog& catalog, std::pmr::memory_resource* resource) {
        catalog.create_namespace({"db"});
        register_table(catalog,
                       resource,
                       orders_collection(),
                       {
                           {components::types::logical_type::INTEGER, "id"},
                           {components::types::logical_type::INTEGER, "user_id"},
                           {components::types::logical_type::DOUBLE, "amount"},
                           {components::types::logical_type::STRING_LITERAL, "status"},
                       });
        register_table(catalog,
                       resource,
                       users_collection(),
                       {
                           {components::types::logical_type::INTEGER, "id"},
                           {components::types::logical_type::STRING_LITERAL, "name"},
                           {components::types::logical_type::STRING_LITERAL, "region"},
                       });
    }

    e2e_roundtrip_result_t require_e2e_roundtrip(std::pmr::memory_resource* resource, const node_ptr& plan) {
        const auto json = substrait_adapter::to_substrait_json(plan);
        const auto json_plan = substrait_adapter::from_substrait_json(resource, json);
        REQUIRE(json_plan.plan != nullptr);
        REQUIRE(json_plan.plan->to_string() == plan->to_string());

        const auto binary = substrait_adapter::to_substrait_binary(plan);
        const auto binary_plan = substrait_adapter::from_substrait_binary(resource, binary);
        REQUIRE(binary_plan.plan != nullptr);
        REQUIRE(binary_plan.plan->to_string() == plan->to_string());

        return {json_plan.plan, binary_plan.plan};
    }

    e2e_roundtrip_result_t require_e2e_roundtrip(std::pmr::memory_resource* resource,
                                                 const node_ptr& plan,
                                                 const components::catalog::catalog& catalog) {
        const auto json = substrait_adapter::to_substrait_json(plan, &catalog);
        const auto json_plan = substrait_adapter::from_substrait_json(resource, json);
        REQUIRE(json_plan.plan != nullptr);
        REQUIRE(json_plan.plan->to_string() == plan->to_string());

        const auto binary = substrait_adapter::to_substrait_binary(plan, &catalog);
        const auto binary_plan = substrait_adapter::from_substrait_binary(resource, binary);
        REQUIRE(binary_plan.plan != nullptr);
        REQUIRE(binary_plan.plan->to_string() == plan->to_string());

        return {json_plan.plan, binary_plan.plan};
    }

    template<class Check>
    void require_each_plan(const e2e_roundtrip_result_t& result, Check check) {
        check(result.json);
        check(result.binary);
    }

    expression_ptr field_projection(std::pmr::memory_resource* resource,
                                    std::string_view source,
                                    std::string_view alias) {
        auto expr = make_scalar_expression(resource, scalar_type::get_field, key(resource, alias));
        expr->append_param(key(resource, source));
        return expr;
    }

    expression_ptr abs_projection(std::pmr::memory_resource* resource,
                                  std::string_view source,
                                  std::string_view alias) {
        auto expr = make_scalar_expression(resource, scalar_type::abs, key(resource, alias));
        expr->append_param(key(resource, source));
        return expr;
    }

    node_match_ptr match_parameter(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   std::string_view field,
                                   compare_type type,
                                   core::parameter_id_t parameter_id) {
        return make_node_match(resource,
                               collection,
                               make_compare_expression(resource,
                                                       type,
                                                       key(resource, field, side_t::left),
                                                       parameter_id));
    }

    components::vector::data_chunk_t orders_chunk(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::INTEGER, "id");
        types.emplace_back(components::types::logical_type::INTEGER, "user_id");
        types.emplace_back(components::types::logical_type::DOUBLE, "amount");

        components::vector::data_chunk_t chunk(resource, types, 2);
        chunk.set_cardinality(2);
        chunk.set_value(0, 0, components::types::logical_value_t(resource, int32_t{1}));
        chunk.set_value(1, 0, components::types::logical_value_t(resource, int32_t{10}));
        chunk.set_value(2, 0, components::types::logical_value_t(resource, 100.5));
        chunk.set_value(0, 1, components::types::logical_value_t(resource, int32_t{2}));
        chunk.set_value(1, 1, components::types::logical_value_t(resource, int32_t{11}));
        chunk.set_value(2, 1, components::types::logical_value_t(resource, 250.0));
        return chunk;
    }

    update_expr_ptr set_amount_from_parameter(std::pmr::memory_resource* resource) {
        update_expr_ptr update = new update_expr_set_t(key(resource, "amount"));
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t(7));
        return update;
    }

    const node_ptr& only_child(const node_ptr& node, node_type expected_type) {
        REQUIRE(node->children().size() == 1);
        REQUIRE(node->children().front()->type() == expected_type);
        return node->children().front();
    }
} // namespace

TEST_CASE("components::planner::substrait::e2e_read_filter_project_sort_limit") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, orders_collection());

    plan->append_child(match_parameter(&resource, orders_collection(), "amount", compare_type::gte, core::parameter_id_t(1)));

    std::vector<expression_ptr> projections;
    projections.emplace_back(field_projection(&resource, "id", "order_id"));
    projections.emplace_back(field_projection(&resource, "user_id", "user_id"));
    projections.emplace_back(abs_projection(&resource, "amount", "amount_abs"));
    plan->append_child(make_node_group(&resource, orders_collection(), projections));

    std::vector<expression_ptr> sort_by_amount;
    sort_by_amount.emplace_back(new sort_expression_t{key(&resource, "amount_abs"), sort_order::desc});
    plan->append_child(make_node_sort(&resource, orders_collection(), sort_by_amount));
    plan->append_child(make_node_limit(&resource, orders_collection(), limit_t(10)));

    auto result = require_e2e_roundtrip(&resource, plan);
    require_each_plan(result, [](const node_ptr& restored) {
        REQUIRE(restored->type() == node_type::aggregate_t);
        REQUIRE(restored->children().size() == 4);
        REQUIRE(restored->children().at(0)->type() == node_type::match_t);
        REQUIRE(restored->children().at(1)->type() == node_type::group_t);
        REQUIRE(restored->children().at(2)->type() == node_type::sort_t);
        REQUIRE(restored->children().at(3)->type() == node_type::limit_t);
        REQUIRE(restored->children().at(1)->expressions().size() == 3);
        REQUIRE(restored->children().at(2)->expressions().size() == 1);
        REQUIRE(static_cast<const node_limit_t*>(restored->children().at(3).get())->limit().limit() == 10);
    });
}

TEST_CASE("components::planner::substrait::e2e_grouped_analytics") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, orders_collection());
    plan->append_child(match_parameter(&resource, orders_collection(), "amount", compare_type::gt, core::parameter_id_t(2)));

    std::vector<expression_ptr> group_expressions;
    group_expressions.emplace_back(field_projection(&resource, "user_id", "user_id"));

    auto count_orders = make_aggregate_expression(&resource, "count", key(&resource, "orders_count"));
    count_orders->append_param(key(&resource, "id"));
    group_expressions.emplace_back(std::move(count_orders));

    auto avg_amount = make_aggregate_expression(&resource, "avg", key(&resource, "avg_amount"));
    avg_amount->append_param(key(&resource, "amount"));
    group_expressions.emplace_back(std::move(avg_amount));

    plan->append_child(make_node_group(&resource, orders_collection(), group_expressions));

    auto result = require_e2e_roundtrip(&resource, plan);
    require_each_plan(result, [](const node_ptr& restored) {
        REQUIRE(restored->children().size() == 2);
        REQUIRE(restored->children().at(0)->type() == node_type::match_t);
        const auto& group = restored->children().at(1);
        REQUIRE(group->type() == node_type::group_t);
        REQUIRE(group->expressions().size() == 3);
        REQUIRE(group->expressions().at(0)->group() == expression_group::scalar);
        REQUIRE(group->expressions().at(1)->group() == expression_group::aggregate);
        REQUIRE(group->expressions().at(2)->group() == expression_group::aggregate);
    });
}

TEST_CASE("components::planner::substrait::e2e_join_filter_project_limit") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, {});

    auto join = make_node_join(&resource, {}, join_type::inner);
    join->append_child(make_node_aggregate(&resource, orders_collection()));
    join->append_child(make_node_aggregate(&resource, users_collection()));
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    key(&resource, "user_id", side_t::left),
                                                    key(&resource, "id", side_t::right)));
    plan->append_child(join);
    plan->append_child(match_parameter(&resource, orders_collection(), "user_id", compare_type::lt, core::parameter_id_t(3)));

    std::vector<expression_ptr> projections;
    projections.emplace_back(field_projection(&resource, "user_id", "order_user_id"));
    projections.emplace_back(field_projection(&resource, "id", "joined_user_id"));
    plan->append_child(make_node_group(&resource, orders_collection(), projections));
    plan->append_child(make_node_limit(&resource, orders_collection(), limit_t(5)));

    auto result = require_e2e_roundtrip(&resource, plan);
    require_each_plan(result, [](const node_ptr& restored) {
        REQUIRE(restored->children().size() == 4);
        const auto& join = restored->children().at(0);
        REQUIRE(join->type() == node_type::join_t);
        REQUIRE(static_cast<const node_join_t*>(join.get())->type() == join_type::inner);
        REQUIRE(join->children().size() == 2);
        REQUIRE(join->expressions().size() == 1);
        REQUIRE(restored->children().at(1)->type() == node_type::match_t);
        REQUIRE(restored->children().at(2)->type() == node_type::group_t);
        REQUIRE(restored->children().at(3)->type() == node_type::limit_t);
    });
}

TEST_CASE("components::planner::substrait::e2e_catalog_schema_join_project_sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    components::catalog::catalog catalog(&resource);
    populate_orders_catalog(catalog, &resource);

    auto plan = make_node_aggregate(&resource, {});
    auto join = make_node_join(&resource, {}, join_type::left);
    join->append_child(make_node_aggregate(&resource, orders_collection()));
    join->append_child(make_node_aggregate(&resource, users_collection()));
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    key(&resource, "user_id", side_t::left),
                                                    key(&resource, "id", side_t::right)));
    plan->append_child(join);
    plan->append_child(match_parameter(&resource, orders_collection(), "status", compare_type::eq, core::parameter_id_t(8)));

    std::vector<expression_ptr> projections;
    projections.emplace_back(field_projection(&resource, "amount", "order_amount"));
    projections.emplace_back(field_projection(&resource, "name", "user_name"));
    projections.emplace_back(field_projection(&resource, "region", "user_region"));
    plan->append_child(make_node_group(&resource, orders_collection(), projections));

    std::vector<expression_ptr> sort_by_region;
    sort_by_region.emplace_back(new sort_expression_t{key(&resource, "user_region"), sort_order::asc});
    plan->append_child(make_node_sort(&resource, orders_collection(), sort_by_region));
    plan->append_child(make_node_limit(&resource, orders_collection(), limit_t(20)));

    auto result = require_e2e_roundtrip(&resource, plan, catalog);
    require_each_plan(result, [](const node_ptr& restored) {
        REQUIRE(restored->children().size() == 5);
        REQUIRE(restored->children().at(0)->type() == node_type::join_t);
        REQUIRE(static_cast<const node_join_t*>(restored->children().at(0).get())->type() == join_type::left);
        REQUIRE(restored->children().at(1)->type() == node_type::match_t);
        const auto& project = restored->children().at(2);
        REQUIRE(project->type() == node_type::group_t);
        REQUIRE(project->expressions().size() == 3);
        REQUIRE(static_cast<const scalar_expression_t*>(project->expressions().at(0).get())->key().as_string() ==
                "order_amount");
        REQUIRE(static_cast<const scalar_expression_t*>(project->expressions().at(1).get())->key().as_string() ==
                "user_name");
        REQUIRE(static_cast<const scalar_expression_t*>(project->expressions().at(2).get())->key().as_string() ==
                "user_region");
        REQUIRE(restored->children().at(3)->type() == node_type::sort_t);
        REQUIRE(restored->children().at(4)->type() == node_type::limit_t);
    });
}

TEST_CASE("components::planner::substrait::e2e_write_insert_delete_update") {
    auto resource = std::pmr::synchronized_pool_resource();

    auto insert_result = require_e2e_roundtrip(&resource,
                                               make_node_insert(&resource, orders_collection(), orders_chunk(&resource)));
    require_each_plan(insert_result, [](const node_ptr& restored) {
        REQUIRE(restored->type() == node_type::insert_t);
        only_child(restored, node_type::data_t);
    });

    auto delete_result = require_e2e_roundtrip(
        &resource,
        make_node_delete_many(&resource,
                              archive_collection(),
                              orders_collection(),
                              match_parameter(&resource, orders_collection(), "amount", compare_type::lte, core::parameter_id_t(4))));
    require_each_plan(delete_result, [](const node_ptr& restored) {
        REQUIRE(restored->type() == node_type::delete_t);
        REQUIRE(static_cast<const node_delete_t*>(restored.get())->collection_from().collection == "orders");
        REQUIRE(restored->children().size() == 2);
        REQUIRE(restored->children().at(0)->type() == node_type::match_t);
        REQUIRE(restored->children().at(1)->type() == node_type::limit_t);
    });

    std::pmr::vector<update_expr_ptr> updates(&resource);
    updates.emplace_back(set_amount_from_parameter(&resource));
    auto update_result = require_e2e_roundtrip(
        &resource,
        make_node_update_many(&resource,
                              archive_collection(),
                              orders_collection(),
                              match_parameter(&resource, orders_collection(), "id", compare_type::eq, core::parameter_id_t(6)),
                              updates,
                              true));
    require_each_plan(update_result, [](const node_ptr& restored) {
        REQUIRE(restored->type() == node_type::update_t);
        const auto* update = static_cast<const node_update_t*>(restored.get());
        REQUIRE(update->collection_from().collection == "orders");
        REQUIRE(update->upsert());
        REQUIRE(update->updates().size() == 1);
        REQUIRE(restored->children().size() == 2);
        REQUIRE(restored->children().at(0)->type() == node_type::match_t);
        REQUIRE(restored->children().at(1)->type() == node_type::limit_t);
    });
}
