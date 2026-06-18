#include <catch2/catch.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/substrait_adapter/from_substrait.hpp>
#include <components/logical_plan/substrait_adapter/to_substrait.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

namespace {
    collection_full_name_t collection_name() { return {"db", "coll"}; }

    components::vector::data_chunk_t sample_chunk(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::INTEGER, "id");
        types.emplace_back(components::types::logical_type::BOOLEAN, "enabled");
        types.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
        types.emplace_back(components::types::logical_type::DOUBLE, "score");

        components::vector::data_chunk_t chunk(resource, types, 2);
        chunk.set_cardinality(2);
        chunk.set_value(0, 0, components::types::logical_value_t(resource, int32_t{1}));
        chunk.set_value(1, 0, components::types::logical_value_t(resource, true));
        chunk.set_value(2, 0, components::types::logical_value_t(resource, std::string("alice")));
        chunk.set_value(3, 0, components::types::logical_value_t(resource, 10.5));
        chunk.set_value(0, 1, components::types::logical_value_t(resource, int32_t{2}));
        chunk.set_value(1, 1, components::types::logical_value_t(resource, false));
        chunk.set_value(2, 1, components::types::logical_value_t(resource, std::string("bob")));
        chunk.set_value(3, 1, components::types::logical_value_t(resource, 20.25));
        return chunk;
    }

    void require_sample_chunk(const components::vector::data_chunk_t& chunk) {
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.column_count() == 4);
        const auto types = chunk.types();
        REQUIRE(types[0].alias() == "id");
        REQUIRE(types[1].alias() == "enabled");
        REQUIRE(types[2].alias() == "name");
        REQUIRE(types[3].alias() == "score");
        REQUIRE(chunk.value(0, 0).value<int32_t>() == 1);
        REQUIRE(chunk.value(1, 0).value<bool>());
        REQUIRE(chunk.value(2, 0).value<std::string_view>() == "alice");
        REQUIRE(chunk.value(3, 0).value<double>() == Approx(10.5));
        REQUIRE(chunk.value(0, 1).value<int32_t>() == 2);
        REQUIRE_FALSE(chunk.value(1, 1).value<bool>());
        REQUIRE(chunk.value(2, 1).value<std::string_view>() == "bob");
        REQUIRE(chunk.value(3, 1).value<double>() == Approx(20.25));
    }

    struct roundtrip_result_t {
        node_ptr json;
        node_ptr binary;
    };

    roundtrip_result_t require_roundtrip(std::pmr::memory_resource* resource, const node_ptr& plan) {
        const auto json = substrait_adapter::to_substrait_json(plan);
        const auto restored_json = substrait_adapter::from_substrait_json(resource, json);
        REQUIRE(restored_json.plan != nullptr);
        REQUIRE(restored_json.plan->to_string() == plan->to_string());

        const auto binary = substrait_adapter::to_substrait_binary(plan);
        const auto restored_binary = substrait_adapter::from_substrait_binary(resource, binary);
        REQUIRE(restored_binary.plan != nullptr);
        REQUIRE(restored_binary.plan->to_string() == plan->to_string());
        return {restored_json.plan, restored_binary.plan};
    }

    template<class Check>
    void require_each(const roundtrip_result_t& result, Check check) {
        check(result.json);
        check(result.binary);
    }

    node_match_ptr make_id_match(std::pmr::memory_resource* resource) {
        return make_node_match(resource,
                               collection_name(),
                               make_compare_expression(resource,
                                                       compare_type::eq,
                                                       key(resource, "id", side_t::left),
                                                       core::parameter_id_t(1)));
    }

    components::expressions::update_expr_ptr make_count_update(std::pmr::memory_resource* resource) {
        update_expr_ptr update = new update_expr_set_t(key(resource, "count"));
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
        return update;
    }
} // namespace

TEST_CASE("components::planner::substrait::roundtrip_node_aggregate") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_aggregate(&resource, collection_name()));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->collection_full_name().database == "db");
        REQUIRE(node->collection_full_name().collection == "coll");
        REQUIRE(node->children().empty());
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_match") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, collection_name());
    plan->append_child(make_id_match(&resource));
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->children().size() == 1);
        REQUIRE(node->children().front()->type() == node_type::match_t);
        REQUIRE(node->children().front()->expressions().size() == 1);
        REQUIRE(node->children().front()->expressions().front()->group() == expression_group::compare);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_group_project") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, collection_name());

    std::vector<expression_ptr> expressions;
    auto id_alias = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "id_alias"));
    id_alias->append_param(key(&resource, "id"));
    expressions.emplace_back(std::move(id_alias));

    auto score_abs = make_scalar_expression(&resource, scalar_type::abs, key(&resource, "score_abs"));
    score_abs->append_param(key(&resource, "score"));
    expressions.emplace_back(std::move(score_abs));

    plan->append_child(make_node_group(&resource, collection_name(), expressions));
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->children().size() == 1);
        const auto& group = node->children().front();
        REQUIRE(group->type() == node_type::group_t);
        REQUIRE(group->expressions().size() == 2);
        const auto* first = static_cast<const scalar_expression_t*>(group->expressions().at(0).get());
        const auto* second = static_cast<const scalar_expression_t*>(group->expressions().at(1).get());
        REQUIRE(first->type() == scalar_type::get_field);
        REQUIRE(first->key().as_string() == "id_alias");
        REQUIRE(second->type() == scalar_type::abs);
        REQUIRE(second->key().as_string() == "score_abs");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_group_aggregate") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, collection_name());

    std::vector<expression_ptr> expressions;
    auto category = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "category"));
    category->append_param(key(&resource, "category"));
    expressions.emplace_back(std::move(category));

    auto count = make_aggregate_expression(&resource, "count", key(&resource, "cnt"));
    count->append_param(key(&resource, "id"));
    expressions.emplace_back(std::move(count));

    plan->append_child(make_node_group(&resource, collection_name(), expressions));
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->children().size() == 1);
        const auto& group = node->children().front();
        REQUIRE(group->type() == node_type::group_t);
        REQUIRE(group->expressions().size() == 2);
        REQUIRE(group->expressions().at(0)->group() == expression_group::scalar);
        REQUIRE(group->expressions().at(1)->group() == expression_group::aggregate);
        const auto* aggregate = static_cast<const aggregate_expression_t*>(group->expressions().at(1).get());
        REQUIRE(aggregate->function_name() == "count");
        REQUIRE(aggregate->key().as_string() == "cnt");
        REQUIRE(aggregate->params().size() == 1);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, collection_name());

    std::vector<expression_ptr> expressions;
    expressions.emplace_back(new sort_expression_t{key(&resource, "score"), sort_order::desc});
    plan->append_child(make_node_sort(&resource, collection_name(), expressions));

    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->children().size() == 1);
        const auto& sort = node->children().front();
        REQUIRE(sort->type() == node_type::sort_t);
        REQUIRE(sort->expressions().size() == 1);
        const auto* sort_expr = static_cast<const sort_expression_t*>(sort->expressions().front().get());
        REQUIRE(sort_expr->key().as_string() == "score");
        REQUIRE(sort_expr->order() == sort_order::desc);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_join") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, {});

    auto join = make_node_join(&resource, {}, join_type::left);
    join->append_child(make_node_aggregate(&resource, {"db", "orders"}));
    join->append_child(make_node_aggregate(&resource, {"db", "users"}));
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    key(&resource, "user_id", side_t::left),
                                                    key(&resource, "id", side_t::right)));
    plan->append_child(join);

    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->children().size() == 1);
        const auto& join = node->children().front();
        REQUIRE(join->type() == node_type::join_t);
        REQUIRE(static_cast<const node_join_t*>(join.get())->type() == join_type::left);
        REQUIRE(join->children().size() == 2);
        REQUIRE(join->children().at(0)->collection_full_name().collection == "orders");
        REQUIRE(join->children().at(1)->collection_full_name().collection == "users");
        REQUIRE(join->expressions().size() == 1);
        REQUIRE(join->expressions().front()->group() == expression_group::compare);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_limit") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_aggregate(&resource, collection_name());
    plan->append_child(make_node_limit(&resource, collection_name(), limit_t(25)));
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::aggregate_t);
        REQUIRE(node->children().size() == 1);
        REQUIRE(node->children().front()->type() == node_type::limit_t);
        REQUIRE(static_cast<const node_limit_t*>(node->children().front().get())->limit().limit() == 25);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_create_database") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_create_database(&resource, {"db", {}}));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::create_database_t);
        REQUIRE(node->collection_full_name().database == "db");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_drop_database") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_drop_database(&resource, {"db", {}}));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::drop_database_t);
        REQUIRE(node->collection_full_name().database == "db");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_create_collection") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_create_collection(&resource, collection_name()));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::create_collection_t);
        REQUIRE(node->collection_full_name().database == "db");
        REQUIRE(node->collection_full_name().collection == "coll");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_drop_collection") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_drop_collection(&resource, collection_name()));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::drop_collection_t);
        REQUIRE(node->collection_full_name().database == "db");
        REQUIRE(node->collection_full_name().collection == "coll");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_create_index") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_create_index(&resource, collection_name(), "idx_ab", index_type::composite);
    plan->keys().emplace_back(&resource, "a");
    plan->keys().emplace_back(&resource, "b");
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::create_index_t);
        REQUIRE(node->collection_full_name().collection == "coll");
        auto* index = static_cast<node_create_index_t*>(node.get());
        REQUIRE(index->name() == "idx_ab");
        REQUIRE(index->type() == index_type::composite);
        REQUIRE(index->keys().size() == 2);
        REQUIRE(index->keys().at(0).as_string() == "a");
        REQUIRE(index->keys().at(1).as_string() == "b");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_drop_index") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_drop_index(&resource, collection_name(), "idx_ab"));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::drop_index_t);
        REQUIRE(node->collection_full_name().collection == "coll");
        REQUIRE(static_cast<const node_drop_index_t*>(node.get())->name() == "idx_ab");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_create_type") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::vector<components::types::complex_logical_type> fields;
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
    fields.emplace_back(components::types::logical_type::INTEGER, "count");
    auto type = components::types::complex_logical_type::create_struct("metric_payload", fields);

    auto plan = make_node_create_type(&resource, std::move(type));
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::create_type_t);
    });

    const auto restored = substrait_adapter::from_substrait_json(&resource, substrait_adapter::to_substrait_json(plan));
    const auto restored_type = reinterpret_cast<const node_create_type_ptr&>(restored.plan)->type();
    REQUIRE(restored_type.type_name() == "metric_payload");
    REQUIRE(restored_type.child_types().size() == 2);
    REQUIRE(restored_type.child_types()[0].alias() == "name");
    REQUIRE(restored_type.child_types()[1].alias() == "count");
}

TEST_CASE("components::planner::substrait::roundtrip_node_drop_type") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_drop_type(&resource, std::string("metric_payload")));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::drop_type_t);
        REQUIRE(static_cast<const node_drop_type_t*>(node.get())->name() == "metric_payload");
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_data") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_raw_data(&resource, sample_chunk(&resource));

    const auto json = substrait_adapter::to_substrait_json(plan);
    const auto restored_json = substrait_adapter::from_substrait_json(&resource, json);
    REQUIRE(restored_json.plan->type() == node_type::data_t);
    require_sample_chunk(reinterpret_cast<const node_data_ptr&>(restored_json.plan)->data_chunk());

    const auto binary = substrait_adapter::to_substrait_binary(plan);
    const auto restored_binary = substrait_adapter::from_substrait_binary(&resource, binary);
    REQUIRE(restored_binary.plan->type() == node_type::data_t);
    require_sample_chunk(reinterpret_cast<const node_data_ptr&>(restored_binary.plan)->data_chunk());
}

TEST_CASE("components::planner::substrait::roundtrip_node_insert") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto plan = make_node_insert(&resource, collection_name(), sample_chunk(&resource));
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::insert_t);
        REQUIRE(node->collection_full_name().collection == "coll");
        REQUIRE(node->children().size() == 1);
        REQUIRE(node->children().front()->type() == node_type::data_t);
        require_sample_chunk(reinterpret_cast<const node_data_ptr&>(node->children().front())->data_chunk());
    });

    const auto restored = substrait_adapter::from_substrait_json(&resource, substrait_adapter::to_substrait_json(plan));
    REQUIRE(restored.plan->type() == node_type::insert_t);
    const auto insert = reinterpret_cast<const node_insert_ptr&>(restored.plan);
    REQUIRE(insert->children().size() == 1);
    REQUIRE(insert->children().front()->type() == node_type::data_t);
    require_sample_chunk(reinterpret_cast<const node_data_ptr&>(insert->children().front())->data_chunk());
}

TEST_CASE("components::planner::substrait::roundtrip_node_delete") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto result = require_roundtrip(&resource, make_node_delete_one(&resource, collection_name(), make_id_match(&resource)));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::delete_t);
        REQUIRE(node->collection_full_name().collection == "coll");
        REQUIRE(node->children().size() == 2);
        REQUIRE(node->children().at(0)->type() == node_type::match_t);
        REQUIRE(node->children().at(1)->type() == node_type::limit_t);
        REQUIRE(static_cast<const node_limit_t*>(node->children().at(1).get())->limit().limit() == 1);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_update") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<update_expr_ptr> updates(&resource);
    updates.emplace_back(make_count_update(&resource));
    auto plan = make_node_update_one(&resource, collection_name(), make_id_match(&resource), updates, false);
    auto result = require_roundtrip(&resource, plan);
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::update_t);
        REQUIRE(node->collection_full_name().collection == "coll");
        const auto* update = static_cast<const node_update_t*>(node.get());
        REQUIRE_FALSE(update->upsert());
        REQUIRE(update->updates().size() == 1);
        REQUIRE(node->children().size() == 2);
        REQUIRE(node->children().at(0)->type() == node_type::match_t);
        REQUIRE(node->children().at(1)->type() == node_type::limit_t);
        REQUIRE(static_cast<const node_limit_t*>(node->children().at(1).get())->limit().limit() == 1);
    });
}

TEST_CASE("components::planner::substrait::roundtrip_node_function") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<param_storage> args(&resource);
    args.emplace_back(core::parameter_id_t(3));
    args.emplace_back(key(&resource, "field_a"));
    auto result = require_roundtrip(&resource, make_node_function(&resource, std::string("my_func"), std::move(args)));
    require_each(result, [](const node_ptr& node) {
        REQUIRE(node->type() == node_type::function_t);
        const auto* function = static_cast<const node_function_t*>(node.get());
        REQUIRE(function->name() == "my_func");
        REQUIRE(function->args().size() == 2);
        REQUIRE(std::holds_alternative<core::parameter_id_t>(function->args().at(0)));
        REQUIRE(std::holds_alternative<components::expressions::key_t>(function->args().at(1)));
        REQUIRE(std::get<components::expressions::key_t>(function->args().at(1)).as_string() == "field_a");
    });
}
