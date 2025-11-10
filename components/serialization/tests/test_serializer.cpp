#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <tests/generaty.hpp>

using namespace components::serializer;
using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() { return {database_name, collection_name}; }

TEST_CASE("serialization::document") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto doc1 = gen_doc(10, &resource);
    {
        msgpack_serializer_t serializer(&resource);
        serializer.start_array(1);
        serializer.append(doc1);
        serializer.end_array();
        msgpack_deserializer_t deserializer(serializer.result());
        auto doc2 = deserializer.deserialize_document(0);
        REQUIRE(doc1->count() == doc2->count());
        REQUIRE(doc1->get_string("/_id") == doc2->get_string("/_id"));
        REQUIRE(doc1->get_long("/count") == doc2->get_long("/count"));
        REQUIRE(doc1->get_string("/countStr") == doc2->get_string("/countStr"));
        REQUIRE(doc1->get_double("/countDouble") == Approx(doc2->get_double("/countDouble")));
        REQUIRE(doc1->get_bool("/countBool") == doc2->get_bool("/countBool"));
        REQUIRE(doc1->get_array("/countArray")->count() == doc2->get_array("/countArray")->count());
        REQUIRE(doc1->get_dict("/countDict")->count() == doc2->get_dict("/countDict")->count());
        REQUIRE(doc1->get_dict("/null") == doc2->get_dict("/null"));
    }
}
TEST_CASE("serialization::data_chunk") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto chunk1 = gen_data_chunk(10, &resource);
    {
        msgpack_serializer_t serializer(&resource);
        serializer.start_array(1);
        chunk1.serialize(&serializer);
        serializer.end_array();
        msgpack_deserializer_t deserializer(serializer.result());
        deserializer.advance_array(0);
        auto chunk2 = components::vector::data_chunk_t::deserialize(&deserializer);
        deserializer.pop_array();
        for (size_t i = 0; i < chunk1.column_count(); i++) {
            for (size_t j = 0; j < chunk1.size(); j++) {
                REQUIRE(chunk1.value(i, j) == chunk2.value(i, j));
            }
        }
    }
}
TEST_CASE("serialization::expressions") {
    auto resource = std::pmr::synchronized_pool_resource();
    {
        auto expr_and = make_compare_union_expression(&resource, compare_type::union_and);
        expr_and->append_child(make_compare_expression(&resource,
                                                       compare_type::gt,
                                                       side_t::left,
                                                       key{"some key"},
                                                       core::parameter_id_t{1}));
        expr_and->append_child(make_compare_expression(&resource,
                                                       compare_type::lt,
                                                       side_t::right,
                                                       key{"some other key"},
                                                       core::parameter_id_t{2}));

        {
            msgpack_serializer_t serializer(&resource);
            serializer.start_array(1);
            expr_and->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            // res is not stored in a readable format
            msgpack_deserializer_t deserializer(res);
            deserializer.advance_array(0);
            auto type = deserializer.current_type();
            assert(type == serialization_type::expression_compare);
            auto deserialized_res = compare_expression_t::deserialize(&deserializer);
            deserializer.pop_array();
        }
    }
    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        expressions.emplace_back(std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(&resource, aggregate_type::sum, key("total"));
        auto expr_multiply = make_scalar_expression(&resource, scalar_type::multiply);
        expr_multiply->append_param(key("price"));
        expr_multiply->append_param(key("quantity"));
        agg_expr->append_param(std::move(expr_multiply));
        expressions.emplace_back(std::move(agg_expr));
        agg_expr = make_aggregate_expression(&resource, aggregate_type::avg, key("avg_quantity"));
        agg_expr->append_param(key("quantity"));
        expressions.emplace_back(std::move(agg_expr));
        auto node_group = make_node_group(&resource, get_name(), expressions);
        {
            msgpack_serializer_t serializer(&resource);
            serializer.start_array(1);
            node_group->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            msgpack_deserializer_t deserializer(res);
            deserializer.advance_array(0);
            auto type = deserializer.current_type();
            assert(type == serialization_type::logical_node_group);
            auto deserialized_res = node_t::deserialize(&deserializer);
            REQUIRE(node_group->to_string() == deserialized_res->to_string());
            deserializer.pop_array();
        }
    }
}
TEST_CASE("serialization::logical_plan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return components::document::value_t{tape.get(), value}; };
    auto node_delete = make_node_delete_many(
        &resource,
        {database_name, collection_name},
        make_node_match(
            &resource,
            {database_name, collection_name},
            make_compare_expression(&resource, compare_type::gt, side_t::left, key{"count"}, core::parameter_id_t{1})));
    auto params = make_parameter_node(&resource);
    params->add_parameter(core::parameter_id_t{1}, new_value(90));
    {
        msgpack_serializer_t serializer(&resource);
        serializer.start_array(2);
        node_delete->serialize(&serializer);
        params->serialize(&serializer);
        serializer.end_array();
        auto res = serializer.result();
        msgpack_deserializer_t deserializer(res);
        deserializer.advance_array(0);
        {
            auto type = deserializer.current_type();
            assert(type == serialization_type::logical_node_delete);
            auto deserialized_res = node_t::deserialize(&deserializer);
            REQUIRE(node_delete->to_string() == deserialized_res->to_string());
        }
        deserializer.pop_array();
        deserializer.advance_array(1);
        {
            auto type = deserializer.current_type();
            assert(type == serialization_type::parameters);
            auto deserialized_res = parameter_node_t::deserialize(&deserializer);
            REQUIRE(params->parameters().parameters == deserialized_res->parameters().parameters);
        }
        deserializer.pop_array();
    }
}