#include <catch2/catch.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;
using namespace components::expressions;

#define TEST_SIMPLE_UPDATE(QUERY, RESULT, PARAMS, FIELDS)                                                              \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());           \
        auto node = result.node;                                                                                       \
        auto agg = result.params;                                                                                      \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().parameters.size() == PARAMS.size());                                                 \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                \
        }                                                                                                              \
        REQUIRE(node->database_name() == "testdatabase");                                                              \
        REQUIRE(node->collection_name() == "testcollection");                                                          \
        auto updates = static_cast<components::logical_plan::node_update_t&>(*node).updates();                         \
        REQUIRE(updates == FIELDS);                                                                                    \
    }

using v = components::types::logical_value_t;
using vec = std::vector<v>;
using fields = std::pmr::vector<update_expr_ptr>;

TEST_CASE("components::sql::update") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "count"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10;",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, 10ul)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "name"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET name = 'new name';",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, "new name")}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "is_doc"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = true;",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, true)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "count"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "name"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{1});
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "is_doc"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{2});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10, name = 'new name', is_doc = true;",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, 10ul), v(&resource, "new name"), v(&resource, true)}),
                           f);
    }
}

TEST_CASE("components::sql::update_where") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "count"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10 WHERE id = 1;",
                           R"_($update: {$upsert: 0, $match: {"id": {$eq: #1}}, $limit: -1})_",
                           vec({v(&resource, 10ul), v(&resource, 1ul)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "name"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET name = 'new name' WHERE name = 'old_name';",
                           R"_($update: {$upsert: 0, $match: {"name": {$eq: #1}}, $limit: -1})_",
                           vec({v(&resource, "new name"), v(&resource, "old_name")}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "is_doc"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = true WHERE is_doc = false;",
                           R"_($update: {$upsert: 0, $match: {"is_doc": {$eq: #1}}, $limit: -1})_",
                           vec({v(&resource, true), v(&resource, false)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "count"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "name"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{1});
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "is_doc"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{2});
        TEST_SIMPLE_UPDATE(
            "UPDATE TestDatabase.TestCollection SET count = 10, name = 'new name', is_doc = true "
            "WHERE id > 10 AND name = 'old_name' AND is_doc = false;",
            R"_($update: {$upsert: 0, $match: {$and: ["id": {$gt: #3}, "name": {$eq: #4}, "is_doc": {$eq: #5}]}, $limit: -1})_",
            vec({v(&resource, 10ul), v(&resource, "new name"), v(&resource, true), v(&resource, 10ul), v(&resource, "old_name"), v(&resource, false)}),
            f);
    }
}

TEST_CASE("components::sql::update_from") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "price"}));
        update_expr_ptr calculate = new update_expr_calculate_t(update_expr_type::mult);
        calculate->left() =
            new update_expr_get_value_t(components::expressions::key_t{&resource, "price", side_t::undefined});
        calculate->right() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.back()->left() = std::move(calculate);
        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET price = price * 1.5;)_",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, 1.5f)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "price"}));
        update_expr_ptr calculate_1 = new update_expr_calculate_t(update_expr_type::mult);
        calculate_1->left() =
            new update_expr_get_value_t(components::expressions::key_t{&resource, "price", side_t::right});
        calculate_1->right() =
            new update_expr_get_value_t(components::expressions::key_t{&resource, "discount", side_t::left});
        update_expr_ptr calculate_2 = new update_expr_calculate_t(update_expr_type::sub);
        calculate_2->left() =
            new update_expr_get_value_t(components::expressions::key_t{&resource, "price", side_t::right});
        calculate_2->right() = std::move(calculate_1);
        f.back()->left() = std::move(calculate_2);
        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection
SET price = OtherTestCollection.price - (OtherTestCollection.price * TestCollection.discount)
FROM OtherTestCollection
WHERE TestCollection.id = OtherTestCollection.id;)_",
                           R"_($update: {$upsert: 0, $match: {"id": {$eq: "id"}}, $limit: -1})_",
                           vec({}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{std::pmr::vector<std::pmr::string>{
            {std::pmr::string{"struct_type", &resource}, std::pmr::string{"field", &resource}},
            &resource}}));
        update_expr_ptr calculate = new update_expr_calculate_t(update_expr_type::add);
        calculate->left() = new update_expr_get_value_t(components::expressions::key_t{
            std::pmr::vector<std::pmr::string>{
                {std::pmr::string{"struct_type", &resource}, std::pmr::string{"field", &resource}},
                &resource},
            side_t::undefined});
        calculate->right() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.back()->left() = std::move(calculate);
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET struct_type.field = (struct_type).field + 1;",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, 1ul)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "array_type"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE(
            "UPDATE TestDatabase.TestCollection SET array_type = ARRAY[1,2,3,4];",
            R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
            vec({components::types::logical_value_t::create_array(&arena_resource, components::types::logical_type::BIGINT,
                                                                  {v(&resource, 1l), v(&resource, 2l), v(&resource, 3l), v(&resource, 4l)})}),
            f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{std::pmr::vector<std::pmr::string>{
            {std::pmr::string{"array_type", &resource}, std::pmr::string{"4", &resource}},
            &resource}}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET array_type[4] = 196;",
                           R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                           vec({v(&resource, 196l)}),
                           f);
    }
}