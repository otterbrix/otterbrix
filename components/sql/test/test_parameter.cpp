#include <catch2/catch.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transform_result.hpp>
#include <components/sql/transformer/transformer.hpp>

using namespace components;
using namespace components::sql;
using namespace components::sql::transform;
using namespace components::expressions;

using v = types::logical_value_t;
using vec = std::vector<v>;

#define TEST_PARAMS_SELECT(QUERY, RESULT, BIND)                                                                        \
    {                                                                                                                  \
        SECTION(QUERY) {                                                                                               \
            auto select = linitial(raw_parser(&arena_resource, QUERY));                                                \
            auto binder = transformer.transform(pg_cell_to_node_cast(select));                                         \
            TEST_PARAMS(RESULT, BIND)                                                                                  \
        }                                                                                                              \
    }

#define TEST_PARAMS(RESULT, BIND)                                                                                      \
    {                                                                                                                  \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            binder.bind(i + 1, BIND.at(i));                                                                            \
        }                                                                                                              \
        auto result = std::get<result_view>(binder.finalize());                                                        \
        auto node = result.node;                                                                                       \
        auto agg = result.params;                                                                                      \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().parameters.size() == BIND.size());                                                   \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == BIND.at(i));                                  \
        }                                                                                                              \
    }

#define TEST_SIMPLE_UPDATE(QUERY, RESULT, BIND, FIELDS)                                                                \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = linitial(raw_parser(&arena_resource, QUERY));                                                      \
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));                                               \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            binder.bind(i + 1, BIND.at(i));                                                                            \
        }                                                                                                              \
        auto result = std::get<result_view>(binder.finalize());                                                        \
        auto node = result.node;                                                                                       \
        auto agg = result.params;                                                                                      \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().parameters.size() == BIND.size());                                                   \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == BIND.at(i));                                  \
        }                                                                                                              \
        REQUIRE(node->database_name() == "testdatabase");                                                              \
        REQUIRE(node->collection_name() == "testcollection");                                                          \
    }

TEST_CASE("components::sql::select_bind") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_PARAMS_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 AND name = $2 AND "count" = $1;)_",
        R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
        vec({v(10l), v(&resource, std::string("doc 10"))}));

    TEST_PARAMS_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 OR name = $2;)_",
                       R"_($aggregate: {$match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}]}})_",
                       vec({v(42l), v(&resource, std::string("abc"))}));

    TEST_PARAMS_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE id > $1 AND flag = $2;)_",
                       R"_($aggregate: {$match: {$and: ["id": {$gt: #0}, "flag": {$eq: #1}]}})_",
                       vec({v(5l), v(true)}));
}

TEST_CASE("components::sql::update_bind") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);
    using fields = std::pmr::vector<update_expr_ptr>;

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "count"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET count = $1 WHERE id = $2;)_",
                           R"_($update: {$upsert: 0, $match: {"id": {$eq: #1}}, $limit: -1})_",
                           vec({v(999l), v(1l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "name"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "flag"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{1});

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET name = $1, flag = $2 WHERE "count" > $3;)_",
                           R"_($update: {$upsert: 0, $match: {"count": {$gt: #2}}, $limit: -1})_",
                           vec({v(&resource, std::string("ok")), v(true), v(100l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{&resource, "rating"}));
        update_expr_ptr calculate = new update_expr_calculate_t(update_expr_type::add);
        calculate->left() =
            new update_expr_get_value_t(components::expressions::key_t{&resource, "rating", side_t::undefined});
        calculate->right() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.back()->left() = std::move(calculate);

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET rating = rating + $1 WHERE flag = $2;)_",
                           R"_($update: {$upsert: 0, $match: {"flag": {$eq: #1}}, $limit: -1})_",
                           vec({v(5l), v(true)}),
                           f);
    }
}

TEST_CASE("components::sql::insert_bind") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("insert simple bind") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, name) VALUES ($1, $2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, v(42l));
        binder.bind(2, v(&resource, std::string("inserted")));
        auto result = std::get<result_view>(binder.finalize());
        auto node = result.node;
        REQUIRE(node->database_name() == "testdatabase");
        REQUIRE(node->collection_name() == "testcollection");

        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == v(42l));
        REQUIRE(chunk.value(1, 0) == v(&resource, "inserted"));
    }

    SECTION("insert with repeated param") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, parent_id) VALUES ($1, $1);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));

        REQUIRE(binder.all_bound() == false);
        binder.bind(1, v(123l));
        REQUIRE(binder.all_bound() == true);

        auto result = std::get<result_view>(binder.finalize());
        auto node = result.node;

        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == v(123));
        REQUIRE(chunk.value(1, 0) == v(123));
    }

    SECTION("insert multi-bind") {
        auto select = linitial(raw_parser(&arena_resource,
                                          "INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES "
                                          "($1, $2, $3), ($4, $5, $6);"));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));
        auto result = std::get<result_view>(binder.bind(1, v(1ul))
                                                .bind(2, v(&resource, "Name1"))
                                                .bind(3, v(10ul))
                                                .bind(4, v(2ul))
                                                .bind(5, v(&resource, "Name2"))
                                                .bind(6, v(20ul))
                                                .finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->collection_name() == "testcollection");
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.value(0, 0) == v(1));
        REQUIRE(chunk.value(1, 0) == v(&resource, "Name1"));
        REQUIRE(chunk.value(2, 0) == v(10));
        REQUIRE(chunk.value(0, 1) == v(2));
        REQUIRE(chunk.value(1, 1) == v(&resource, "Name2"));
        REQUIRE(chunk.value(2, 1) == v(20));
    }
}

TEST_CASE("components::sql::transform_result") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("not all bound") {
        auto stmt = linitial(
            raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection WHERE id = $1 AND name = $2;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, 42l);
        auto result = binder.finalize();
        REQUIRE(std::holds_alternative<transform::bind_error>(result));
    }

    SECTION("reuse binder") {
        auto query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 AND name = $2 AND "count" = $1;)_";
        auto select = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));
        TEST_PARAMS(R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                    vec({v(10l), v(&resource, std::string("doc 10"))}));

        TEST_PARAMS(R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                    vec({v(3.14), v(&resource, std::string("another doc 10"))}));

        TEST_PARAMS(R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                    vec({v(false), v(&resource, std::string("another another doc 10"))}));
    }

    SECTION("intrusive ptr update") {
        auto resource = std::pmr::synchronized_pool_resource();
        std::pmr::monotonic_buffer_resource arena_resource(&resource);
        transform::transformer transformer(&resource);

        auto query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1;)_";
        auto select = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));

        binder.bind(1, v(&resource, "doc"));
        auto agg = std::get<result_view>(binder.finalize()).params;
        REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(0))) == v(&resource, "doc"));

        binder.bind(1, v(100l)).finalize();
        REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(0))) == v(100l));
    }

    SECTION("reuse insert binding") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, parent_id) VALUES ($1, $2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, v(123l)).bind(2, v(false));
        auto node = std::get<result_view>(binder.finalize()).node;

        const auto& keys = reinterpret_cast<logical_plan::node_insert_ptr&>(node)->key_translation();
        binder.bind(1, v(true)).bind(2, v(&resource, std::string("doc 10"))).finalize();

        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == v(true));
        REQUIRE(chunk.value(1, 0) == v(&resource, std::string("doc 10")));
        REQUIRE(reinterpret_cast<logical_plan::node_insert_ptr&>(node)->key_translation() == keys);
    }
}
