#include <catch2/catch.hpp>
#include <components/document/document.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transform_result.hpp>
#include <components/sql/transformer/transformer.hpp>

using namespace components;
using namespace components::sql;
using namespace components::sql::transform;
using namespace components::document;
using namespace components::expressions;

using v = components::document::value_t;
using vec = std::vector<v>;

#define TEST_PARAMS(QUERY, RESULT, BIND)                                                                               \
    {                                                                                                                  \
        SECTION(QUERY) {                                                                                               \
            auto select = linitial(raw_parser(&arena_resource, QUERY));                                                \
            auto binder = transformer.transform(pg_cell_to_node_cast(select));                                         \
            for (auto i = 0ul; i < BIND.size(); ++i) {                                                                 \
                binder.bind(i + 1, BIND.at(i));                                                                        \
            }                                                                                                          \
            auto result = std::get<result_view>(binder.finalize());                                                    \
            auto node = result.node;                                                                                   \
            auto agg = result.params;                                                                                  \
            REQUIRE(node->to_string() == RESULT);                                                                      \
            REQUIRE(agg->parameters().parameters.size() == BIND.size());                                               \
            for (auto i = 0ul; i < BIND.size(); ++i) {                                                                 \
                REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == BIND.at(i));                              \
            }                                                                                                          \
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

TEST_CASE("sql::select_bind") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_PARAMS(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 AND name = $2 AND "count" = $1;)_",
                R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                vec({new_value(10l), new_value(std::pmr::string("doc 10"))}));

    TEST_PARAMS(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 OR name = $2;)_",
                R"_($aggregate: {$match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}]}})_",
                vec({new_value(42l), new_value(std::pmr::string("abc"))}));

    TEST_PARAMS(R"_(SELECT * FROM TestDatabase.TestCollection WHERE id > $1 AND flag = $2;)_",
                R"_($aggregate: {$match: {$and: ["id": {$gt: #0}, "flag": {$eq: #1}]}})_",
                vec({new_value(5l), new_value(true)}));
}

TEST_CASE("sql::update_bind") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };
    using fields = std::pmr::vector<update_expr_ptr>;

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{"count"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET count = $1 WHERE id = $2;)_",
                           R"_($update: {$upsert: 0, $match: {"id": {$eq: #1}}, $limit: -1})_",
                           vec({new_value(999l), new_value(1l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{"name"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{"flag"}));
        f.back()->left() = new update_expr_get_const_value_t(core::parameter_id_t{1});

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET name = $1, flag = $2 WHERE "count" > $3;)_",
                           R"_($update: {$upsert: 0, $match: {"count": {$gt: #2}}, $limit: -1})_",
                           vec({new_value(std::pmr::string("ok")), new_value(true), new_value(100l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(new update_expr_set_t(components::expressions::key_t{"rating"}));
        update_expr_ptr calculate = new update_expr_calculate_t(update_expr_type::add);
        calculate->left() = new update_expr_get_value_t(components::expressions::key_t{"rating"}, side_t::undefined);
        calculate->right() = new update_expr_get_const_value_t(core::parameter_id_t{0});
        f.back()->left() = std::move(calculate);

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET rating = rating + $1 WHERE flag = $2;)_",
                           R"_($update: {$upsert: 0, $match: {"flag": {$eq: #1}}, $limit: -1})_",
                           vec({new_value(5l), new_value(true)}),
                           f);
    }
}

TEST_CASE("sql::insert_bind") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    SECTION("insert simple bind") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, name) VALUES ($1, $2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, new_value(42l));
        binder.bind(2, new_value(std::pmr::string("inserted")));
        auto result = std::get<result_view>(binder.finalize());
        auto node = result.node;
        REQUIRE(node->database_name() == "testdatabase");
        REQUIRE(node->collection_name() == "testcollection");

        auto data_node = reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front());
        REQUIRE(data_node->documents().size() == 1);
        auto doc = data_node->documents().front();
        REQUIRE(doc->get_long("id") == 42);
        REQUIRE(doc->get_string("name") == "inserted");
    }

    SECTION("insert with repeated param") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, parent_id) VALUES ($1, $1);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, new_value(123l));
        auto result = std::get<result_view>(binder.finalize());
        auto node = result.node;

        auto data_node = reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front());
        REQUIRE(data_node->documents().size() == 1);
        auto doc = data_node->documents().front();
        REQUIRE(doc->get_long("id") == 123);
        REQUIRE(doc->get_long("parent_id") == 123);
    }

    SECTION("insert multi-bind") {
        auto select = linitial(raw_parser(&arena_resource,
                                          "INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES "
                                          "($1, $2, $3), ($4, $5, $6);"));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));
        auto result = std::get<result_view>(binder.bind(1, new_value(1ul))
                                                .bind(2, new_value("Name1"))
                                                .bind(3, new_value(10ul))
                                                .bind(4, new_value(2ul))
                                                .bind(5, new_value("Name2"))
                                                .bind(6, new_value(20ul))
                                                .finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->collection_name() == "testcollection");

        auto data_node = reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front());
        REQUIRE(data_node->documents().size() == 2);

        auto doc1 = data_node->documents().front();
        auto doc2 = data_node->documents().back();

        REQUIRE(doc1->get_long("id") == 1);
        REQUIRE(doc1->get_string("name") == "Name1");
        REQUIRE(doc1->get_long("count") == 10);

        REQUIRE(doc2->get_long("id") == 2);
        REQUIRE(doc2->get_string("name") == "Name2");
        REQUIRE(doc2->get_long("count") == 20);
    }
}

TEST_CASE("sql::transform_result") {
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

    SECTION("finalize") {
        auto stmt = linitial(raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        auto result = binder.finalize();
        REQUIRE(std::holds_alternative<transform::result_view>(result));
        REQUIRE(std::holds_alternative<transform::bind_error>(binder.finalize()));
    }
}
