#include <catch2/catch.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

TEST_CASE("sql::insert_into") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("insert into with TestDatabase") {
        auto select =
            linitial(raw_parser(&arena_resource,
                                "INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES (1, 'Name', 1);"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "testdatabase");
        REQUIRE(node->collection_name() == "testcollection");
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(1));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t("Name"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(1));
    }

    SECTION("insert into without TestDatabase") {
        auto select = linitial(
            raw_parser(&arena_resource, "INSERT INTO TestCollection (id, name, count) VALUES (1, 'Name', 1);"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(1));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t("Name"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(1));
    }

    SECTION("insert into with quoted") {
        auto select = linitial(
            raw_parser(&arena_resource, R"(INSERT INTO TestCollection (id, "name", "count") VALUES (1, 'Name', 1);)"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(1));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t("Name"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(1));
    }

    SECTION("insert into struct") {
        auto select = linitial(raw_parser(
            &arena_resource,
            R"(INSERT INTO TestCollection (struct_type.field_1, struct_type.field_3) VALUES(43, 'some text');)"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(43));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t("some text"));
    }

    SECTION("insert into multi-documents") {
        auto select = linitial(raw_parser(&arena_resource,
                                          "INSERT INTO TestCollection (id, name, count) VALUES "
                                          "(1, 'Name1', 1), "
                                          "(2, 'Name2', 2), "
                                          "(3, 'Name3', 3), "
                                          "(4, 'Name4', 4), "
                                          "(5, 'Name5', 5);"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 5);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(1));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t("Name1"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(1));
        REQUIRE(chunk.value(0, 4) == components::types::logical_value_t(5));
        REQUIRE(chunk.value(1, 4) == components::types::logical_value_t("Name5"));
        REQUIRE(chunk.value(2, 4) == components::types::logical_value_t(5));
    }

    SECTION("insert from select") {
        auto select = linitial(raw_parser(&arena_resource, R"_(INSERT INTO table2 (column1, column2, column3)
SELECT column1, column2, column3
FROM table1
WHERE condition = true;)_"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "table2");
        REQUIRE(reinterpret_cast<components::logical_plan::node_aggregate_ptr&>(node->children().front())
                    ->database_name() == "");
        REQUIRE(reinterpret_cast<components::logical_plan::node_aggregate_ptr&>(node->children().front())
                    ->collection_name() == "table1");
    }
}