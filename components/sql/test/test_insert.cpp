#include <catch2/catch.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

namespace {
    // Transformer now wraps DML in sequence_t(resolve_*..., consumer); descend
    // to the consumer when the test wants to inspect insert_t-specific shape.
    components::logical_plan::node_ptr dml_consumer(components::logical_plan::node_ptr n) {
        if (n && n->type() == components::logical_plan::node_type::sequence_t) {
            return n->children().back();
        }
        return n;
    }
} // namespace

TEST_CASE("components::sql::insert_into") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("insert into with TestDatabase") {
        auto select =
            linitial(raw_parser(&arena_resource,
                                "INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES (1, 'Name', 1);"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena_resource, 1l));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t(&arena_resource, "Name"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(&arena_resource, 1l));
    }

    SECTION("insert into without TestDatabase") {
        auto select = linitial(
            raw_parser(&arena_resource, "INSERT INTO TestCollection (id, name, count) VALUES (1, 'Name', 1);"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena_resource, 1l));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t(&arena_resource, "Name"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(&arena_resource, 1l));
    }

    SECTION("insert into with quoted") {
        auto select = linitial(
            raw_parser(&arena_resource, R"(INSERT INTO TestCollection (id, "name", "count") VALUES (1, 'Name', 1);)"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena_resource, 1l));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t(&arena_resource, "Name"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(&arena_resource, 1l));
    }

    SECTION("insert into struct") {
        auto select = linitial(raw_parser(
            &arena_resource,
            R"(INSERT INTO TestCollection (struct_type.field_1, struct_type.field_3) VALUES(43, 'some text');)"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena_resource, 43l));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t(&arena_resource, "some text"));
    }

    SECTION("insert into array") {
        auto select =
            linitial(raw_parser(&arena_resource, R"(INSERT INTO TestCollection (array_type) VALUES(ARRAY[1, 2, 3]);)"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 1);
        auto arr =
            components::types::logical_value_t::create_array(&arena_resource,
                                                             components::types::logical_type::BIGINT,
                                                             {components::types::logical_value_t{&arena_resource, 1l},
                                                              components::types::logical_value_t{&arena_resource, 2l},
                                                              components::types::logical_value_t{&arena_resource, 3l}});
        REQUIRE(chunk.value(0, 0) == arr);
    }

    SECTION("insert into multi-documents") {
        auto select = linitial(raw_parser(&arena_resource,
                                          "INSERT INTO TestCollection (id, name, count) VALUES "
                                          "(1, 'Name1', 1), "
                                          "(2, 'Name2', 2), "
                                          "(3, 'Name3', 3), "
                                          "(4, 'Name4', 4), "
                                          "(5, 'Name5', 5);"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            reinterpret_cast<components::logical_plan::node_data_ptr&>(node->children().front())->data_chunk();
        REQUIRE(chunk.size() == 5);
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena_resource, 1l));
        REQUIRE(chunk.value(1, 0) == components::types::logical_value_t(&arena_resource, "Name1"));
        REQUIRE(chunk.value(2, 0) == components::types::logical_value_t(&arena_resource, 1l));
        REQUIRE(chunk.value(0, 4) == components::types::logical_value_t(&arena_resource, 5l));
        REQUIRE(chunk.value(1, 4) == components::types::logical_value_t(&arena_resource, "Name5"));
        REQUIRE(chunk.value(2, 4) == components::types::logical_value_t(&arena_resource, 5l));
    }

    SECTION("insert from select") {
        auto select = linitial(raw_parser(&arena_resource, R"_(INSERT INTO table2 (column1, column2, column3)
SELECT column1, column2, column3
FROM table1
WHERE condition = true;)_"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(
            static_cast<const std::string&>(
                reinterpret_cast<components::logical_plan::node_aggregate_ptr&>(node->children().front())->dbname()) ==
            "");
        REQUIRE(
            static_cast<const std::string&>(
                reinterpret_cast<components::logical_plan::node_aggregate_ptr&>(node->children().front())->relname()) ==
            "table1");
    }
}
