#include <catch2/catch.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

namespace {
    const components::vector::data_chunk_t& parse_chunk(const components::logical_plan::node_ptr& node) {
        return reinterpret_cast<const components::logical_plan::node_data_ptr&>(node->children().front())
            ->data_chunk();
    }

    components::logical_plan::node_ptr parse_insert(std::pmr::memory_resource* arena,
                                                    std::pmr::synchronized_pool_resource* resource,
                                                    const char* sql) {
        transform::transformer transformer(resource);
        auto select = linitial(raw_parser(arena, sql));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        return result.node;
    }
} // namespace

TEST_CASE("components::sql::insert_json::basic") {
    std::pmr::synchronized_pool_resource resource;
    std::pmr::monotonic_buffer_resource arena(&resource);

    SECTION("single row, single scalar field") {
        auto node = parse_insert(&arena, &resource, "INSERT INTO db.t VALUES (json('{\"a\": 42}'));");
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "db");
        REQUIRE(node->collection_name() == "t");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.column_count() == 1);
        REQUIRE(chunk.data[0].type().alias() == "a");
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena, int64_t{42}));
    }

    SECTION("multiple rows with overlapping keys") {
        auto node = parse_insert(
            &arena,
            &resource,
            "INSERT INTO db.t VALUES (json('{\"a\": 1, \"b\": 10}')), (json('{\"a\": 2, \"c\": \"x\"}'));");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.column_count() == 3);

        auto find_col = [&](std::string_view name) -> int {
            for (size_t i = 0; i < chunk.column_count(); ++i) {
                if (chunk.data[i].type().alias() == name) return static_cast<int>(i);
            }
            return -1;
        };
        int col_a = find_col("a");
        int col_b = find_col("b");
        int col_c = find_col("c");
        REQUIRE(col_a >= 0);
        REQUIRE(col_b >= 0);
        REQUIRE(col_c >= 0);

        // Row 0 has a=1, b=10, c=NULL
        REQUIRE(chunk.data[col_a].validity().row_is_valid(0));
        REQUIRE(chunk.data[col_b].validity().row_is_valid(0));
        REQUIRE_FALSE(chunk.data[col_c].validity().row_is_valid(0));
        REQUIRE(chunk.value(col_a, 0) == components::types::logical_value_t(&arena, int64_t{1}));
        REQUIRE(chunk.value(col_b, 0) == components::types::logical_value_t(&arena, int64_t{10}));

        // Row 1 has a=2, b=NULL, c="x"
        REQUIRE(chunk.data[col_a].validity().row_is_valid(1));
        REQUIRE_FALSE(chunk.data[col_b].validity().row_is_valid(1));
        REQUIRE(chunk.data[col_c].validity().row_is_valid(1));
        REQUIRE(chunk.value(col_a, 1) == components::types::logical_value_t(&arena, int64_t{2}));
        REQUIRE(chunk.value(col_c, 1) == components::types::logical_value_t(&arena, std::string{"x"}));
    }

    SECTION("empty object produces zero columns") {
        auto node = parse_insert(&arena, &resource, "INSERT INTO db.t VALUES (json('{}'));");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.size() == 1);
        REQUIRE(chunk.column_count() == 0);
    }

    SECTION("batch with one empty row pads others with nulls") {
        auto node = parse_insert(&arena, &resource,
                                 "INSERT INTO db.t VALUES (json('{}')), (json('{\"a\": 7}'));");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.column_count() == 1);
        REQUIRE(chunk.data[0].type().alias() == "a");
        REQUIRE_FALSE(chunk.data[0].validity().row_is_valid(0));
        REQUIRE(chunk.data[0].validity().row_is_valid(1));
        REQUIRE(chunk.value(0, 1) == components::types::logical_value_t(&arena, int64_t{7}));
    }

    SECTION("nested object flattens to dotted path") {
        auto node = parse_insert(&arena, &resource,
                                 "INSERT INTO db.t VALUES (json('{\"a\": {\"b\": {\"c\": 3}}}'));");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.column_count() == 1);
        REQUIRE(chunk.data[0].type().alias() == "a.b.c");
        REQUIRE(chunk.value(0, 0) == components::types::logical_value_t(&arena, int64_t{3}));
    }

    SECTION("boolean and double types") {
        auto node = parse_insert(&arena, &resource,
                                 "INSERT INTO db.t VALUES (json('{\"flag\": true, \"ratio\": 0.25}'));");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.column_count() == 2);
        auto find_col = [&](std::string_view name) -> int {
            for (size_t i = 0; i < chunk.column_count(); ++i) {
                if (chunk.data[i].type().alias() == name) return static_cast<int>(i);
            }
            return -1;
        };
        int col_flag = find_col("flag");
        int col_ratio = find_col("ratio");
        REQUIRE(col_flag >= 0);
        REQUIRE(col_ratio >= 0);
        REQUIRE(chunk.data[col_flag].type().type() == components::types::logical_type::BOOLEAN);
        REQUIRE(chunk.data[col_ratio].type().type() == components::types::logical_type::DOUBLE);
    }

    SECTION("json null values are treated as missing") {
        auto node = parse_insert(&arena, &resource,
                                 "INSERT INTO db.t VALUES (json('{\"a\": 1, \"b\": null}'));");
        const auto& chunk = parse_chunk(node);
        REQUIRE(chunk.column_count() == 1);
        REQUIRE(chunk.data[0].type().alias() == "a");
    }
}

TEST_CASE("components::sql::insert_json::errors") {
    std::pmr::synchronized_pool_resource resource;
    std::pmr::monotonic_buffer_resource arena(&resource);
    transform::transformer transformer(&resource);

    SECTION("inconsistent types across rows rejected") {
        auto select = linitial(raw_parser(
            &arena,
            "INSERT INTO db.t VALUES (json('{\"a\": 1}')), (json('{\"a\": \"x\"}'));"));
        REQUIRE_THROWS(transformer.transform(pg_cell_to_node_cast(select)));
    }

    SECTION("invalid JSON string rejected") {
        auto select = linitial(raw_parser(&arena, "INSERT INTO db.t VALUES (json('{not json'));"));
        REQUIRE_THROWS(transformer.transform(pg_cell_to_node_cast(select)));
    }

    SECTION("non-object JSON root rejected") {
        auto select = linitial(raw_parser(&arena, "INSERT INTO db.t VALUES (json('[1,2,3]'));"));
        REQUIRE_THROWS(transformer.transform(pg_cell_to_node_cast(select)));
    }
}
