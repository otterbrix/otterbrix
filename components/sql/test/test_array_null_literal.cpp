#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;
using namespace components::types;

namespace {

    logical_value_t
    inserted_literal(transformer& transformer, std::pmr::monotonic_buffer_resource& arena, const char* sql) {
        auto stmt = linitial(raw_parser(&arena, sql));
        auto transformed = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE_FALSE(transformed.has_error());
        auto node = transformed.value().sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        return chunk.value(0, 0);
    }

} // namespace

TEST_CASE("components::sql::array_null_literal::an_array_of_only_NULL_is_text") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena(&resource);
    transformer transformer(&resource);

    SECTION("array[NULL]") {
        auto v = inserted_literal(transformer, arena, "INSERT INTO t (v) VALUES (ARRAY[NULL]);");
        REQUIRE(v.type().type() == logical_type::ARRAY);
        CHECK(v.type().child_type().type() == logical_type::STRING_LITERAL);
        REQUIRE(v.children().size() == 1);
        CHECK(v.children()[0].is_null());
    }

    SECTION("array[NULL, NULL]") {
        auto v = inserted_literal(transformer, arena, "INSERT INTO t (v) VALUES (ARRAY[NULL, NULL]);");
        REQUIRE(v.type().type() == logical_type::ARRAY);
        CHECK(v.type().child_type().type() == logical_type::STRING_LITERAL);
        REQUIRE(v.children().size() == 2);
    }

    SECTION("array[array[NULL]]") {
        auto v = inserted_literal(transformer, arena, "INSERT INTO t (v) VALUES (ARRAY[ARRAY[NULL]]);");
        REQUIRE(v.type().type() == logical_type::ARRAY);
        REQUIRE(v.type().child_type().type() == logical_type::ARRAY);
        CHECK(v.type().child_type().child_type().type() == logical_type::STRING_LITERAL);
    }

    SECTION("array[NULL, 1] keeps the typed element") {
        auto v = inserted_literal(transformer, arena, "INSERT INTO t (v) VALUES (ARRAY[NULL, 1]);");
        REQUIRE(v.type().type() == logical_type::ARRAY);
        CHECK(v.type().child_type().type() == logical_type::BIGINT);
    }

    SECTION("array[] stays untyped: INSERT reconciles it to the column") {
        auto v = inserted_literal(transformer, arena, "INSERT INTO t (v) VALUES (ARRAY[]);");
        REQUIRE(v.type().type() == logical_type::ARRAY);
        CHECK(v.type().child_type().type() == logical_type::NA);
        CHECK(v.children().empty());
    }
}
