#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

// Transformer now wraps DELETE in sequence_t(resolve_*..., delete); descend
// to the delete consumer to inspect delete_t shape.
#define TEST_SIMPLE_DELETE(QUERY, RESULT, PARAMS)                                                                      \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = ([](auto _w) {                                                                                   \
            REQUIRE_FALSE(_w.has_error());                                                                             \
            return _w.value();                                                                                         \
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));                                            \
        auto node = result.sub_queries.back();                                                                         \
        if (node->type() == components::logical_plan::node_type::sequence_t) {                                         \
            node = node->children().back();                                                                            \
        }                                                                                                              \
        auto agg = result.parameters;                                                                                  \
        REQUIRE(node->type() == components::logical_plan::node_type::delete_t);                                        \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().parameters.size() == PARAMS.size());                                                 \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                \
        }                                                                                                              \
    }

using v = components::types::logical_value_t;
using vec = std::vector<v>;

TEST_CASE("components::sql::delete_from_where") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection WHERE number == 10;",
                       R"_($delete: <oid:0> {$match: {"number": {$eq: #0}}, $limit: -1})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection WHERE (struct_type).number == 10;",
                       R"_($delete: <oid:0> {$match: {"struct_type/number": {$eq: #0}}, $limit: -1})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection WHERE array_type[15] == 10;",
                       R"_($delete: <oid:0> {$match: {"array_type/15": {$eq: #0}}, $limit: -1})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_DELETE(
        "DELETE FROM TestDatabase.TestCollection WHERE NOT (number = 10) AND NOT(name = 'doc 10' OR count = 2);",
        R"_($delete: <oid:0> {$match: {$and: [$not: ["number": {$eq: #0}], $not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}, $limit: -1})_",
        vec({v(&resource, 10l), v(&resource, std::string("doc 10")), v(&resource, 2l)}));

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection USING TestDatabase.OtherTestCollection WHERE "
                       "TestCollection.number = OtherTestCollection.number;",
                       R"_($delete: <oid:0> {$match: {"number": {$eq: "number"}}, $limit: -1})_",
                       vec());
}
