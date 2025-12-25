#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

#define TEST_SIMPLE_DELETE(QUERY, RESULT, PARAMS)                                                                      \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());           \
        auto node = result.node;                                                                                       \
        auto agg = result.params;                                                                                      \
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
                       R"_($delete: {$match: {"number": {$eq: #0}}, $limit: -1})_",
                       vec({v(10l)}));

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection WHERE (struct_type).number == 10;",
                       R"_($delete: {$match: {"struct_type/number": {$eq: #0}}, $limit: -1})_",
                       vec({v(10l)}));

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection WHERE array_type[15] == 10;",
                       R"_($delete: {$match: {"array_type/15": {$eq: #0}}, $limit: -1})_",
                       vec({v(10l)}));

    TEST_SIMPLE_DELETE(
        "DELETE FROM TestDatabase.TestCollection WHERE NOT (number = 10) AND NOT(name = 'doc 10' OR count = 2);",
        R"_($delete: {$match: {$and: [$not: ["number": {$eq: #0}], $not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}, $limit: -1})_",
        vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection USING TestDatabase.OtherTestCollection WHERE "
                       "TestCollection.number = OtherTestCollection.number;",
                       R"_($delete: {$match: {"number": {$eq: "number"}}, $limit: -1})_",
                       vec());
}
