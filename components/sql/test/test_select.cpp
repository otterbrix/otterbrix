#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

using v = components::document::value_t;
using vec = std::vector<v>;

#define TEST_SIMPLE_UPDATE(QUERY, RESULT, PARAMS)                                                                      \
    {                                                                                                                  \
        SECTION(QUERY) {                                                                                               \
            transform::transformer transformer(&resource);                                                             \
            auto select = linitial(raw_parser(&arena_resource, QUERY));                                                \
            auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());       \
            auto node = result.node;                                                                                   \
            auto agg = result.params;                                                                                  \
            REQUIRE(node->to_string() == RESULT);                                                                      \
            REQUIRE(agg->parameters().parameters.size() == PARAMS.size());                                             \
            for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                               \
                REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                            \
            }                                                                                                          \
        }                                                                                                              \
    }

TEST_CASE("sql::select_from_where") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection;)_", R"_($aggregate: {})_", vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM UID.TestDatabase.TestSchema.TestCollection;)_", R"_($aggregate: {})_", vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10;)_",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 AND name = 'doc 10' AND "count" = 2;)_",
        R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]}})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE ((((number = 10 AND name = 'doc 10'))));)_",
                       R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}]}})_",
                       vec({new_value(10l), new_value(std::pmr::string("doc 10"))}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 OR name = 'doc 10' OR "count" = 2;)_",
        R"_($aggregate: {$match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]}})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 AND name = 'doc 10' OR "count" = 2;)_",
        R"_($aggregate: {$match: {$or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}]}})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE (number = 10 AND name = 'doc 10') OR "count" = 2;)_",
        R"_($aggregate: {$match: {$or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}]}})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 AND (name = 'doc 10' OR "count" = 2);)_",
        R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, $or: ["name": {$eq: #1}, "count": {$eq: #2}]]}})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE ((number = 10 AND name = 'doc 10') OR "count" = 2) AND )_"
        R"_(((number = 10 AND name = 'doc 10') OR "count" = 2) AND )_"
        R"_(((number = 10 AND name = 'doc 10') OR "count" = 2);)_",
        R"_($aggregate: {$match: {$and: [)_"
        R"_($or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}], )_"
        R"_($or: [$and: ["number": {$eq: #3}, "name": {$eq: #4}], "count": {$eq: #5}], )_"
        R"_($or: [$and: ["number": {$eq: #6}, "name": {$eq: #7}], "count": {$eq: #8}])_"
        R"_(]}})_",
        vec({new_value(10l),
             new_value(std::pmr::string("doc 10")),
             new_value(2l),
             new_value(10l),
             new_value(std::pmr::string("doc 10")),
             new_value(2l),
             new_value(10l),
             new_value(std::pmr::string("doc 10")),
             new_value(2l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number == 10;)_",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number != 10;)_",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number <> 10;)_",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number < 10;)_",
                       R"_($aggregate: {$match: {"number": {$lt: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number <= 10;)_",
                       R"_($aggregate: {$match: {"number": {$lte: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number > 10;)_",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number >= 10;)_",
                       R"_($aggregate: {$match: {"number": {$gte: #0}}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT(number >= 10);)_",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT number >= 10;)_",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (number = 10) AND NOT(name = 'doc 10' OR "count" = 2);)_",
        R"_($aggregate: {$match: {$and: [$not: ["number": {$eq: #0}], )_"
        R"_($not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection WHERE name LIKE 'pattern';)_",
                       R"_($aggregate: {$match: {"name": {$regex: #0}}})_",
                       vec({new_value(std::pmr::string("pattern"))}));
}

TEST_CASE("sql::select_from_order_by") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number;)_",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number ASC;)_",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number DESC;)_",
                       R"_($aggregate: {$sort: {number: -1}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number, name;)_",
                       R"_($aggregate: {$sort: {number: 1, name: 1}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number ASC, name DESC;)_",
                       R"_($aggregate: {$sort: {number: 1, name: -1}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number, "count" ASC, name, value DESC;)_",
                       R"_($aggregate: {$sort: {number: 1, count: 1, name: 1, value: -1}})_",
                       vec());

    TEST_SIMPLE_UPDATE(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number > 10 ORDER BY number ASC, name DESC;)_",
        R"_($aggregate: {$match: {"number": {$gt: #0}}, $sort: {number: 1, name: -1}})_",
        vec({new_value(10l)}));
}

TEST_CASE("sql::select_from_fields") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_UPDATE(R"_(SELECT number, name, "count" FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {number, name, count}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT number, name as title FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {number, title: "$name"}})_",
                       vec());

    TEST_SIMPLE_UPDATE(R"_(SELECT number, name title FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {number, title: "$name"}})_",
                       vec());

    TEST_SIMPLE_UPDATE(
        R"_(SELECT number, 10 size, 'title' title, true "on", false "off" FROM TestDatabase.TestCollection;)_",
        R"_($aggregate: {$group: {number, size: #0, title: #1, on: #2, off: #3}})_",
        vec({new_value(10l), new_value(std::pmr::string("title")), new_value(true), new_value(false)}));
}