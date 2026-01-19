#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

#define TEST_JOIN(QUERY, RESULT, PARAMS)                                                                               \
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
    }

using v = components::types::logical_value_t;
using vec = std::vector<v>;

TEST_CASE("components::sql::join") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("join types") {
        TEST_JOIN(R"_(select * from col1 join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 inner join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 full outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: full, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 left outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: left, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 right outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: right, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 cross join col2;)_",
                  R"_($aggregate: {$join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}})_",
                  vec());
    }

    SECTION("join specifics") {
        TEST_JOIN(
            R"_(select col1.id, col2.id_col1 from db.col as col1 JOIN col2 on col1.id = col2.id_col1;)_",
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}, $group: {id, id_col1}})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.id = col2.id_col1 and col1.name = col2.name;)_",
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, $and: ["id": {$eq: "id_col1"}, "name": {$eq: "name"}]}})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.id = col2.id_col1 )_"
            R"_(join col3 on id = col3.id_col1 and id = col3.id_col2;)_",
            R"_($aggregate: {$join: {$type: inner, $join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}, )_"
            R"_($aggregate: {}, $and: ["id": {$eq: "id_col1"}, "id": {$eq: "id_col2"}]}})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on (col1.struct_type).field = (col2.struct_type).field;)_",
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "struct_type/field": {$eq: "struct_type/field"}}})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.array_type[1] = col2.array_type[2];)_",
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "array_type/1": {$eq: "array_type/2"}}})_",
            vec());
    }

    SECTION("join names") {
        auto select = linitial(raw_parser(&arena_resource,
                                          "SELECT * from uid1.db1.sch1.test1 inner join uid2.db2.sch2.test2 on x = y "
                                          "full outer join uid3.db3.sch3.test3 on y = z;"));
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(select)).finalize());
        auto join = result.node->children().front();
        REQUIRE(join->children().back()->collection_full_name() ==
                collection_full_name_t("uid3", "db3", "sch3", "test3"));

        auto nested_join = join->children().front();
        REQUIRE(nested_join->children().front()->collection_full_name() ==
                collection_full_name_t("uid1", "db1", "sch1", "test1"));
        REQUIRE(nested_join->children().back()->collection_full_name() ==
                collection_full_name_t("uid2", "db2", "sch2", "test2"));
    }
}
