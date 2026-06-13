#include <catch2/catch.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

#define TEST_JOIN(QUERY, RESULT, PARAMS)                                                                               \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = ([](auto _w) {                                                                                   \
            REQUIRE_FALSE(_w.has_error());                                                                             \
            return _w.value();                                                                                         \
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));                                            \
        auto node = result.sub_queries.back();                                                                         \
        auto agg = result.parameters;                                                                                  \
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
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}, $select: {id, id_col1}})_",
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
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto join = result.sub_queries.back()->children().front();
        // The transformer normalizes (db.schema.tbl) into dbname=db (db
        // preferred over schema when both are present in cfn).
        {
            auto* agg = static_cast<const components::logical_plan::node_aggregate_t*>(join->children().back().get());
            REQUIRE(static_cast<const std::string&>(agg->dbname()) == "db3");
            REQUIRE(static_cast<const std::string&>(agg->relname()) == "test3");
        }

        auto nested_join = join->children().front();
        {
            auto* agg =
                static_cast<const components::logical_plan::node_aggregate_t*>(nested_join->children().front().get());
            REQUIRE(static_cast<const std::string&>(agg->dbname()) == "db1");
            REQUIRE(static_cast<const std::string&>(agg->relname()) == "test1");
        }
        {
            auto* agg =
                static_cast<const components::logical_plan::node_aggregate_t*>(nested_join->children().back().get());
            REQUIRE(static_cast<const std::string&>(agg->dbname()) == "db2");
            REQUIRE(static_cast<const std::string&>(agg->relname()) == "test2");
        }
    }

    // Regression coverage for LEFT OUTER JOIN (gap G11): the OUTER keyword must
    // continue to parse and lower into a $type: left join through future refactors.
    SECTION("left outer join regression") {
        TEST_JOIN(
            R"_(select * from a left outer join b on a.x = b.x and a.y = b.y;)_",
            R"_($aggregate: {$join: {$type: left, $aggregate: {}, $aggregate: {}, $and: ["x": {$eq: "x"}, "y": {$eq: "y"}]}})_",
            vec());

        TEST_JOIN(
            R"_(select * from a left outer join b on a.id = b.fk where b.status = 'active';)_",
            R"_($aggregate: {$join: {$type: left, $aggregate: {}, $aggregate: {}, "id": {$eq: "fk"}}, $match: {"status": {$eq: #0}}})_",
            vec({v(&resource, "active")}));
    }

    // ----------------------------------------------------------------------
    // SQL-89 comma-join: `FROM a, b [, c ...] WHERE ...`
    //
    // libpg_query parses each table in the FROM list as a separate top-level
    // entry (T_RangeVar / T_RangeFunction / T_RangeSubselect). The transformer
    // synthesizes a left-deep JoinExpr tree with jointype=INNER and quals=NULL
    // on every link, which jointype_to_ql promotes to join_type::cross. The
    // WHERE clause stays as a sibling match_t on the aggregate root, where it
    // filters the cross-product output (operator_match feeds the merged chunk
    // in as both left and right operands, so column refs resolve correctly
    // against the join's merged schema regardless of side_t).
    // ----------------------------------------------------------------------
    SECTION("comma join (SQL-89)") {
        // Two-table comma-join with a single equality predicate. The
        // synthesized cross-join lowers into the same shape as
        // `col1 cross join col2`; the equality lives in a separate
        // sibling match_t (not shown in the join's to_string).
        TEST_JOIN(
            R"_(select * from col1, col2 where col1.id = col2.id_col1;)_",
            R"_($aggregate: {$join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}, $match: {"id": {$eq: "id_col1"}}})_",
            vec());

        // Three-table comma-join. Left-deep synthesis yields
        // ((T1 cross T2) cross T3) — nested join_t inside the outer join_t,
        // exactly mirroring `T1 JOIN T2 ON ... JOIN T3 ON ...`.
        TEST_JOIN(
            R"_(select * from col1, col2, col3 where col1.id = col2.id_col1 and col1.id = col3.id_col1;)_",
            R"_($aggregate: {$join: {$type: cross, $join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}, $aggregate: {}, $all_true}, $match: {$and: ["id": {$eq: "id_col1"}, "id": {$eq: "id_col1"}]}})_",
            vec());

        // WHERE with a multi-clause AND: both predicates land in the same
        // match_t. Cross-join itself stays $all_true.
        TEST_JOIN(
            R"_(select * from col1, col2 where col1.id = col2.id_col1 and col1.name = col2.name;)_",
            R"_($aggregate: {$join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}, $match: {$and: ["id": {$eq: "id_col1"}, "name": {$eq: "name"}]}})_",
            vec());

        // Column ambiguity case: both tables carry `id`. The unqualified
        // `id` on the WHERE LHS still parses through the transformer; the
        // validator later resolves it against the merged join schema.
        // Transformer output keeps the bare name with no side annotation.
        TEST_JOIN(
            R"_(select * from col1, col2 where id = col2.id;)_",
            R"_($aggregate: {$join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}, $match: {"id": {$eq: "id"}}})_",
            vec());

        // SELECT-projection over comma-join: column references on both sides
        // resolve via the names collection (left=col1, right=col2), so
        // qualified columns survive into the $select clause.
        TEST_JOIN(
            R"_(select col1.id, col2.id_col1 from col1, col2 where col1.id = col2.id_col1;)_",
            R"_($aggregate: {$join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}, $match: {"id": {$eq: "id_col1"}}, $select: {id, id_col1}})_",
            vec());
    }
}
