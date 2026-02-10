#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

using v = components::types::logical_value_t;
using vec = std::vector<v>;

#define TEST_SIMPLE_FUNCTION(QUERY, RESULT, PARAMS)                                                                    \
    {                                                                                                                  \
        SECTION(QUERY) {                                                                                               \
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

TEST_CASE("components::sql::functions") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM some_udf();)_",
                         R"_($aggregate: {$function: {name: {"some_udf"}, args: {}}})_",
                         vec());

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM some_udf(5, 10);)_",
                         R"_($aggregate: {$function: {name: {"some_udf"}, args: {#0, #1}}})_",
                         vec({v(&resource, 5), v(&resource, 10)}));

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM users WHERE is_active_user(id);)_",
                         R"_($aggregate: {$match: {$function: {name: {"is_active_user"}, args: {"$id"}}}})_",
                         vec());

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM users WHERE is_active_user((data).id);)_",
                         R"_($aggregate: {$match: {$function: {name: {"is_active_user"}, args: {"$data/id"}}}})_",
                         vec());

    TEST_SIMPLE_FUNCTION(R"_(SELECT id, text, some_udf(text) AS some_alias FROM some_table;)_",
                         R"_($aggregate: {$group: {id, text, some_alias: {$udf: "$text"}}})_",
                         vec());

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT *, some_udf_1(foo_name) FROM some_udf_2(1) AS some_alias;)_",
        R"_($aggregate: {$function: {name: {"some_udf_2"}, args: {#0}}, $group: {some_udf_1: {$udf: "$foo_name"}}})_",
        vec({v(&resource, 1)}));

    TEST_SIMPLE_FUNCTION(R"_(SELECT some_udf(5, 10);)_",
                         R"_($aggregate: {$group: {some_udf: {$udf: [#0, #1]}}})_",
                         vec({v(&resource, 5), v(&resource, 10)}));

    TEST_SIMPLE_FUNCTION(R"_(SELECT name, some_udf(name, number) AS some_alias;)_",
                         R"_($aggregate: {$group: {name, some_alias: {$udf: ["$name", "$number"]}}})_",
                         vec());

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT * FROM col1 join col2 on udf(col1.id, col2.id);)_",
        R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, $function: {name: {"udf"}, args: {"$id", "$id"}}}})_",
        vec());

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT * FROM col1 join col2 on udf(col1.id, (col2.struct_type).field);)_",
        R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, $function: {name: {"udf"}, args: {"$id", "$struct_type/field"}}}})_",
        vec());

    /*
    TEST_SIMPLE_FUNCTION(
        R"_(SELECT (myfunc(x)).* FROM some_table;)_",
        R"_()_",
        vec());

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT (myfunc(x)).a, (myfunc(x)).b, (myfunc(x)).c FROM some_table;)_",
        R"_()_",
        vec());
    */
}
