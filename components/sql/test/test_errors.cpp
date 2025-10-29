#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

#define TEST_PARSER_ERROR(QUERY, RESULT)                                                                               \
    SECTION(QUERY) {                                                                                                   \
        bool exception_thrown = false;                                                                                 \
        try {                                                                                                          \
            auto select = linitial(raw_parser(&arena_resource, QUERY));                                                \
        } catch (const parser_exception_t& e) {                                                                        \
            exception_thrown = true;                                                                                   \
            REQUIRE(std::string_view{e.what()} == RESULT);                                                             \
        }                                                                                                              \
        REQUIRE(exception_thrown);                                                                                     \
    }

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        bool exception_thrown = false;                                                                                 \
        try {                                                                                                          \
            auto result = transformer.transform(transform::pg_cell_to_node_cast(select));                              \
        } catch (const parser_exception_t& e) {                                                                        \
            exception_thrown = true;                                                                                   \
            REQUIRE(std::string_view{e.what()} == RESULT);                                                             \
        }                                                                                                              \
        REQUIRE(exception_thrown);                                                                                     \
    }

using v = components::document::value_t;
using vec = std::vector<v>;
using fields = std::vector<std::pair<std::string, v>>;

TEST_CASE("sql::errors") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_PARSER_ERROR("INVALID QUERY", R"_(syntax error at or near "INVALID")_");
    TEST_PARSER_ERROR("CREATE DATABASE;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("DROP DATABASE;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("CREATE TABLE;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("DROP TABLE;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("SELECT * FROM;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("INSERT INTO;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("DELETE FROM;", R"_(syntax error at or near ";")_");
    TEST_PARSER_ERROR("UPDATE table_name TO;", R"_(syntax error at or near "TO")_");

    TEST_PARSER_ERROR("delete from schema.table where number == 10 group by name;",
                      R"_(syntax error at or near "group")_");

    TEST_PARSER_ERROR("delete from schema.table where number == 10 order by name;",
                      R"_(syntax error at or near "order")_");

    TEST_PARSER_ERROR("delete from schema. where number == 10;", R"_(syntax error at or near "==")_");

    TEST_PARSER_ERROR("delete from .table where number == 10;", R"_(syntax error at or near ".")_");

    TEST_PARSER_ERROR("delete from schema.table where number == 10 name = 'doc 10';",
                      R"_(syntax error at or near "name")_");

    TEST_PARSER_ERROR("delete from schema.table where number == 10 and and = 'doc 10';",
                      R"_(syntax error at or near "and")_");

    TEST_PARSER_ERROR(R"_(select number name "count" from schema.table;)_", R"_(syntax error at or near ""count"")_");

    TEST_PARSER_ERROR(R"_(select number as, name, "count" from schema.table;)_", R"_(syntax error at or near ",")_");

    TEST_PARSER_ERROR(R"_(select name, title, sum(count) from schema.table group by;)_",
                      R"_(syntax error at or near ";")_");

    TEST_PARSER_ERROR(R"_(select name, title, sum(count) from schema.table group by having;)_",
                      R"_(syntax error at or near "having")_");

    TEST_PARSER_ERROR(R"_(select name, title, sum(count) from schema.table group by name, ;)_",
                      R"_(syntax error at or near ";")_");

    TEST_PARSER_ERROR(R"_(select name, title, sum(count) from schema.table group by name title;)_",
                      R"_(syntax error at or near "title")_");

    TEST_PARSER_ERROR("update schema.table set name = 'new name',;", R"_(syntax error at or near ";")_");

    TEST_PARSER_ERROR("update schema.table set name = 'new name' count = 10;", R"_(syntax error at or near "count")_");

    TEST_PARSER_ERROR("update schema.table set name = 'new name' where number == 10 group by name;",
                      R"_(syntax error at or near "group")_");

    TEST_PARSER_ERROR("update schema. set name = 'new name' where number == 10;", R"_(syntax error at or near "=")_");

    TEST_PARSER_ERROR("update .table set name = 'new name' where number == 10;", R"_(syntax error at or near ".")_");

    TEST_PARSER_ERROR("update schema.table set name = 'new name' where number == 10 name = 'doc 10';",
                      R"_(syntax error at or near "name")_");

    TEST_PARSER_ERROR("update schema.table set name = 'new name' where number == 10 and and = 'doc 10';",
                      R"_(syntax error at or near "and")_");

    TEST_PARSER_ERROR("INSERT INTO 5 (id, name, count) VALUES (1, 'Name', 1);", R"_(syntax error at or near "5")_");

    TEST_PARSER_ERROR("INSERT INTO schema. (id, name, count) VALUES (1, 'Name', 1);",
                      R"_(syntax error at or near "(")_");

    TEST_PARSER_ERROR("INSERT INTO schema.5 (id, name, count) VALUES (1, 'Name', 1);",
                      R"_(syntax error at or near ".5")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name count) VALUES (1, 'Name', 1);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, 5, count) VALUES (1, 'Name', 1);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (*) VALUES (1, 'Name', 1);", R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table () VALUES (1, 'Name', 1);", R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name, count) SET VALUES (1, 'Name', 1);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, 'name', count) VALUES (1, 'Name', 1);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name, count) VALUES (1, Name, 1);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name, count) VALUES ();", R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name, count) VALUES (1, 'Name', 1, 2);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name, count) VALUES "
                      "(1, 'Name1', 1), "
                      "(2, 'Name2', 2, 2), "
                      "(3, 'Name3', 3), "
                      "(4, 'Name4', 4), "
                      "(5, 'Name5', 5);",
                      R"_(syntax error at or near "table")_");

    TEST_PARSER_ERROR("INSERT INTO table (id, name, count) VALUES "
                      "(1, 'Name1', 1), "
                      "(2, 'Name2', 2), "
                      "(3, 'Name3', 3), "
                      "(4, 'Name4', 4), "
                      "(5, 'Name5', 5), ;",
                      R"_(syntax error at or near "table")_");

    TEST_TRANSFORMER_ERROR("CREATE INDEX ON TEST_DATABASE.TEST_COLLECTION (count);",
                           R"_(incorrect create index arguments)_");

    TEST_PARSER_ERROR("CREATE INDEX base ON TEST_DATABASE. (count);", R"_(syntax error at or near "(")_");

    TEST_PARSER_ERROR("CREATE INDEX base ON .TEST_COLLECTION (count);", R"_(syntax error at or near ".")_");

    TEST_PARSER_ERROR("CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION count",
                      R"_(syntax error at or near "count")_");

    TEST_PARSER_ERROR("CREATE base ON TEST_DATABASE.TEST_COLLECTION (count);", R"_(syntax error at or near "base")_");

    TEST_PARSER_ERROR("CREATE INDEX base ON (count);", R"_(syntax error at or near "(")_");

    TEST_PARSER_ERROR("CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION;", R"_(syntax error at or near ";")_");

    TEST_TRANSFORMER_ERROR("DROP INDEX TEST_DATABASE.TEST_COLLECTION;", R"_(incorrect drop: arguments size)_");
}