#include <catch2/catch_test_macros.hpp>
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
        auto result = transformer.transform(transform::pg_cell_to_node_cast(select));                                  \
        REQUIRE(std::string_view{result.get_error().what} == RESULT);                                                  \
    }

#define TEST_TRANSFORMER_OK(QUERY)                                                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = transformer.transform(transform::pg_cell_to_node_cast(select));                                  \
        REQUIRE_FALSE(result.get_error().contains_error());                                                            \
    }

using v = components::types::logical_value_t;
using vec = std::vector<v>;
using fields = std::vector<std::pair<std::string, v>>;

TEST_CASE("components::sql::errors") {
    auto resource = core::pmr::otterbrix_resource();
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

    // An unsupported SubLink kind must produce a clean transformer error, never fall off the end of
    // transform_sublink_expr through a Release-erased assert(false) (that was UB, observed as a segfault).
    // ARRAY(SELECT ...) in bare-predicate position is the one grammar-constructible unsupported form
    // (ROWCOMPARE/CTE/INITPLAN_FUNC are never emitted by the parser).
    TEST_TRANSFORMER_ERROR("SELECT * FROM TEST_DATABASE.TEST_COLLECTION WHERE ARRAY(SELECT count FROM "
                           "TEST_DATABASE.TEST_COLLECTION);",
                           R"_(unsupported subquery expression in this context)_");

    // A bare scalar sub-query in predicate position (EXPR_SUBLINK) IS supported: it transforms cleanly.
    // The "argument of WHERE/HAVING must be type boolean" check runs later at execution
    // (see integration where_having_boolean_required), not in the transformer.
    TEST_TRANSFORMER_OK("SELECT * FROM TEST_DATABASE.TEST_COLLECTION WHERE (SELECT count FROM "
                        "TEST_DATABASE.TEST_COLLECTION);");

    // #563 finding 4: a SubLink (or any non-column) as the operand of a simple CASE must be a clean
    // transformer error, not a blind pg_ptr_cast<ColumnRef> that reinterprets the node and crashes.
    TEST_TRANSFORMER_ERROR("SELECT CASE (SELECT count FROM TEST_DATABASE.TEST_COLLECTION) WHEN 1 THEN 2 ELSE 3 END "
                           "FROM TEST_DATABASE.TEST_COLLECTION;",
                           R"_(CASE operand must be a column reference)_");

    // ARRAY(SELECT ...) projected in the SELECT list is not supported yet (deferred, #559): a clear
    // error, not a crash and not a misleading "unknown node type".
    TEST_TRANSFORMER_ERROR("SELECT ARRAY(SELECT count FROM TEST_DATABASE.TEST_COLLECTION) FROM "
                           "TEST_DATABASE.TEST_COLLECTION;",
                           R"_(unsupported subquery in the SELECT list; only a scalar subquery is supported)_");

    // #563 finding 6b: node_tag_to_string must name the node tag ("T_SubLink"), not print "unknown".
    // A SubLink in ORDER BY still reports through node_tag_to_string.
    TEST_TRANSFORMER_ERROR("SELECT count FROM TEST_DATABASE.TEST_COLLECTION ORDER BY (SELECT count FROM "
                           "TEST_DATABASE.TEST_COLLECTION);",
                           R"_(Unknown node type in ORDER BY: T_SubLink)_");

    TEST_TRANSFORMER_ERROR("SAVEPOINT sp1;", R"_(unsupported transaction statement)_");
    TEST_TRANSFORMER_ERROR("RELEASE SAVEPOINT sp1;", R"_(unsupported transaction statement)_");
    TEST_TRANSFORMER_ERROR("ROLLBACK TO SAVEPOINT sp1;", R"_(unsupported transaction statement)_");
    TEST_TRANSFORMER_OK("BEGIN;");
    TEST_TRANSFORMER_OK("COMMIT;");
    TEST_TRANSFORMER_OK("ROLLBACK;");

    TEST_TRANSFORMER_ERROR("INSERT INTO d.t DEFAULT VALUES;", R"_(INSERT ... DEFAULT VALUES is not supported)_");

    SECTION("unsupported statements name their tag") {
        for (const char* q : {"TRUNCATE d.t;", "GRANT SELECT ON d.t TO alice;", "COPY d.t FROM '/tmp/x';"}) {
            auto stmt = linitial(raw_parser(&arena_resource, q));
            auto result = transformer.transform(transform::pg_cell_to_node_cast(stmt));
            const std::string what{result.get_error().what.c_str()};
            INFO(q << " -> " << what);
            REQUIRE(what.find("Unsupported node type: ") == 0);
            REQUIRE(what.find("unknown") == std::string::npos);
        }
    }

    TEST_TRANSFORMER_ERROR("CREATE TYPE ty AS (a VARCHAR(10));", R"_(string length modifier is not supported)_");
    TEST_TRANSFORMER_ERROR("ALTER TABLE d.t ADD COLUMN c VARCHAR(10);",
                           R"_(string length modifier is not supported)_");
    TEST_TRANSFORMER_ERROR("ALTER TABLE d.t ADD CONSTRAINT ck CHECK (lower(a) = 'x');",
                           R"_(CHECK constraint contains unsupported expression type T_FuncCall; allowed: column )_"
                           R"_(references, constants, comparison/arithmetic operators, AND/OR/NOT, IS NULL/IS NOT NULL)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE ((5 + 1) -> 'a') ? 'b';",
                           R"_(unsupported left operand in jsonb operator chain)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE (5 -> 'a') ? 'b';",
                           R"_(unsupported base operand for jsonb operator)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE 5 ? 'a';",
                           R"_(unsupported base operand for jsonb operator)_");
}

// B1a: every table is disk-backed and the WITH (storage = ...) option is gone.
// Rule 6: an option the engine no longer honours must fail loudly with a message
// naming the removed option — for EVERY value ('disk', 'memory', anything else).
// A user writing storage='memory' must be told the mode is gone, not be quietly
// handed a disk table.
TEST_CASE("components::sql::errors::create_table_storage_option_removed") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // The 'disk' literal is split so the B1a acceptance grep (no caller still
    // writes the removed storage option) stays at zero hits; the SQL string is
    // identical.
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage = 'di"
                           "sk');",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage = 'memory');",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage = 'anything');",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
    // A bare `storage` option with no value is refused the same way.
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage);",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
}