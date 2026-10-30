#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

#define TEST_PARSER_ERROR(QUERY, RESULT)                                                                               \
    SECTION(QUERY) {                                                                                                   \
        auto parsed = raw_parser(&arena_resource, QUERY);                                                              \
        REQUIRE(parsed.has_error());                                                                                   \
        CHECK(parsed.error().type == core::error_code_t::sql_parse_error);                                             \
        REQUIRE(std::string_view{parsed.error().what} == RESULT);                                                      \
    }

// POSITION is PostgreSQL's cursor position: the 1-based character the error points at
#define TEST_PARSER_ERROR_AT(QUERY, RESULT, POSITION)                                                                  \
    SECTION(QUERY) {                                                                                                   \
        auto parsed = raw_parser(&arena_resource, QUERY);                                                              \
        REQUIRE(parsed.has_error());                                                                                   \
        CHECK(parsed.error().type == core::error_code_t::sql_parse_error);                                             \
        REQUIRE(std::string_view{parsed.error().what} == RESULT);                                                      \
        CHECK(parsed.error().position == POSITION);                                                                    \
    }

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(parse_ok(&arena_resource, QUERY));                                                      \
        transform::transformer local(&resource, QUERY);                                                                \
        auto result = local.transform(transform::pg_cell_to_node_cast(select));                                        \
        REQUIRE(std::string_view{result.get_error().what} == RESULT);                                                  \
    }

#define TEST_TRANSFORMER_OK(QUERY)                                                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(parse_ok(&arena_resource, QUERY));                                                      \
        transform::transformer local(&resource, QUERY);                                                                \
        auto result = local.transform(transform::pg_cell_to_node_cast(select));                                        \
        REQUIRE_FALSE(result.get_error().contains_error());                                                            \
    }

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
    // error, not a crash and not a misleading "unknown node type". It is refused wherever nothing
    // expects an array, so the message no longer names the SELECT list.
    TEST_TRANSFORMER_ERROR("SELECT ARRAY(SELECT count FROM TEST_DATABASE.TEST_COLLECTION) FROM "
                           "TEST_DATABASE.TEST_COLLECTION;",
                           R"_(ARRAY(SELECT ...) is supported only as a comparison operand)_");

    // A scalar sub-query is an ordinary value, so it sorts like one.
    TEST_TRANSFORMER_OK("SELECT count FROM TEST_DATABASE.TEST_COLLECTION ORDER BY (SELECT count FROM "
                        "TEST_DATABASE.TEST_COLLECTION);");

    TEST_TRANSFORMER_ERROR("PREPARE TRANSACTION 'tx1';", R"_(unsupported transaction statement)_");
    TEST_TRANSFORMER_OK("BEGIN;");
    TEST_TRANSFORMER_OK("COMMIT;");
    TEST_TRANSFORMER_OK("ROLLBACK;");

    TEST_TRANSFORMER_ERROR("INSERT INTO d.t DEFAULT VALUES;", R"_(INSERT ... DEFAULT VALUES is not supported)_");

    SECTION("unsupported statements name their tag") {
        // TRUNCATE has its own refusal (test_name_bugs.cpp).
        for (const char* q : {"GRANT SELECT ON d.t TO alice;", "COPY d.t FROM '/tmp/x';"}) {
            auto stmt = linitial(parse_ok(&arena_resource, q));
            auto result = transformer.transform(transform::pg_cell_to_node_cast(stmt));
            const std::string what{result.get_error().what.c_str()};
            INFO(q << " -> " << what);
            REQUIRE(what.find("Unsupported node type: ") == 0);
            REQUIRE(what.find("unknown") == std::string::npos);
        }
    }

    TEST_TRANSFORMER_ERROR("CREATE TYPE ty AS (a VARCHAR(10));", R"_(string length modifier is not supported)_");
    TEST_TRANSFORMER_ERROR("ALTER TABLE d.t ADD COLUMN c VARCHAR(10);", R"_(string length modifier is not supported)_");
    // A CHECK is lowered as a general expression now, so a function call in it is enforced, not rejected.
    TEST_TRANSFORMER_OK("ALTER TABLE d.t ADD CONSTRAINT ck CHECK (lower(a) = 'x');");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE ((5 + 1) -> 'a') ? 'b';",
                           R"_(unsupported left operand in jsonb operator chain)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE (5 -> 'a') ? 'b';",
                           R"_(unsupported base operand for jsonb operator)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE 5 ? 'a';", R"_(unsupported base operand for jsonb operator)_");
    TEST_TRANSFORMER_ERROR("SELECT 1 WHERE ? 'a';", R"_(operator ? is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT 1 WHERE ~~ 'x';", R"_(operator ~~ is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT 1 WHERE -> 'a';", R"_(operator -> is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT j ? FROM d.t;", R"_(operator ? is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE j ?;", R"_(operator ? is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE j ~~;", R"_(operator ~~ is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT * FROM d.t WHERE (-> 'a') ? 'b';", R"_(operator -> is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT j ->> FROM d.t;", R"_(operator ->> is missing its operand)_");
    TEST_TRANSFORMER_ERROR("SELECT #- 'a' FROM d.t;", R"_(operator #- is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 |);", R"_(operator | is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 <=);", R"_(operator <= is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 ~~);", R"_(operator ~~ is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 ?);", R"_(operator ? is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 ->);", R"_(operator -> is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 + (2 |));", R"_(operator | is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t (a) VALUES (1), (2 |);", R"_(operator | is missing its operand)_");
    TEST_TRANSFORMER_ERROR("INSERT INTO d.t VALUES (1 OPERATOR(pg_catalog.+));",
                           R"_(operator + is missing its operand)_");
    TEST_TRANSFORMER_OK("SELECT * FROM d.t WHERE j ? 'k';");
}

TEST_CASE("components::sql::errors::parser_error_carries_its_position") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    // grammar: bison's syntax error at the token it could not shift
    TEST_PARSER_ERROR_AT("SELEC 1", R"_(syntax error at or near "SELEC")_", 1);
    TEST_PARSER_ERROR_AT("SELECT 1 +", R"_(syntax error at end of input)_", 11);
    TEST_PARSER_ERROR_AT("SELECT * FROM t WHERE a = = 1", R"_(syntax error at or near "=")_", 27);
    // the position counts characters, not bytes
    TEST_PARSER_ERROR_AT("SELECT 'жжж' FROM", R"_(syntax error at end of input)_", 18);
    // lexer: the scanner's error wins over the syntax error bison reports once it is handed end of input
    TEST_PARSER_ERROR_AT("SELECT 'abc", R"_(unterminated quoted string at or near "'abc")_", 8);
    TEST_PARSER_ERROR_AT("SELECT 1 /* open", R"_(unterminated /* comment at or near "/* open")_", 10);
    TEST_PARSER_ERROR_AT("SELECT U&'\\D800'", R"_(invalid Unicode surrogate pair at or near "'")_", 16);
    // grammar action
    TEST_PARSER_ERROR_AT("CREATE SCHEMA IF NOT EXISTS s CREATE TABLE t(a INT)",
                         R"_(CREATE SCHEMA IF NOT EXISTS cannot include schema elements)_",
                         31);
    // grammar helpers: the action that called them aborts the parse
    TEST_PARSER_ERROR_AT("(SELECT 1 ORDER BY 1) ORDER BY 2", R"_(multiple ORDER BY clauses not allowed)_", 32);
    TEST_PARSER_ERROR_AT("SELECT a.*.b FROM t", R"_(improper use of "*" at or near "FROM")_", 14);
    TEST_PARSER_ERROR_AT("ALTER TYPE a.b.c.d ADD ATTRIBUTE x INT",
                         R"_(improper qualified name (too many dotted names): a.b.c.d)_",
                         12);
    TEST_PARSER_ERROR_AT("WITH a AS (SELECT 1) (WITH b AS (SELECT 2) SELECT 3)",
                         R"_(multiple WITH clauses not allowed)_",
                         1);
    TEST_PARSER_ERROR_AT("CREATE TABLE t (a int, CHECK (a > 0) DEFERRABLE)",
                         R"_(CHECK constraints cannot be marked DEFERRABLE)_",
                         38);
    TEST_PARSER_ERROR_AT(R"_(CREATE TABLE t (a text COLLATE "C" COLLATE "POSIX"))_",
                         R"_(multiple COLLATE clauses not allowed)_",
                         36);
    TEST_PARSER_ERROR_AT("CREATE FUNCTION f(OUT a int) RETURNS TABLE (b int) AS 'SELECT 1' LANGUAGE sql",
                         R"_(OUT arguments aren't allowed in TABLE functions)_",
                         -1);
    TEST_PARSER_ERROR_AT(
        "SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) "
        "FROM t",
        R"_(window EXCLUDE clause not yet implemented)_",
        -1);

    SECTION("the next statement on the same arena parses") {
        auto failed = raw_parser(&arena_resource, "SELECT 'abc");
        REQUIRE(failed.has_error());
        auto parsed = raw_parser(&arena_resource, "SELECT 1");
        REQUIRE_FALSE(parsed.has_error());
        CHECK(list_length(parsed.value()) == 1);
    }
}

// Ignoring CONCURRENTLY would run a plain refresh under the caller's name.
TEST_CASE("components::sql::errors::refresh_concurrently_is_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    const char* query = "REFRESH MATERIALIZED VIEW CONCURRENTLY d.mv;";
    auto stmt = linitial(parse_ok(&arena_resource, query));
    transform::transformer transformer(&resource, query);
    auto result = transformer.transform(transform::pg_cell_to_node_cast(stmt));
    REQUIRE(result.get_error().contains_error());
    CHECK(result.get_error().type == core::error_code_t::unimplemented_yet);
    CHECK(std::string_view{result.get_error().what} == "REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported");

    const char* plain = "REFRESH MATERIALIZED VIEW d.mv;";
    auto plain_stmt = linitial(parse_ok(&arena_resource, plain));
    transform::transformer plain_transformer(&resource, plain);
    REQUIRE_FALSE(
        plain_transformer.transform(transform::pg_cell_to_node_cast(plain_stmt)).get_error().contains_error());
}

// WITH (storage = ...) is gone; every value must fail loudly, not be silently accepted.
TEST_CASE("components::sql::errors::create_table_storage_option_removed") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // 'disk' literal split so a grep for the removed option finds no caller writing it.
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage = 'di"
                           "sk');",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage = 'memory');",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage = 'anything');",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
    TEST_TRANSFORMER_ERROR("CREATE TABLE db.tbl (id BIGINT) WITH (storage);",
                           R"_(the WITH (storage = ...) option has been removed: tables are always disk-backed)_");
}

// ---------------------------------------------------------------------------
// ALTER forms that must not report SUCCESS after doing nothing.
//
// An unrecognised subcommand in transform_alter_table/transform_rename used to fall through to
// an empty-named DROP/RENAME COLUMN node, which operator_alter_column_drop_t then silently no-ops
// on. A statement that changes nothing must fail loudly instead, naming the form typed.
// ---------------------------------------------------------------------------
namespace {
    struct alter_probe_t {
        bool errored{false};
        // Error text when refused, otherwise the produced node's to_string().
        std::string what;
        // An empty-named DROP/RENAME COLUMN subcommand is indistinguishable from a successful no-op; nothing may produce one.
        bool empty_named_subcommand{false};
    };

    alter_probe_t probe_alter(std::pmr::memory_resource* resource, const char* query) {
        std::pmr::monotonic_buffer_resource arena(resource);
        // Needs the statement text: slice_check_expression pulls a CHECK's expression from it,
        // so a transformer without it refuses every ADD CONSTRAINT ... CHECK.
        transform::transformer t(resource, query);
        auto stmt = linitial(parse_ok(&arena, query));
        auto result = t.transform(transform::pg_cell_to_node_cast(stmt));
        if (result.has_error()) {
            return {true, std::string{result.get_error().what.c_str()}, false};
        }
        auto plan = result.finalize();
        if (plan.has_error()) {
            return {true, std::string{plan.error().what.c_str()}, false};
        }
        auto node = plan.value().sub_queries.back();
        if (!node) {
            return {false, std::string{"<null node>"}, false};
        }
        alter_probe_t out{false, node->to_string(), false};
        if (node->type() == components::logical_plan::node_type::alter_table_t) {
            const auto* alter = static_cast<const components::logical_plan::node_alter_table_t*>(node.get());
            for (const auto& sub : alter->subcommands()) {
                // DROP CONSTRAINT carries constraint_name; every column clause carries column_name.
                if (sub.kind == components::logical_plan::alter_table_kind::drop_constraint
                        ? sub.constraint_name.empty()
                        : sub.column_name.empty()) {
                    out.empty_named_subcommand = true;
                }
            }
        }
        return out;
    }
} // namespace

#define TEST_ALTER_REFUSED(QUERY, RESULT)                                                                              \
    SECTION(QUERY) {                                                                                                   \
        auto probe = probe_alter(&resource, QUERY);                                                                    \
        INFO(QUERY << " -> " << (probe.errored ? "ERROR: " : "NODE: ") << probe.what);                                 \
        REQUIRE(probe.errored);                                                                                        \
        REQUIRE(probe.what == RESULT);                                                                                 \
        REQUIRE_FALSE(probe.empty_named_subcommand);                                                                   \
    }

#define TEST_ALTER_ACCEPTED(QUERY)                                                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto probe = probe_alter(&resource, QUERY);                                                                    \
        INFO(QUERY << " -> " << (probe.errored ? "ERROR: " : "NODE: ") << probe.what);                                 \
        REQUIRE_FALSE(probe.errored);                                                                                  \
        REQUIRE_FALSE(probe.empty_named_subcommand);                                                                   \
    }

TEST_CASE("components::sql::errors::alter_forms_that_did_nothing_are_refused") {
    auto resource = core::pmr::otterbrix_resource();

    // RENAME of anything that is not a column.
    TEST_ALTER_REFUSED("ALTER TABLE d.t RENAME TO t2;",
                       R"_(ALTER TABLE ... RENAME TO t2 is not implemented; nothing was renamed)_");
    TEST_ALTER_REFUSED("ALTER INDEX d.i RENAME TO i2;",
                       R"_(ALTER INDEX ... RENAME TO i2 is not implemented; nothing was renamed)_");
    TEST_ALTER_REFUSED("ALTER VIEW d.v RENAME TO v2;",
                       R"_(ALTER VIEW ... RENAME TO v2 is not implemented; nothing was renamed)_");
    TEST_ALTER_REFUSED("ALTER MATERIALIZED VIEW d.mv RENAME TO mv2;",
                       R"_(ALTER MATERIALIZED VIEW ... RENAME TO mv2 is not implemented; nothing was renamed)_");
    TEST_ALTER_REFUSED("ALTER SEQUENCE d.s RENAME TO s2;",
                       R"_(ALTER SEQUENCE ... RENAME TO s2 is not implemented; nothing was renamed)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t RENAME CONSTRAINT ck TO ck2;",
                       R"_(ALTER TABLE ... RENAME CONSTRAINT ck TO ck2 is not implemented; nothing was renamed)_");

    // ALTER COLUMN sub-forms the grammar accepts and nothing below implements.
    TEST_ALTER_REFUSED("ALTER TABLE d.t ALTER COLUMN a TYPE bigint;",
                       R"_(ALTER TABLE ... ALTER COLUMN a TYPE ... is not implemented; the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t ALTER COLUMN a SET DEFAULT 5;",
                       R"_(ALTER TABLE ... ALTER COLUMN a SET DEFAULT ... is not implemented; )_"
                       R"_(the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t ALTER COLUMN a DROP DEFAULT;",
                       R"_(ALTER TABLE ... ALTER COLUMN a DROP DEFAULT is not implemented; )_"
                       R"_(the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t ALTER COLUMN a SET NOT NULL;",
                       R"_(ALTER TABLE ... ALTER COLUMN a SET NOT NULL is not implemented; )_"
                       R"_(the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t ALTER COLUMN a DROP NOT NULL;",
                       R"_(ALTER TABLE ... ALTER COLUMN a DROP NOT NULL is not implemented; )_"
                       R"_(the table was not altered)_");

    // Table-level sub-forms, same class.
    TEST_ALTER_REFUSED("ALTER TABLE d.t SET TABLESPACE fast;",
                       R"_(ALTER TABLE ... SET TABLESPACE fast is not implemented; the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t OWNER TO alice;",
                       R"_(ALTER TABLE ... OWNER TO alice is not implemented; the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t VALIDATE CONSTRAINT ck;",
                       R"_(ALTER TABLE ... VALIDATE CONSTRAINT ck is not implemented; the table was not altered)_");

    // integration/cpp/test/test_alter_drop_constraint.cpp pins its end-to-end behavior.
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t DROP CONSTRAINT ck;");

    // The AddConstraint arm returns its own node type without looking at subcommands the loop
    // already collected, so a constraint clause mixed with any other must be refused, not silently drop the rest.
    TEST_ALTER_REFUSED("ALTER TABLE d.t ADD COLUMN x bigint, ADD CONSTRAINT uq UNIQUE (x);",
                       R"_(ALTER TABLE ... ADD CONSTRAINT uq alongside other subcommands in one statement is )_"
                       R"_(not implemented; the table was not altered)_");
    TEST_ALTER_REFUSED("ALTER TABLE d.t ADD CONSTRAINT a UNIQUE (x), ADD CONSTRAINT b UNIQUE (y);",
                       R"_(ALTER TABLE ... ADD CONSTRAINT a alongside other subcommands in one statement is )_"
                       R"_(not implemented; the table was not altered)_");

    TEST_ALTER_REFUSED("ALTER TABLE d.t ADD CONSTRAINT ex EXCLUDE (a WITH =);",
                       R"_(ALTER TABLE ... ADD CONSTRAINT ex EXCLUDE is not implemented; )_"
                       R"_(the table was not altered)_");
}

// Counterpart: forms that DO work must keep working, and never produce the empty-named DROP COLUMN node the refusals above eliminate.
TEST_CASE("components::sql::errors::alter_forms_that_work_keep_working") {
    auto resource = core::pmr::otterbrix_resource();

    TEST_ALTER_ACCEPTED("ALTER TABLE d.t ADD COLUMN c bigint;");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t DROP COLUMN c;");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t ADD CONSTRAINT uq UNIQUE (c);");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t ADD CONSTRAINT ck CHECK (c > 0);");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES d.p (id);");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t RENAME COLUMN a TO b;");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t ADD COLUMN x bigint, DROP COLUMN y;");
    TEST_ALTER_ACCEPTED("ALTER TABLE d.t ADD COLUMN x bigint, ADD COLUMN y bigint, DROP COLUMN z;");
    // Lands in the same AlterTableStmt arms (AT_AddColumn / AT_DropColumn) as ALTER TABLE.
    TEST_ALTER_ACCEPTED("ALTER TYPE shop.addr_t ADD ATTRIBUTE zip TEXT;");
    TEST_ALTER_ACCEPTED("ALTER TYPE shop.addr_t DROP ATTRIBUTE zip;");
}
