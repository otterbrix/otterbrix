#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::types;
using namespace components::sql::transform;

// Entries registered on one resolve slot (0 when the plan needs no lookup of
// that kind).
inline std::size_t entry_count(const node_catalog_resolve_ptr& node) { return node ? node->entries().size() : 0; }

// The transformer does not wrap plans: sub_queries.back() IS the consumer node,
// and the catalog lookups it needs live on the plan. Assert both — this is what
// the old "$sequence[N]" child-count assertions were really pinning down.
#define TEST_TRANSFORMER_OK(QUERY, ROOT_TYPE, NS_COUNT, TBL_COUNT)                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = parse_ok(&arena_resource, QUERY)->lst.front().data;                                                \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        const auto& plan = result.value();                                                                             \
        REQUIRE(plan.sub_queries.back()->type() == ROOT_TYPE);                                                         \
        REQUIRE(entry_count(plan.catalog_resolves.namespaces) == NS_COUNT);                                            \
        REQUIRE(entry_count(plan.catalog_resolves.tables) == TBL_COUNT);                                               \
    }

#define TEST_TRANSFORMER_DATABASE_OK(QUERY, ROOT_TYPE)                                                                 \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = parse_ok(&arena_resource, QUERY)->lst.front().data;                                                \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        const auto& plan = result.value();                                                                             \
        REQUIRE(plan.sub_queries.back()->type() == ROOT_TYPE);                                                         \
        REQUIRE(entry_count(plan.catalog_resolves.database) == 1);                                                     \
        REQUIRE(entry_count(plan.catalog_resolves.namespaces) == 0);                                                   \
        REQUIRE(entry_count(plan.catalog_resolves.tables) == 0);                                                       \
    }

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto create = linitial(parse_ok(&arena_resource, QUERY));                                                      \
        REQUIRE(transformer.transform(pg_cell_to_node_cast(create)).has_error());                                      \
    }

#define TEST_TRANSFORMER_EXPECT_SCHEMA(QUERY, CHECK_FN)                                                                \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = linitial(parse_ok(&arena_resource, QUERY));                                                        \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        auto node = result.value().sub_queries.back();                                                                 \
        auto* data = static_cast<node_create_collection_t*>(node.get());                                               \
        const auto& schema = data->schema();                                                                           \
        CHECK_FN(schema);                                                                                              \
    }

namespace {
    template<typename T>
    bool contains(const std::pmr::vector<complex_logical_type>& schema, T&& pred) {
        return std::find_if(schema.begin(), schema.end(), std::move(pred)) != schema.end();
    }
} // namespace

TEST_CASE("components::sql::database") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // A database is a pg_database row: CREATE / DROP DATABASE register one database lookup, no schema.
    TEST_TRANSFORMER_DATABASE_OK("CREATE DATABASE db_name", node_type::create_database_t);
    TEST_TRANSFORMER_DATABASE_OK("CREATE DATABASE db_name;", node_type::create_database_t);
    TEST_TRANSFORMER_DATABASE_OK("CREATE DATABASE db_name;          ", node_type::create_database_t);
    TEST_TRANSFORMER_DATABASE_OK("CREATE DATABASE db_name; -- comment", node_type::create_database_t);
    TEST_TRANSFORMER_DATABASE_OK("CREATE DATABASE db_name; /* multiline\ncomments */", node_type::create_database_t);
    TEST_TRANSFORMER_DATABASE_OK("CREATE /* comment */ DATABASE db_name;", node_type::create_database_t);
    // The drop node carries the name and gets database_oid pasted on by enrich.
    TEST_TRANSFORMER_DATABASE_OK("DROP DATABASE db_name;", node_type::drop_t);
}

TEST_CASE("components::sql::schema") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // A schema is a pg_namespace row of the current database: one schema lookup.
    TEST_TRANSFORMER_OK("CREATE SCHEMA s;", node_type::create_schema_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE SCHEMA IF NOT EXISTS s;", node_type::create_schema_t, 1, 0);
    TEST_TRANSFORMER_OK("DROP SCHEMA s;", node_type::drop_t, 1, 0);
    TEST_TRANSFORMER_OK("DROP SCHEMA s CASCADE;", node_type::drop_t, 1, 0);
    TEST_TRANSFORMER_ERROR("DROP SCHEMA d.s;", "DROP SCHEMA takes a simple name");
    TEST_TRANSFORMER_ERROR("CREATE SCHEMA s CREATE TABLE t (a BIGINT);", "schema elements are not implemented");
}

TEST_CASE("components::sql::table") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_OK("CREATE TABLE uuid.db_name.schema.table_name()", node_type::create_collection_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE TABLE db_name.schema.table_name()", node_type::create_collection_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE TABLE db_name.table_name()", node_type::create_collection_t, 1, 0);
    // No schema written: the executor fills it from the place, so the transformer looks up no schema.
    TEST_TRANSFORMER_OK("CREATE TABLE table_name()", node_type::create_collection_t, 0, 0);

    // DROP TABLE registers a namespace + table lookup; the drop node carries the
    // names and gets namespace_oid + table_oid pasted on by enrich.
    TEST_TRANSFORMER_OK("DROP TABLE uuid.db_name.schema.table_name", node_type::drop_t, 1, 1);
    TEST_TRANSFORMER_OK("DROP TABLE db_name.schema.table_name", node_type::drop_t, 1, 1);
    TEST_TRANSFORMER_OK("DROP TABLE db_name.table_name", node_type::drop_t, 1, 1);
    // No db prefix → the table lookup carries an empty dbname, and no namespace
    // lookup is registered at all.
    TEST_TRANSFORMER_OK("DROP TABLE table_name", node_type::drop_t, 0, 1);

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name(test integer, test1 string)",
                                   [](const std::pmr::vector<complex_logical_type>& sch) {
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "test" && type.type() == logical_type::INTEGER;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "test1" &&
                                                  type.type() == logical_type::STRING_LITERAL;
                                       }));
                                   });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 blob, t2 uint, t3 uhugeint, t4 timestamp, t5 decimal(5, 4))",
        [](const std::pmr::vector<complex_logical_type>& sch) {
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t1" && t.type() == logical_type::BLOB;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t2" && t.type() == logical_type::UINTEGER;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t3" && t.type() == logical_type::UHUGEINT;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t4" && t.type() == logical_type::TIMESTAMP;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                if (t.type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(t.extension());
                return t.alias() == "t5" && decimal->width() == 5 && decimal->scale() == 4;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 decimal(21, 3)[10], t2 int[100], t3 boolean[8])",
        [](const std::pmr::vector<complex_logical_type>& sch) {
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                if (array->internal_type().type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(array->internal_type().extension());
                return type.alias() == "t1" && decimal->width() == 21 && decimal->scale() == 3 && array->size() == 10;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                return type.alias() == "t2" && array->internal_type() == logical_type::INTEGER && array->size() == 100;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                return type.alias() == "t3" && array->internal_type() == logical_type::BOOLEAN && array->size() == 8;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name(t1 float, t2 double, t3 float[100])",
                                   [](const std::pmr::vector<complex_logical_type>& sch) {
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "t1" && type.type() == logical_type::FLOAT;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "t2" && type.type() == logical_type::DOUBLE;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           if (type.type() != logical_type::ARRAY)
                                               return false;
                                           auto array = static_cast<array_logical_type_extension*>(type.extension());
                                           return type.alias() == "t3" &&
                                                  array->internal_type() == logical_type::FLOAT && array->size() == 100;
                                       }));
                                   });

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name("
                                   "  t1 DATE,"
                                   "  t2 TIME,"
                                   "  t3 TIME WITH TIME ZONE,"
                                   "  t4 TIMESTAMP,"
                                   "  t5 TIMESTAMP WITH TIME ZONE,"
                                   "  t6 INTERVAL"
                                   ")",
                                   [](const std::pmr::vector<complex_logical_type>& sch) {
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t1" && t.type() == logical_type::DATE;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t2" && t.type() == logical_type::TIME;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t3" && t.type() == logical_type::TIME_TZ;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t4" && t.type() == logical_type::TIMESTAMP;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t5" && t.type() == logical_type::TIMESTAMP_TZ;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t6" && t.type() == logical_type::INTERVAL;
                                       }));
                                   });

    SECTION("incorrect types") {
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal)",
                               R"_(Incorrect modifiers for DECIMAL, width and scale required)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal(10))",
                               R"_(Incorrect modifiers for DECIMAL, width and scale required)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal(correct, expressions))",
                               R"_(Incorrect width or scale for DECIMAL, must be integer)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal(10, 5, something))",
                               R"_(Incorrect modifiers for DECIMAL, width and scale required)_");
    }
}

TEST_CASE("components::sql::index") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // CREATE INDEX registers TWO table lookups, like DROP INDEX below: the indexed table, and the
    // index's own name (probing pg_class for a relation already using it).
    SECTION("create with uuid") {
        auto create =
            parse_ok(&arena_resource, "CREATE INDEX some_idx ON uuid.db.schema.table (field);")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_index_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    SECTION("create with schema") {
        auto create = parse_ok(&arena_resource, "CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_index_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    TEST_TRANSFORMER_OK("CREATE INDEX some_idx ON db.table (field);", node_type::create_index_t, 1, 2);

    // DROP INDEX names TWO pg_class rows — the parent table and the index — so it
    // registers two table lookups, and the drop node carries both names.
    SECTION("drop with uuid") {
        auto drop = parse_ok(&arena_resource, "DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    SECTION("drop with schema") {
        auto drop = parse_ok(&arena_resource, "DROP INDEX db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    TEST_TRANSFORMER_OK("DROP INDEX db.table.some_idx", node_type::drop_t, 1, 2);
}

TEST_CASE("components::sql::types") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // An unqualified name looks up no schema: the executor completes it with the session's place.
    TEST_TRANSFORMER_OK("CREATE TYPE custom_type_name AS (f1 int, f2 string);", node_type::create_type_t, 0, 0);

    TEST_TRANSFORMER_OK("CREATE TYPE custom_enum AS ENUM ('f1', 'f2', 'f3');", node_type::create_type_t, 0, 0);

    TEST_TRANSFORMER_OK("DROP TYPE custom_type_name", node_type::drop_t, 0, 0);

    TEST_TRANSFORMER_OK("CREATE TABLE table_ (custom_type_name custom_type);", node_type::create_collection_t, 0, 0);

    // INSERT is wrapped in sequence_t(resolve_table, resolve_constraint,
    // insert) — no dbname so no resolve_namespace.
    TEST_TRANSFORMER_OK("INSERT INTO table_ (custom_type_name) VALUES (ROW('text', 42))", node_type::insert_t, 0, 1);
}

// Reading only objects->lst.front() would plan `DROP TABLE a, b` as a silent no-op drop of `a` alone.
TEST_CASE("components::sql::drop_names_every_object_or_refuses") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto refusal_of = [&](const char* query) {
        auto stmt = parse_ok(&arena_resource, query)->lst.front().data;
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(result.has_error());
        return std::string{result.error().what};
    };

    SECTION("DROP TABLE a, b") {
        const std::string what = refusal_of("DROP TABLE db_name.first_table, db_name.second_table");
        CHECK(what.find("second_table") != std::string::npos);
    }

    SECTION("DROP SEQUENCE a, b") {
        const std::string what = refusal_of("DROP SEQUENCE db_name.seq_a, db_name.seq_b");
        CHECK(what.find("seq_b") != std::string::npos);
    }

    SECTION("DROP VIEW a, b, c") {
        const std::string what = refusal_of("DROP VIEW db_name.v1, db_name.v2, db_name.v3");
        CHECK(what.find("v2") != std::string::npos);
        CHECK(what.find("v3") != std::string::npos);
    }

    SECTION("DROP INDEX a, b") {
        const std::string what = refusal_of("DROP INDEX db_name.tbl.idx_a, db_name.tbl.idx_b");
        CHECK(what.find("idx_b") != std::string::npos);
    }

    SECTION("DROP TYPE a, b") {
        const std::string what = refusal_of("DROP TYPE type_a, type_b");
        CHECK(what.find("type_b") != std::string::npos);
    }

    SECTION("a single object is still planned") {
        auto stmt = parse_ok(&arena_resource, "DROP TABLE db_name.only_one")->lst.front().data;
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value().sub_queries.back()->type() == node_type::drop_t);
    }
}

// Any method that is not the literal "hash" must not collapse into index_type::single.
TEST_CASE("components::sql::create_index_access_method") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = parse_ok(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };

    SECTION("USING gin is refused, and the refusal names gin") {
        auto result = plan_of("CREATE INDEX gin_idx ON db.tbl USING gin (field);");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("gin") != std::string::npos);
    }

    SECTION("a misspelled method is refused, and the refusal names it") {
        auto result = plan_of("CREATE INDEX typo_idx ON db.tbl USING hsah (field);");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("hsah") != std::string::npos);
    }

    SECTION("USING brin is refused") {
        REQUIRE(plan_of("CREATE INDEX brin_idx ON db.tbl USING brin (field);").has_error());
    }

    SECTION("USING spgist is refused") {
        REQUIRE(plan_of("CREATE INDEX sp_idx ON db.tbl USING spgist (field);").has_error());
    }

    SECTION("USING hash still builds a hashed index") {
        auto result = plan_of("CREATE INDEX h_idx ON db.tbl USING hash (field);");
        REQUIRE_FALSE(result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::create_index_t);
        CHECK(static_cast<node_create_index_t*>(node.get())->type() == index_type::hashed);
    }

    SECTION("USING btree, and the omitted clause, still build a single index") {
        for (const char* query :
             {"CREATE INDEX b_idx ON db.tbl USING btree (field);", "CREATE INDEX d_idx ON db.tbl (field);"}) {
            auto result = plan_of(query);
            REQUIRE_FALSE(result.has_error());
            auto node = result.value().sub_queries.back();
            REQUIRE(node->type() == node_type::create_index_t);
            CHECK(static_cast<node_create_index_t*>(node.get())->type() == index_type::single);
        }
    }
}

// IndexStmt's unique/whereClause/options/tableSpace fields must each be refused if unread, not silently dropped.
TEST_CASE("components::sql::create_index_declared_clauses_are_not_dropped") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = parse_ok(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };

    SECTION("CREATE UNIQUE INDEX is refused, and the refusal says UNIQUE") {
        auto result = plan_of("CREATE UNIQUE INDEX u_idx ON db.tbl (field);");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("UNIQUE") != std::string::npos);
    }

    SECTION("a partial-index WHERE is refused, not silently widened to a full index") {
        auto result = plan_of("CREATE INDEX p_idx ON db.tbl (field) WHERE field > 0;");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("WHERE") != std::string::npos);
    }

    SECTION("WITH options are refused, not dropped") {
        auto result = plan_of("CREATE INDEX w_idx ON db.tbl (field) WITH (fillfactor = 70);");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("WITH") != std::string::npos);
    }

    SECTION("TABLESPACE is refused, not dropped") {
        auto result = plan_of("CREATE INDEX t_idx ON db.tbl (field) TABLESPACE fast_disk;");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("TABLESPACE") != std::string::npos);
    }
}

// A function is named like a table: up to database.schema.function; a four-part name is refused, never registered
// under an empty part.
TEST_CASE("components::sql::create_function_shape_is_carried_or_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = parse_ok(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };

    SECTION("a three-part name is database.schema.function") {
        auto result = plan_of("CREATE FUNCTION cat.sch.fn(x INT) RETURNS INT AS 'x -> x';");
        REQUIRE_FALSE(result.has_error());
        const auto& root = result.value().sub_queries.back();
        REQUIRE(root->type() == node_type::create_macro_t);
        CHECK(root->target().database.t == "cat");
        CHECK(root->target().schema.t == "sch");
        CHECK(static_cast<const node_create_macro_t*>(root.get())->macroname().t == "fn");
    }

    SECTION("a four-part name is refused, and the refusal spells the name out") {
        auto result = plan_of("CREATE FUNCTION u.cat.sch.fn(x INT) RETURNS INT AS 'x -> x';");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("u.cat.sch.fn") != std::string::npos);
    }

    SECTION("an unnamed parameter is refused: a macro parameter is addressed by name") {
        auto result = plan_of("CREATE FUNCTION db.f(INT) RETURNS INT AS 'x -> x';");
        REQUIRE(result.has_error());
    }

    SECTION("a parameter DEFAULT is refused, not dropped") {
        auto result = plan_of("CREATE FUNCTION db.f(x INT DEFAULT 5) RETURNS INT AS 'x -> x';");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("x") != std::string::npos);
    }

    SECTION("an OUT parameter is refused: a macro has no output parameters") {
        auto result = plan_of("CREATE FUNCTION db.f(OUT x INT) RETURNS INT AS 'x -> x';");
        REQUIRE(result.has_error());
    }

    SECTION("RETURNS TABLE is refused: its columns are not input parameters") {
        // Otherwise the grammar merges the TABLE columns into `parameters`, giving the macro the wrong arity.
        auto result = plan_of("CREATE FUNCTION db.f(x INT) RETURNS TABLE (y INT) AS 'x -> x';");
        REQUIRE(result.has_error());
    }

    SECTION("an option other than AS is refused, and the refusal names it") {
        auto result = plan_of("CREATE FUNCTION db.f(x INT) RETURNS INT LANGUAGE sql;");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("language") != std::string::npos);
    }

    SECTION("an empty AS body is refused") {
        auto result = plan_of("CREATE FUNCTION db.f(x INT) RETURNS INT AS '';");
        REQUIRE(result.has_error());
    }

    SECTION("a two-part AS clause is refused: an object file is not a macro body") {
        auto result = plan_of("CREATE FUNCTION db.f(x INT) RETURNS INT AS 'obj_file', 'link_symbol';");
        REQUIRE(result.has_error());
    }

    SECTION("OR REPLACE is refused, not silently degraded to plain CREATE") {
        auto result = plan_of("CREATE OR REPLACE FUNCTION db.f(x INT) RETURNS INT AS 'x -> x';");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("OR REPLACE") != std::string::npos);
    }

    SECTION("a WITH definition is refused, not dropped") {
        auto result = plan_of("CREATE FUNCTION db.f(x INT) RETURNS INT AS 'x -> x' WITH (isStrict);");
        REQUIRE(result.has_error());
    }

    SECTION("the supported shape still comes through whole") {
        auto result = plan_of("CREATE FUNCTION db.add2(x INT, y INT) RETURNS INT AS 'x, y -> x + y';");
        REQUIRE_FALSE(result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::create_macro_t);
        auto* macro = static_cast<node_create_macro_t*>(node.get());
        CHECK(macro->macroname().t == "add2");
        CHECK(macro->target().schema.t == "db");
        REQUIRE(macro->parameters().size() == 2);
        CHECK(macro->parameters()[0] == "x");
        CHECK(macro->parameters()[1] == "y");
        CHECK(macro->body_sql() == "x, y -> x + y");
    }

    SECTION("a one-part name still lands on relname, not on the schema") {
        auto result = plan_of("CREATE FUNCTION solo(x INT) RETURNS INT AS 'x -> x';");
        REQUIRE_FALSE(result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::create_macro_t);
        auto* macro = static_cast<node_create_macro_t*>(node.get());
        CHECK(macro->macroname().t == "solo");
        CHECK(macro->target().schema.t.empty());
    }
}

// CREATE honours IF NOT EXISTS; this pins the other half of the pair. IF EXISTS rides the DROP node, like
// DropStmt.missing_ok in PostgreSQL, so a plan-to-SQL writer sees it next to the behavior.
TEST_CASE("components::sql::drop_carries_missing_ok") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto if_exists_of = [&](const char* query) {
        auto stmt = linitial(parse_ok(&arena_resource, query));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::drop_t);
        return static_cast<const node_drop_t*>(node.get())->if_exists();
    };

    SECTION("DROP TABLE IF EXISTS carries missing_ok") { REQUIRE(if_exists_of("DROP TABLE IF EXISTS db.t;")); }
    SECTION("plain DROP TABLE stays loud") { REQUIRE_FALSE(if_exists_of("DROP TABLE db.t;")); }
    SECTION("DROP INDEX IF EXISTS carries missing_ok") { REQUIRE(if_exists_of("DROP INDEX IF EXISTS db.t.idx;")); }
    SECTION("plain DROP INDEX stays loud") { REQUIRE_FALSE(if_exists_of("DROP INDEX db.t.idx;")); }
    SECTION("DROP VIEW IF EXISTS carries missing_ok") { REQUIRE(if_exists_of("DROP VIEW IF EXISTS db.v;")); }
    SECTION("DROP SEQUENCE IF EXISTS carries missing_ok") { REQUIRE(if_exists_of("DROP SEQUENCE IF EXISTS db.s;")); }
    SECTION("DROP TYPE IF EXISTS carries missing_ok") { REQUIRE(if_exists_of("DROP TYPE IF EXISTS mood;")); }
    SECTION("DROP DATABASE IF EXISTS carries missing_ok (its own DropdbStmt flag)") {
        REQUIRE(if_exists_of("DROP DATABASE IF EXISTS db;"));
    }
    SECTION("plain DROP DATABASE stays loud") { REQUIRE_FALSE(if_exists_of("DROP DATABASE db;")); }
    SECTION("the flag does not leak into the next statement") {
        REQUIRE(if_exists_of("DROP TABLE IF EXISTS db.t;"));
        REQUIRE_FALSE(if_exists_of("DROP TABLE db.t;"));
    }
}

// gram.y's opt_drop_behavior has three alternatives but two values: the empty one and a
// written RESTRICT deliberately collapse to the same restrict_ (PostgreSQL parity).
TEST_CASE("components::sql::drop_carries_written_behavior") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::catalog::drop_behavior_t;

    auto behavior_of = [&](const char* query) {
        auto stmt = linitial(parse_ok(&arena_resource, query));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::drop_t);
        return static_cast<node_drop_t*>(node.get())->behavior();
    };

    SECTION("DROP TABLE CASCADE") { REQUIRE(behavior_of("DROP TABLE db.t CASCADE;") == drop_behavior_t::cascade_); }
    SECTION("bare DROP TABLE defaults to RESTRICT") {
        REQUIRE(behavior_of("DROP TABLE db.t;") == drop_behavior_t::restrict_);
    }
    SECTION("DROP TABLE RESTRICT is the same value as the bare form") {
        REQUIRE(behavior_of("DROP TABLE db.t RESTRICT;") == drop_behavior_t::restrict_);
    }
    SECTION("DROP VIEW CASCADE") { REQUIRE(behavior_of("DROP VIEW db.v CASCADE;") == drop_behavior_t::cascade_); }
    SECTION("DROP SEQUENCE CASCADE") {
        REQUIRE(behavior_of("DROP SEQUENCE db.s CASCADE;") == drop_behavior_t::cascade_);
    }
    SECTION("DROP TYPE CASCADE — the arm that does not build through wrap_one") {
        REQUIRE(behavior_of("DROP TYPE mood CASCADE;") == drop_behavior_t::cascade_);
    }
    SECTION("DROP INDEX CASCADE — the other arm that does not build through wrap_one") {
        REQUIRE(behavior_of("DROP INDEX db.t.idx CASCADE;") == drop_behavior_t::cascade_);
    }
}

// The same reading, per ALTER TABLE clause. The subcommand carries it because a
// multi-clause ALTER can write a different word on each clause.
TEST_CASE("components::sql::alter_drop_column_carries_written_behavior") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::catalog::drop_behavior_t;

    auto subcommands_of = [&](const char* query) {
        auto stmt = linitial(parse_ok(&arena_resource, query));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::alter_table_t);
        return static_cast<node_alter_table_t*>(node.get())->subcommands();
    };

    SECTION("ALTER TABLE ... DROP COLUMN CASCADE") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN c CASCADE;");
        REQUIRE(subs.size() == 1);
        REQUIRE(subs.front().behavior == drop_behavior_t::cascade_);
    }
    SECTION("bare DROP COLUMN defaults to RESTRICT") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN c;");
        REQUIRE(subs.size() == 1);
        REQUIRE(subs.front().behavior == drop_behavior_t::restrict_);
    }
    SECTION("DROP COLUMN RESTRICT is the same value as the bare form") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN c RESTRICT;");
        REQUIRE(subs.size() == 1);
        REQUIRE(subs.front().behavior == drop_behavior_t::restrict_);
    }
    SECTION("one word per clause, not one per statement") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN a CASCADE, DROP COLUMN b;");
        REQUIRE(subs.size() == 2);
        REQUIRE(subs.front().behavior == drop_behavior_t::cascade_);
        REQUIRE(subs.back().behavior == drop_behavior_t::restrict_);
        // IF EXISTS is per-clause too (AlterTableCmd.missing_ok), and must not have been swapped with the behavior.
        auto mixed = subcommands_of("ALTER TABLE db.t DROP COLUMN a CASCADE, DROP COLUMN IF EXISTS b;");
        REQUIRE(mixed.size() == 2);
        REQUIRE_FALSE(mixed.front().if_exists);
        REQUIRE(mixed.back().if_exists);
        REQUIRE(mixed.back().behavior == drop_behavior_t::restrict_);
    }
    SECTION("every clause may say IF EXISTS, so it is not one index per statement") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN IF EXISTS a, DROP CONSTRAINT IF EXISTS c, "
                                   "DROP COLUMN b;");
        REQUIRE(subs.size() == 3);
        REQUIRE(subs[0].if_exists);
        REQUIRE(subs[1].if_exists);
        REQUIRE_FALSE(subs[2].if_exists);
    }
}

// ALTER TABLE IF EXISTS is the table's flag (AlterTableStmt.missing_ok): it rides whichever node the statement
// lowers to, so the executor and a plan-to-SQL writer read it where the subcommands are.
TEST_CASE("components::sql::alter_table_carries_missing_ok") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    auto node_of = [&](const char* query) {
        // With the statement text: a CHECK's expression is sliced from it.
        transform::transformer transformer(&resource, query);
        auto stmt = linitial(parse_ok(&arena_resource, query));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!result.has_error());
        return result.value().sub_queries.back();
    };
    auto alter_if_exists = [&](const char* query) {
        auto node = node_of(query);
        REQUIRE(node->type() == node_type::alter_table_t);
        return static_cast<const node_alter_table_t*>(node.get())->if_exists();
    };
    auto constraint_if_exists = [&](const char* query) {
        auto node = node_of(query);
        REQUIRE(node->type() == node_type::create_constraint_t);
        return static_cast<const node_create_constraint_t*>(node.get())->if_exists();
    };

    SECTION("DROP COLUMN") {
        REQUIRE(alter_if_exists("ALTER TABLE IF EXISTS db.t DROP COLUMN c;"));
        REQUIRE_FALSE(alter_if_exists("ALTER TABLE db.t DROP COLUMN c;"));
    }
    SECTION("the table's flag is not the clause's") {
        auto node = node_of("ALTER TABLE db.t DROP COLUMN IF EXISTS c;");
        const auto* alter = static_cast<const node_alter_table_t*>(node.get());
        REQUIRE_FALSE(alter->if_exists());
        REQUIRE(alter->subcommands().front().if_exists);
    }
    SECTION("RENAME COLUMN") {
        REQUIRE(alter_if_exists("ALTER TABLE IF EXISTS db.t RENAME COLUMN a TO b;"));
        REQUIRE_FALSE(alter_if_exists("ALTER TABLE db.t RENAME COLUMN a TO b;"));
    }
    SECTION("ADD CONSTRAINT lowers to the constraint node and keeps the flag") {
        REQUIRE(constraint_if_exists("ALTER TABLE IF EXISTS db.t ADD CONSTRAINT uq UNIQUE (c);"));
        REQUIRE_FALSE(constraint_if_exists("ALTER TABLE db.t ADD CONSTRAINT uq UNIQUE (c);"));
        REQUIRE(constraint_if_exists("ALTER TABLE IF EXISTS db.t ADD CONSTRAINT ck CHECK (c > 0);"));
        REQUIRE(
            constraint_if_exists("ALTER TABLE IF EXISTS db.t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES db.p (id);"));
    }
}

// A write target keeps every slot it was written with; a missing database or schema is the session's place,
// completed by the executor (types too), so the transformer records nothing as unreachable.
TEST_CASE("components::sql::write_targets_keep_their_slots") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto target_of = [&](const char* sql) {
        auto stmt = parse_ok(&arena_resource, sql)->lst.front().data;
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE_FALSE(result.has_error());
        const auto& target = result.value().sub_queries.back()->target();
        return std::vector<std::string>{target.connection.t, target.database.t, target.schema.t, target.collection.t};
    };
    using slots = std::vector<std::string>;

    CHECK(target_of("CREATE TABLE uuid.db_name.schema.table_name()") ==
          slots{"uuid", "db_name", "schema", "table_name"});
    CHECK(target_of("DROP TABLE uuid.db_name.schema.table_name") == slots{"uuid", "db_name", "schema", "table_name"});
    CHECK(target_of("DROP TABLE db_name.schema.table_name") == slots{"", "db_name", "schema", "table_name"});

    CHECK(target_of("CREATE TABLE db_name.table_name()") == slots{"", "", "db_name", "table_name"});
    CHECK(target_of("DROP TABLE db_name.table_name") == slots{"", "", "db_name", "table_name"});

    CHECK(target_of("CREATE TYPE db_name.type_name AS (id int)") == slots{"", "", "db_name", ""});
    CHECK(target_of("DROP TYPE db_name.type_name") == slots{"", "", "db_name", "type_name"});

    CHECK(target_of("CREATE TABLE table_name()") == slots{"", "", "", "table_name"});
    CHECK(target_of("CREATE TYPE type_name AS (id int)") == slots{"", "", "", ""});
    CHECK(target_of("DROP TYPE type_name") == slots{"", "", "", "type_name"});
}
