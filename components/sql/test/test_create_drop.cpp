#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
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
        auto stmt = raw_parser(&arena_resource, QUERY)->lst.front().data;                                              \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        const auto& plan = result.value();                                                                             \
        REQUIRE(plan.sub_queries.back()->type() == ROOT_TYPE);                                                         \
        REQUIRE(entry_count(plan.catalog_resolves.namespaces) == NS_COUNT);                                            \
        REQUIRE(entry_count(plan.catalog_resolves.tables) == TBL_COUNT);                                               \
    }

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto create = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        REQUIRE(transformer.transform(pg_cell_to_node_cast(create)).has_error());                                      \
    }

#define TEST_TRANSFORMER_EXPECT_SCHEMA(QUERY, CHECK_FN)                                                                \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = linitial(raw_parser(&arena_resource, QUERY));                                                      \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        auto node = result.value().sub_queries.back();                                                                 \
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);                                               \
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

    TEST_TRANSFORMER_OK("CREATE DATABASE db_name", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;          ", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; -- comment", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; /* multiline\ncomments */", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE /* comment */ DATABASE db_name;", node_type::create_database_t, 1, 0);
    // DROP DATABASE registers only its namespace lookup; the drop node carries the
    // dbname and gets namespace_oid pasted on by enrich.
    TEST_TRANSFORMER_OK("DROP DATABASE db_name;", node_type::drop_t, 1, 0);
}

TEST_CASE("components::sql::table") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE uuid.db_name.schema.table_name()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_collection_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
    }

    SECTION("create with schema") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE db_name.schema.table_name()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_collection_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
    }

    TEST_TRANSFORMER_OK("CREATE TABLE db_name.table_name()", node_type::create_collection_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE TABLE table_name()", node_type::create_collection_t, 0, 0);

    // DROP TABLE registers a namespace + table lookup; the drop node carries the
    // names and gets namespace_oid + table_oid pasted on by enrich.
    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE uuid.db_name.schema.table_name")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 1);
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE db_name.schema.table_name")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 1);
    }

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

    // CREATE INDEX registers TWO table lookups, like DROP INDEX below: the indexed
    // table AND the index's own name — the second probes pg_class for a relation
    // already answering to the new name, so a taken name refuses.
    SECTION("create with uuid") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON uuid.db.schema.table (field);")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_index_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    SECTION("create with schema") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
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
        auto drop = raw_parser(&arena_resource, "DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX db.schema.table.some_idx")->lst.front().data;
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

    // CREATE TYPE is wrapped in sequence_t(resolve_ns?, resolve_field_types..., create_type).
    TEST_TRANSFORMER_OK("CREATE TYPE custom_type_name AS (f1 int, f2 string);", node_type::create_type_t, 1, 0);

    TEST_TRANSFORMER_OK("CREATE TYPE custom_enum AS ENUM ('f1', 'f2', 'f3');", node_type::create_type_t, 1, 0);

    // DROP TYPE is wrapped in sequence_t(resolve_ns, resolve_type, drop_type).
    TEST_TRANSFORMER_OK("DROP TYPE custom_type_name", node_type::drop_t, 1, 0);

    // CREATE TABLE with a custom type is wrapped in sequence_t(resolve_type, create_collection).
    TEST_TRANSFORMER_OK("CREATE TABLE table_ (custom_type_name custom_type);", node_type::create_collection_t, 0, 0);

    // INSERT is wrapped in sequence_t(resolve_table, resolve_constraint,
    // insert) — no dbname so no resolve_namespace.
    TEST_TRANSFORMER_OK("INSERT INTO table_ (custom_type_name) VALUES (ROW('text', 42))", node_type::insert_t, 0, 1);
}

// A statement that names several objects must not report success after touching
// one of them. Reading `objects->lst.front()` and never looking at the rest makes
// `DROP TABLE a, b` plan a single drop of `a`, execute cleanly, and leave `b`
// exactly where it was — with nothing in the answer to say so.
TEST_CASE("components::sql::drop_names_every_object_or_refuses") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto refusal_of = [&](const char* query) {
        auto stmt = raw_parser(&arena_resource, query)->lst.front().data;
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(result.has_error());
        return std::string{result.error().what};
    };

    SECTION("DROP TABLE a, b") {
        // Unrefused: no error at all, and one drop_t naming only `first_table`.
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

    // One object per statement stays exactly as it was.
    SECTION("a single object is still planned") {
        auto stmt = raw_parser(&arena_resource, "DROP TABLE db_name.only_one")->lst.front().data;
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value().sub_queries.back()->type() == node_type::drop_t);
    }
}

// CREATE INDEX ... USING <method> must not collapse every method that is not the
// literal "hash" into index_type::single: `USING gin`, `USING brin`, `USING spgist`
// and a plain typo would all build a btree-shaped single index, report success, and
// write that into the catalog under the name the user asked for.
TEST_CASE("components::sql::create_index_access_method") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = raw_parser(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };

    SECTION("USING gin is refused, and the refusal names gin") {
        // Uncollapsed the other way: success, with the node carrying index_type::single.
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
        CHECK(reinterpret_cast<node_create_index_ptr&>(node)->type() == index_type::hashed);
    }

    SECTION("USING btree, and the omitted clause, still build a single index") {
        for (const char* query : {"CREATE INDEX b_idx ON db.tbl USING btree (field);",
                                  "CREATE INDEX d_idx ON db.tbl (field);"}) {
            auto result = plan_of(query);
            REQUIRE_FALSE(result.has_error());
            auto node = result.value().sub_queries.back();
            REQUIRE(node->type() == node_type::create_index_t);
            CHECK(reinterpret_cast<node_create_index_ptr&>(node)->type() == index_type::single);
        }
    }
}

// A clause the node cannot carry must be refused, not dropped. IndexStmt arrives
// with `unique`, `whereClause`, `options` and `tableSpace` filled by the grammar
// (gram.y: `CREATE opt_unique INDEX ... opt_reloptions OptTableSpace where_clause`).
// Read by none of them, `CREATE UNIQUE INDEX` builds an ordinary index that admits
// duplicates, a partial-index WHERE builds a full index, and WITH options and
// TABLESPACE vanish — every one reporting success while doing something other than
// what was declared.
TEST_CASE("components::sql::create_index_declared_clauses_are_not_dropped") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = raw_parser(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };

    SECTION("CREATE UNIQUE INDEX is refused, and the refusal says UNIQUE") {
        // Dropped instead: success, and a plain (non-unique) index under the name the
        // user asked for, with the declared uniqueness enforced by nothing.
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

// CREATE FUNCTION is lowered to a macro, and a macro is addressed by ONE name,
// carries NAMED parameters and expands to its AS body — nothing else. Dropping a
// piece that cannot be carried costs the NAME itself in the worst case:
// transform_create_function reads a one-part and a two-part funcname, so with no
// else a three-part name (`CREATE FUNCTION a.b.c(...)`) leaves BOTH dbname and
// relname empty — the macro registered under the empty string, the statement
// reporting success.
TEST_CASE("components::sql::create_function_shape_is_carried_or_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = raw_parser(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };

    SECTION("a three-part name is refused, and the refusal spells the name out") {
        // Dropped instead: success, and a macro registered under the EMPTY name.
        auto result = plan_of("CREATE FUNCTION cat.sch.fn(x INT) RETURNS INT AS 'x -> x';");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("cat.sch.fn") != std::string::npos);
    }

    SECTION("an unnamed parameter is refused: a macro parameter is addressed by name") {
        // Dropped instead: success, the parameter skipped and the macro's arity lying.
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
        // Dropped instead: success — the grammar merges the TABLE columns into
        // `parameters`, so they become macro parameters and the arity is wrong.
        auto result = plan_of("CREATE FUNCTION db.f(x INT) RETURNS TABLE (y INT) AS 'x -> x';");
        REQUIRE(result.has_error());
    }

    SECTION("an option other than AS is refused, and the refusal names it") {
        // Dropped instead: success with an EMPTY body — there is no AS clause at all.
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
        // With the replace flag unread, the statement fails as a duplicate against an
        // existing function instead of replacing it, and nothing says why.
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
        auto& macro = reinterpret_cast<node_create_macro_ptr&>(node);
        CHECK(macro->macroname() == "add2");
        CHECK(macro->dbname() == "db");
        REQUIRE(macro->parameters().size() == 2);
        CHECK(macro->parameters()[0] == "x");
        CHECK(macro->parameters()[1] == "y");
        CHECK(macro->body_sql() == "x, y -> x + y");
    }

    SECTION("a one-part name still lands on relname, not on dbname") {
        auto result = plan_of("CREATE FUNCTION solo(x INT) RETURNS INT AS 'x -> x';");
        REQUIRE_FALSE(result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::create_macro_t);
        auto& macro = reinterpret_cast<node_create_macro_ptr&>(node);
        CHECK(macro->macroname() == "solo");
        CHECK(macro->dbname().empty());
    }
}

// The grammar sets DropStmt.missing_ok for every `DROP ... IF EXISTS` form and
// node_drop_t::missing_ok carries it, but only if transform_drop reads it: unread,
// `DROP INDEX IF EXISTS` reaches the planner with missing_ok=false and the one
// no-op success PostgreSQL grants that form is unreachable from SQL. CREATE honours
// IF NOT EXISTS; this pins the other half of the pair.
TEST_CASE("components::sql::drop_carries_missing_ok") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto transform_drop = [&](const char* query) {
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::drop_t);
        return boost::intrusive_ptr{static_cast<node_drop_t*>(node.get())};
    };

    SECTION("DROP TABLE IF EXISTS carries missing_ok") {
        REQUIRE(transform_drop("DROP TABLE IF EXISTS db.t;")->missing_ok());
    }
    SECTION("plain DROP TABLE stays loud") {
        REQUIRE_FALSE(transform_drop("DROP TABLE db.t;")->missing_ok());
    }
    SECTION("DROP INDEX IF EXISTS carries missing_ok") {
        REQUIRE(transform_drop("DROP INDEX IF EXISTS db.t.idx;")->missing_ok());
    }
    SECTION("plain DROP INDEX stays loud") {
        REQUIRE_FALSE(transform_drop("DROP INDEX db.t.idx;")->missing_ok());
    }
    SECTION("DROP VIEW IF EXISTS carries missing_ok") {
        REQUIRE(transform_drop("DROP VIEW IF EXISTS db.v;")->missing_ok());
    }
    SECTION("DROP SEQUENCE IF EXISTS carries missing_ok") {
        REQUIRE(transform_drop("DROP SEQUENCE IF EXISTS db.s;")->missing_ok());
    }
    SECTION("DROP TYPE IF EXISTS carries missing_ok") {
        REQUIRE(transform_drop("DROP TYPE IF EXISTS mood;")->missing_ok());
    }
    SECTION("DROP DATABASE IF EXISTS carries missing_ok (its own DropdbStmt flag)") {
        REQUIRE(transform_drop("DROP DATABASE IF EXISTS db;")->missing_ok());
    }
    SECTION("plain DROP DATABASE stays loud") {
        REQUIRE_FALSE(transform_drop("DROP DATABASE db;")->missing_ok());
    }
}

// The word the statement wrote about its dependents, as far as this layer can read it.
//
// gram.y's opt_drop_behavior has THREE alternatives and TWO values: the empty one yields
// DROP_RESTRICT, the same token the written word yields. So `DROP TABLE t RESTRICT` and
// `DROP TABLE t` are one value here, and both are read as `unspecified` — "the statement
// named neither word". Reading DROP_RESTRICT as restrict_ instead would flip every bare
// DROP in the tree from CASCADE to a dependency refusal in one hop.
//
// A written CASCADE is separable, and is carried. It resolves the same way `unspecified`
// does today (catalog::refuses_on_dependency), so no outcome moves; what changes is that
// the plan node now says what the statement said, and stays right when GitHub #638 moves
// the unwritten default to RESTRICT.
TEST_CASE("components::sql::drop_carries_written_behavior") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    using components::catalog::drop_behavior_t;

    auto behavior_of = [&](const char* query) {
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!result.has_error());
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::drop_t);
        return static_cast<node_drop_t*>(node.get())->behavior();
    };

    SECTION("DROP TABLE CASCADE") {
        REQUIRE(behavior_of("DROP TABLE db.t CASCADE;") == drop_behavior_t::cascade_);
    }
    SECTION("bare DROP TABLE names neither word") {
        REQUIRE(behavior_of("DROP TABLE db.t;") == drop_behavior_t::unspecified);
    }
    SECTION("DROP TABLE RESTRICT is not yet separable from the bare form") {
        // NOT a wish: the grammar hands both spellings the same token. Pinned so that the
        // day gram.y grows a third value, this line fails and points at what to change.
        REQUIRE(behavior_of("DROP TABLE db.t RESTRICT;") == drop_behavior_t::unspecified);
    }
    SECTION("DROP VIEW CASCADE") {
        REQUIRE(behavior_of("DROP VIEW db.v CASCADE;") == drop_behavior_t::cascade_);
    }
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
        auto stmt = linitial(raw_parser(&arena_resource, query));
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
    SECTION("bare DROP COLUMN names neither word") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN c;");
        REQUIRE(subs.size() == 1);
        REQUIRE(subs.front().behavior == drop_behavior_t::unspecified);
    }
    SECTION("DROP COLUMN RESTRICT is not yet separable from the bare form") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN c RESTRICT;");
        REQUIRE(subs.size() == 1);
        REQUIRE(subs.front().behavior == drop_behavior_t::unspecified);
    }
    SECTION("one word per clause, not one per statement") {
        auto subs = subcommands_of("ALTER TABLE db.t DROP COLUMN a CASCADE, DROP COLUMN b;");
        REQUIRE(subs.size() == 2);
        REQUIRE(subs.front().behavior == drop_behavior_t::cascade_);
        REQUIRE(subs.back().behavior == drop_behavior_t::unspecified);
        // IF EXISTS is per-clause too, and must not have been swapped with the behavior.
        REQUIRE_FALSE(subs.front().missing_ok);
        REQUIRE_FALSE(subs.back().missing_ok);
    }
}
